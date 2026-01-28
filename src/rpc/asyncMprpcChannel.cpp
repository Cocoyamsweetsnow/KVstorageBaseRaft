#include "asyncMprpcChannel.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "mprpccontroller.h"
#include "rpcheader.pb.h"
#include "serviceDiscovery.h"
#include "util.h"

namespace rpc {

AsyncMprpcChannel::AsyncMprpcChannel(const std::string& ip, short port,
                                     bool connectNow, int ioThreads, bool useFiberIO)
    : clientFd_(-1)
    , ip_(ip)
    , port_(port)
    , useDiscovery_(false)
    , nextCallId_(1)
    , defaultTimeoutMs_(5000)  // 默认5秒超时
    , running_(true)
    , nonBlocking_(true)
    , useFiberIO_(useFiberIO)
    , timeoutCheckIntervalMs_(10) {
    
    if (useFiberIO_) {
        startFiberIO(ioThreads);
    }

    if (connectNow) {
        std::string errMsg;
        bool connected = newConnect(ip_.c_str(), port_, &errMsg);
        int tryCount = 3;
        while (!connected && tryCount-- > 0) {
            DPrintf("[AsyncMprpcChannel] 连接失败: %s, 重试中...", errMsg.c_str());
            connected = newConnect(ip_.c_str(), port_, &errMsg);
        }
        if (!connected) {
            DPrintf("[AsyncMprpcChannel] 最终连接失败: %s", errMsg.c_str());
        }
    }

    // 启动I/O线程
    if (!useFiberIO_) {
        for (int i = 0; i < ioThreads; ++i) {
            ioThreads_.emplace_back(&AsyncMprpcChannel::ioThreadFunc, this);
        }
    }
}

AsyncMprpcChannel::AsyncMprpcChannel(const std::string& serviceName,
                                     std::shared_ptr<ServiceDiscovery> discovery,
                                     const std::string& hashKey,
                                     int ioThreads,
                                     bool useFiberIO)
    : clientFd_(-1)
    , port_(0)
    , serviceName_(serviceName)
    , discovery_(discovery)
    , hashKey_(hashKey)
    , useDiscovery_(true)
    , nextCallId_(1)
    , defaultTimeoutMs_(5000)
    , running_(true)
    , nonBlocking_(true)
    , useFiberIO_(useFiberIO)
    , timeoutCheckIntervalMs_(10) {
    
    if (useFiberIO_) {
        startFiberIO(ioThreads);
    }

    // 启动I/O线程
    if (!useFiberIO_) {
        for (int i = 0; i < ioThreads; ++i) {
            ioThreads_.emplace_back(&AsyncMprpcChannel::ioThreadFunc, this);
        }
    }
}

AsyncMprpcChannel::~AsyncMprpcChannel() {
    running_ = false;
    
    // 等待I/O线程结束
    if (!useFiberIO_) {
        for (auto& thread : ioThreads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    } else if (ioManager_) {
        if (timeoutTimer_) {
            timeoutTimer_->cancel();
            timeoutTimer_.reset();
        }
        ioManager_->stop();
        ioManager_.reset();
    }
    
    closeConnection();
    
    // 取消所有等待中的调用
    std::lock_guard<std::mutex> lock(pendingMutex_);
    for (auto& pair : pendingCalls_) {
        if (pair.second->future) {
            pair.second->future->complete(RpcStatus::CANCELLED, nullptr, 
                                          "Channel destroyed");
        }
    }
    pendingCalls_.clear();
}

void AsyncMprpcChannel::setHashKey(const std::string& hashKey) {
    hashKey_ = hashKey;
}

bool AsyncMprpcChannel::resolveServiceAddress() {
    if (!discovery_) {
        DPrintf("[AsyncMprpcChannel] Service discovery is null");
        return false;
    }

    ServiceInstance instance = discovery_->selectInstance(serviceName_, hashKey_);
    if (!instance.isValid()) {
        DPrintf("[AsyncMprpcChannel] No available instance for service: %s", 
                serviceName_.c_str());
        return false;
    }

    ip_ = instance.ip;
    port_ = instance.port;

    DPrintf("[AsyncMprpcChannel] Resolved service %s to %s:%d", 
            serviceName_.c_str(), ip_.c_str(), port_);
    return true;
}

bool AsyncMprpcChannel::newConnect(const char* ip, uint16_t port, std::string* errMsg) {
    int clientfd = socket(AF_INET, SOCK_STREAM, 0);
    if (clientfd == -1) {
        char errtxt[512] = {0};
        snprintf(errtxt, sizeof(errtxt), "create socket error! errno:%d", errno);
        *errMsg = errtxt;
        return false;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip);

    if (connect(clientfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        close(clientfd);
        char errtxt[512] = {0};
        snprintf(errtxt, sizeof(errtxt), "connect fail! errno:%d", errno);
        *errMsg = errtxt;
        return false;
    }

    // 设置非阻塞模式
    if (nonBlocking_) {
        int flags = fcntl(clientfd, F_GETFL, 0);
        fcntl(clientfd, F_SETFL, flags | O_NONBLOCK);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    clientFd_ = clientfd;
    DPrintf("[AsyncMprpcChannel] 连接成功 ip:%s port:%d", ip, port);
    if (useFiberIO_) {
        registerReadEvent();
    }
    return true;
}

void AsyncMprpcChannel::closeConnection() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (clientFd_ != -1) {
        if (useFiberIO_ && ioManager_ && running_) {
            ioManager_->cancelAll(clientFd_);
        }
        close(clientFd_);
        clientFd_ = -1;
    }
}

void AsyncMprpcChannel::disconnect() {
    closeConnection();
}

bool AsyncMprpcChannel::reconnect() {
    closeConnection();
    
    // 如果使用服务发现，先解析地址
    if (useDiscovery_) {
        if (!resolveServiceAddress()) {
            return false;
        }
    }
    
    std::string errMsg;
    return newConnect(ip_.c_str(), port_, &errMsg);
}

uint64_t AsyncMprpcChannel::generateCallId() {
    return nextCallId_.fetch_add(1);
}

size_t AsyncMprpcChannel::getPendingCallCount() const {
    std::lock_guard<std::mutex> lock(pendingMutex_);
    return pendingCalls_.size();
}

bool AsyncMprpcChannel::sendRequest(const google::protobuf::MethodDescriptor* method,
                                    const google::protobuf::Message* request,
                                    uint64_t callId,
                                    std::string* errMsg) {
    const google::protobuf::ServiceDescriptor* sd = method->service();
    std::string service_name = sd->name();
    std::string method_name = method->name();

    // 序列化请求
    std::string args_str;
    if (!request->SerializeToString(&args_str)) {
        *errMsg = "serialize request error!";
        return false;
    }
    uint32_t args_size = args_str.size();

    // 构建RPC头
    RPC::RpcHeader rpcHeader;
    rpcHeader.set_service_name(service_name);
    rpcHeader.set_method_name(method_name);
    rpcHeader.set_args_size(args_size);

    std::string rpc_header_str;
    if (!rpcHeader.SerializeToString(&rpc_header_str)) {
        *errMsg = "serialize rpc header error!";
        return false;
    }

    // 构建发送数据
    std::string send_rpc_str;
    {
        google::protobuf::io::StringOutputStream string_output(&send_rpc_str);
        google::protobuf::io::CodedOutputStream coded_output(&string_output);
        coded_output.WriteVarint32(static_cast<uint32_t>(rpc_header_str.size()));
        coded_output.WriteString(rpc_header_str);
    }
    send_rpc_str += args_str;

    // 发送数据
    std::lock_guard<std::mutex> lock(mutex_);
    if (clientFd_ == -1) {
        *errMsg = "connection not established";
        return false;
    }

    ssize_t totalSent = 0;
    ssize_t dataLen = send_rpc_str.size();
    const char* data = send_rpc_str.c_str();

    while (totalSent < dataLen) {
        ssize_t sent = send(clientFd_, data + totalSent, dataLen - totalSent, MSG_NOSIGNAL);
        if (sent == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 非阻塞模式下，等待一下再试
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            char errtxt[512] = {0};
            snprintf(errtxt, sizeof(errtxt), "send error! errno:%d", errno);
            *errMsg = errtxt;
            return false;
        }
        totalSent += sent;
    }

    return true;
}

void AsyncMprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                                   google::protobuf::RpcController* controller,
                                   const google::protobuf::Message* request,
                                   google::protobuf::Message* response,
                                   google::protobuf::Closure* done) {
    if (done == nullptr) {
        // 同步调用模式
        auto future = callMethodAsync(method, controller, request, response, defaultTimeoutMs_);
        auto result = future->wait();
        if (!result.isSuccess()) {
            if (controller) {
                controller->SetFailed(result.getErrorMsg());
            }
        }
    } else {
        // 异步调用模式（Closure回调）
        uint64_t callId = generateCallId();
        
        auto context = std::make_shared<AsyncCallContext>();
        context->callId = callId;
        context->response = response;
        context->done = done;
        context->controller = controller;
        context->startTime = std::chrono::steady_clock::now();
        context->timeoutMs = defaultTimeoutMs_;
        context->future = std::make_shared<GenericRpcFuture>();

        {
            std::lock_guard<std::mutex> lock(pendingMutex_);
            pendingCalls_[callId] = context;
        }

        std::string errMsg;
        if (!sendRequest(method, request, callId, &errMsg)) {
            std::lock_guard<std::mutex> lock(pendingMutex_);
            pendingCalls_.erase(callId);
            if (controller) {
                controller->SetFailed(errMsg);
            }
            if (done) {
                done->Run();
            }
        }
    }
}

GenericRpcFuture::ptr AsyncMprpcChannel::callMethodAsync(
    const google::protobuf::MethodDescriptor* method,
    google::protobuf::RpcController* controller,
    const google::protobuf::Message* request,
    google::protobuf::Message* response,
    int64_t timeoutMs) {
    
    uint64_t callId = generateCallId();
    
    auto context = std::make_shared<AsyncCallContext>();
    context->callId = callId;
    context->response = response;
    context->done = nullptr;
    context->controller = controller;
    context->startTime = std::chrono::steady_clock::now();
    context->timeoutMs = timeoutMs > 0 ? timeoutMs : defaultTimeoutMs_;
    context->future = std::make_shared<GenericRpcFuture>();

    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingCalls_[callId] = context;
    }

    // 如果使用服务发现模式，先解析地址
    if (useDiscovery_ && (ip_.empty() || port_ == 0)) {
        if (!resolveServiceAddress()) {
            std::lock_guard<std::mutex> plock(pendingMutex_);
            pendingCalls_.erase(callId);
            std::string errMsg = "Failed to resolve service address for: " + serviceName_;
            context->future->complete(RpcStatus::FAILED, nullptr, errMsg);
            return context->future;
        }
    }

    // 检查连接
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (clientFd_ == -1) {
            std::string errMsg;
            bool connected = newConnect(ip_.c_str(), port_, &errMsg);
            if (!connected) {
                std::lock_guard<std::mutex> plock(pendingMutex_);
                pendingCalls_.erase(callId);
                context->future->complete(RpcStatus::FAILED, nullptr, errMsg);
                return context->future;
            }
        }
    }

    std::string errMsg;
    if (!sendRequest(method, request, callId, &errMsg)) {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingCalls_.erase(callId);
        context->future->complete(RpcStatus::FAILED, nullptr, errMsg);
        if (controller) {
            controller->SetFailed(errMsg);
        }
    }

    return context->future;
}

void AsyncMprpcChannel::callMethodWithCallback(
    const google::protobuf::MethodDescriptor* method,
    google::protobuf::RpcController* controller,
    const google::protobuf::Message* request,
    google::protobuf::Message* response,
    std::function<void(const RpcResult&)> callback,
    int64_t timeoutMs) {
    
    auto future = callMethodAsync(method, controller, request, response, timeoutMs);
    future->setCallback([callback](const RpcResult& result, google::protobuf::Message*) {
        if (callback) {
            callback(result);
        }
    });
}

void AsyncMprpcChannel::handleResponse(uint64_t callId, const char* data, int size) {
    std::shared_ptr<AsyncCallContext> context;
    
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        auto it = pendingCalls_.find(callId);
        if (it == pendingCalls_.end()) {
            DPrintf("[AsyncMprpcChannel] 收到未知callId的响应: %lu", callId);
            return;
        }
        context = it->second;
        pendingCalls_.erase(it);
    }

    // 解析响应
    if (context->response && context->response->ParseFromArray(data, size)) {
        context->future->complete(RpcStatus::SUCCESS, context->response);
        if (context->done) {
            context->done->Run();
        }
    } else {
        std::string errMsg = "parse response error";
        if (context->controller) {
            context->controller->SetFailed(errMsg);
        }
        context->future->complete(RpcStatus::FAILED, nullptr, errMsg);
        if (context->done) {
            context->done->Run();
        }
    }
}

void AsyncMprpcChannel::checkTimeouts() {
    auto now = std::chrono::steady_clock::now();
    std::vector<std::shared_ptr<AsyncCallContext>> timedOutCalls;

    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        for (auto it = pendingCalls_.begin(); it != pendingCalls_.end();) {
            auto& context = it->second;
            if (context->timeoutMs > 0) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - context->startTime).count();
                if (elapsed >= context->timeoutMs) {
                    timedOutCalls.push_back(context);
                    it = pendingCalls_.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }

    // 在锁外处理超时
    for (auto& context : timedOutCalls) {
        std::string errMsg = "RPC call timeout";
        if (context->controller) {
            context->controller->SetFailed(errMsg);
        }
        context->future->complete(RpcStatus::TIMEOUT, nullptr, errMsg);
        if (context->done) {
            context->done->Run();
        }
    }
}

void AsyncMprpcChannel::ioThreadFunc() {
    const int BUFFER_SIZE = 65536;
    char* buffer = new char[BUFFER_SIZE];
    
    while (running_) {
        // 检查超时
        checkTimeouts();

        int fd;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            fd = clientFd_;
        }

        if (fd == -1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        // 尝试接收数据
        ssize_t recvLen = recv(fd, buffer, BUFFER_SIZE, MSG_DONTWAIT);
        
        if (recvLen > 0) {
            // 简化处理：假设每次收到完整的响应
            // 实际应用中需要处理粘包/拆包问题
            
            // 查找第一个等待中的调用并处理响应
            std::shared_ptr<AsyncCallContext> context;
            {
                std::lock_guard<std::mutex> lock(pendingMutex_);
                if (!pendingCalls_.empty()) {
                    auto it = pendingCalls_.begin();
                    context = it->second;
                    pendingCalls_.erase(it);
                }
            }

            if (context) {
                // 解析响应
                if (context->response && context->response->ParseFromArray(buffer, recvLen)) {
                    context->future->complete(RpcStatus::SUCCESS, context->response);
                    if (context->done) {
                        context->done->Run();
                    }
                } else {
                    std::string errMsg = "parse response error";
                    if (context->controller) {
                        context->controller->SetFailed(errMsg);
                    }
                    context->future->complete(RpcStatus::FAILED, nullptr, errMsg);
                    if (context->done) {
                        context->done->Run();
                    }
                }
            }
        } else if (recvLen == 0) {
            // 连接关闭
            DPrintf("[AsyncMprpcChannel] 连接被关闭");
            closeConnection();
            
            // 通知所有等待中的调用
            std::vector<std::shared_ptr<AsyncCallContext>> allCalls;
            {
                std::lock_guard<std::mutex> lock(pendingMutex_);
                for (auto& pair : pendingCalls_) {
                    allCalls.push_back(pair.second);
                }
                pendingCalls_.clear();
            }
            
            for (auto& ctx : allCalls) {
                ctx->future->complete(RpcStatus::FAILED, nullptr, "Connection closed");
                if (ctx->done) {
                    ctx->done->Run();
                }
            }
        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
            // 发生错误
            DPrintf("[AsyncMprpcChannel] recv error: %d", errno);
        }

        // 短暂休眠避免CPU空转
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    
    delete[] buffer;
}

void AsyncMprpcChannel::startFiberIO(int ioThreads) {
    if (ioManager_) {
        return;
    }
    ioManager_ = std::make_shared<monsoon::IOManager>(ioThreads, false, "rpc-iomanager");
    timeoutTimer_ = ioManager_->addTimer(timeoutCheckIntervalMs_, [this]() {
        if (running_) {
            checkTimeouts();
        }
    }, true);
}

void AsyncMprpcChannel::registerReadEvent() {
    if (!ioManager_) {
        return;
    }
    int fd = -1;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        fd = clientFd_;
    }
    if (fd == -1) {
        return;
    }
    ioManager_->addEvent(fd, monsoon::READ, [this, fd]() {
        handleReadable(fd);
    });
}

void AsyncMprpcChannel::handleReadable(int fd) {
    if (!running_) {
        return;
    }
    const int BUFFER_SIZE = 65536;
    char buffer[BUFFER_SIZE];
    bool needReRegister = true;

    while (true) {
        ssize_t recvLen = recv(fd, buffer, BUFFER_SIZE, MSG_DONTWAIT);
        if (recvLen > 0) {
            std::shared_ptr<AsyncCallContext> context;
            {
                std::lock_guard<std::mutex> lock(pendingMutex_);
                if (!pendingCalls_.empty()) {
                    auto it = pendingCalls_.begin();
                    context = it->second;
                    pendingCalls_.erase(it);
                }
            }

            if (context) {
                if (context->response && context->response->ParseFromArray(buffer, recvLen)) {
                    context->future->complete(RpcStatus::SUCCESS, context->response);
                    if (context->done) {
                        context->done->Run();
                    }
                } else {
                    std::string errMsg = "parse response error";
                    if (context->controller) {
                        context->controller->SetFailed(errMsg);
                    }
                    context->future->complete(RpcStatus::FAILED, nullptr, errMsg);
                    if (context->done) {
                        context->done->Run();
                    }
                }
            }
            continue;
        }

        if (recvLen == 0) {
            DPrintf("[AsyncMprpcChannel] 连接被关闭");
            closeConnection();
            std::vector<std::shared_ptr<AsyncCallContext>> allCalls;
            {
                std::lock_guard<std::mutex> lock(pendingMutex_);
                for (auto& pair : pendingCalls_) {
                    allCalls.push_back(pair.second);
                }
                pendingCalls_.clear();
            }
            for (auto& ctx : allCalls) {
                ctx->future->complete(RpcStatus::FAILED, nullptr, "Connection closed");
                if (ctx->done) {
                    ctx->done->Run();
                }
            }
            needReRegister = false;
            break;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }

        DPrintf("[AsyncMprpcChannel] recv error: %d", errno);
        break;
    }

    if (needReRegister && running_) {
        registerReadEvent();
    }
}

}  // namespace rpc
