#include "include/rpcConnectionPool.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "include/rpcheader.pb.h"
#include "../common/include/util.h"

namespace rpc {

//==============================================================================
// RpcConnection 实现
//==============================================================================

std::atomic<uint64_t> RpcConnection::nextId_{1};

RpcConnection::RpcConnection(const std::string& ip, uint16_t port, RpcConnectionPool* pool)
    : id_(nextId_.fetch_add(1))
    , fd_(-1)
    , ip_(ip)
    , port_(port)
    , state_(ConnectionState::DISCONNECTED)
    , pool_(pool)
    , createTime_(std::chrono::steady_clock::now())
    , lastActiveTime_(std::chrono::steady_clock::now())
    , usageCount_(0) {
}

RpcConnection::~RpcConnection() {
    disconnect();
}

bool RpcConnection::connect(int64_t timeoutMs) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
    
    state_ = ConnectionState::CONNECTING;
    
    // 创建socket
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ == -1) {
        DPrintf("[RpcConnection %lu] 创建socket失败: errno=%d", id_, errno);
        state_ = ConnectionState::FAILED;
        return false;
    }
    
    // 设置非阻塞模式以支持连接超时
    int flags = fcntl(fd_, F_GETFL, 0);
    fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
    
    // 设置TCP选项
    int optval = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval));
    
    // 设置keepalive
    setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval));
    
    // 构建目标地址
    struct sockaddr_in serverAddr;
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port_);
    serverAddr.sin_addr.s_addr = inet_addr(ip_.c_str());
    
    // 发起连接
    int ret = ::connect(fd_, (struct sockaddr*)&serverAddr, sizeof(serverAddr));
    if (ret == -1) {
        if (errno != EINPROGRESS) {
            DPrintf("[RpcConnection %lu] 连接失败: ip=%s, port=%d, errno=%d", 
                    id_, ip_.c_str(), port_, errno);
            close(fd_);
            fd_ = -1;
            state_ = ConnectionState::FAILED;
            return false;
        }
        
        // 等待连接完成
        struct pollfd pfd;
        pfd.fd = fd_;
        pfd.events = POLLOUT;
        
        int pollRet = poll(&pfd, 1, static_cast<int>(timeoutMs));
        if (pollRet <= 0) {
            DPrintf("[RpcConnection %lu] 连接超时: ip=%s, port=%d", 
                    id_, ip_.c_str(), port_);
            close(fd_);
            fd_ = -1;
            state_ = ConnectionState::FAILED;
            return false;
        }
        
        // 检查连接是否成功
        int error = 0;
        socklen_t len = sizeof(error);
        if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0) {
            DPrintf("[RpcConnection %lu] 连接错误: ip=%s, port=%d, error=%d", 
                    id_, ip_.c_str(), port_, error);
            close(fd_);
            fd_ = -1;
            state_ = ConnectionState::FAILED;
            return false;
        }
    }
    
    state_ = ConnectionState::CONNECTED;
    lastActiveTime_ = std::chrono::steady_clock::now();
    DPrintf("[RpcConnection %lu] 连接成功: ip=%s, port=%d", id_, ip_.c_str(), port_);
    return true;
}

void RpcConnection::disconnect() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
        DPrintf("[RpcConnection %lu] 连接已断开", id_);
    }
    state_ = ConnectionState::DISCONNECTED;
}

bool RpcConnection::isConnected() const {
    ConnectionState s = state_.load();
    return s == ConnectionState::CONNECTED || s == ConnectionState::BUSY;
}

bool RpcConnection::healthCheck() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (fd_ == -1) {
        return false;
    }
    
    // 使用poll检测socket状态
    struct pollfd pfd;
    pfd.fd = fd_;
    pfd.events = POLLIN | POLLOUT;
    
    int ret = poll(&pfd, 1, 0);
    if (ret < 0) {
        return false;
    }
    
    // 检查是否有错误或对端关闭
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
        return false;
    }
    
    // 检查TCP连接状态（通过尝试发送0字节数据）
    char buf;
    ssize_t result = recv(fd_, &buf, 1, MSG_PEEK | MSG_DONTWAIT);
    if (result == 0) {
        // 对端关闭连接
        return false;
    }
    if (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        return false;
    }
    
    lastActiveTime_ = std::chrono::steady_clock::now();
    return true;
}

bool RpcConnection::sendRequest(const google::protobuf::MethodDescriptor* method,
                                 const google::protobuf::Message* request,
                                 std::string* errMsg) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (fd_ == -1) {
        *errMsg = "连接未建立";
        return false;
    }
    
    const google::protobuf::ServiceDescriptor* sd = method->service();
    std::string serviceName = sd->name();
    std::string methodName = method->name();
    
    // 序列化请求
    std::string argsStr;
    if (!request->SerializeToString(&argsStr)) {
        *errMsg = "序列化请求失败";
        return false;
    }
    uint32_t argsSize = argsStr.size();
    
    // 构建RPC头
    RPC::RpcHeader rpcHeader;
    rpcHeader.set_service_name(serviceName);
    rpcHeader.set_method_name(methodName);
    rpcHeader.set_args_size(argsSize);
    
    std::string rpcHeaderStr;
    if (!rpcHeader.SerializeToString(&rpcHeaderStr)) {
        *errMsg = "序列化RPC头失败";
        return false;
    }
    
    // 构建发送数据
    std::string sendRpcStr;
    {
        google::protobuf::io::StringOutputStream stringOutput(&sendRpcStr);
        google::protobuf::io::CodedOutputStream codedOutput(&stringOutput);
        codedOutput.WriteVarint32(static_cast<uint32_t>(rpcHeaderStr.size()));
        codedOutput.WriteString(rpcHeaderStr);
    }
    sendRpcStr += argsStr;
    
    // 发送数据
    ssize_t totalSent = 0;
    ssize_t dataLen = sendRpcStr.size();
    const char* data = sendRpcStr.c_str();
    
    while (totalSent < dataLen) {
        ssize_t sent = send(fd_, data + totalSent, dataLen - totalSent, MSG_NOSIGNAL);
        if (sent == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 等待可写
                struct pollfd pfd;
                pfd.fd = fd_;
                pfd.events = POLLOUT;
                int pollRet = poll(&pfd, 1, 1000);  // 等待1秒
                if (pollRet <= 0) {
                    char errtxt[512] = {0};
                    snprintf(errtxt, sizeof(errtxt), "发送超时: errno=%d", errno);
                    *errMsg = errtxt;
                    return false;
                }
                continue;
            }
            char errtxt[512] = {0};
            snprintf(errtxt, sizeof(errtxt), "发送失败: errno=%d", errno);
            *errMsg = errtxt;
            return false;
        }
        totalSent += sent;
    }
    
    lastActiveTime_ = std::chrono::steady_clock::now();
    return true;
}

bool RpcConnection::receiveResponse(google::protobuf::Message* response,
                                     int64_t timeoutMs,
                                     std::string* errMsg) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (fd_ == -1) {
        *errMsg = "连接未建立";
        return false;
    }
    
    // 等待数据可读
    struct pollfd pfd;
    pfd.fd = fd_;
    pfd.events = POLLIN;
    
    int pollRet = poll(&pfd, 1, static_cast<int>(timeoutMs));
    if (pollRet < 0) {
        char errtxt[512] = {0};
        snprintf(errtxt, sizeof(errtxt), "poll失败: errno=%d", errno);
        *errMsg = errtxt;
        return false;
    }
    if (pollRet == 0) {
        *errMsg = "接收响应超时";
        return false;
    }
    
    // 接收数据
    const int BUFFER_SIZE = 65536;
    char buffer[BUFFER_SIZE];
    
    ssize_t recvLen = recv(fd_, buffer, BUFFER_SIZE, 0);
    if (recvLen <= 0) {
        if (recvLen == 0) {
            *errMsg = "连接已关闭";
        } else {
            char errtxt[512] = {0};
            snprintf(errtxt, sizeof(errtxt), "接收失败: errno=%d", errno);
            *errMsg = errtxt;
        }
        return false;
    }
    
    // 解析响应
    if (!response->ParseFromArray(buffer, recvLen)) {
        *errMsg = "解析响应失败";
        return false;
    }
    
    lastActiveTime_ = std::chrono::steady_clock::now();
    return true;
}

void RpcConnection::markBusy() {
    state_ = ConnectionState::BUSY;
    usageCount_.fetch_add(1);
}

void RpcConnection::markIdle() {
    state_ = ConnectionState::CONNECTED;
    updateLastActiveTime();
}

void RpcConnection::updateLastActiveTime() {
    std::lock_guard<std::mutex> lock(mutex_);
    lastActiveTime_ = std::chrono::steady_clock::now();
}

//==============================================================================
// RpcConnectionPool 实现
//==============================================================================

RpcConnectionPool::RpcConnectionPool(const std::string& ip, uint16_t port,
                                     const ConnectionPoolConfig& config)
    : ip_(ip)
    , port_(port)
    , config_(config)
    , running_(false) {
}

RpcConnectionPool::~RpcConnectionPool() {
    shutdown();
}

bool RpcConnectionPool::initialize() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (running_) {
        return true;
    }
    
    DPrintf("[RpcConnectionPool] 初始化连接池: ip=%s, port=%d, min=%zu, max=%zu",
            ip_.c_str(), port_, config_.minConnections, config_.maxConnections);
    
    // 创建最小连接数的连接
    for (size_t i = 0; i < config_.minConnections; ++i) {
        auto conn = createConnection();
        if (conn && conn->isConnected()) {
            idleConnections_.push_back(conn);
            allConnections_[conn->getId()] = conn;
        }
    }
    
    running_ = true;
    
    // 启动健康检查线程
    if (config_.enableHealthCheck) {
        healthCheckThread_ = std::thread(&RpcConnectionPool::healthCheckThread, this);
    }
    
    DPrintf("[RpcConnectionPool] 初始化完成，当前连接数: %zu", allConnections_.size());
    return !allConnections_.empty() || config_.minConnections == 0;
}

void RpcConnectionPool::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) {
            return;
        }
        running_ = false;
    }
    
    cv_.notify_all();
    
    // 等待健康检查线程结束
    if (healthCheckThread_.joinable()) {
        healthCheckThread_.join();
    }
    
    // 关闭所有连接
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& pair : allConnections_) {
        pair.second->disconnect();
    }
    
    idleConnections_.clear();
    activeConnections_.clear();
    allConnections_.clear();
    
    DPrintf("[RpcConnectionPool] 连接池已关闭: ip=%s, port=%d", ip_.c_str(), port_);
}

RpcConnection::ptr RpcConnectionPool::acquire(int64_t timeoutMs) {
    if (timeoutMs < 0) {
        timeoutMs = config_.acquireTimeoutMs;
    }
    
    auto deadline = std::chrono::steady_clock::now() + 
                    std::chrono::milliseconds(timeoutMs);
    
    std::unique_lock<std::mutex> lock(mutex_);
    
    while (running_) {
        // 优先从空闲队列获取
        while (!idleConnections_.empty()) {
            auto conn = idleConnections_.front();
            idleConnections_.pop_front();
            
            // 检查连接是否有效
            if (conn && conn->isConnected()) {
                conn->markBusy();
                activeConnections_[conn->getId()] = conn;
                DPrintf("[RpcConnectionPool] 获取连接: id=%lu, 空闲=%zu, 活跃=%zu",
                        conn->getId(), idleConnections_.size(), activeConnections_.size());
                return conn;
            } else {
                // 移除无效连接
                allConnections_.erase(conn->getId());
            }
        }
        
        // 空闲队列为空，尝试创建新连接
        if (allConnections_.size() < config_.maxConnections) {
            lock.unlock();
            auto conn = createConnection();
            lock.lock();
            
            if (conn && conn->isConnected()) {
                conn->markBusy();
                allConnections_[conn->getId()] = conn;
                activeConnections_[conn->getId()] = conn;
                DPrintf("[RpcConnectionPool] 创建新连接: id=%lu, 总数=%zu",
                        conn->getId(), allConnections_.size());
                return conn;
            }
        }
        
        // 无法创建新连接，等待连接被归还
        if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
            DPrintf("[RpcConnectionPool] 获取连接超时");
            return nullptr;
        }
    }
    
    return nullptr;
}

void RpcConnectionPool::release(RpcConnection::ptr conn) {
    if (!conn) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = activeConnections_.find(conn->getId());
    if (it == activeConnections_.end()) {
        DPrintf("[RpcConnectionPool] 尝试归还不属于此连接池的连接: id=%lu", conn->getId());
        return;
    }
    
    activeConnections_.erase(it);
    
    // 检查连接是否仍然有效
    if (conn->isConnected() && running_) {
        conn->markIdle();
        idleConnections_.push_back(conn);
        DPrintf("[RpcConnectionPool] 归还连接: id=%lu, 空闲=%zu, 活跃=%zu",
                conn->getId(), idleConnections_.size(), activeConnections_.size());
    } else {
        // 连接已失效，从池中移除
        allConnections_.erase(conn->getId());
        DPrintf("[RpcConnectionPool] 移除失效连接: id=%lu", conn->getId());
    }
    
    cv_.notify_one();
}

RpcConnection::ptr RpcConnectionPool::createConnection() {
    auto conn = std::make_shared<RpcConnection>(ip_, port_, this);
    
    int retryCount = 0;
    while (retryCount < config_.maxRetryCount) {
        if (conn->connect(config_.connectionTimeoutMs)) {
            return conn;
        }
        ++retryCount;
        DPrintf("[RpcConnectionPool] 连接失败，重试 %d/%d", retryCount, config_.maxRetryCount);
        std::this_thread::sleep_for(std::chrono::milliseconds(100 * retryCount));
    }
    
    return nullptr;
}

void RpcConnectionPool::healthCheckThread() {
    while (running_) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(config_.healthCheckIntervalMs));
        
        if (!running_) {
            break;
        }
        
        // 健康检查和清理
        cleanupIdleConnections();
        ensureMinConnections();
    }
}

void RpcConnectionPool::cleanupIdleConnections() {
    auto now = std::chrono::steady_clock::now();
    std::vector<RpcConnection::ptr> toRemove;
    
    {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = idleConnections_.begin();
        while (it != idleConnections_.end()) {
            auto& conn = *it;
            
            // 检查空闲超时
            auto idleTime = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - conn->getLastActiveTime()).count();
            
            // 保留最小连接数
            if (allConnections_.size() > config_.minConnections && 
                idleTime > config_.idleTimeoutMs) {
                toRemove.push_back(conn);
                allConnections_.erase(conn->getId());
                it = idleConnections_.erase(it);
                continue;
            }
            
            // 健康检查
            if (!conn->healthCheck()) {
                toRemove.push_back(conn);
                allConnections_.erase(conn->getId());
                it = idleConnections_.erase(it);
                continue;
            }
            
            ++it;
        }
    }
    
    // 断开已移除的连接
    for (auto& conn : toRemove) {
        conn->disconnect();
        DPrintf("[RpcConnectionPool] 清理连接: id=%lu", conn->getId());
    }
}

void RpcConnectionPool::ensureMinConnections() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    while (allConnections_.size() < config_.minConnections && running_) {
        auto conn = createConnection();
        if (conn && conn->isConnected()) {
            idleConnections_.push_back(conn);
            allConnections_[conn->getId()] = conn;
            DPrintf("[RpcConnectionPool] 补充连接: id=%lu, 当前总数=%zu",
                    conn->getId(), allConnections_.size());
        } else {
            break;  // 无法创建新连接，停止尝试
        }
    }
}

void RpcConnectionPool::removeConnection(RpcConnection::ptr conn) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    activeConnections_.erase(conn->getId());
    allConnections_.erase(conn->getId());
    
    // 从空闲队列中移除
    auto it = std::find_if(idleConnections_.begin(), idleConnections_.end(),
                           [&conn](const RpcConnection::ptr& c) {
                               return c->getId() == conn->getId();
                           });
    if (it != idleConnections_.end()) {
        idleConnections_.erase(it);
    }
}

ConnectionPoolStats RpcConnectionPool::getStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::lock_guard<std::mutex> statsLock(statsMutex_);
    
    ConnectionPoolStats stats = stats_;
    stats.totalConnections = allConnections_.size();
    stats.activeConnections = activeConnections_.size();
    stats.idleConnections = idleConnections_.size();
    stats.failedConnections = 0;  // 可以添加更详细的追踪
    
    return stats;
}

size_t RpcConnectionPool::getConnectionCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return allConnections_.size();
}

size_t RpcConnectionPool::getIdleConnectionCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return idleConnections_.size();
}

size_t RpcConnectionPool::getActiveConnectionCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return activeConnections_.size();
}

void RpcConnectionPool::recordRequest(bool success, double responseTimeMs) {
    std::lock_guard<std::mutex> lock(statsMutex_);
    
    stats_.totalRequests++;
    if (success) {
        stats_.successfulRequests++;
    } else {
        stats_.failedRequests++;
    }
    
    // 更新平均响应时间
    recentResponseTimes_.push_back(responseTimeMs);
    if (recentResponseTimes_.size() > MAX_RESPONSE_TIME_SAMPLES) {
        recentResponseTimes_.erase(recentResponseTimes_.begin());
    }
    
    double sum = 0;
    for (double t : recentResponseTimes_) {
        sum += t;
    }
    stats_.avgResponseTimeMs = sum / recentResponseTimes_.size();
}

//==============================================================================
// ConnectionGuard 实现
//==============================================================================

ConnectionGuard::ConnectionGuard(RpcConnectionPool::ptr pool, int64_t timeoutMs)
    : pool_(pool)
    , conn_(nullptr) {
    if (pool_) {
        conn_ = pool_->acquire(timeoutMs);
    }
}

ConnectionGuard::~ConnectionGuard() {
    release();
}

ConnectionGuard::ConnectionGuard(ConnectionGuard&& other) noexcept
    : pool_(std::move(other.pool_))
    , conn_(std::move(other.conn_)) {
    other.pool_ = nullptr;
    other.conn_ = nullptr;
}

ConnectionGuard& ConnectionGuard::operator=(ConnectionGuard&& other) noexcept {
    if (this != &other) {
        release();
        pool_ = std::move(other.pool_);
        conn_ = std::move(other.conn_);
        other.pool_ = nullptr;
        other.conn_ = nullptr;
    }
    return *this;
}

void ConnectionGuard::release() {
    if (pool_ && conn_) {
        pool_->release(conn_);
        conn_ = nullptr;
    }
}

//==============================================================================
// ConnectionPoolManager 实现
//==============================================================================

ConnectionPoolManager& ConnectionPoolManager::getInstance() {
    static ConnectionPoolManager instance;
    return instance;
}

ConnectionPoolManager::ConnectionPoolManager() = default;

ConnectionPoolManager::~ConnectionPoolManager() {
    shutdownAll();
}

std::string ConnectionPoolManager::makeKey(const std::string& ip, uint16_t port) {
    return ip + ":" + std::to_string(port);
}

RpcConnectionPool::ptr ConnectionPoolManager::getPool(const std::string& ip, 
                                                       uint16_t port,
                                                       const ConnectionPoolConfig& config) {
    std::string key = makeKey(ip, port);
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = pools_.find(key);
    if (it != pools_.end()) {
        return it->second;
    }
    
    // 创建新的连接池
    auto pool = std::make_shared<RpcConnectionPool>(ip, port, config);
    if (!pool->initialize()) {
        DPrintf("[ConnectionPoolManager] 创建连接池失败: %s", key.c_str());
        // 即使初始化时没有成功创建连接，也返回连接池对象，后续可以懒创建
    }
    
    pools_[key] = pool;
    DPrintf("[ConnectionPoolManager] 创建连接池: %s", key.c_str());
    return pool;
}

void ConnectionPoolManager::removePool(const std::string& ip, uint16_t port) {
    std::string key = makeKey(ip, port);
    
    RpcConnectionPool::ptr pool;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = pools_.find(key);
        if (it != pools_.end()) {
            pool = it->second;
            pools_.erase(it);
        }
    }
    
    if (pool) {
        pool->shutdown();
        DPrintf("[ConnectionPoolManager] 移除连接池: %s", key.c_str());
    }
}

void ConnectionPoolManager::shutdownAll() {
    std::unordered_map<std::string, RpcConnectionPool::ptr> poolsCopy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        poolsCopy.swap(pools_);
    }
    
    for (auto& pair : poolsCopy) {
        pair.second->shutdown();
    }
    
    DPrintf("[ConnectionPoolManager] 所有连接池已关闭");
}

std::unordered_map<std::string, ConnectionPoolStats> 
ConnectionPoolManager::getAllStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::unordered_map<std::string, ConnectionPoolStats> result;
    for (const auto& pair : pools_) {
        result[pair.first] = pair.second->getStats();
    }
    return result;
}

void ConnectionPoolManager::setDefaultConfig(const ConnectionPoolConfig& config) {
    defaultConfig_ = config;
}

//==============================================================================
// PooledMprpcChannel 实现
//==============================================================================

PooledMprpcChannel::PooledMprpcChannel(RpcConnectionPool::ptr pool, 
                                       int64_t defaultTimeoutMs)
    : pool_(pool)
    , defaultTimeoutMs_(defaultTimeoutMs) {
}

PooledMprpcChannel::PooledMprpcChannel(const std::string& ip, uint16_t port,
                                       const ConnectionPoolConfig& config,
                                       int64_t defaultTimeoutMs)
    : pool_(ConnectionPoolManager::getInstance().getPool(ip, port, config))
    , defaultTimeoutMs_(defaultTimeoutMs) {
}

PooledMprpcChannel::~PooledMprpcChannel() = default;

void PooledMprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                                    google::protobuf::RpcController* controller,
                                    const google::protobuf::Message* request,
                                    google::protobuf::Message* response,
                                    google::protobuf::Closure* done) {
    auto startTime = std::chrono::steady_clock::now();
    bool success = false;
    std::string errMsg;
    
    // 从连接池获取连接
    ConnectionGuard guard(pool_);
    if (!guard.isValid()) {
        errMsg = "获取连接失败";
        if (controller) {
            controller->SetFailed(errMsg);
        }
        if (done) {
            done->Run();
        }
        return;
    }
    
    // 发送请求
    if (!guard->sendRequest(method, request, &errMsg)) {
        if (controller) {
            controller->SetFailed(errMsg);
        }
        if (done) {
            done->Run();
        }
        return;
    }
    
    // 接收响应
    if (!guard->receiveResponse(response, defaultTimeoutMs_, &errMsg)) {
        if (controller) {
            controller->SetFailed(errMsg);
        }
        if (done) {
            done->Run();
        }
        return;
    }
    
    success = true;
    
    // 记录统计
    auto endTime = std::chrono::steady_clock::now();
    double responseTimeMs = std::chrono::duration<double, std::milli>(
        endTime - startTime).count();
    pool_->recordRequest(success, responseTimeMs);
    
    if (done) {
        done->Run();
    }
}

GenericRpcFuture::ptr PooledMprpcChannel::callMethodAsync(
    const google::protobuf::MethodDescriptor* method,
    google::protobuf::RpcController* controller,
    const google::protobuf::Message* request,
    google::protobuf::Message* response,
    int64_t timeoutMs) {
    
    auto future = std::make_shared<GenericRpcFuture>();
    
    // 在新线程中执行异步调用
    std::thread([this, method, controller, request, response, timeoutMs, future]() {
        auto startTime = std::chrono::steady_clock::now();
        std::string errMsg;
        bool success = false;
        
        // 从连接池获取连接
        ConnectionGuard guard(pool_);
        if (!guard.isValid()) {
            errMsg = "获取连接失败";
            if (controller) {
                controller->SetFailed(errMsg);
            }
            future->complete(RpcStatus::FAILED, nullptr, errMsg);
            return;
        }
        
        // 发送请求
        if (!guard->sendRequest(method, request, &errMsg)) {
            if (controller) {
                controller->SetFailed(errMsg);
            }
            future->complete(RpcStatus::FAILED, nullptr, errMsg);
            return;
        }
        
        // 接收响应
        int64_t actualTimeout = timeoutMs > 0 ? timeoutMs : defaultTimeoutMs_;
        if (!guard->receiveResponse(response, actualTimeout, &errMsg)) {
            if (controller) {
                controller->SetFailed(errMsg);
            }
            if (errMsg.find("超时") != std::string::npos) {
                future->complete(RpcStatus::TIMEOUT, nullptr, errMsg);
            } else {
                future->complete(RpcStatus::FAILED, nullptr, errMsg);
            }
            return;
        }
        
        success = true;
        
        // 记录统计
        auto endTime = std::chrono::steady_clock::now();
        double responseTimeMs = std::chrono::duration<double, std::milli>(
            endTime - startTime).count();
        pool_->recordRequest(success, responseTimeMs);
        
        future->complete(RpcStatus::SUCCESS, response);
    }).detach();
    
    return future;
}

void PooledMprpcChannel::callMethodWithCallback(
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

}  // namespace rpc
