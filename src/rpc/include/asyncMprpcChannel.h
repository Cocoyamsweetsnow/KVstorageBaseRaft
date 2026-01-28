#ifndef ASYNC_MPRPC_CHANNEL_H
#define ASYNC_MPRPC_CHANNEL_H

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

#include "rpcFuture.h"
#include "iomanager.hpp"

class ServiceDiscovery;

namespace rpc
{


struct AsyncCallContext
{
    uint64_t callId;                                 // 调用ID
    google::protobuf::Message *response;             // 响应消息指针
    GenericRpcFuture::ptr future;                    // Future对象
    google::protobuf::Closure *done;                 // 完成回调
    google::protobuf::RpcController *controller;     // 控制器
    std::chrono::steady_clock::time_point startTime; // 调用开始时间
    int64_t timeoutMs;                               // 超时时间（毫秒）
};


class AsyncMprpcChannel : public google::protobuf::RpcChannel
{
public:
    using ptr = std::shared_ptr<AsyncMprpcChannel>;

    // 构造函数：直接指定IP和端口
    AsyncMprpcChannel(const std::string &ip, short port, bool connectNow = true, int ioThreads = 1,
                      bool useFiberIO = false);

    // 构造函数：通过服务发现获取服务地址
    // serviceName: 服务名称
    // discovery: 服务发现组件
    // hashKey: 用于一致性哈希的key
    // ioThreads: I/O线程数量
    AsyncMprpcChannel(const std::string& serviceName,
                      std::shared_ptr<ServiceDiscovery> discovery,
                      const std::string& hashKey = "",
                      int ioThreads = 1,
                      bool useFiberIO = false);

    ~AsyncMprpcChannel();

    void CallMethod(const google::protobuf::MethodDescriptor *method, google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request, google::protobuf::Message *response,
                    google::protobuf::Closure *done) override;

    GenericRpcFuture::ptr callMethodAsync(const google::protobuf::MethodDescriptor *method,
                                          google::protobuf::RpcController *controller,
                                          const google::protobuf::Message *request, google::protobuf::Message *response,
                                          int64_t timeoutMs = 0);

    void callMethodWithCallback(const google::protobuf::MethodDescriptor *method,
                                google::protobuf::RpcController *controller, const google::protobuf::Message *request,
                                google::protobuf::Message *response, std::function<void(const RpcResult &)> callback,
                                int64_t timeoutMs = 0);

    
    void setDefaultTimeout(int64_t timeoutMs) { defaultTimeoutMs_ = timeoutMs; }


    int64_t getDefaultTimeout() const { return defaultTimeoutMs_; }

   
    bool isConnected() const { return clientFd_ != -1; }

    
    void disconnect();

    
    bool reconnect();

    
    size_t getPendingCallCount() const;

    // 设置一致性哈希key
    void setHashKey(const std::string& hashKey);

    // 检查是否使用服务发现模式
    bool isServiceDiscoveryMode() const { return useDiscovery_; }

private:
    // 连接管理
    bool newConnect(const char *ip, uint16_t port, std::string *errMsg);
    void closeConnection();

    // 通过服务发现解析地址
    bool resolveServiceAddress();

    // 发送RPC请求
    bool sendRequest(const google::protobuf::MethodDescriptor *method, const google::protobuf::Message *request,
                     uint64_t callId, std::string *errMsg);

    // I/O线程函数
    void ioThreadFunc();

    // 协程I/O管理
    void startFiberIO(int ioThreads);
    void registerReadEvent();
    void handleReadable(int fd);

    // 处理接收到的响应
    void handleResponse(uint64_t callId, const char *data, int size);

    // 超时检查
    void checkTimeouts();

    // 生成调用ID
    uint64_t generateCallId();

private:
    // 连接信息
    int clientFd_;
    std::string ip_;
    uint16_t port_;

    // 服务发现相关
    std::string serviceName_;
    std::shared_ptr<ServiceDiscovery> discovery_;
    std::string hashKey_;
    bool useDiscovery_;

    // 线程安全
    mutable std::mutex mutex_;
    mutable std::mutex pendingMutex_;

    // 异步调用管理
    std::unordered_map<uint64_t, std::shared_ptr<AsyncCallContext>> pendingCalls_;
    std::atomic<uint64_t> nextCallId_;
    int64_t defaultTimeoutMs_;

    // I/O线程
    std::vector<std::thread> ioThreads_;
    std::atomic<bool> running_;

    // 协程I/O模式
    bool useFiberIO_;
    int timeoutCheckIntervalMs_;
    std::shared_ptr<monsoon::IOManager> ioManager_;
    monsoon::Timer::ptr timeoutTimer_;

    // 非阻塞模式标志
    bool nonBlocking_;
};


template <typename StubType> class AsyncRpcCaller
{
public:
    AsyncRpcCaller(AsyncMprpcChannel::ptr channel) : channel_(channel)
    {
        stub_ = std::make_unique<StubType>(channel_.get());
    }

    StubType *stub() { return stub_.get(); }
    AsyncMprpcChannel::ptr channel() { return channel_; }

private:
    AsyncMprpcChannel::ptr channel_;
    std::unique_ptr<StubType> stub_;
};

} // namespace rpc

#endif 
