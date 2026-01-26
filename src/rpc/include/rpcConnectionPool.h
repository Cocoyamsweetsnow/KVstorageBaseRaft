#ifndef RPC_CONNECTION_POOL_H
#define RPC_CONNECTION_POOL_H

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "rpcFuture.h"

namespace rpc
{

// 前向声明
class RpcConnectionPool;
class ConnectionPoolManager;

// 连接状态枚举
enum class ConnectionState
{
    DISCONNECTED, // 未连接
    CONNECTING,   // 连接中
    CONNECTED,    // 已连接
    BUSY,         // 使用中
    FAILED        // 连接失败
};

// 连接池配置
struct ConnectionPoolConfig
{
    size_t minConnections = 2;             // 最小连接数
    size_t maxConnections = 10;            // 最大连接数
    int64_t connectionTimeoutMs = 5000;    // 连接超时（毫秒）
    int64_t idleTimeoutMs = 60000;         // 空闲超时（毫秒）
    int64_t healthCheckIntervalMs = 30000; // 健康检查间隔（毫秒）
    int64_t acquireTimeoutMs = 3000;       // 获取连接超时（毫秒）
    int maxRetryCount = 3;                 // 最大重试次数
    bool enableHealthCheck = true;         // 是否启用健康检查
    bool enableAutoReconnect = true;       // 是否启用自动重连
};

// 连接统计信息
struct ConnectionPoolStats
{
    size_t totalConnections = 0;     // 总连接数
    size_t activeConnections = 0;    // 活跃连接数（使用中）
    size_t idleConnections = 0;      // 空闲连接数
    size_t failedConnections = 0;    // 失败的连接数
    uint64_t totalRequests = 0;      // 总请求数
    uint64_t successfulRequests = 0; // 成功请求数
    uint64_t failedRequests = 0;     // 失败请求数
    uint64_t timeoutRequests = 0;    // 超时请求数
    double avgResponseTimeMs = 0.0;  // 平均响应时间
};

// RPC连接封装类，封装单个TCP连接，提供连接状态管理、发送接收等功能
class RpcConnection : public std::enable_shared_from_this<RpcConnection>
{
public:
    using ptr = std::shared_ptr<RpcConnection>;
    using WeakPtr = std::weak_ptr<RpcConnection>;

    RpcConnection(const std::string &ip, uint16_t port, RpcConnectionPool *pool = nullptr);
    ~RpcConnection();

    // 禁止拷贝
    RpcConnection(const RpcConnection &) = delete;
    RpcConnection &operator=(const RpcConnection &) = delete;

    // 建立连接，timeoutMs为连接超时时间（毫秒），返回是否连接成功
    bool connect(int64_t timeoutMs = 5000);

    // 断开连接
    void disconnect();

    // 检查连接是否可用
    bool isConnected() const;

    // 健康检查（发送心跳或简单检测），返回连接是否健康
    bool healthCheck();

    // 发送RPC请求，返回是否发送成功
    bool sendRequest(const google::protobuf::MethodDescriptor *method, const google::protobuf::Message *request,
                     std::string *errMsg);

    // 接收RPC响应，返回是否接收成功
    bool receiveResponse(google::protobuf::Message *response, int64_t timeoutMs, std::string *errMsg);

    // Getters
    int getFd() const { return fd_; }
    const std::string &getIp() const { return ip_; }
    uint16_t getPort() const { return port_; }
    ConnectionState getState() const { return state_.load(); }

    std::chrono::steady_clock::time_point getLastActiveTime() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return lastActiveTime_;
    }

    std::chrono::steady_clock::time_point getCreateTime() const { return createTime_; }

    uint64_t getUsageCount() const { return usageCount_.load(); }

    // 标记连接开始使用
    void markBusy();

    // 标记连接使用完毕
    void markIdle();

    // 更新最后活跃时间
    void updateLastActiveTime();

    // 获取连接唯一标识
    uint64_t getId() const { return id_; }

private:
    static std::atomic<uint64_t> nextId_;

    uint64_t id_;                        // 连接唯一ID
    int fd_;                             // socket文件描述符
    std::string ip_;                     // 目标IP
    uint16_t port_;                      // 目标端口
    std::atomic<ConnectionState> state_; // 连接状态
    RpcConnectionPool *pool_;            // 所属连接池（可为空）

    mutable std::mutex mutex_;
    std::chrono::steady_clock::time_point createTime_;     // 创建时间
    std::chrono::steady_clock::time_point lastActiveTime_; // 最后活跃时间
    std::atomic<uint64_t> usageCount_;                     // 使用次数
};

// RPC连接池类，管理到单一目标（ip:port）的连接池
class RpcConnectionPool : public std::enable_shared_from_this<RpcConnectionPool>
{
public:
    using ptr = std::shared_ptr<RpcConnectionPool>;

    // 构造函数
    RpcConnectionPool(const std::string &ip, uint16_t port,
                      const ConnectionPoolConfig &config = ConnectionPoolConfig());

    ~RpcConnectionPool();

    // 禁止拷贝
    RpcConnectionPool(const RpcConnectionPool &) = delete;
    RpcConnectionPool &operator=(const RpcConnectionPool &) = delete;

    // 初始化连接池（创建最小连接数的连接），返回是否初始化成功
    bool initialize();

    // 关闭连接池
    void shutdown();

    // 获取一个可用连接，timeoutMs为等待超时时间（毫秒），-1表示使用默认配置
    RpcConnection::ptr acquire(int64_t timeoutMs = -1);

    // 归还连接
    void release(RpcConnection::ptr conn);

    // 获取连接池统计信息
    ConnectionPoolStats getStats() const;

    // 获取当前连接数
    size_t getConnectionCount() const;

    // 获取空闲连接数
    size_t getIdleConnectionCount() const;

    // 获取活跃连接数
    size_t getActiveConnectionCount() const;

    // Getters
    const std::string &getIp() const { return ip_; }
    uint16_t getPort() const { return port_; }
    const ConnectionPoolConfig &getConfig() const { return config_; }

    // 更新请求统计
    void recordRequest(bool success, double responseTimeMs);

private:
    // 创建新连接
    RpcConnection::ptr createConnection();

    // 健康检查线程函数
    void healthCheckThread();

    // 清理空闲超时的连接
    void cleanupIdleConnections();

    // 确保有足够的连接
    void ensureMinConnections();

    // 移除失效连接
    void removeConnection(RpcConnection::ptr conn);

private:
    std::string ip_;
    uint16_t port_;
    ConnectionPoolConfig config_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;

    std::deque<RpcConnection::ptr> idleConnections_;                     // 空闲连接队列
    std::unordered_map<uint64_t, RpcConnection::ptr> activeConnections_; // 活跃连接
    std::unordered_map<uint64_t, RpcConnection::ptr> allConnections_;    // 所有连接

    std::atomic<bool> running_;
    std::thread healthCheckThread_;

    // 统计信息
    mutable std::mutex statsMutex_;
    ConnectionPoolStats stats_;
    std::vector<double> recentResponseTimes_;
    static constexpr size_t MAX_RESPONSE_TIME_SAMPLES = 100;
};

// RAII风格的连接持有者，自动管理连接的获取和释放
class ConnectionGuard
{
public:
    ConnectionGuard(RpcConnectionPool::ptr pool, int64_t timeoutMs = -1);
    ~ConnectionGuard();

    // 禁止拷贝
    ConnectionGuard(const ConnectionGuard &) = delete;
    ConnectionGuard &operator=(const ConnectionGuard &) = delete;

    // 支持移动
    ConnectionGuard(ConnectionGuard &&other) noexcept;
    ConnectionGuard &operator=(ConnectionGuard &&other) noexcept;

    // 获取连接，如果获取失败返回nullptr
    RpcConnection::ptr get() const { return conn_; }

    // 检查是否有有效连接
    bool isValid() const { return conn_ != nullptr; }

    // 显式释放连接（通常不需要手动调用）
    void release();

    // 便捷操作符
    RpcConnection *operator->() const { return conn_.get(); }
    explicit operator bool() const { return isValid(); }

private:
    RpcConnectionPool::ptr pool_;
    RpcConnection::ptr conn_;
};

// 全局连接池管理器（单例模式），管理多个目标的连接池
class ConnectionPoolManager
{
public:
    // 获取单例实例
    static ConnectionPoolManager &getInstance();

    // 禁止拷贝和移动
    ConnectionPoolManager(const ConnectionPoolManager &) = delete;
    ConnectionPoolManager &operator=(const ConnectionPoolManager &) = delete;
    ConnectionPoolManager(ConnectionPoolManager &&) = delete;
    ConnectionPoolManager &operator=(ConnectionPoolManager &&) = delete;

    // 获取或创建指定目标的连接池
    RpcConnectionPool::ptr getPool(const std::string &ip, uint16_t port,
                                   const ConnectionPoolConfig &config = ConnectionPoolConfig());

    // 移除指定目标的连接池
    void removePool(const std::string &ip, uint16_t port);

    // 关闭所有连接池
    void shutdownAll();

    // 获取所有连接池的统计信息
    std::unordered_map<std::string, ConnectionPoolStats> getAllStats() const;

    // 设置默认配置
    void setDefaultConfig(const ConnectionPoolConfig &config);

    // 获取默认配置
    const ConnectionPoolConfig &getDefaultConfig() const { return defaultConfig_; }

private:
    ConnectionPoolManager();
    ~ConnectionPoolManager();

    static std::string makeKey(const std::string &ip, uint16_t port);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, RpcConnectionPool::ptr> pools_;
    ConnectionPoolConfig defaultConfig_;
};

// 基于连接池的RPC Channel，使用连接池进行RPC调用
class PooledMprpcChannel : public google::protobuf::RpcChannel
{
public:
    using ptr = std::shared_ptr<PooledMprpcChannel>;

    // 构造函数：pool 连接池，defaultTimeoutMs 默认超时时间
    PooledMprpcChannel(RpcConnectionPool::ptr pool, int64_t defaultTimeoutMs = 5000);

    // 便捷构造函数（自动从管理器获取连接池）
    PooledMprpcChannel(const std::string &ip, uint16_t port,
                       const ConnectionPoolConfig &config = ConnectionPoolConfig(), int64_t defaultTimeoutMs = 5000);

    ~PooledMprpcChannel() override;

    // 实现RpcChannel接口
    void CallMethod(const google::protobuf::MethodDescriptor *method, google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request, google::protobuf::Message *response,
                    google::protobuf::Closure *done) override;

    // 异步调用方法
    GenericRpcFuture::ptr callMethodAsync(const google::protobuf::MethodDescriptor *method,
                                          google::protobuf::RpcController *controller,
                                          const google::protobuf::Message *request, google::protobuf::Message *response,
                                          int64_t timeoutMs = 0);

    // 带回调的异步调用
    void callMethodWithCallback(const google::protobuf::MethodDescriptor *method,
                                google::protobuf::RpcController *controller, const google::protobuf::Message *request,
                                google::protobuf::Message *response, std::function<void(const RpcResult &)> callback,
                                int64_t timeoutMs = 0);

    // Getters and Setters
    void setDefaultTimeout(int64_t timeoutMs) { defaultTimeoutMs_ = timeoutMs; }
    int64_t getDefaultTimeout() const { return defaultTimeoutMs_; }
    RpcConnectionPool::ptr getPool() const { return pool_; }

private:
    RpcConnectionPool::ptr pool_;
    int64_t defaultTimeoutMs_;
};

} // namespace rpc

#endif // RPC_CONNECTION_POOL_H
