/*
 * 演示使用连接池进行RPC调用：
 * 1. 基本连接池使用
 * 2. ConnectionGuard RAII管理
 * 3. PooledMprpcChannel使用
 * 4. 多线程并发调用
 * 5. 连接池统计信息
 */

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "mprpccontroller.h"
#include "rpcConnectionPool.h"
#include "rpcExample/friend.pb.h"

using namespace rpc;

/**
 * 演示1：基本连接池使用
 */
void demoBasicPool(RpcConnectionPool::ptr pool)
{
    std::cout << "\n========== 演示1：基本连接池使用 ==========\n";

    // 手动获取和释放连接
    auto conn = pool->acquire();
    if (!conn)
    {
        std::cout << "获取连接失败！\n";
        return;
    }

    std::cout << "成功获取连接 ID: " << conn->getId() << "\n";
    std::cout << "连接状态: " << (conn->isConnected() ? "已连接" : "未连接") << "\n";

    // 模拟使用连接
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 归还连接
    pool->release(conn);
    std::cout << "连接已归还\n";

    // 查看连接池状态
    std::cout << "当前连接池状态:\n";
    std::cout << "  总连接数: " << pool->getConnectionCount() << "\n";
    std::cout << "  空闲连接: " << pool->getIdleConnectionCount() << "\n";
    std::cout << "  活跃连接: " << pool->getActiveConnectionCount() << "\n";
}

/**
 * 演示2：使用ConnectionGuard RAII管理
 */
void demoConnectionGuard(RpcConnectionPool::ptr pool)
{
    std::cout << "\n========== 演示2：ConnectionGuard RAII管理 ==========\n";

    {
        // 使用RAII自动管理连接
        ConnectionGuard guard(pool);

        if (!guard.isValid())
        {
            std::cout << "获取连接失败！\n";
            return;
        }

        std::cout << "通过ConnectionGuard获取连接 ID: " << guard->getId() << "\n";

        // 使用连接进行操作...
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        std::cout << "作用域结束，连接将自动归还...\n";
    }
    // guard超出作用域，连接自动归还

    std::cout << "连接已自动归还，当前空闲连接数: " << pool->getIdleConnectionCount() << "\n";
}

/**
 * 演示3：使用PooledMprpcChannel进行RPC调用
 */
void demoPooledChannel(PooledMprpcChannel::ptr channel)
{
    std::cout << "\n========== 演示3：PooledMprpcChannel RPC调用 ==========\n";

    fixbug::FiendServiceRpc_Stub stub(channel.get());

    fixbug::GetFriendsListRequest request;
    request.set_userid(1000);

    fixbug::GetFriendsListResponse response;
    MprpcController controller;

    // 同步调用
    stub.GetFriendsList(&controller, &request, &response, nullptr);

    if (controller.Failed())
    {
        std::cout << "RPC调用失败: " << controller.ErrorText() << "\n";
    }
    else
    {
        std::cout << "RPC调用成功！\n";
        if (response.result().errcode() == 0)
        {
            std::cout << "获取好友列表成功，共 " << response.friends_size() << " 位好友\n";
            for (int i = 0; i < response.friends_size(); ++i)
            {
                std::cout << "  " << (i + 1) << ". " << response.friends(i) << "\n";
            }
        }
        else
        {
            std::cout << "业务错误: " << response.result().errmsg() << "\n";
        }
    }
}

/**
 * 演示4：异步调用
 */
void demoPooledAsyncCall(PooledMprpcChannel::ptr channel)
{
    std::cout << "\n========== 演示4：连接池异步调用 ==========\n";

    fixbug::GetFriendsListRequest request;
    request.set_userid(2000);

    auto response = std::make_shared<fixbug::GetFriendsListResponse>();
    MprpcController controller;

    const google::protobuf::MethodDescriptor *method =
        fixbug::FiendServiceRpc::descriptor()->FindMethodByName("GetFriendsList");

    // 异步调用
    auto future = channel->callMethodAsync(method, &controller, &request, response.get(), 5000);

    std::cout << "异步调用已发起，等待响应...\n";

    // 等待结果
    RpcResult result = future->wait();

    if (result.isSuccess())
    {
        std::cout << "异步RPC调用成功！好友数量: " << response->friends_size() << "\n";
    }
    else
    {
        std::cout << "异步RPC调用失败: " << result.getErrorMsg() << "\n";
    }
}

/**
 * 演示5：多线程并发调用（展示连接池的威力）
 */
void demoConcurrentCalls(RpcConnectionPool::ptr pool)
{
    std::cout << "\n========== 演示5：多线程并发调用 ==========\n";

    const int NUM_THREADS = 10;
    const int CALLS_PER_THREAD = 5;

    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};
    std::vector<std::thread> threads;

    auto startTime = std::chrono::steady_clock::now();

    // 创建多个线程并发调用
    for (int t = 0; t < NUM_THREADS; ++t)
    {
        threads.emplace_back([&pool, &successCount, &failCount, t, CALLS_PER_THREAD]() {
            for (int i = 0; i < CALLS_PER_THREAD; ++i)
            {
                // 使用RAII获取连接
                ConnectionGuard guard(pool, 3000); // 3秒超时

                if (!guard.isValid())
                {
                    failCount++;
                    continue;
                }

                // 模拟RPC调用
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                successCount++;
            }
        });
    }

    // 等待所有线程完成
    for (auto &t : threads)
    {
        t.join();
    }

    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    std::cout << "并发测试完成！\n";
    std::cout << "  总调用数: " << (NUM_THREADS * CALLS_PER_THREAD) << "\n";
    std::cout << "  成功: " << successCount.load() << "\n";
    std::cout << "  失败: " << failCount.load() << "\n";
    std::cout << "  总耗时: " << duration.count() << "ms\n";
    std::cout << "  连接池状态:\n";
    std::cout << "    总连接数: " << pool->getConnectionCount() << "\n";
    std::cout << "    空闲连接: " << pool->getIdleConnectionCount() << "\n";
    std::cout << "    活跃连接: " << pool->getActiveConnectionCount() << "\n";
}

/**
 * 演示6：使用全局连接池管理器
 */
void demoConnectionPoolManager(const std::string &ip, short port)
{
    std::cout << "\n========== 演示6：全局连接池管理器 ==========\n";

    // 通过管理器获取连接池（自动创建或复用）
    auto &manager = ConnectionPoolManager::getInstance();

    // 配置连接池参数
    ConnectionPoolConfig config;
    config.minConnections = 2;
    config.maxConnections = 8;
    config.idleTimeoutMs = 30000;
    config.acquireTimeoutMs = 2000;

    // 获取连接池（如果不存在会自动创建）
    auto pool1 = manager.getPool(ip, port, config);
    std::cout << "获取连接池1: " << ip << ":" << port << "\n";

    // 再次获取同一目标的连接池（复用已存在的）
    auto pool2 = manager.getPool(ip, port);
    std::cout << "获取连接池2: " << ip << ":" << port << "\n";

    std::cout << "pool1 == pool2: " << (pool1 == pool2 ? "是" : "否") << "\n";

    // 获取所有连接池的统计信息
    auto allStats = manager.getAllStats();
    std::cout << "\n所有连接池统计:\n";
    for (const auto &[addr, stats] : allStats)
    {
        std::cout << "  " << addr << ":\n";
        std::cout << "    总连接: " << stats.totalConnections << "\n";
        std::cout << "    活跃: " << stats.activeConnections << "\n";
        std::cout << "    空闲: " << stats.idleConnections << "\n";
        std::cout << "    总请求: " << stats.totalRequests << "\n";
        std::cout << "    成功率: "
                  << (stats.totalRequests > 0 ? (100.0 * stats.successfulRequests / stats.totalRequests) : 0) << "%\n";
    }
}

/**
 * 演示7：连接池统计信息
 */
void demoPoolStats(RpcConnectionPool::ptr pool)
{
    std::cout << "\n========== 演示7：连接池统计信息 ==========\n";

    auto stats = pool->getStats();

    std::cout << "连接池统计:\n";
    std::cout << "  总连接数: " << stats.totalConnections << "\n";
    std::cout << "  活跃连接: " << stats.activeConnections << "\n";
    std::cout << "  空闲连接: " << stats.idleConnections << "\n";
    std::cout << "  失败连接: " << stats.failedConnections << "\n";
    std::cout << "  总请求数: " << stats.totalRequests << "\n";
    std::cout << "  成功请求: " << stats.successfulRequests << "\n";
    std::cout << "  失败请求: " << stats.failedRequests << "\n";
    std::cout << "  超时请求: " << stats.timeoutRequests << "\n";
    std::cout << "  平均响应时间: " << stats.avgResponseTimeMs << "ms\n";
}

int main(int argc, char **argv)
{
    std::string ip = "127.0.1.1";
    short port = 7788;

    if (argc >= 3)
    {
        ip = argv[1];
        port = static_cast<short>(std::atoi(argv[2]));
    }

    std::cout << "================================================\n";
    std::cout << "       RPC连接池示例程序\n";
    std::cout << "================================================\n";
    std::cout << "目标服务器: " << ip << ":" << port << "\n";

    // 配置连接池
    ConnectionPoolConfig config;
    config.minConnections = 2;            // 最小2个连接
    config.maxConnections = 10;           // 最大10个连接
    config.connectionTimeoutMs = 3000;    // 连接超时3秒
    config.idleTimeoutMs = 60000;         // 空闲超时60秒
    config.acquireTimeoutMs = 2000;       // 获取连接超时2秒
    config.enableHealthCheck = true;      // 启用健康检查
    config.healthCheckIntervalMs = 15000; // 15秒检查一次

    // 创建连接池
    auto pool = std::make_shared<RpcConnectionPool>(ip, port, config);

    if (!pool->initialize())
    {
        std::cout << "警告：连接池初始化时没有成功创建连接，但仍可继续（懒创建）\n";
    }
    else
    {
        std::cout << "连接池初始化成功！\n";
    }

    // 创建基于连接池的Channel
    auto channel = std::make_shared<PooledMprpcChannel>(pool, 5000);

    try
    {
        // 演示1：基本连接池使用
        demoBasicPool(pool);

        // 演示2：ConnectionGuard RAII管理
        demoConnectionGuard(pool);

        // 演示3：使用PooledMprpcChannel进行RPC调用
        demoPooledChannel(channel);

        // 演示4：异步调用
        demoPooledAsyncCall(channel);

        // 演示5：多线程并发调用
        demoConcurrentCalls(pool);

        // 演示6：全局连接池管理器
        demoConnectionPoolManager(ip, port);

        // 演示7：连接池统计
        demoPoolStats(pool);
    }
    catch (const std::exception &e)
    {
        std::cout << "异常: " << e.what() << "\n";
    }

    std::cout << "\n========== 示例程序结束 ==========\n";

    // 关闭连接池
    pool->shutdown();

    // 关闭所有全局连接池
    ConnectionPoolManager::getInstance().shutdownAll();

    return 0;
}
