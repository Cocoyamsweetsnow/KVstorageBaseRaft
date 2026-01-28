/*
 * 演示了三种异步RPC调用方式：
 * 1. Future模式 - 阻塞等待
 * 2. Future模式 - 超时等待
 * 3. Future模式 - 回调通知
 */

#include <iostream>
#include <thread>
#include <vector>

#include "asyncMprpcChannel.h"
#include "mprpccontroller.h"
#include "rpcExample/friend.pb.h"

using namespace rpc;

/**
 * 演示方式1：Future阻塞等待
 */
void demoFutureWait(AsyncMprpcChannel::ptr channel)
{
    std::cout << "\n========== 方式1：Future阻塞等待 ==========\n";

    fixbug::FiendServiceRpc_Stub stub(channel.get());

    fixbug::GetFriendsListRequest request;
    request.set_userid(1000);

    fixbug::GetFriendsListResponse response;
    MprpcController controller;

    // 获取方法描述符
    const google::protobuf::MethodDescriptor *method =
        fixbug::FiendServiceRpc::descriptor()->FindMethodByName("GetFriendsList");

    // 发起异步调用
    auto future = channel->callMethodAsync(method, &controller, &request, &response, 5000);

    std::cout << "异步调用已发起，等待响应...\n";

    // 阻塞等待结果
    RpcResult result = future->wait();

    if (result.isSuccess())
    {
        std::cout << "RPC调用成功！\n";
        if (response.result().errcode() == 0)
        {
            std::cout << "获取好友列表成功，共 " << response.friends_size() << " 位好友：\n";
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
    else
    {
        std::cout << "RPC调用失败: " << result.getErrorMsg() << "\n";
    }
}

/**
 * 演示方式2：Future超时等待
 */
void demoFutureTimeout(AsyncMprpcChannel::ptr channel)
{
    std::cout << "\n========== 方式2：Future超时等待 ==========\n";

    fixbug::FiendServiceRpc_Stub stub(channel.get());

    fixbug::GetFriendsListRequest request;
    request.set_userid(2000);

    fixbug::GetFriendsListResponse response;
    MprpcController controller;

    const google::protobuf::MethodDescriptor *method =
        fixbug::FiendServiceRpc::descriptor()->FindMethodByName("GetFriendsList");

    auto future = channel->callMethodAsync(method, &controller, &request, &response);

    std::cout << "异步调用已发起，设置1秒超时...\n";

    // 带超时的等待（1秒）
    RpcResult result = future->waitFor(1000);

    if (result.isTimeout())
    {
        std::cout << "RPC调用超时！\n";
    }
    else if (result.isSuccess())
    {
        std::cout << "RPC调用成功！\n";
        std::cout << "好友数量: " << response.friends_size() << "\n";
    }
    else
    {
        std::cout << "RPC调用失败: " << result.getErrorMsg() << "\n";
    }
}

/**
 * 演示方式3：回调通知
 */
void demoCallback(AsyncMprpcChannel::ptr channel)
{
    std::cout << "\n========== 方式3：回调通知 ==========\n";

    fixbug::GetFriendsListRequest request;
    request.set_userid(3000);

    // 动态分配响应对象（回调中使用）
    auto response = std::make_shared<fixbug::GetFriendsListResponse>();
    MprpcController controller;

    const google::protobuf::MethodDescriptor *method =
        fixbug::FiendServiceRpc::descriptor()->FindMethodByName("GetFriendsList");

    // 使用回调方式
    std::atomic<bool> done{false};

    channel->callMethodWithCallback(
        method, &controller, &request, response.get(),
        [&done, response](const RpcResult &result) {
            if (result.isSuccess())
            {
                std::cout << "[回调] RPC调用成功！好友数量: " << response->friends_size() << "\n";
            }
            else
            {
                std::cout << "[回调] RPC调用失败: " << result.getErrorMsg() << "\n";
            }
            done = true;
        },
        5000 // 5秒超时
    );

    std::cout << "异步调用已发起，等待回调...\n";

    // 等待回调完成
    while (!done)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

/**
 * 演示并发异步调用
 */
void demoConcurrentCalls(AsyncMprpcChannel::ptr channel)
{
    std::cout << "\n========== 演示并发异步调用 ==========\n";

    const int NUM_CALLS = 5;
    std::vector<GenericRpcFuture::ptr> futures;
    std::vector<std::shared_ptr<fixbug::GetFriendsListResponse>> responses;

    const google::protobuf::MethodDescriptor *method =
        fixbug::FiendServiceRpc::descriptor()->FindMethodByName("GetFriendsList");

    // 发起多个并发调用
    for (int i = 0; i < NUM_CALLS; ++i)
    {
        fixbug::GetFriendsListRequest request;
        request.set_userid(4000 + i);

        auto response = std::make_shared<fixbug::GetFriendsListResponse>();
        responses.push_back(response);

        auto future = channel->callMethodAsync(method, nullptr, &request, response.get(), 5000);
        futures.push_back(future);

        std::cout << "发起第 " << (i + 1) << " 个异步调用\n";
    }

    std::cout << "所有调用已发起，等待响应...\n";

    // 等待所有调用完成
    for (int i = 0; i < NUM_CALLS; ++i)
    {
        RpcResult result = futures[i]->wait();
        if (result.isSuccess())
        {
            std::cout << "调用 " << (i + 1) << " 成功，好友数: " << responses[i]->friends_size() << "\n";
        }
        else
        {
            std::cout << "调用 " << (i + 1) << " 失败: " << result.getErrorMsg() << "\n";
        }
    }
}

/**
 *演示使用Future设置回调
 */
void demoFutureWithCallback(AsyncMprpcChannel::ptr channel)
{
    std::cout << "\n========== 演示Future+回调模式 ==========\n";

    fixbug::GetFriendsListRequest request;
    request.set_userid(5000);

    auto response = std::make_shared<fixbug::GetFriendsListResponse>();

    const google::protobuf::MethodDescriptor *method =
        fixbug::FiendServiceRpc::descriptor()->FindMethodByName("GetFriendsList");

    auto future = channel->callMethodAsync(method, nullptr, &request, response.get());

    std::atomic<bool> callbackCalled{false};

    // 在Future上设置回调
    future->setCallback([&callbackCalled, response](const RpcResult &result, google::protobuf::Message *msg) {
        std::cout << "[Future回调] 调用完成，状态: ";
        switch (result.getStatus())
        {
        case RpcStatus::SUCCESS:
            std::cout << "成功\n";
            break;
        case RpcStatus::FAILED:
            std::cout << "失败 - " << result.getErrorMsg() << "\n";
            break;
        case RpcStatus::TIMEOUT:
            std::cout << "超时\n";
            break;
        case RpcStatus::CANCELLED:
            std::cout << "已取消\n";
            break;
        default:
            std::cout << "未知状态\n";
        }
        callbackCalled = true;
    });

    std::cout << "已设置回调，异步等待中...\n";

    // 可以继续做其他事情
    int count = 0;
    while (!callbackCalled && count < 50)
    {
        std::cout << "主线程执行其他任务... " << ++count << "\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (!callbackCalled)
    {
        std::cout << "等待超时，取消调用\n";
        future->cancel();
    }
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
    std::cout << "       异步RPC调用示例程序\n";
    std::cout << "================================================\n";
    std::cout << "连接服务器: " << ip << ":" << port << "\n";

    // 创建异步RPC通道
    auto channel = std::make_shared<AsyncMprpcChannel>(ip, port, true, 2, true);

    if (!channel->isConnected())
    {
        std::cout << "连接服务器失败！\n";
        return 1;
    }

    std::cout << "连接成功！\n";

    // 演示各种异步调用方式
    try
    {
        // 方式1：Future阻塞等待
        demoFutureWait(channel);

        // 方式2：Future超时等待
        demoFutureTimeout(channel);

        // 方式3：回调通知
        demoCallback(channel);

        // 演示并发调用
        demoConcurrentCalls(channel);

        // 演示Future+回调
        demoFutureWithCallback(channel);
    }
    catch (const std::exception &e)
    {
        std::cout << "异常: " << e.what() << "\n";
    }

    std::cout << "\n========== 示例程序结束 ==========\n";

    return 0;
}
