// 示例：带服务发现的RPC客户端
// 客户端通过ZooKeeper自动发现服务，无需手动指定服务地址
// 支持一致性哈希负载均衡

#include "rpcExample/friend.pb.h"
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <chrono>

#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "serviceDiscovery.h"

void printUsage(const char* program) {
    std::cout << "用法: " << program << " [zkAddress] [hashKey]" << std::endl;
    std::cout << "  zkAddress: ZooKeeper地址 (默认: 127.0.0.1:2181)" << std::endl;
    std::cout << "  hashKey: 一致性哈希key (默认: 空, 随机选择实例)" << std::endl;
    std::cout << std::endl;
    std::cout << "示例:" << std::endl;
    std::cout << "  " << program << "                           # 使用默认ZK地址，随机选择实例" << std::endl;
    std::cout << "  " << program << " 192.168.1.100:2181        # 使用指定ZK地址" << std::endl;
    std::cout << "  " << program << " 127.0.0.1:2181 user123    # 使用一致性哈希，相同key路由到相同实例" << std::endl;
}

int main(int argc, char **argv) {
    // 解析命令行参数
    std::string zkAddress = "127.0.0.1:2181";
    std::string hashKey = "";  // 空表示随机选择

    if (argc >= 2) {
        if (std::string(argv[1]) == "-h" || std::string(argv[1]) == "--help") {
            printUsage(argv[0]);
            return 0;
        }
        zkAddress = argv[1];
    }
    if (argc >= 3) {
        hashKey = argv[2];
    }

    std::cout << "===== RPC客户端（带服务发现） =====" << std::endl;
    std::cout << "ZooKeeper地址: " << zkAddress << std::endl;
    std::cout << "一致性哈希Key: " << (hashKey.empty() ? "(随机选择)" : hashKey) << std::endl;
    std::cout << "=================================" << std::endl;

    // 创建服务发现组件
    auto discovery = std::make_shared<ServiceDiscovery>(zkAddress);
    if (!discovery->init()) {
        std::cerr << "初始化服务发现失败！" << std::endl;
        return 1;
    }

    // 订阅服务（可选，首次调用时会自动订阅）
    std::string serviceName = "FiendServiceRpc";
    discovery->subscribe(serviceName);

    std::cout << "已订阅服务: " << serviceName << std::endl;
    std::cout << "当前可用实例数: " << discovery->getInstanceCount(serviceName) << std::endl;

    // 设置服务变化回调（可选）
    discovery->setServiceChangeCallback([](const std::string& service, 
                                           const std::vector<ServiceInstance>& instances) {
        std::cout << "\n[服务变化通知] 服务: " << service 
                  << ", 实例数: " << instances.size() << std::endl;
        for (const auto& inst : instances) {
            std::cout << "  - " << inst.address << std::endl;
        }
    });

    // 方式1：使用服务发现创建Channel
    // 每次RPC调用时，Channel会自动从服务发现组件获取服务地址
    MprpcChannel channel(serviceName, discovery, hashKey);

    // 创建Stub
    fixbug::FiendServiceRpc_Stub stub(&channel);

    // 多次调用，演示一致性哈希效果
    for (int i = 0; i < 3; ++i) {
        std::cout << "\n--- 第 " << (i + 1) << " 次调用 ---" << std::endl;

        // 准备请求
        fixbug::GetFriendsListRequest request;
        request.set_userid(1000 + i);

        // 准备响应
        fixbug::GetFriendsListResponse response;

        // 准备控制器
        MprpcController controller;

        // 发起RPC调用
        // 服务发现会自动选择一个服务实例
        // 如果设置了hashKey，相同的key会路由到相同的实例
        stub.GetFriendsList(&controller, &request, &response, nullptr);

        // 检查调用结果
        if (controller.Failed()) {
            std::cerr << "RPC调用失败: " << controller.ErrorText() << std::endl;
        } else {
            if (response.result().errcode() == 0) {
                std::cout << "RPC调用成功!" << std::endl;
                std::cout << "好友列表:" << std::endl;
                for (int j = 0; j < response.friends_size(); ++j) {
                    std::cout << "  - " << response.friends(j) << std::endl;
                }
            } else {
                std::cout << "RPC返回错误: " << response.result().errmsg() << std::endl;
            }
        }

        // 短暂等待
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    std::cout << "\n演示完成！" << std::endl;

    // 关闭服务发现
    discovery->shutdown();

    return 0;
}
