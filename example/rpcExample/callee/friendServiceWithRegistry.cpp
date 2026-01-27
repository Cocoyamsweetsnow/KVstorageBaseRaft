// 示例：带服务注册的RPC服务端
// 服务启动时自动注册到ZooKeeper，客户端可通过服务发现找到此服务

#include "rpcExample/friend.pb.h"
#include <iostream>
#include <string>
#include <vector>
#include <csignal>
#include <atomic>

#include "rpcprovider.h"

// 全局变量用于优雅退出
std::atomic<bool> g_running(true);

void signalHandler(int signum) {
    std::cout << "\n收到信号 " << signum << ", 正在退出..." << std::endl;
    g_running = false;
}

class FriendService : public fixbug::FiendServiceRpc {
 public:
    std::vector<std::string> GetFriendsList(uint32_t userid) {
        std::cout << "本地执行 GetFriendsList 服务! userid:" << userid << std::endl;
        std::vector<std::string> vec;
        vec.push_back("张三");
        vec.push_back("李四");
        vec.push_back("王五");
        return vec;
    }

    // 重写基类方法
    void GetFriendsList(::google::protobuf::RpcController *controller, 
                        const ::fixbug::GetFriendsListRequest *request,
                        ::fixbug::GetFriendsListResponse *response, 
                        ::google::protobuf::Closure *done) override {
        uint32_t userid = request->userid();
        std::vector<std::string> friendsList = GetFriendsList(userid);
        
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        for (std::string &name : friendsList) {
            std::string *p = response->add_friends();
            *p = name;
        }
        done->Run();
    }
};

int main(int argc, char **argv) {
    // 注册信号处理
    //signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // 解析命令行参数
    short port = 7788;
    int nodeIndex = 1;
    std::string zkAddress = "127.0.0.1:2181";  // 默认ZK地址

    if (argc >= 2) {
        port = static_cast<short>(std::stoi(argv[1]));
    }
    if (argc >= 3) {
        nodeIndex = std::stoi(argv[2]);
    }
    if (argc >= 4) {
        zkAddress = argv[3];
    }

    std::cout << "===== RPC服务端（带服务注册） =====" << std::endl;
    std::cout << "端口: " << port << std::endl;
    std::cout << "节点索引: " << nodeIndex << std::endl;
    std::cout << "ZooKeeper地址: " << zkAddress << std::endl;
    std::cout << "=================================" << std::endl;

    // 创建RPC Provider
    RpcProvider provider;

    // 注册服务
    provider.NotifyService(new FriendService());

    // 配置ZooKeeper地址，启用服务注册功能
    // 服务启动后会自动注册到ZK的 /rpc_services/FiendServiceRpc/ip:port 路径
    provider.setZkAddress(zkAddress);

    std::cout << "正在启动服务并注册到ZooKeeper..." << std::endl;

    // 启动RPC服务节点
    // Run()会：
    // 1. 启动TCP服务器监听指定端口
    // 2. 将所有注册的服务自动注册到ZooKeeper
    // 3. 进入事件循环，等待RPC调用
    provider.Run(nodeIndex, port);

    return 0;
}
