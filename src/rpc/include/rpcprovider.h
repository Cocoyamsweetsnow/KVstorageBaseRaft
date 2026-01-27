#pragma once
#include <google/protobuf/descriptor.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/TcpServer.h>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "google/protobuf/service.h"

class ServiceRegistry;

// 框架提供的专门发布rpc服务的网络对象类
// 支持服务注册到ZooKeeper，实现服务发现功能
class RpcProvider {
 public:
  RpcProvider();
  ~RpcProvider();

  // 这里是框架提供给外部使用的，可以发布rpc方法的函数接口
  void NotifyService(google::protobuf::Service *service);

  // 设置ZooKeeper地址，启用服务注册功能
  // zkAddress: ZK地址，格式 "ip:port" 或 "ip1:port1,ip2:port2"
  void setZkAddress(const std::string& zkAddress);

  // 启动rpc服务节点，开始提供rpc远程网络调用服务
  // 如果设置了ZK地址，会自动将服务注册到ZooKeeper
  void Run(int nodeIndex, short port);

  // 获取所有已注册的服务名列表
  std::vector<std::string> getServiceNames() const;

  // 检查服务注册是否启用
  bool isRegistryEnabled() const;

 private:
  // 组合EventLoop
  muduo::net::EventLoop m_eventLoop;
  std::shared_ptr<muduo::net::TcpServer> m_muduo_server;

  // service服务类型信息
  struct ServiceInfo {
    google::protobuf::Service *m_service;                                                     // 保存服务对象
    std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> m_methodMap;  // 保存服务方法
  };
  // 存储注册成功的服务对象和其服务方法的所有信息
  std::unordered_map<std::string, ServiceInfo> m_serviceMap;

  // ZooKeeper地址
  std::string m_zkAddress;

  // 服务注册组件
  std::shared_ptr<ServiceRegistry> m_serviceRegistry;

  // 服务实例的IP和端口
  std::string m_localIp;
  int m_localPort = 0;

  // 初始化服务注册
  bool initServiceRegistry();

  // 注册所有服务到ZooKeeper
  void registerAllServices();

  // 新的socket连接回调
  void OnConnection(const muduo::net::TcpConnectionPtr &);
  // 已建立连接用户的读写事件回调
  void OnMessage(const muduo::net::TcpConnectionPtr &, muduo::net::Buffer *, muduo::Timestamp);
  // Closure的回调操作，用于序列化rpc的响应和网络发送
  void SendRpcResponse(const muduo::net::TcpConnectionPtr &, google::protobuf::Message *);
};
