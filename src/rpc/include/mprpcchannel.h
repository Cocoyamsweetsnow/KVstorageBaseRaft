#ifndef MPRPCCHANNEL_H
#define MPRPCCHANNEL_H

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <algorithm>  
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <random>  
#include <string>
#include <unordered_map>
#include <vector>

class ServiceDiscovery;

// 真正负责发送和接受的前后处理工作
// 如消息的组织方式，向哪个节点发送等等
// 支持两种模式：
// 1. 直接指定IP和端口
// 2. 通过服务发现自动获取服务地址
class MprpcChannel : public google::protobuf::RpcChannel {
 public:
  // 构造函数：直接指定IP和端口
  MprpcChannel(std::string ip, short port, bool connectNow);

  // 构造函数：通过服务发现获取服务地址
  // serviceName: 服务名称（如 "FriendService"）
  // discovery: 服务发现组件
  // hashKey: 用于一致性哈希的key，相同key会路由到相同实例
  MprpcChannel(const std::string& serviceName, 
               std::shared_ptr<ServiceDiscovery> discovery,
               const std::string& hashKey = "");

  ~MprpcChannel();

  // 所有通过stub代理对象调用的rpc方法，都走到这里了，统一做rpc方法调用的数据数据序列化和网络发送
  void CallMethod(const google::protobuf::MethodDescriptor *method, 
                  google::protobuf::RpcController *controller,
                  const google::protobuf::Message *request, 
                  google::protobuf::Message *response,
                  google::protobuf::Closure *done) override;

  // 设置一致性哈希的key
  void setHashKey(const std::string& hashKey);

  // 检查是否使用服务发现模式
  bool isServiceDiscoveryMode() const;

 private:
  int m_clientFd;
  std::string m_ip;    // 服务IP
  uint16_t m_port;     // 服务端口
  
  // 服务发现相关
  std::string m_serviceName;                       // 服务名称
  std::shared_ptr<ServiceDiscovery> m_discovery;   // 服务发现组件
  std::string m_hashKey;                           // 一致性哈希key
  bool m_useDiscovery;                             // 是否使用服务发现
  
  // 建立新连接
  bool newConnect(const char *ip, uint16_t port, std::string *errMsg);

  // 通过服务发现获取服务地址
  bool resolveServiceAddress();
};

#endif
