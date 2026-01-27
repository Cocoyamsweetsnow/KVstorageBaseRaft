#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "zkClient.h"

// 服务实例元数据
struct ServiceMetadata {
  std::string version;          // 服务版本
  int weight = 100;             // 权重（用于加权负载均衡）
  std::string protocol;         // 协议类型
  std::string group;            // 服务分组
  int64_t registerTime = 0;     // 注册时间戳
};

// 服务注册类
// 负责将本地服务注册到ZooKeeper，支持自动心跳和断线重连
class ServiceRegistry {
 public:
  // zkHosts: ZooKeeper地址，格式 "ip:port" 或 "ip1:port1,ip2:port2"
  explicit ServiceRegistry(const std::string& zkHosts);
  ~ServiceRegistry();

  // 禁止拷贝
  ServiceRegistry(const ServiceRegistry&) = delete;
  ServiceRegistry& operator=(const ServiceRegistry&) = delete;

  // 初始化，连接ZooKeeper
  bool init();

  // 关闭，断开连接并注销所有服务
  void shutdown();

  // 注册服务
  // serviceName: 服务名称（如 "FriendService"）
  // ip: 服务IP地址
  // port: 服务端口
  // metadata: 可选的服务元数据
  // 返回是否注册成功
  bool registerService(const std::string& serviceName, 
                       const std::string& ip, 
                       int port,
                       const ServiceMetadata& metadata = ServiceMetadata());

  // 注册多个服务（同一个服务实例提供多个服务）
  bool registerServices(const std::vector<std::string>& serviceNames,
                        const std::string& ip,
                        int port,
                        const ServiceMetadata& metadata = ServiceMetadata());

  // 注销服务
  // serviceName: 服务名称
  void unregisterService(const std::string& serviceName);

  // 注销所有服务
  void unregisterAll();

  // 检查服务是否已注册
  bool isRegistered(const std::string& serviceName) const;

  // 获取ZooKeeper根路径
  static std::string getRootPath() { return ROOT_PATH; }

  // 获取服务路径
  static std::string getServicePath(const std::string& serviceName);

 private:
  // 序列化元数据为字符串
  std::string serializeMetadata(const ServiceMetadata& metadata) const;

  // 确保服务父路径存在
  bool ensureServicePath(const std::string& serviceName);

 private:
  static constexpr const char* ROOT_PATH = "/rpc_services";

  std::string m_zkHosts;
  std::shared_ptr<ZkClient> m_zkClient;
  mutable std::mutex m_mutex;

  // 已注册的服务: serviceName -> zkNodePath
  std::unordered_map<std::string, std::string> m_registeredServices;
};
