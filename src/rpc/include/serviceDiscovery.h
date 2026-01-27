#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "consistentHash.h"
#include "zkClient.h"

// 服务实例信息
struct ServiceInstance {
  std::string ip;
  int port = 0;
  std::string address;      // ip:port 格式
  std::string serviceName;
  int weight = 100;
  std::string version;
  std::string group;

  bool isValid() const {
    return !ip.empty() && port > 0;
  }
};

// 服务变化通知回调
using ServiceChangeCallback = std::function<void(const std::string& serviceName,
                                                  const std::vector<ServiceInstance>& instances)>;

// 服务发现类
// 负责从ZooKeeper发现服务实例，支持本地缓存和实时更新
class ServiceDiscovery {
 public:
  // zkHosts: ZooKeeper地址
  // virtualNodes: 一致性哈希的虚拟节点数
  explicit ServiceDiscovery(const std::string& zkHosts, int virtualNodes = 150);
  ~ServiceDiscovery();

  // 禁止拷贝
  ServiceDiscovery(const ServiceDiscovery&) = delete;
  ServiceDiscovery& operator=(const ServiceDiscovery&) = delete;

  // 初始化，连接ZooKeeper
  bool init();

  // 关闭
  void shutdown();

  // 订阅服务
  // 订阅后会立即获取服务列表，并监听后续变化
  bool subscribe(const std::string& serviceName);

  // 取消订阅
  void unsubscribe(const std::string& serviceName);

  // 发现服务，返回所有实例
  // 如果还没订阅，会先订阅
  std::vector<ServiceInstance> discoverService(const std::string& serviceName);

  // 使用一致性哈希选择一个服务实例
  // serviceName: 服务名
  // hashKey: 用于哈希的key（如用户ID），相同key会路由到相同实例
  // 如果hashKey为空，则随机选择
  ServiceInstance selectInstance(const std::string& serviceName, 
                                  const std::string& hashKey = "");

  // 获取服务实例数量
  size_t getInstanceCount(const std::string& serviceName) const;

  // 设置服务变化回调
  void setServiceChangeCallback(ServiceChangeCallback callback);

  // 检查是否已连接
  bool isConnected() const;

 private:
  // 从ZK获取服务实例列表
  std::vector<ServiceInstance> fetchServiceInstances(const std::string& serviceName);

  // 解析实例地址 "ip:port" 格式
  ServiceInstance parseInstanceAddress(const std::string& serviceName, 
                                        const std::string& address);

  // 解析元数据
  void parseMetadata(ServiceInstance& instance, const std::string& data);

  // 处理服务变化
  void onServiceChanged(const std::string& serviceName);

  // 更新服务的一致性哈希环
  void updateConsistentHash(const std::string& serviceName,
                            const std::vector<ServiceInstance>& instances);

  // 获取服务路径
  std::string getServicePath(const std::string& serviceName) const;

 private:
  static constexpr const char* ROOT_PATH = "/rpc_services";

  std::string m_zkHosts;
  std::shared_ptr<ZkClient> m_zkClient;
  int m_virtualNodes;

  // 读写锁保护缓存
  mutable std::shared_mutex m_cacheMutex;

  // 服务缓存: serviceName -> instances
  std::unordered_map<std::string, std::vector<ServiceInstance>> m_serviceCache;

  // 每个服务的一致性哈希环
  std::unordered_map<std::string, std::shared_ptr<ConsistentHash>> m_consistentHashMap;

  // 已订阅的服务
  std::unordered_map<std::string, bool> m_subscribedServices;

  // 服务变化回调
  ServiceChangeCallback m_changeCallback;
  std::mutex m_callbackMutex;
};
