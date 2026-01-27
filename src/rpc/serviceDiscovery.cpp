#include "serviceDiscovery.h"
#include <iostream>
#include <random>
#include <sstream>

ServiceDiscovery::ServiceDiscovery(const std::string& zkHosts, int virtualNodes)
    : m_zkHosts(zkHosts)
    , m_zkClient(std::make_shared<ZkClient>())
    , m_virtualNodes(virtualNodes) {
}

ServiceDiscovery::~ServiceDiscovery() {
  shutdown();
}

bool ServiceDiscovery::init() {
  if (!m_zkClient->start(m_zkHosts)) {
    std::cerr << "[ServiceDiscovery] Failed to connect to ZooKeeper: " 
              << m_zkHosts << std::endl;
    return false;
  }

  std::cout << "[ServiceDiscovery] Initialized successfully" << std::endl;
  return true;
}

void ServiceDiscovery::shutdown() {
  {
    std::unique_lock<std::shared_mutex> lock(m_cacheMutex);
    m_serviceCache.clear();
    m_consistentHashMap.clear();
    m_subscribedServices.clear();
  }

  if (m_zkClient) {
    m_zkClient->stop();
  }

  std::cout << "[ServiceDiscovery] Shutdown completed" << std::endl;
}

bool ServiceDiscovery::subscribe(const std::string& serviceName) {
  if (serviceName.empty()) {
    return false;
  }

  if (!m_zkClient->isConnected()) {
    std::cerr << "[ServiceDiscovery] Not connected to ZooKeeper" << std::endl;
    return false;
  }

  {
    std::shared_lock<std::shared_mutex> lock(m_cacheMutex);
    if (m_subscribedServices.count(serviceName) > 0) {
      return true;  // 已经订阅
    }
  }

  std::string servicePath = getServicePath(serviceName);

  // 检查服务路径是否存在
  if (!m_zkClient->exists(servicePath)) {
    std::cerr << "[ServiceDiscovery] Service path not found: " 
              << servicePath << std::endl;
    // 仍然订阅，等待服务注册
  }

  // 注册Watch，监听子节点变化
  m_zkClient->watchChildren(servicePath, [this, serviceName](const std::string& path) {
    onServiceChanged(serviceName);
  });

  // 获取当前服务实例
  auto instances = fetchServiceInstances(serviceName);

  {
    std::unique_lock<std::shared_mutex> lock(m_cacheMutex);
    m_subscribedServices[serviceName] = true;
    m_serviceCache[serviceName] = instances;
    updateConsistentHash(serviceName, instances);
  }

  std::cout << "[ServiceDiscovery] Subscribed to service: " << serviceName 
            << ", instances: " << instances.size() << std::endl;
  return true;
}

void ServiceDiscovery::unsubscribe(const std::string& serviceName) {
  std::unique_lock<std::shared_mutex> lock(m_cacheMutex);
  m_subscribedServices.erase(serviceName);
  m_serviceCache.erase(serviceName);
  m_consistentHashMap.erase(serviceName);
  std::cout << "[ServiceDiscovery] Unsubscribed from service: " << serviceName << std::endl;
}

std::vector<ServiceInstance> ServiceDiscovery::discoverService(const std::string& serviceName) {
  // 先检查缓存
  {
    std::shared_lock<std::shared_mutex> lock(m_cacheMutex);
    auto it = m_serviceCache.find(serviceName);
    if (it != m_serviceCache.end()) {
      return it->second;
    }
  }

  // 没有缓存，先订阅
  if (!subscribe(serviceName)) {
    return {};
  }

  // 再次获取缓存
  std::shared_lock<std::shared_mutex> lock(m_cacheMutex);
  auto it = m_serviceCache.find(serviceName);
  if (it != m_serviceCache.end()) {
    return it->second;
  }

  return {};
}

ServiceInstance ServiceDiscovery::selectInstance(const std::string& serviceName,
                                                  const std::string& hashKey) {
  // 先确保已订阅
  auto instances = discoverService(serviceName);
  if (instances.empty()) {
    std::cerr << "[ServiceDiscovery] No available instances for: " 
              << serviceName << std::endl;
    return ServiceInstance();
  }

  std::shared_lock<std::shared_mutex> lock(m_cacheMutex);

  if (hashKey.empty()) {
    // 如果没有hashKey，随机选择一个
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<size_t> dist(0, instances.size() - 1);
    return instances[dist(gen)];
  }

  // 使用一致性哈希选择
  auto hashIt = m_consistentHashMap.find(serviceName);
  if (hashIt == m_consistentHashMap.end() || hashIt->second->empty()) {
    // 回退到随机选择
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<size_t> dist(0, instances.size() - 1);
    return instances[dist(gen)];
  }

  std::string selectedAddress = hashIt->second->getNode(hashKey);
  
  // 在实例列表中查找
  for (const auto& instance : instances) {
    if (instance.address == selectedAddress) {
      return instance;
    }
  }

  // 找不到，返回第一个
  return instances.front();
}

size_t ServiceDiscovery::getInstanceCount(const std::string& serviceName) const {
  std::shared_lock<std::shared_mutex> lock(m_cacheMutex);
  auto it = m_serviceCache.find(serviceName);
  if (it != m_serviceCache.end()) {
    return it->second.size();
  }
  return 0;
}

void ServiceDiscovery::setServiceChangeCallback(ServiceChangeCallback callback) {
  std::lock_guard<std::mutex> lock(m_callbackMutex);
  m_changeCallback = std::move(callback);
}

bool ServiceDiscovery::isConnected() const {
  return m_zkClient && m_zkClient->isConnected();
}

std::vector<ServiceInstance> ServiceDiscovery::fetchServiceInstances(const std::string& serviceName) {
  std::vector<ServiceInstance> instances;

  std::string servicePath = getServicePath(serviceName);
  auto children = m_zkClient->getChildren(servicePath);

  for (const auto& child : children) {
    ServiceInstance instance = parseInstanceAddress(serviceName, child);
    if (instance.isValid()) {
      // 获取节点数据（元数据）
      std::string fullPath = servicePath + "/" + child;
      std::string data = m_zkClient->getNodeData(fullPath);
      if (!data.empty()) {
        parseMetadata(instance, data);
      }
      instances.push_back(instance);
    }
  }

  return instances;
}

ServiceInstance ServiceDiscovery::parseInstanceAddress(const std::string& serviceName,
                                                        const std::string& address) {
  ServiceInstance instance;
  instance.serviceName = serviceName;
  instance.address = address;

  // 解析 "ip:port" 格式
  size_t colonPos = address.rfind(':');
  if (colonPos != std::string::npos && colonPos > 0) {
    instance.ip = address.substr(0, colonPos);
    try {
      instance.port = std::stoi(address.substr(colonPos + 1));
    } catch (const std::exception&) {
      instance.port = 0;
    }
  }

  return instance;
}

void ServiceDiscovery::parseMetadata(ServiceInstance& instance, const std::string& data) {
  // 解析 key=value 格式的元数据
  std::istringstream iss(data);
  std::string line;

  while (std::getline(iss, line)) {
    size_t eqPos = line.find('=');
    if (eqPos == std::string::npos) {
      continue;
    }

    std::string key = line.substr(0, eqPos);
    std::string value = line.substr(eqPos + 1);

    if (key == "weight") {
      try {
        instance.weight = std::stoi(value);
      } catch (const std::exception&) {
        instance.weight = 100;
      }
    } else if (key == "version") {
      instance.version = value;
    } else if (key == "group") {
      instance.group = value;
    }
  }
}

void ServiceDiscovery::onServiceChanged(const std::string& serviceName) {
  std::cout << "[ServiceDiscovery] Service changed: " << serviceName << std::endl;

  // 重新获取服务实例
  auto instances = fetchServiceInstances(serviceName);

  ServiceChangeCallback callback;
  {
    std::unique_lock<std::shared_mutex> lock(m_cacheMutex);
    m_serviceCache[serviceName] = instances;
    updateConsistentHash(serviceName, instances);
  }

  {
    std::lock_guard<std::mutex> lock(m_callbackMutex);
    callback = m_changeCallback;
  }

  // 调用回调通知
  if (callback) {
    callback(serviceName, instances);
  }

  std::cout << "[ServiceDiscovery] Updated instances for " << serviceName 
            << ": " << instances.size() << std::endl;
}

void ServiceDiscovery::updateConsistentHash(const std::string& serviceName,
                                            const std::vector<ServiceInstance>& instances) {
  // 需要在持有锁的情况下调用
  auto& hashRing = m_consistentHashMap[serviceName];
  if (!hashRing) {
    hashRing = std::make_shared<ConsistentHash>(m_virtualNodes);
  }

  // 收集所有实例地址
  std::vector<std::string> addresses;
  for (const auto& instance : instances) {
    if (instance.isValid()) {
      addresses.push_back(instance.address);
    }
  }

  // 更新哈希环
  hashRing->updateNodes(addresses);
}

std::string ServiceDiscovery::getServicePath(const std::string& serviceName) const {
  return std::string(ROOT_PATH) + "/" + serviceName;
}
