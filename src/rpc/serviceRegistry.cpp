#include "serviceRegistry.h"
#include <chrono>
#include <iostream>
#include <sstream>

ServiceRegistry::ServiceRegistry(const std::string& zkHosts)
    : m_zkHosts(zkHosts)
    , m_zkClient(std::make_shared<ZkClient>()) {
}

ServiceRegistry::~ServiceRegistry() {
  shutdown();
}

bool ServiceRegistry::init() {
  if (!m_zkClient->start(m_zkHosts)) {
    std::cerr << "[ServiceRegistry] Failed to connect to ZooKeeper: " 
              << m_zkHosts << std::endl;
    return false;
  }

  // 确保根路径存在
  if (!m_zkClient->exists(ROOT_PATH)) {
    std::string result = m_zkClient->createNode(ROOT_PATH, "", false, false);
    if (result.empty()) {
      std::cerr << "[ServiceRegistry] Failed to create root path: " 
                << ROOT_PATH << std::endl;
      return false;
    }
  }

  std::cout << "[ServiceRegistry] Initialized successfully" << std::endl;
  return true;
}

void ServiceRegistry::shutdown() {
  // 注销所有服务
  unregisterAll();

  // 断开ZK连接
  if (m_zkClient) {
    m_zkClient->stop();
  }

  std::cout << "[ServiceRegistry] Shutdown completed" << std::endl;
}

bool ServiceRegistry::registerService(const std::string& serviceName,
                                       const std::string& ip,
                                       int port,
                                       const ServiceMetadata& metadata) {
  if (serviceName.empty() || ip.empty() || port <= 0) {
    std::cerr << "[ServiceRegistry] Invalid parameters" << std::endl;
    return false;
  }

  if (!m_zkClient->isConnected()) {
    std::cerr << "[ServiceRegistry] Not connected to ZooKeeper" << std::endl;
    return false;
  }

  std::lock_guard<std::mutex> lock(m_mutex);

  // 检查是否已注册
  if (m_registeredServices.count(serviceName) > 0) {
    std::cout << "[ServiceRegistry] Service already registered: " 
              << serviceName << std::endl;
    return true;
  }

  // 确保服务路径存在
  if (!ensureServicePath(serviceName)) {
    return false;
  }

  // 构建实例节点路径: /rpc_services/ServiceName/ip:port
  std::string instancePath = getServicePath(serviceName) + "/" + ip + ":" + std::to_string(port);

  // 序列化元数据
  std::string data = serializeMetadata(metadata);

  // 创建临时节点（ephemeral），服务断开时ZK会自动删除
  std::string createdPath = m_zkClient->createNode(instancePath, data, true, false);
  if (createdPath.empty()) {
    std::cerr << "[ServiceRegistry] Failed to register service: " 
              << serviceName << std::endl;
    return false;
  }

  // 记录已注册的服务
  m_registeredServices[serviceName] = createdPath;

  std::cout << "[ServiceRegistry] Registered service: " << serviceName 
            << " at " << ip << ":" << port << std::endl;
  return true;
}

bool ServiceRegistry::registerServices(const std::vector<std::string>& serviceNames,
                                        const std::string& ip,
                                        int port,
                                        const ServiceMetadata& metadata) {
  bool allSuccess = true;
  for (const auto& serviceName : serviceNames) {
    if (!registerService(serviceName, ip, port, metadata)) {
      allSuccess = false;
    }
  }
  return allSuccess;
}

void ServiceRegistry::unregisterService(const std::string& serviceName) {
  std::lock_guard<std::mutex> lock(m_mutex);

  auto it = m_registeredServices.find(serviceName);
  if (it == m_registeredServices.end()) {
    return;
  }

  // 删除ZK节点
  if (m_zkClient->isConnected()) {
    m_zkClient->deleteNode(it->second);
  }

  m_registeredServices.erase(it);
  std::cout << "[ServiceRegistry] Unregistered service: " << serviceName << std::endl;
}

void ServiceRegistry::unregisterAll() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if (m_zkClient->isConnected()) {
    for (const auto& pair : m_registeredServices) {
      m_zkClient->deleteNode(pair.second);
      std::cout << "[ServiceRegistry] Unregistered service: " << pair.first << std::endl;
    }
  }

  m_registeredServices.clear();
}

bool ServiceRegistry::isRegistered(const std::string& serviceName) const {
  std::lock_guard<std::mutex> lock(m_mutex);
  return m_registeredServices.count(serviceName) > 0;
}

std::string ServiceRegistry::getServicePath(const std::string& serviceName) {
  return std::string(ROOT_PATH) + "/" + serviceName;
}

std::string ServiceRegistry::serializeMetadata(const ServiceMetadata& metadata) const {
  // 简单的key=value格式序列化
  std::ostringstream oss;
  oss << "version=" << metadata.version << "\n";
  oss << "weight=" << metadata.weight << "\n";
  oss << "protocol=" << metadata.protocol << "\n";
  oss << "group=" << metadata.group << "\n";
  oss << "registerTime=" << std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  return oss.str();
}

bool ServiceRegistry::ensureServicePath(const std::string& serviceName) {
  std::string servicePath = getServicePath(serviceName);
  
  if (!m_zkClient->exists(servicePath)) {
    std::string result = m_zkClient->createNode(servicePath, "", false, false);
    if (result.empty()) {
      std::cerr << "[ServiceRegistry] Failed to create service path: " 
                << servicePath << std::endl;
      return false;
    }
  }
  
  return true;
}
