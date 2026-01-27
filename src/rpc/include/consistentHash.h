#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

// 一致性哈希负载均衡器
// 使用虚拟节点技术实现负载均衡，当节点变化时只影响相邻节点的请求分布
class ConsistentHash {
 public:
  // virtualNodes: 每个真实节点对应的虚拟节点数量
  // 虚拟节点越多，负载分布越均匀，但内存占用越大
  explicit ConsistentHash(int virtualNodes = 150);
  ~ConsistentHash() = default;

  // 禁止拷贝
  ConsistentHash(const ConsistentHash&) = delete;
  ConsistentHash& operator=(const ConsistentHash&) = delete;

  // 添加节点
  // node: 节点标识，通常为 "ip:port" 格式
  void addNode(const std::string& node);

  // 移除节点
  void removeNode(const std::string& node);

  // 根据key获取对应的节点
  // key: 用于哈希的键，如用户ID、请求ID等
  // 返回选中的节点标识，如果没有节点返回空字符串
  std::string getNode(const std::string& key);

  // 批量更新节点列表
  // 会先清空现有节点，然后添加新节点
  void updateNodes(const std::vector<std::string>& nodes);

  // 获取当前所有真实节点
  std::vector<std::string> getAllNodes() const;

  // 获取节点数量
  size_t getNodeCount() const;

  // 检查是否为空
  bool empty() const;

  // 清空所有节点
  void clear();

 private:
  // MurmurHash2算法，用于生成哈希值
  // 比普通的字符串哈希更均匀
  uint32_t murmurHash2(const std::string& key) const;

  // 生成虚拟节点的key
  std::string makeVirtualNodeKey(const std::string& node, int index) const;

 private:
  int m_virtualNodes;                        // 每个真实节点的虚拟节点数
  std::map<uint32_t, std::string> m_ring;    // 哈希环：哈希值 -> 节点标识
  std::set<std::string> m_realNodes;         // 真实节点集合
  mutable std::mutex m_mutex;                // 线程安全锁
};
