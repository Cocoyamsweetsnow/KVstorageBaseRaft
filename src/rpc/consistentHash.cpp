#include "consistentHash.h"
#include <algorithm>

ConsistentHash::ConsistentHash(int virtualNodes)
    : m_virtualNodes(virtualNodes) {
  if (m_virtualNodes < 1) {
    m_virtualNodes = 150;
  }
}

void ConsistentHash::addNode(const std::string& node) {
  if (node.empty()) {
    return;
  }

  std::lock_guard<std::mutex> lock(m_mutex);

  // 检查是否已存在
  if (m_realNodes.count(node) > 0) {
    return;
  }

  // 添加到真实节点集合
  m_realNodes.insert(node);

  // 为该节点创建虚拟节点
  for (int i = 0; i < m_virtualNodes; ++i) {
    std::string virtualKey = makeVirtualNodeKey(node, i);
    uint32_t hash = murmurHash2(virtualKey);
    m_ring[hash] = node;
  }
}

void ConsistentHash::removeNode(const std::string& node) {
  if (node.empty()) {
    return;
  }

  std::lock_guard<std::mutex> lock(m_mutex);

  // 检查是否存在
  if (m_realNodes.count(node) == 0) {
    return;
  }

  // 从真实节点集合移除
  m_realNodes.erase(node);

  // 移除该节点的所有虚拟节点
  for (int i = 0; i < m_virtualNodes; ++i) {
    std::string virtualKey = makeVirtualNodeKey(node, i);
    uint32_t hash = murmurHash2(virtualKey);
    m_ring.erase(hash);
  }
}

std::string ConsistentHash::getNode(const std::string& key) {
  std::lock_guard<std::mutex> lock(m_mutex);

  if (m_ring.empty()) {
    return "";
  }

  // 计算key的哈希值
  uint32_t hash = murmurHash2(key);

  // 在哈希环上顺时针查找第一个节点
  auto it = m_ring.lower_bound(hash);
  if (it == m_ring.end()) {
    // 如果没找到，回到环的起点
    it = m_ring.begin();
  }

  return it->second;
}

void ConsistentHash::updateNodes(const std::vector<std::string>& nodes) {
  std::lock_guard<std::mutex> lock(m_mutex);

  // 清空现有节点
  m_ring.clear();
  m_realNodes.clear();

  // 添加新节点（不加锁，因为已经持有锁）
  for (const auto& node : nodes) {
    if (node.empty()) {
      continue;
    }

    m_realNodes.insert(node);

    for (int i = 0; i < m_virtualNodes; ++i) {
      std::string virtualKey = makeVirtualNodeKey(node, i);
      uint32_t hash = murmurHash2(virtualKey);
      m_ring[hash] = node;
    }
  }
}

std::vector<std::string> ConsistentHash::getAllNodes() const {
  std::lock_guard<std::mutex> lock(m_mutex);
  return std::vector<std::string>(m_realNodes.begin(), m_realNodes.end());
}

size_t ConsistentHash::getNodeCount() const {
  std::lock_guard<std::mutex> lock(m_mutex);
  return m_realNodes.size();
}

bool ConsistentHash::empty() const {
  std::lock_guard<std::mutex> lock(m_mutex);
  return m_realNodes.empty();
}

void ConsistentHash::clear() {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_ring.clear();
  m_realNodes.clear();
}

uint32_t ConsistentHash::murmurHash2(const std::string& key) const {
  // MurmurHash2算法实现
  // 参考：https://github.com/aappleby/smhasher
  const uint32_t m = 0x5bd1e995;
  const int r = 24;
  const uint32_t seed = 0xEE6B27EB;

  const unsigned char* data = reinterpret_cast<const unsigned char*>(key.c_str());
  int len = static_cast<int>(key.length());

  uint32_t h = seed ^ len;

  while (len >= 4) {
    uint32_t k = *reinterpret_cast<const uint32_t*>(data);

    k *= m;
    k ^= k >> r;
    k *= m;

    h *= m;
    h ^= k;

    data += 4;
    len -= 4;
  }

  // 处理剩余字节
  switch (len) {
    case 3:
      h ^= data[2] << 16;
      [[fallthrough]];
    case 2:
      h ^= data[1] << 8;
      [[fallthrough]];
    case 1:
      h ^= data[0];
      h *= m;
  }

  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;

  return h;
}

std::string ConsistentHash::makeVirtualNodeKey(const std::string& node, int index) const {
  // 生成虚拟节点的key，格式: node#index
  return node + "#" + std::to_string(index);
}
