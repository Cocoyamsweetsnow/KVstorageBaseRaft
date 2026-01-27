#pragma once

#include <zookeeper/zookeeper.h>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <condition_variable>
#include <atomic>

// ZooKeeper客户端封装类
// 提供连接管理、节点操作、Watch机制等功能
class ZkClient {
 public:
  using WatchCallback = std::function<void(const std::string& path)>;

  ZkClient();
  ~ZkClient();

  // 禁止拷贝
  ZkClient(const ZkClient&) = delete;
  ZkClient& operator=(const ZkClient&) = delete;

  // 连接ZooKeeper服务器
  // hosts: ZK地址，格式 "ip:port" 或 "ip1:port1,ip2:port2"
  // timeout: 会话超时时间（毫秒）
  // 返回是否连接成功
  bool start(const std::string& hosts, int timeout = 30000);

  // 断开连接
  void stop();

  // 检查是否已连接
  bool isConnected() const;

  // 创建节点
  // path: 节点路径
  // data: 节点数据
  // ephemeral: 是否为临时节点（服务断开时自动删除）
  // sequential: 是否为顺序节点
  // 返回创建的节点路径（顺序节点会返回带序号的路径）
  std::string createNode(const std::string& path, const std::string& data, 
                         bool ephemeral = false, bool sequential = false);

  // 删除节点
  bool deleteNode(const std::string& path);

  // 检查节点是否存在
  bool exists(const std::string& path);

  // 获取节点数据
  std::string getNodeData(const std::string& path);

  // 设置节点数据
  bool setNodeData(const std::string& path, const std::string& data);

  // 获取子节点列表
  std::vector<std::string> getChildren(const std::string& path);

  // 监听子节点变化
  // 当子节点发生变化时，会调用callback
  // 注意：Watch是一次性的，触发后需要重新注册
  void watchChildren(const std::string& path, WatchCallback callback);

  // 监听节点数据变化
  void watchNode(const std::string& path, WatchCallback callback);

  // 递归创建路径（确保父节点存在）
  bool createPathRecursive(const std::string& path);

 private:
  // ZK连接状态回调（静态函数，供ZK库调用）
  static void connectionWatcher(zhandle_t* zh, int type, int state, 
                                const char* path, void* watcherCtx);

  // 子节点Watch回调
  static void childrenWatcher(zhandle_t* zh, int type, int state,
                              const char* path, void* watcherCtx);

  // 节点数据Watch回调
  static void nodeWatcher(zhandle_t* zh, int type, int state,
                          const char* path, void* watcherCtx);

  // 处理Watch事件
  void handleChildrenWatch(const std::string& path);
  void handleNodeWatch(const std::string& path);

 private:
  zhandle_t* m_zhandle;                          // ZK句柄
  std::string m_hosts;                           // ZK地址
  int m_timeout;                                 // 会话超时
  std::atomic<bool> m_connected;                 // 连接状态
  std::mutex m_mutex;                            // 保护回调映射
  std::condition_variable m_connectedCv;         // 连接完成条件变量
  std::mutex m_connMutex;                        // 连接互斥锁

  // Watch回调映射
  std::unordered_map<std::string, WatchCallback> m_childrenWatchers;
  std::unordered_map<std::string, WatchCallback> m_nodeWatchers;
};
