#include "zkClient.h"
#include <iostream>
#include <chrono>

ZkClient::ZkClient() 
    : m_zhandle(nullptr)
    , m_timeout(30000)
    , m_connected(false) {
}

ZkClient::~ZkClient() {
  stop();
}

bool ZkClient::start(const std::string& hosts, int timeout) {
  if (m_zhandle != nullptr) {
    std::cout << "[ZkClient] Already connected" << std::endl;
    return true;
  }

  m_hosts = hosts;
  m_timeout = timeout;
  m_connected = false;

  // 初始化ZK连接，使用异步方式
  // connectionWatcher会在连接状态变化时被调用
  m_zhandle = zookeeper_init(hosts.c_str(), connectionWatcher, timeout, 
                             nullptr, this, 0);
  
  if (m_zhandle == nullptr) {
    std::cerr << "[ZkClient] zookeeper_init failed" << std::endl;
    return false;
  }

  // 等待连接完成
  std::unique_lock<std::mutex> lock(m_connMutex);
  bool success = m_connectedCv.wait_for(lock, std::chrono::milliseconds(timeout), 
                                        [this] { return m_connected.load(); });

  if (!success) {
    std::cerr << "[ZkClient] Connection timeout" << std::endl;
    zookeeper_close(m_zhandle);
    m_zhandle = nullptr;
    return false;
  }

  std::cout << "[ZkClient] Connected to ZooKeeper: " << hosts << std::endl;
  return true;
}

void ZkClient::stop() {
  if (m_zhandle != nullptr) {
    zookeeper_close(m_zhandle);
    m_zhandle = nullptr;
    m_connected = false;
    std::cout << "[ZkClient] Disconnected from ZooKeeper" << std::endl;
  }
}

bool ZkClient::isConnected() const {
  return m_connected.load() && m_zhandle != nullptr;
}

void ZkClient::connectionWatcher(zhandle_t* zh, int type, int state, 
                                 const char* path, void* watcherCtx) {
  ZkClient* client = static_cast<ZkClient*>(watcherCtx);
  if (client == nullptr) {
    return;
  }

  if (type == ZOO_SESSION_EVENT) {
    if (state == ZOO_CONNECTED_STATE) {
      // 连接成功
      client->m_connected = true;
      std::unique_lock<std::mutex> lock(client->m_connMutex);
      client->m_connectedCv.notify_all();
      std::cout << "[ZkClient] Session connected" << std::endl;
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
      // 会话过期
      client->m_connected = false;
      std::cerr << "[ZkClient] Session expired" << std::endl;
    } else if (state == ZOO_CONNECTING_STATE) {
      // 正在连接
      std::cout << "[ZkClient] Connecting..." << std::endl;
    }
  }
}

std::string ZkClient::createNode(const std::string& path, const std::string& data,
                                 bool ephemeral, bool sequential) {
  if (!isConnected()) {
    std::cerr << "[ZkClient] Not connected" << std::endl;
    return "";
  }

  int flags = 0;
  if (ephemeral) {
    flags |= ZOO_EPHEMERAL;
  }
  if (sequential) {
    flags |= ZOO_SEQUENCE;
  }

  char pathBuffer[256] = {0};
  int ret = zoo_create(m_zhandle, path.c_str(), data.c_str(), data.length(),
                       &ZOO_OPEN_ACL_UNSAFE, flags, pathBuffer, sizeof(pathBuffer));

  if (ret == ZOK) {
    std::cout << "[ZkClient] Created node: " << pathBuffer << std::endl;
    return std::string(pathBuffer);
  } else if (ret == ZNODEEXISTS) {
    std::cout << "[ZkClient] Node already exists: " << path << std::endl;
    return path;
  } else {
    std::cerr << "[ZkClient] Failed to create node: " << path 
              << ", error: " << zerror(ret) << std::endl;
    return "";
  }
}

bool ZkClient::deleteNode(const std::string& path) {
  if (!isConnected()) {
    return false;
  }

  int ret = zoo_delete(m_zhandle, path.c_str(), -1);
  if (ret == ZOK || ret == ZNONODE) {
    return true;
  }

  std::cerr << "[ZkClient] Failed to delete node: " << path 
            << ", error: " << zerror(ret) << std::endl;
  return false;
}

bool ZkClient::exists(const std::string& path) {
  if (!isConnected()) {
    return false;
  }

  struct Stat stat;
  int ret = zoo_exists(m_zhandle, path.c_str(), 0, &stat);
  return ret == ZOK;
}

std::string ZkClient::getNodeData(const std::string& path) {
  if (!isConnected()) {
    return "";
  }

  char buffer[4096] = {0};
  int bufferLen = sizeof(buffer);
  struct Stat stat;

  int ret = zoo_get(m_zhandle, path.c_str(), 0, buffer, &bufferLen, &stat);
  if (ret == ZOK) {
    return std::string(buffer, bufferLen);
  }

  std::cerr << "[ZkClient] Failed to get node data: " << path 
            << ", error: " << zerror(ret) << std::endl;
  return "";
}

bool ZkClient::setNodeData(const std::string& path, const std::string& data) {
  if (!isConnected()) {
    return false;
  }

  int ret = zoo_set(m_zhandle, path.c_str(), data.c_str(), data.length(), -1);
  if (ret == ZOK) {
    return true;
  }

  std::cerr << "[ZkClient] Failed to set node data: " << path 
            << ", error: " << zerror(ret) << std::endl;
  return false;
}

std::vector<std::string> ZkClient::getChildren(const std::string& path) {
  std::vector<std::string> result;
  if (!isConnected()) {
    return result;
  }

  struct String_vector children;
  int ret = zoo_get_children(m_zhandle, path.c_str(), 0, &children);
  
  if (ret == ZOK) {
    for (int i = 0; i < children.count; ++i) {
      result.push_back(children.data[i]);
    }
    deallocate_String_vector(&children);
  } else if (ret != ZNONODE) {
    std::cerr << "[ZkClient] Failed to get children: " << path 
              << ", error: " << zerror(ret) << std::endl;
  }

  return result;
}

void ZkClient::watchChildren(const std::string& path, WatchCallback callback) {
  if (!isConnected()) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_childrenWatchers[path] = callback;
  }

  // 注册Watch并获取当前子节点
  struct String_vector children;
  int ret = zoo_wget_children(m_zhandle, path.c_str(), childrenWatcher, 
                              this, &children);
  
  if (ret == ZOK) {
    deallocate_String_vector(&children);
  } else {
    std::cerr << "[ZkClient] Failed to watch children: " << path 
              << ", error: " << zerror(ret) << std::endl;
  }
}

void ZkClient::watchNode(const std::string& path, WatchCallback callback) {
  if (!isConnected()) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_nodeWatchers[path] = callback;
  }

  char buffer[1] = {0};
  int bufferLen = 0;
  struct Stat stat;
  
  int ret = zoo_wget(m_zhandle, path.c_str(), nodeWatcher, this, 
                     buffer, &bufferLen, &stat);
  
  if (ret != ZOK && ret != ZNONODE) {
    std::cerr << "[ZkClient] Failed to watch node: " << path 
              << ", error: " << zerror(ret) << std::endl;
  }
}

void ZkClient::childrenWatcher(zhandle_t* zh, int type, int state,
                               const char* path, void* watcherCtx) {
  if (type == ZOO_CHILD_EVENT) {
    ZkClient* client = static_cast<ZkClient*>(watcherCtx);
    if (client != nullptr && path != nullptr) {
      client->handleChildrenWatch(path);
    }
  }
}

void ZkClient::nodeWatcher(zhandle_t* zh, int type, int state,
                           const char* path, void* watcherCtx) {
  if (type == ZOO_CHANGED_EVENT || type == ZOO_DELETED_EVENT) {
    ZkClient* client = static_cast<ZkClient*>(watcherCtx);
    if (client != nullptr && path != nullptr) {
      client->handleNodeWatch(path);
    }
  }
}

void ZkClient::handleChildrenWatch(const std::string& path) {
  WatchCallback callback;
  {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_childrenWatchers.find(path);
    if (it != m_childrenWatchers.end()) {
      callback = it->second;
    }
  }

  if (callback) {
    // 重新注册Watch（ZK的Watch是一次性的）
    watchChildren(path, callback);
    // 调用回调
    callback(path);
  }
}

void ZkClient::handleNodeWatch(const std::string& path) {
  WatchCallback callback;
  {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_nodeWatchers.find(path);
    if (it != m_nodeWatchers.end()) {
      callback = it->second;
    }
  }

  if (callback) {
    // 重新注册Watch
    watchNode(path, callback);
    // 调用回调
    callback(path);
  }
}

bool ZkClient::createPathRecursive(const std::string& path) {
  if (path.empty() || path[0] != '/') {
    return false;
  }

  // 逐级创建路径
  size_t pos = 1;
  while (pos < path.length()) {
    pos = path.find('/', pos);
    if (pos == std::string::npos) {
      pos = path.length();
    }

    std::string subPath = path.substr(0, pos);
    if (!exists(subPath)) {
      std::string result = createNode(subPath, "", false, false);
      if (result.empty()) {
        return false;
      }
    }
    ++pos;
  }

  return true;
}
