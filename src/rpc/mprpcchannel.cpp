#include "mprpcchannel.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <string>
#include "mprpccontroller.h"
#include "rpcheader.pb.h"
#include "serviceDiscovery.h"
#include "util.h"

MprpcChannel::MprpcChannel(std::string ip, short port, bool connectNow) 
    : m_ip(ip)
    , m_port(port)
    , m_clientFd(-1)
    , m_useDiscovery(false) {
  // 使用tcp编程，完成rpc方法的远程调用
  if (!connectNow) {
    return;
  }
  std::string errMsg;
  auto rt = newConnect(ip.c_str(), port, &errMsg);
  int tryCount = 3;
  while (!rt && tryCount--) {
    std::cout << errMsg << std::endl;
    rt = newConnect(ip.c_str(), port, &errMsg);
  }
}

MprpcChannel::MprpcChannel(const std::string& serviceName, 
                           std::shared_ptr<ServiceDiscovery> discovery,
                           const std::string& hashKey)
    : m_clientFd(-1)
    , m_port(0)
    , m_serviceName(serviceName)
    , m_discovery(discovery)
    , m_hashKey(hashKey)
    , m_useDiscovery(true) {
  // 服务发现模式，延迟解析服务地址
}

MprpcChannel::~MprpcChannel() {
  if (m_clientFd != -1) {
    close(m_clientFd);
    m_clientFd = -1;
  }
}

void MprpcChannel::setHashKey(const std::string& hashKey) {
  m_hashKey = hashKey;
}

bool MprpcChannel::isServiceDiscoveryMode() const {
  return m_useDiscovery;
}

bool MprpcChannel::resolveServiceAddress() {
  if (!m_discovery) {
    std::cerr << "[MprpcChannel] Service discovery is null" << std::endl;
    return false;
  }

  // 使用一致性哈希选择服务实例
  ServiceInstance instance = m_discovery->selectInstance(m_serviceName, m_hashKey);
  if (!instance.isValid()) {
    std::cerr << "[MprpcChannel] No available instance for service: " 
              << m_serviceName << std::endl;
    return false;
  }

  m_ip = instance.ip;
  m_port = instance.port;

  std::cout << "[MprpcChannel] Resolved service " << m_serviceName 
            << " to " << m_ip << ":" << m_port << std::endl;
  return true;
}

void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                              google::protobuf::RpcController* controller, 
                              const google::protobuf::Message* request,
                              google::protobuf::Message* response, 
                              google::protobuf::Closure* done) {
  // 如果使用服务发现模式，先解析服务地址
  if (m_useDiscovery) {
    if (!resolveServiceAddress()) {
      controller->SetFailed("Failed to resolve service address for: " + m_serviceName);
      return;
    }
  }

  if (m_clientFd == -1) {
    std::string errMsg;
    bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
    if (!rt) {
      DPrintf("[func-MprpcChannel::CallMethod]重连接ip：{%s} port{%d}失败", m_ip.c_str(), m_port);
      controller->SetFailed(errMsg);
      return;
    } else {
      DPrintf("[func-MprpcChannel::CallMethod]连接ip：{%s} port{%d}成功", m_ip.c_str(), m_port);
    }
  }

  const google::protobuf::ServiceDescriptor* sd = method->service();
  std::string service_name = sd->name();     
  std::string method_name = method->name();  


  uint32_t args_size{};
  std::string args_str;
  if (request->SerializeToString(&args_str)) {
    args_size = args_str.size();
  } else {
    controller->SetFailed("serialize request error!");
    return;
  }
  RPC::RpcHeader rpcHeader;
  rpcHeader.set_service_name(service_name);
  rpcHeader.set_method_name(method_name);
  rpcHeader.set_args_size(args_size);

  std::string rpc_header_str;
  if (!rpcHeader.SerializeToString(&rpc_header_str)) {
    controller->SetFailed("serialize rpc header error!");
    return;
  }

  std::string send_rpc_str;  
  {
    google::protobuf::io::StringOutputStream string_output(&send_rpc_str);
    google::protobuf::io::CodedOutputStream coded_output(&string_output);

    coded_output.WriteVarint32(static_cast<uint32_t>(rpc_header_str.size()));

    coded_output.WriteString(rpc_header_str);
  }

  send_rpc_str += args_str;

  while (-1 == send(m_clientFd, send_rpc_str.c_str(), send_rpc_str.size(), 0)) {
    char errtxt[512] = {0};
    sprintf(errtxt, "send error! errno:%d", errno);
    std::cout << "尝试重新连接，对方ip：" << m_ip << " 对方端口" << m_port << std::endl;
    close(m_clientFd);
    m_clientFd = -1;
    std::string errMsg;
    bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
    if (!rt) {
      controller->SetFailed(errMsg);
      return;
    }
  }
  
  char recv_buf[1024] = {0};
  int recv_size = 0;
  if (-1 == (recv_size = recv(m_clientFd, recv_buf, 1024, 0))) {
    close(m_clientFd);
    m_clientFd = -1;
    char errtxt[512] = {0};
    sprintf(errtxt, "recv error! errno:%d", errno);
    controller->SetFailed(errtxt);
    return;
  }

  if (!response->ParseFromArray(recv_buf, recv_size)) {
    char errtxt[1050] = {0};
    sprintf(errtxt, "parse error! response_str:%s", recv_buf);
    controller->SetFailed(errtxt);
    return;
  }
}

bool MprpcChannel::newConnect(const char* ip, uint16_t port, std::string* errMsg) {
  int clientfd = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == clientfd) {
    char errtxt[512] = {0};
    sprintf(errtxt, "create socket error! errno:%d", errno);
    m_clientFd = -1;
    *errMsg = errtxt;
    return false;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = inet_addr(ip);
  // 连接rpc服务节点
  if (-1 == connect(clientfd, (struct sockaddr*)&server_addr, sizeof(server_addr))) {
    close(clientfd);
    char errtxt[512] = {0};
    sprintf(errtxt, "connect fail! errno:%d", errno);
    m_clientFd = -1;
    *errMsg = errtxt;
    return false;
  }
  m_clientFd = clientfd;
  return true;
}
