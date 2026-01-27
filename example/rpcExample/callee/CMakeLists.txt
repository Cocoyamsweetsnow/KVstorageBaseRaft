
set(SRC_LIST friendService.cpp ../friend.pb.cc)
add_executable(provider ${SRC_LIST})
target_link_libraries(provider rpc_lib protobuf muduo_net muduo_base pthread zookeeper_mt)

# 带服务注册的RPC服务端示例
set(REGISTRY_SRC_LIST friendServiceWithRegistry.cpp ../friend.pb.cc)
add_executable(provider_with_registry ${REGISTRY_SRC_LIST})
target_link_libraries(provider_with_registry rpc_lib protobuf muduo_net muduo_base pthread zookeeper_mt)
