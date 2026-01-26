# set(SRC_LIST calluserservice.cc ../user.pb.cc)
set(SRC_LIST callFriendService.cpp ../friend.pb.cc)
add_executable(consumer ${SRC_LIST})
target_link_libraries(consumer rpc_lib protobuf)

# 异步RPC调用示例
set(ASYNC_SRC_LIST asyncCallFriendService.cpp ../friend.pb.cc)
add_executable(async_consumer ${ASYNC_SRC_LIST})
target_link_libraries(async_consumer rpc_lib protobuf pthread)

# 连接池RPC调用示例
set(POOLED_SRC_LIST pooledCallFriendService.cpp ../friend.pb.cc)
add_executable(pooled_consumer ${POOLED_SRC_LIST})
target_link_libraries(pooled_consumer rpc_lib protobuf pthread)