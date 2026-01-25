#ifndef RAFTSERVERRPC_H
#define RAFTSERVERRPC_H

#include "kvServerRPC.pb.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"
#include <iostream>

// 维护当前节点对其他某一个结点的所有rpc通信，包括接收其他节点的rpc和发送
// 对于一个节点来说，对于任意其他的节点都要维护一个rpc连接，
class raftServerRpcUtil
{
private:
    raftKVRpcProctoc::kvServerRpc_Stub *stub;

public:

    //响应其他节点的方法
    bool Get(raftKVRpcProctoc::GetArgs *GetArgs, raftKVRpcProctoc::GetReply *reply);
    bool PutAppend(raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);

    raftServerRpcUtil(std::string ip, short port);
    ~raftServerRpcUtil();
};

#endif 
