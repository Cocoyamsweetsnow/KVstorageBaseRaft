#ifndef RAFTRPC_H
#define RAFTRPC_H

#include "raftRPC.pb.h"

// 维护当前节点对其他某一个结点的所有rpc发送通信的功能
// 对于一个raft节点来说，对于任意其他的节点都要维护一个rpc连接，即MprpcChannel
class RaftRpcUtil
{
private:
    raftRpcProctoc::raftRpc_Stub *stub_;

public:

    bool AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response);
    bool InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args,
                         raftRpcProctoc::InstallSnapshotResponse *response);
    bool RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response);

    RaftRpcUtil(std::string ip, short port);
    ~RaftRpcUtil();
};

#endif 
