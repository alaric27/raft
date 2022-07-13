package com.yundepot.raft;

import com.yundepot.oaa.config.GenericOption;
import com.yundepot.raft.bean.*;
import com.yundepot.raft.service.RaftService;
import com.yundepot.rpc.RpcClient;

/**
 * 集群内节点之前通信
 * @author zhaiyanan
 * @date 2019/6/15 17:30
 */
public class PeerClient {
    private RpcClient rpcClient;
    private RaftService raftService;

    public PeerClient(String host, int port) {
        this.rpcClient = new RpcClient(host, port);
        this.rpcClient.option(GenericOption.TCP_HEARTBEAT_SWITCH, true);
        this.rpcClient.start();
        raftService = this.rpcClient.create(RaftService.class);
    }

    public void shutdown() {
        this.rpcClient.shutdown();
    }

    public VoteResponse vote(VoteRequest request) {
        return raftService.vote(request);
    }

    public AppendLogResponse appendLog(AppendLogRequest request) {
        return raftService.appendLog(request);
    }

    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        return raftService.installSnapshot(request);
    }
}
