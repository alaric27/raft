package com.yundepot.raft.service;

import com.yundepot.raft.RaftNode;
import com.yundepot.raft.statemachine.StateMachine;
import com.yundepot.raft.common.LogType;
import com.yundepot.raft.bean.Pair;
import com.yundepot.raft.util.ByteUtil;
import com.yundepot.rpc.RpcClient;

/**
 * @author zhaiyanan
 * @date 2019/6/20 20:55
 */
public class PairServiceImpl implements PairService {

    private RaftNode raftNode;
    private StateMachine stateMachine;

    public PairServiceImpl(RaftNode raftNode, StateMachine stateMachine) {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
    }

    @Override
    public void put(Pair pair) {
        put(pair.getKey(), pair.getValue());
    }

    @Override
    public void put(byte[] key, byte[] value) {
        // 如果自己不是leader，将写请求转发给leader
        if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
            RpcClient rpcClient = raftNode.getPeerMap().get(raftNode.getLeaderId()).getRaftClient().getRpcClient();
            rpcClient.create(PairService.class).put(key, value);
        } else {
            // 数据同步写入raft集群
            byte[] data = ByteUtil.encode(key, value);
            raftNode.replicate(data, LogType.DATA);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        return stateMachine.get(key);
    }
}
