package com.yundepot.raft.service;

import com.yundepot.raft.RaftNode;
import com.yundepot.raft.bean.Response;
import com.yundepot.raft.common.ResponseCode;
import com.yundepot.raft.statemachine.StateMachine;
import com.yundepot.raft.common.LogType;
import com.yundepot.raft.util.ByteUtil;

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
    public Response put(byte[] key, byte[] value) {
        if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
            return Response.fail(ResponseCode.NOT_LEADER.getValue(), raftNode.getLeaderId());
        }

        // 数据同步写入raft集群
        byte[] data = ByteUtil.encode(key, value);
        raftNode.replicate(data, LogType.DATA);
        return Response.success();
    }

    @Override
    public Response get(byte[] key) {
        if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
            return Response.fail(ResponseCode.NOT_LEADER.getValue(), raftNode.getLeaderId());
        }
        return Response.success(stateMachine.get(key));
    }
}
