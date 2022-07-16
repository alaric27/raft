package com.yundepot.raft.service;

import com.yundepot.raft.RaftNode;
import com.yundepot.raft.bean.Response;
import com.yundepot.raft.common.ConsistencyLevel;
import com.yundepot.raft.common.ResponseCode;
import com.yundepot.raft.statemachine.StateMachine;
import com.yundepot.raft.common.LogType;
import com.yundepot.raft.util.ByteUtil;
import com.yundepot.raft.util.ClusterUtil;

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
            return Response.fail(ResponseCode.NOT_LEADER.getValue(), ClusterUtil.getServer(raftNode.getClusterConfig(), raftNode.getLeaderId()));
        }

        // 数据同步写入raft集群
        byte[] data = ByteUtil.encode(key, value);
        raftNode.replicate(data, LogType.DATA);
        return Response.success();
    }

    @Override
    public Response get(byte[] key) {
        // 线性一致性读
        if (ConsistencyLevel.LINE.getValue() == raftNode.getRaftConfig().getConsistencyLevel()) {
            // 线性一致性不允许读follower
            if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
                return Response.fail(ResponseCode.NOT_LEADER.getValue(), ClusterUtil.getServer(raftNode.getClusterConfig(), raftNode.getLeaderId()));
            }
            raftNode.getLock().unlock();
            try {
                long readIndex = raftNode.getCommitIndex();
                // 发送心跳等待确认当前节点是否依然为leader
                raftNode.sendHeartbeat();
                raftNode.getPeerMap().values().forEach(peer -> peer.setLastResponseStatus(false));
                if (!raftNode.awaitAppend()) {
                    return Response.fail(ResponseCode.FAIL.getValue());
                }

                // 等待节点日志应用到readIndex
                if (!raftNode.awaitCommit(readIndex)) {
                    return Response.fail(ResponseCode.FAIL.getValue());
                }
            } finally {
                raftNode.getLock().unlock();
            }
        }
        return Response.success(stateMachine.get(key));
    }
}
