package com.yundepot.raft.service;

import com.yundepot.raft.RaftNode;
import com.yundepot.raft.bean.GetRequest;
import com.yundepot.raft.bean.Pair;
import com.yundepot.raft.bean.Range;
import com.yundepot.raft.bean.Response;
import com.yundepot.raft.common.ConsistencyLevel;
import com.yundepot.raft.common.ResponseCode;
import com.yundepot.raft.common.LogType;
import com.yundepot.raft.util.ByteUtil;
import com.yundepot.raft.util.ConfigUtil;

import java.util.Objects;

/**
 * @author zhaiyanan
 * @date 2019/6/20 20:55
 */
public class PairServiceImpl implements PairService {

    private RaftNode raftNode;

    public PairServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public Response set(Pair pair) {
        if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
            return Response.fail(ResponseCode.NOT_LEADER.getValue(), ConfigUtil.getServer(raftNode.getConfiguration(), raftNode.getLeaderId()));
        }

        long timeout = System.currentTimeMillis() + pair.getTimeout() * 1000;
        pair.setTimeout(timeout);
        // 数据同步写入raft集群
        byte[] data = ByteUtil.encodePair(pair);
        raftNode.replicate(data, LogType.SET);
        return Response.success();
    }

    @Override
    public Response get(GetRequest request) {
        // 线性一致性读
        if (Objects.equals(request.getConsistencyLevel(), ConsistencyLevel.LINE.getValue())) {
            raftNode.getLock().unlock();
            try {
                // 获取leader 的readIndex
                Long readIndex = raftNode.getLeaderCommitIndex();
                if (Objects.isNull(readIndex)) {
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
        return Response.success(raftNode.get(request.getKey()));
    }

    @Override
    public Response delete(byte[] key) {
        if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
            return Response.fail(ResponseCode.NOT_LEADER.getValue(), ConfigUtil.getServer(raftNode.getConfiguration(), raftNode.getLeaderId()));
        }

        // 数据同步写入raft集群
        raftNode.replicate(key, LogType.DELETE);
        return Response.success();
    }

    @Override
    public Response deleteRange(Range range) {
        if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
            return Response.fail(ResponseCode.NOT_LEADER.getValue(), ConfigUtil.getServer(raftNode.getConfiguration(), raftNode.getLeaderId()));
        }

        byte[] bytes = ByteUtil.encodeRange(range);
        // 数据同步写入raft集群
        raftNode.replicate(bytes, LogType.DELETE_RANGE);
        return Response.success();
    }
}
