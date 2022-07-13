package com.yundepot.raft.service;

import com.yundepot.raft.RaftNode;
import com.yundepot.raft.bean.*;
import com.yundepot.raft.common.ResponseCode;
import com.yundepot.raft.util.ClusterUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:33
 */
@Slf4j
public class RaftServiceImpl implements RaftService{

    private RaftNode raftNode;
    public RaftServiceImpl(RaftNode node) {
        this.raftNode = node;
    }

    @Override
    public VoteResponse preVote(VoteRequest request) {
        raftNode.getLock().lock();
        try {
            VoteResponse voteResponse = VoteResponse.builder().voteGranted(false)
                    .term(raftNode.getCurrentTerm()).build();

            if (!ClusterUtil.containsServer(raftNode.getCluster(), request.getCandidateId())) {
                return voteResponse;
            }
            //如果term < currentTerm返回 false
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return voteResponse;
            }

            // 更新自己的term
            if (request.getTerm() > raftNode.getCurrentTerm()) {
                raftNode.stepDown(request.getTerm());
            }

            // 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他
            boolean votedForFlag = raftNode.getVotedFor() == 0 || raftNode.getVotedFor() == request.getCandidateId();
            if (votedForFlag && isNewer(request)) {
                voteResponse.setVoteGranted(true);
                voteResponse.setTerm(raftNode.getCurrentTerm());
            }
            return voteResponse;
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public VoteResponse vote(VoteRequest request) {
        raftNode.getLock().lock();
        try {
            VoteResponse voteResponse = VoteResponse.builder().voteGranted(false)
                    .term(raftNode.getCurrentTerm()).build();

            if (!ClusterUtil.containsServer(raftNode.getCluster(), request.getCandidateId())) {
                return voteResponse;
            }
            //如果term < currentTerm返回 false
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return voteResponse;
            }

            // 更新自己的term
            if (request.getTerm() > raftNode.getCurrentTerm()) {
                raftNode.stepDown(request.getTerm());
            }

            // 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他
            boolean votedForFlag = raftNode.getVotedFor() == 0 || raftNode.getVotedFor() == request.getCandidateId();
            if (votedForFlag && isNewer(request)) {
                // 给别人投票后，下台，重置选举
                raftNode.stepDown(request.getTerm());
                raftNode.setVotedFor(request.getCandidateId());
                raftNode.getStateStorage().update(raftNode.getCurrentTerm(), raftNode.getVotedFor());
                voteResponse.setVoteGranted(true);
                voteResponse.setTerm(raftNode.getCurrentTerm());
            }
            return voteResponse;
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public AppendLogResponse appendLog(AppendLogRequest request) {
        raftNode.getLock().lock();
        try {
            long lastLogIndex = raftNode.getLastLogIndex();
            AppendLogResponse response = new AppendLogResponse();
            response.setTerm(raftNode.getCurrentTerm());
            response.setSuccess(false);
            response.setLastLogIndex(lastLogIndex);

            //如果 term < currentTerm 就返回 false
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return response;
            }

            raftNode.stepDown(request.getTerm());

            // 前一个日志索引大于最后的索引，代表日志不连续，直接返回
            if (request.getPrevLogIndex() > lastLogIndex) {
                return response;
            }

            // 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则重新同步
            if (raftNode.getEntryTerm(request.getPrevLogIndex()) != request.getPrevLogTerm()) {
                return response;
            }

            raftNode.setLeaderId(request.getLeaderId());
            raftNode.appendLogEntry(request);
            response.setSuccess(true);
            response.setLastLogIndex(raftNode.getLastLogIndex());
            return response;
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        InstallSnapshotResponse response = new InstallSnapshotResponse();
        response.setResCode(ResponseCode.FAIL.getValue());
        raftNode.getLock().lock();
        try {
            response.setTerm(raftNode.getCurrentTerm());
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return response;
            }
            raftNode.stepDown(request.getTerm());
            if (raftNode.getLeaderId() == 0) {
                raftNode.setLeaderId(request.getLeaderId());
            }
            if (!raftNode.getStateMachine().installSnapshot(request)) {
                return response;
            }
            // 成功安装快照后清除所有日志
            if (request.isDone()) {
                raftNode.getLogStore().deleteSuffix(0);
            }
            response.setResCode(ResponseCode.SUCCESS.getValue());
        } finally {
            raftNode.getLock().unlock();
        }
        return response;
    }

    /**
     * 判断投票请求是不是比当前节点更新
     * 如果最后日志的term 大于当前节点的最后日志的term 或
     * 最后日志的索引大于等于当前节点的索引
     * @param request
     * @return
     */
    private boolean isNewer(VoteRequest request) {
        return request.getLastLogTerm() > raftNode.getLastLogTerm()
                || (request.getLastLogTerm() == raftNode.getLastLogTerm()
                && request.getLastLogIndex() >= raftNode.getLastLogIndex());
    }
}
