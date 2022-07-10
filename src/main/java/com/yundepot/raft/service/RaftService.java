package com.yundepot.raft.service;

import com.yundepot.raft.bean.*;

/**
 * raft 内部接口
 * @author zhaiyanan
 * @date 2019/6/15 18:00
 */
public interface RaftService {

    /**
     * 投票
     * @param request
     * @return
     */
    VoteResponse requestVote(VoteRequest request);

    /**
     * 追加日志
     * @param request
     * @return
     */
    AppendLogResponse appendLog(AppendLogRequest request);

    /**
     * 安装快照
     * @param request
     * @return
     */
    InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request);

}
