package com.yundepot.raft.common;

/**
 * @author zhaiyanan
 * @date 2019/6/17 21:07
 */
public enum RaftRole {
    /**
     * 跟随者
     */
    FOLLOWER,

    /**
     * 候选人
     */
    CANDIDATE,

    /**
     * 领导者
     */
    LEADER
}
