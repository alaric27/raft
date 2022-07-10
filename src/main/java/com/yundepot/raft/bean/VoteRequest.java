package com.yundepot.raft.bean;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:01
 */
@Data
@ToString
public class VoteRequest implements Serializable {

    /**
     * 请求选票的候选人的id
     */
    private int candidateId;

    /**
     * 候选人的任期号
     */
    private long term;

    /**
     * 候选人的最后日志条目的任期号
     */
    private long lastLogTerm;

    /**
     * 候选人最后日志条目的索引值
     */
    private long lastLogIndex;
}
