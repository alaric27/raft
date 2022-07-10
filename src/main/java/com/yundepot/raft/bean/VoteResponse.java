package com.yundepot.raft.bean;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:04
 */
@Data
@Builder
@ToString
public class VoteResponse implements Serializable {

    /**
     * 当前任期号，以便于候选人去更新自己的任期号
     */
    private long term;

    /**
     * 候选人赢得了此张选票时为真
     */
    private boolean voteGranted;

}
