package com.yundepot.raft.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * 节点元数据
 * @author zhaiyanan
 * @date 2019/6/16 17:00
 */
@Data
public class NodeMeta implements Serializable {

    /**
     * 服务器最后一次知道的任期号
     */
    private long currentTerm;

    /**
     * 在当前任期获得选票的候选人的ID
     */
    private int votedFor;
}
