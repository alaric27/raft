package com.yundepot.raft.bean;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:17
 */
@Data
@ToString
public class AppendLogResponse implements Serializable {

    private boolean success;
    private long term;

    /**
     * 为了加快日志同步，follower返回最后一条日志的索引
     */
    private long lastLogIndex;
}
