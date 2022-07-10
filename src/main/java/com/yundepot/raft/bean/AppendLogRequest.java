package com.yundepot.raft.bean;


import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:07
 */
@Data
@ToString(exclude = {"logEntryList"})
public class AppendLogRequest implements Serializable {

    /**
     * 服务id
     */
    private int leaderId;

    /**
     * 任期号
     */
    private long term;

    /**
     * 之前的日志索引
     */
    private long prevLogIndex;

    /**
     * prevLogIndex日志的任期号
     */
    private long prevLogTerm;

    /**
     * 准备存储的日志
     */
    private List<LogEntry> logEntryList = new ArrayList<>();

    /**
     * 领导人已经提交的日志索引值
     */
    private long leaderCommit;
}
