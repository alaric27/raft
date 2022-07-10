package com.yundepot.raft.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:12
 */
@Data
public class LogEntry implements Serializable {

    private long term;
    private long index;
    private int logType;
    private byte[] data;
}
