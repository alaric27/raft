package com.yundepot.raft.bean;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:25
 */
@Data
@ToString(exclude = {"data"})
public class InstallSnapshotRequest implements Serializable {

    private long term;
    private int leaderId;
    private long lastIncludedIndex;
    private long lastIncludedTerm;
    private long offset;
    private byte[] data;
    private boolean done;

    private boolean first;
    private String fileName;
}
