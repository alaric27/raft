package com.yundepot.raft.bean;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:31
 */
@Data
@ToString
public class InstallSnapshotResponse implements Serializable {

    private int resCode;
    private long term;
}
