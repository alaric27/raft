package com.yundepot.raft.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:28
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SnapshotMetadata {

    /**
     * 快照中包含的最后日志条目的索引值
     */
    private long lastIncludedIndex;

    /**
     * 快照中包含的最后日志条目的任期号
     */
    private long lastIncludedTerm;

    /**
     * 集群配置
     */
    private Cluster cluster;

}
