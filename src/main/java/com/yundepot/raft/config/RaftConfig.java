package com.yundepot.raft.config;

import lombok.Data;

/**
 * @author zhaiyanan
 * @date 2019/6/21 15:08
 */
@Data
public class RaftConfig {

    /**
     * raft 数据存储路径
     */
    private String rootDir;

    /**
     * 当前节点
     */
    private int server;

    /**
     * 集群
     */
    private String cluster;

    /**
     * 同步线程池最大值
     */
    private int tpMax = 400;

    /**
     * 同步线程池最小值
     */
    private int tpMin = 20;

    /**
     * snapshot定时器执行间隔, 单位秒
     */
    private int snapshotPeriod = 3600;

    /**
     * true表示主节点保存后就返回，然后异步同步给从节点
     * false表示主节点同步给大多数从节点后才返回
     */
    private boolean asyncReplicate = false;

    /**
     * 同步写入日志文件
     */
    private boolean syncWriteLogFile = true;

    /**
     * replicate最大等待超时时间，单位ms
     */
    private long maxAwaitTimeout = 3000;

    /**
     * 新加入集群的节点日志差距必须小于catchupGap，才能集群提交
     */
    private long catchupGap = 200;

    /**
     * log entry大小达到snapshotMinLogSize，才做snapshot
     */
    private long snapshotMinLogSize = 1024 * 1024 * 1024;

    /**
     * 重新选举超时时间，如果超过该时间FOLLOWER还没有收到心跳，则开始选举
     */
    private int electionTimeout = 2000;

    /**
     * LEADER向FOLLOWER发送心跳包的时间间隔
     */
    private int heartbeatPeriod = 200;

    /**
     * 同步日志或快照时每次网络请求 最大字节
     */
    private int maxSizePerRequest = 1024 * 1024;

    /**
     * 每次快照删除日志时，最少保留的条数，防止频繁发送快照
     */
    private long keepLogNum = 1024;

    /**
     * 读取时一致性级别, 默认最终一致性
     */
    private int consistencyLevel = 0;
}
