package com.yundepot.raft.bean;

import com.yundepot.raft.PeerClient;
import lombok.Data;

/**
 * 节点
 * @author zhaiyanan
 * @date 2019/6/15 17:28
 */
@Data
public class Peer {

    private Server server;
    private PeerClient peerClient;

    /**
     * 下一个要发的日志索引
     */
    private volatile long nextIndex;

    /**
     * 已被peer接收的最后一条日志索引
     */
    private long matchIndex;

    /**
     * 是否投票批准
     */
    private volatile boolean voteGranted;

    /**
     * 当前peer是否追赶上日志
     */
    private volatile boolean catchUp;

    /**
     * 正在向该节点安装快照
     */
    private volatile boolean installingSnapshot;

    /**
     * 最后一次响应是否正常，用于leader判读当前是否依然能和大多数节点通信
     */
    private volatile boolean lastResponseStatus;

    /**
     * 最后一次成功响应时间, 用于判断该节点是否存活, 依赖系统时钟
     */
    private long lastResponseTime;

    public Peer(Server server) {
        this.server = server;
        this.peerClient = new PeerClient(server.getHost(), server.getPort());
    }

}
