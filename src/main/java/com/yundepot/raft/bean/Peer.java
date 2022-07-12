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
    private long nextIndex;

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

    public Peer(Server server) {
        this.server = server;
        this.peerClient = new PeerClient(server.getHost(), server.getPort());
    }

}
