package com.yundepot.raft.service;

import com.yundepot.raft.bean.*;

/**
 * raft管理接口
 * @author zhaiyanan
 * @date 2019/6/15 18:49
 */
public interface RaftAdminService {

    /**
     * 获取leader
     * @return
     */
    Server getLeader();

    /**
     * 获取集群节点信息
     * @return
     */
    Configuration getConfiguration();

    /**
     * 添加节点
     * @param server
     * @return
     */
    Response addPeer(Server server);

    /**
     * 移除节点
     * @param server
     * @return
     */
    Response removePeer(Server server);

    /**
     * 获取节点的日志提交索引
     * @return
     */
    long getCommitIndex();
}
