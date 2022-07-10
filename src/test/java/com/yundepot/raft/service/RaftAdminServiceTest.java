package com.yundepot.raft.service;

import com.alibaba.fastjson.JSON;
import com.yundepot.raft.RaftClient;
import com.yundepot.raft.bean.Response;
import com.yundepot.raft.bean.Server;
import org.junit.Before;
import org.junit.Test;

/**
 * @author zhaiyanan
 * @date 2022/7/7  16:30
 */
public class RaftAdminServiceTest {

    private RaftAdminService raftAdminService;

    @Before
    public void before() {
        RaftClient raftClient = new RaftClient("127.0.0.1", 2730);
        raftAdminService = raftClient.getRpcClient().create(RaftAdminService .class);
    }

    @Test
    public void getLeader() {
        System.out.println(raftAdminService.getLeader().getServerId());
    }

    @Test
    public void getClusterInfo() {
        System.out.println(JSON.toJSONString(raftAdminService.getClusterInfo()));
    }

    @Test
    public void addPeer() {
        Server server = new Server();
        server.setServerId(4);
        server.setHost("127.0.0.1");
        server.setPort(2730);
        Response response = raftAdminService.addPeer(server);
        System.out.println(JSON.toJSONString(response));
    }

    @Test
    public void removePeer() {
        Server server = new Server();
        server.setServerId(4);
        server.setHost("127.0.0.1");
        server.setPort(2730);
        raftAdminService.removePeer(server);
    }

    @Test
    public void getCommitIndex() {
        System.out.println(raftAdminService.getCommitIndex());
    }
}