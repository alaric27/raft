package com.yundepot.raft;

import com.alibaba.fastjson.JSON;
import com.yundepot.raft.bean.Response;
import com.yundepot.raft.bean.Server;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * @author zhaiyanan
 * @date 2022/7/12  14:24
 */
public class RaftClientTest {
    private RaftClient raftClient;

    @Before
    public void before() {
        raftClient = new RaftClient("1:127.0.0.1:2727,2:127.0.0.1:2728,3:127.0.0.1:2729");
    }

    @Test
    public void set() {
        long start = System.currentTimeMillis();
        for (long i = 1; i < 1000; i++) {
            byte[] key = ("aaa" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = getRandomString(10).getBytes(StandardCharsets.UTF_8);
            raftClient.set(key, value);
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void get() {
        byte[] bytes = raftClient.get("aaa320".getBytes(StandardCharsets.UTF_8));
        System.out.println(new String(bytes));
    }

    @Test
    public void delete() {
        byte[] key = "aaa320".getBytes(StandardCharsets.UTF_8);
        raftClient.delete(key);
    }

    @Test
    public void getLeader() {
        System.out.println(raftClient.getLeader().getServerId());
    }

    @Test
    public void getClusterInfo() {
        System.out.println(JSON.toJSONString(raftClient.getClusterInfo()));
    }

    @Test
    public void addPeer() {
        Server server = new Server();
        server.setServerId(4);
        server.setHost("127.0.0.1");
        server.setPort(2730);
        Response response = raftClient.addPeer(server);
        System.out.println(JSON.toJSONString(response));
    }

    @Test
    public void removePeer() {
        Server server = new Server();
        server.setServerId(4);
        server.setHost("127.0.0.1");
        server.setPort(2730);
        Response response = raftClient.removePeer(server);
        System.out.println(JSON.toJSONString(response));
    }

    @Test
    public void getCommitIndex() {
        System.out.println(raftClient.getCommitIndex());
    }

    public static String getRandomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
}
