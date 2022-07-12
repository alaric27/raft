package com.yundepot.raft;

import com.yundepot.raft.bean.Cluster;
import com.yundepot.raft.bean.Response;
import com.yundepot.raft.bean.Server;
import com.yundepot.raft.common.ResponseCode;
import com.yundepot.raft.service.PairService;
import com.yundepot.raft.service.RaftAdminService;
import com.yundepot.raft.util.ClusterUtil;
import com.yundepot.rpc.RpcClient;

/**
 * raft 对外客户端
 * @author zhaiyanan
 * @date 2022/7/12  14:21
 */
public class RaftClient {

    private Server leader;
    private RpcClient rpcClient;
    private RaftAdminService adminService;
    private PairService pairService;
    private Cluster cluster;

    public RaftClient(String cluster) {
        this(ClusterUtil.parserCluster(cluster));
    }

    public RaftClient(Cluster cluster) {
        this.cluster = cluster;
        this.leader = cluster.getServers().stream().findAny().get();
        connect(leader);
    }

    /**
     * 写入数据
     */
    public void put(byte[] key, byte[] value) {
        // todo 连接异常情况
        Response response = null;
        try {
            response = pairService.put(key, value);
            handleResponse(response);
        } catch (Exception e) {
            System.out.println();
        }
    }

    /**
     * 读取数据
     * @param key
     * @return
     */
    public byte[] get(byte[] key) {
        Response response = pairService.get(key);
        handleResponse(response);
        return (byte[]) response.getData();
    }

    /**
     * 获取leader
     * @return
     */
    public Server getLeader() {
        return adminService.getLeader();
    }

    /**
     * 获取集群节点信息
     * @return
     */
    public Cluster getClusterInfo() {
        return adminService.getClusterInfo();
    }

    /**
     * 添加节点
     * @param server
     * @return
     */
    public Response addPeer(Server server) {
        Response response = adminService.addPeer(server);
        handleResponse(response);
        return response;
    }

    /**
     * 移除节点
     * @param server
     * @return
     */
    public Response removePeer(Server server) {
        Response response = adminService.removePeer(server);
        // todo 重试
        handleResponse(response);
        return response;
    }

    /**
     * 获取节点的日志提交索引
     * @return
     */
    public long getCommitIndex() {
        return adminService.getCommitIndex();
    }

    private void handleResponse(Response response) {
        // 重定向到leader节点
        if (ResponseCode.NOT_LEADER.getValue() == response.getCode()) {
            Server server = (Server) response.getData();
            connect(server);
        }
    }

    private void connect(Server leader) {
        if (rpcClient != null) {
            rpcClient.shutdown();
        }

        this.leader = leader;
        rpcClient = new RpcClient(this.leader.getHost(), this.leader.getPort());
        rpcClient.start();
        this.adminService = rpcClient.create(RaftAdminService.class);
        this.pairService = rpcClient.create(PairService.class);
    }
}
