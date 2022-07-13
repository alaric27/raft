package com.yundepot.raft;

import com.yundepot.oaa.exception.ConnectionException;
import com.yundepot.raft.bean.Cluster;
import com.yundepot.raft.bean.Response;
import com.yundepot.raft.bean.Server;
import com.yundepot.raft.common.ResponseCode;
import com.yundepot.raft.exception.RaftException;
import com.yundepot.raft.service.PairService;
import com.yundepot.raft.service.RaftAdminService;
import com.yundepot.raft.util.ClusterUtil;
import com.yundepot.rpc.RpcClient;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.Callable;

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
        this.leader = cluster.getServerList().get(0);
        connect(leader);
    }

    /**
     * 写入数据
     */
    public void put(byte[] key, byte[] value) {
        execute(()-> pairService.put(key, value));
    }

    /**
     * 读取数据
     * @param key
     * @return
     */
    public byte[] get(byte[] key) {
        Response response = execute(()-> pairService.get(key));
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
        return execute(() -> adminService.addPeer(server));
    }

    /**
     * 移除节点
     * @param server
     * @return
     */
    public Response removePeer(Server server) {
        return execute(() -> adminService.removePeer(server));
    }

    /**
     * 获取节点的日志提交索引
     * @return
     */
    public long getCommitIndex() {
        return adminService.getCommitIndex();
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

    private Response execute(Callable<Response> task) {
        return execute(task, 0);
    }

    /**
     * 执行客户端请求，智能路由到leader节点
     * @param task
     * @param count
     * @return
     */
    private Response execute(Callable<Response> task, int count){
        // 限制重试次数
        if (count > cluster.getServerList().size()) {
            return Response.fail(ResponseCode.FAIL.getValue());
        }

        Response response = null;
        try {
            response = task.call();
            // 重定向到leader节点
            if (ResponseCode.NOT_LEADER.getValue() == response.getCode()) {
                Server server = (Server) response.getData();
                connect(server);
                return execute(task, count + 1);
            }
        } catch (Throwable e) {
            // 处理节点宕机情况, 重试其他节点
            if (e instanceof UndeclaredThrowableException) {
                UndeclaredThrowableException ex = (UndeclaredThrowableException) e;
                Throwable undeclaredThrowable = ex.getUndeclaredThrowable();
                if (undeclaredThrowable instanceof ConnectionException) {
                    // 优化节点选择
                    connect(cluster.getServerList().get(count + 1));
                    return execute(task, count + 1);
                }
            }
            throw new RaftException("execute call remote error", e);
        }
        return response;
    }
}
