package com.yundepot.raft;

import com.yundepot.oaa.common.AbstractLifeCycle;
import com.yundepot.oaa.config.GenericOption;
import com.yundepot.oaa.util.StringUtils;
import com.yundepot.raft.bean.Server;
import com.yundepot.raft.config.RaftConfig;
import com.yundepot.raft.exception.RaftException;
import com.yundepot.raft.service.*;
import com.yundepot.raft.util.ConfigUtil;
import com.yundepot.raft.util.RaftConfigUtil;
import com.yundepot.rpc.RpcServer;

import java.io.File;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:33
 */
public class RaftServer extends AbstractLifeCycle {
    private RaftNode raftNode;
    private RpcServer rpcServer;
    private Server localServer;

    public RaftServer(RaftConfig raftConfig) {
        this.raftNode = new RaftNode(raftConfig);
        this.localServer = ConfigUtil.getServer(ConfigUtil.parserConfig(raftConfig.getCluster()), raftConfig.getServer());
        initRpc();
    }

    private void initRpc() {
        this.rpcServer = new RpcServer(localServer.getPort());
        this.rpcServer.option(GenericOption.TCP_HEARTBEAT_SWITCH, false);
        // 注册Raft节点之间相互调用的服务
        RaftService raftService = new RaftServiceImpl(raftNode);
        rpcServer.addService(RaftService.class.getName(), raftService);

        // 注册集群管理服务
        RaftAdminService raftAdminService = new RaftAdminServiceImpl(raftNode);
        rpcServer.addService(RaftAdminService.class.getName(), raftAdminService);

        // 注册应用自己提供的服务
        PairService pairService = new PairServiceImpl(raftNode);
        rpcServer.addService(PairService.class.getName(), pairService);
    }

    @Override
    public void start() {
        super.start();
        raftNode.start();
        rpcServer.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        raftNode.shutdown();
        rpcServer.shutdown();
    }

    public static void main(String[] args) {
        String configFile = null;
        if (args.length > 0) {
            configFile = args[0];
        }

        RaftConfig raftConfig = RaftConfigUtil.getRaftConfig(configFile);
        checkRaftConfig(raftConfig);
        RaftServer raftServer = new RaftServer(raftConfig);
        raftServer.start();
    }

    private static void checkRaftConfig(RaftConfig config) {
        if (StringUtils.isEmpty(config.getCluster())) {
            throw new RaftException("cluster is null");
        }

        if (StringUtils.isEmpty(config.getRootDir())) {
            throw new RaftException("rootDir is null");
        }
        String rootDir = config.getRootDir();
        if (!rootDir.endsWith(File.separator)) {
            config.setRootDir(config.getRootDir() + File.separator);
        }
    }
}
