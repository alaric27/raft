package com.yundepot.raft;

import com.yundepot.oaa.config.GenericOption;
import com.yundepot.raft.service.RaftService;
import com.yundepot.rpc.RpcClient;
import lombok.Data;

/**
 * @author zhaiyanan
 * @date 2019/6/15 17:30
 */
@Data
public class RaftClient {

    private final String host;
    private final int port;
    private RpcClient rpcClient;
    private RaftService raftService;

    public RaftClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.rpcClient = new RpcClient(host, port);
        this.rpcClient.option(GenericOption.TCP_HEARTBEAT_SWITCH, false);
        this.rpcClient.start();
        raftService = this.rpcClient.create(RaftService.class);
    }

    public void shutdown() {
        this.rpcClient.shutdown();
    }
}
