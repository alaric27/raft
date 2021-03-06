package com.yundepot.raft.service;

import com.alibaba.fastjson.JSON;
import com.yundepot.raft.RaftNode;
import com.yundepot.raft.bean.Configuration;
import com.yundepot.raft.bean.Peer;
import com.yundepot.raft.bean.Response;
import com.yundepot.raft.bean.Server;
import com.yundepot.raft.common.LogType;
import com.yundepot.raft.common.ResponseCode;
import com.yundepot.raft.util.ConfigUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaiyanan
 * @date 2019/6/16 10:24
 */
@Slf4j
public class RaftAdminServiceImpl implements RaftAdminService {
    private RaftNode raftNode;

    public RaftAdminServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public Server getLeader() {
        return ConfigUtil.getServer(raftNode.getConfiguration(), raftNode.getLeaderId());
    }

    @Override
    public Configuration getConfiguration() {
        return raftNode.getConfiguration();
    }

    @Override
    public Response addPeer(Server server) {
        Response response = new Response();
        response.setCode(ResponseCode.FAIL.getValue());

        raftNode.getLock().lock();
        try {
            if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
                return Response.fail(ResponseCode.NOT_LEADER.getValue(), ConfigUtil.getServer(raftNode.getConfiguration(), raftNode.getLeaderId()));
            }

            if (raftNode.getPeerMap().containsKey(server.getServerId())) {
                response.setMsg("already be added/adding to cluster");
                return response;
            }

            if (raftNode.getLastLogTerm() != raftNode.getCurrentTerm()) {
                response.setMsg("current term have not commit any log, can not add peer");
                return response;
            }

            Peer peer = new Peer(server);
            peer.setNextIndex(1);
            raftNode.getPeerMap().putIfAbsent(server.getServerId(), peer);
            raftNode.getExecutorService().submit(() -> raftNode.appendLog(peer));

            // ????????????????????????leader??????
            raftNode.getCatchUpCondition().awaitUninterruptibly();
            if (!peer.isCatchUp()) {
                peer.getPeerClient().shutdown();
                raftNode.getPeerMap().remove(peer.getServer().getServerId());
                return response;
            }

            List<Server> newServerList = new ArrayList<>();
            newServerList.add(server);
            newServerList.addAll(raftNode.getConfiguration().getServerList());
            byte[] bytes = JSON.toJSONBytes(newServerList);
            // ?????????????????????????????????
            boolean success = raftNode.replicate(bytes, LogType.CONFIG);

            if (!success) {
                peer.getPeerClient().shutdown();
                raftNode.getPeerMap().remove(peer.getServer().getServerId());
                return response;
            }
            response.setCode(ResponseCode.SUCCESS.getValue());
        } finally {
            raftNode.getLock().unlock();
        }
        return response;
    }

    @Override
    public Response removePeer(Server server) {
        Response response = new Response();
        response.setCode(ResponseCode.FAIL.getValue());
        raftNode.getLock().lock();
        try {
            if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
                return Response.fail(ResponseCode.NOT_LEADER.getValue(), ConfigUtil.getServer(raftNode.getConfiguration(), raftNode.getLeaderId()));
            }

            if (!ConfigUtil.containsServer(raftNode.getConfiguration(), server.getServerId())) {
                return response;
            }

            if (raftNode.getLastLogTerm() != raftNode.getCurrentTerm()) {
                response.setMsg("current leader have not commit any log, can not add peer");
                return response;
            }

            List<Server> newServerList = ConfigUtil.removeServer(raftNode.getConfiguration().getServerList(), server.getServerId());
            byte[] bytes = JSON.toJSONBytes(newServerList);
            boolean success = raftNode.replicate(bytes, LogType.CONFIG);
            if (success) {
                response.setCode(ResponseCode.SUCCESS.getValue());
            }
        } finally {
            raftNode.getLock().unlock();
        }
        return response;
    }

    @Override
    public long getCommitIndex() {
        return raftNode.getCommitIndex();
    }
}
