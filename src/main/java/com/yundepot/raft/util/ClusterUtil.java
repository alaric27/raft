package com.yundepot.raft.util;


import com.yundepot.raft.bean.Cluster;
import com.yundepot.raft.bean.Server;

import java.util.HashSet;
import java.util.Set;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:33
 */
public class ClusterUtil {

    /**
     * 根据节点id，判断集群内是否包含该节点
     * @param cluster
     * @param serverId
     * @return
     */
    public static boolean containsServer(Cluster cluster, int serverId) {
        return containsServer(cluster.getServers(), serverId);
    }

    public static boolean containsServer(Set<Server> servers, int serverId) {
        for (Server server : servers) {
            if (server.getServerId() == serverId) {
                return true;
            }
        }
        return false;
    }

    public static Set<Server> removeServer(Set<Server> servers, int serverId) {
        Set<Server> newSet = new HashSet<>();
        for (Server server : servers) {
            if (serverId != server.getServerId()) {
                newSet.add(server);
            }
        }
        return newSet;
    }

    /**
     * 根据节点id，获取集群内节点
     * @param cluster
     * @param serverId
     * @return
     */
    public static Server getServer(Cluster cluster, int serverId) {
        return getServer(cluster.getServers(), serverId);
    }

    public static Server getServer(Set<Server> servers, int serverId) {
        for (Server server : servers) {
            if (server.getServerId() == serverId) {
                return server;
            }
        }
        return null;
    }

    /**
     * 解析集群配置
     * @param clusterInfo
     * @return
     */
    public static Cluster parserCluster(String clusterInfo) {
        Set<Server> servers = new HashSet<>();
        String[] splitArray = clusterInfo.split(",");
        for (String serverString : splitArray) {
            servers.add(parserServer(serverString));
        }
        Cluster cluster = new Cluster();
        cluster.setServers(servers);
        return cluster;
    }

    /**
     * 解析server
     * @param serverString
     * @return
     */
    public static Server parserServer(String serverString) {
        String[] splitServer = serverString.split(":");
        Server server = new Server();
        server.setServerId(Integer.parseInt(splitServer[0]));
        server.setHost(splitServer[1]);
        server.setPort(Integer.parseInt(splitServer[2]));
        return server;
    }
}