package com.yundepot.raft.util;


import com.yundepot.raft.bean.Cluster;
import com.yundepot.raft.bean.Server;

import java.util.ArrayList;
import java.util.List;

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
        return containsServer(cluster.getServerList(), serverId);
    }

    public static boolean containsServer(List<Server> serverList, int serverId) {
        for (Server server : serverList) {
            if (server.getServerId() == serverId) {
                return true;
            }
        }
        return false;
    }

    public static List<Server> removeServer(List<Server> serverList, int serverId) {
        List<Server> list = new ArrayList<>();
        for (Server server : serverList) {
            if (serverId != server.getServerId()) {
                list.add(server);
            }
        }
        return list;
    }

    /**
     * 根据节点id，获取集群内节点
     * @param cluster
     * @param serverId
     * @return
     */
    public static Server getServer(Cluster cluster, int serverId) {
        return getServer(cluster.getServerList(), serverId);
    }

    public static Server getServer(List<Server> serverList, int serverId) {
        for (Server server : serverList) {
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
        List<Server> serverList = new ArrayList<>();
        String[] splitArray = clusterInfo.split(",");
        for (String serverString : splitArray) {
            serverList.add(parserServer(serverString));
        }
        Cluster cluster = new Cluster();
        cluster.setServerList(serverList);
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