package com.yundepot.raft.util;


import com.yundepot.raft.bean.Configuration;
import com.yundepot.raft.bean.Server;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:33
 */
public class ConfigUtil {

    /**
     * 根据节点id，判断集群内是否包含该节点
     * @param config
     * @param serverId
     * @return
     */
    public static boolean containsServer(Configuration config, int serverId) {
        return containsServer(config.getServerList(), serverId);
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
     * @param config
     * @param serverId
     * @return
     */
    public static Server getServer(Configuration config, int serverId) {
        return getServer(config.getServerList(), serverId);
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
     * @param serverListStr
     * @return
     */
    public static Configuration parserConfig(String serverListStr) {
        List<Server> serverList = new ArrayList<>();
        String[] splitArray = serverListStr.split(",");
        for (String serverString : splitArray) {
            serverList.add(parserServer(serverString));
        }
        Configuration config = new Configuration();
        config.setServerList(serverList);
        return config;
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