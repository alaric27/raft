package com.yundepot.raft.bean;

import lombok.Data;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:27
 */
@Data
public class Server implements Serializable {
    private int serverId;
    private String host;
    private int port;

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Server server = (Server) obj;
        return serverId == server.serverId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId);
    }
}
