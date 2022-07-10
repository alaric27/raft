package com.yundepot.raft;

import com.yundepot.oaa.util.StringUtils;
import com.yundepot.raft.config.RaftConfig;
import com.yundepot.raft.exception.RaftException;
import com.yundepot.raft.util.RaftConfigUtil;

import java.io.File;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:33
 */
public class RaftServer {
    public static void main(String[] args) {
        String configFile = null;
        if (args.length > 0) {
            configFile = args[0];
        }

        RaftConfig raftConfig = RaftConfigUtil.getRaftConfig(configFile);
        checkRaftConfig(raftConfig);
        RaftNode raftNode = new RaftNode(raftConfig);
        raftNode.start();
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
