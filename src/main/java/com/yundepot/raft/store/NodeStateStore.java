package com.yundepot.raft.store;

import cn.hutool.core.io.FileUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yundepot.raft.bean.NodeState;
import com.yundepot.raft.util.RaftFileUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * 该类方法的调用都在RaftNode的大锁中
 * @author zhaiyanan
 * @date 2022/6/26  13:21
 */
@Slf4j
public class NodeStateStore {

    private String fileName;
    private NodeState nodeState;

    public NodeStateStore(String rootDir) {
        this.fileName = rootDir + File.separator + "NodeState";
    }

    public void load() {
        File file = new File(fileName);
        if (file.exists()) {
            String content = FileUtil.readString(file, StandardCharsets.UTF_8);
            this.nodeState = JSON.parseObject(content, new TypeReference<NodeState>(){}.getType());
        }

        if (nodeState == null) {
            nodeState = new NodeState();
        }
    }

    public void update(long currentTerm, int votedFor) {
        nodeState.setCurrentTerm(currentTerm);
        nodeState.setVotedFor(votedFor);
        RaftFileUtils.updateFile(fileName, JSON.toJSONString(nodeState));
    }

    public NodeState get() {
        return nodeState;
    }
}
