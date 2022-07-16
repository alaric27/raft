package com.yundepot.raft.store;

import cn.hutool.core.io.FileUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yundepot.raft.bean.ClusterConfig;
import com.yundepot.raft.util.RaftFileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * 集群配置, 为了重启后不扫描所有日志获取最信息集群配置, 每次集群配置更新时持久化到本地文件
 * 该类方法的调用都在RaftNode的大锁中
 * @author zhaiyanan
 * @date 2022/7/16  09:26
 */
public class ClusterConfigStore {
    private String fileName;
    private ClusterConfig cluster;

    public ClusterConfigStore(String rootDir) {
        this.fileName = rootDir + File.separator + "ClusterConfig";
    }

    public void load() {
        File file = new File(fileName);
        if (file.exists()) {
            String content = FileUtil.readString(file, StandardCharsets.UTF_8);
            this.cluster = JSON.parseObject(content, new TypeReference<ClusterConfig>(){}.getType());
        }
    }

    public void update(ClusterConfig cluster) {
        this.cluster = cluster;
        RaftFileUtils.updateFile(fileName, JSON.toJSONString(cluster));
    }

    public ClusterConfig get() {
        return this.cluster;
    }
}
