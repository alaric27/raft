package com.yundepot.raft.store;

import cn.hutool.core.io.FileUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yundepot.raft.bean.Configuration;
import com.yundepot.raft.util.RaftFileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * 集群配置, 为了重启后不扫描所有日志获取最信息集群配置, 每次集群配置更新时持久化到本地文件
 * 该类方法的调用都在RaftNode的大锁中
 * @author zhaiyanan
 * @date 2022/7/16  09:26
 */
public class ConfigurationStore {
    private final String fileName;
    private Configuration config;

    public ConfigurationStore(String rootDir) {
        this.fileName = rootDir + File.separator + "Configuration";
    }

    public void load() {
        File file = new File(fileName);
        if (file.exists()) {
            String content = FileUtil.readString(file, StandardCharsets.UTF_8);
            this.config = JSON.parseObject(content, new TypeReference<Configuration>(){}.getType());
        }
    }

    public void update(Configuration config) {
        this.config = config;
        RaftFileUtils.updateFile(fileName, JSON.toJSONString(config));
    }

    public Configuration get() {
        return this.config;
    }
}
