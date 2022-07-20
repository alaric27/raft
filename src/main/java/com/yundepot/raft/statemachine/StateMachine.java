package com.yundepot.raft.statemachine;

import com.yundepot.raft.bean.InstallSnapshotRequest;
import com.yundepot.raft.bean.SnapshotDataFile;
import com.yundepot.raft.bean.SnapshotMetadata;

import java.util.List;

/**
 *
 * @author zhaiyanan
 * @date 2019/6/15 15:15
 */
public interface StateMachine {

    /**
     * 向状态机写入数据
     */
    void set(byte[] key, byte[] value, long timeout);

    /**
     * 读取数据
     */
    byte[] get(byte[] key);

    /**
     * 删除数据
     */
    void delete(byte[] key);

    /**
     * 范围删除
     */
    void deleteRange(byte[] begin, byte[] end);

    /**
     * 写入集群配置
     */
    void setConfig(byte[] value);

    /**
     * 读取集群配置
     */
    byte[] getConfig();

    /**
     * 加载快照到状态机
     */
    void loadSnapshot();

    /**
     * 生成快照
     */
    void takeSnapshot(SnapshotMetadata metadata);

    /**
     * 向状态机安装快照,用于follower接收leader的快照
     */
    boolean installSnapshot(InstallSnapshotRequest request);

    /**
     * 获取快照元数据
     */
    SnapshotMetadata getMetadata();

    /**
     * 打开快照文件
     * @return
     */
    List<SnapshotDataFile> openSnapshotDataFile();

    /**
     * 关闭快照文件
     */
    void closeSnapshotDataFile(List<SnapshotDataFile> list);

    /**
     * 关闭状态机
     */
    void shutdown();
}
