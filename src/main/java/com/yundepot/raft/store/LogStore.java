package com.yundepot.raft.store;

import com.yundepot.raft.bean.LogEntry;
import java.util.List;

/**
 * @author zhaiyanan
 * @date 2022/6/25  09:45
 */
public interface LogStore {

    /**
     * 加载日志
     */
    void loadLog();

    /**
     * 获取第一条日志索引
     * @return
     */
    long getFirstLogIndex();

    /**
     * 获取最后一条日志索引
     * @return
     */
    long getLastLogIndex();

    /**
     * 根据索引获取日志条目
     * @param logIndex
     * @return
     */
    LogEntry getEntry(long logIndex);

    /**
     * 追加添加日志条目
     * @param logEntryList
     * @return
     */
    void append(List<LogEntry> logEntryList);

    /**
     * 删除日志 [firstLogIndex, index)
     * @param logIndex
     * @return
     */
    void deletePrefix(long logIndex);

    /**
     * 删除日志 (index, lastLogIndex]
     * @param logIndex
     * @return
     */
    void deleteSuffix(long logIndex);

    /**
     * 日志文件大小
     */
    long getFileSize();

    /**
     * 关闭日志存储
     */
    void shutdown();
}
