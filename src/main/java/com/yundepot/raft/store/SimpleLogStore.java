package com.yundepot.raft.store;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileMode;
import com.yundepot.raft.bean.LogEntry;
import com.yundepot.raft.exception.RaftException;
import com.yundepot.raft.util.RaftFileUtils;
import com.yundepot.raft.util.LogEntryUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * 简单日志存储类, 推荐使用 RocksDBLogStore
 * 非线程安全，依赖上层业务锁
 * @author zhaiyanan
 * @date 2022/6/25  10:07
 */
@Slf4j
public class SimpleLogStore implements LogStore {

    private String logDir;
    private String logFile = "raft.log";
    private String tmpLogFile = "raft.log.tmp";
    private List<LogEntry> entries = new ArrayList<>();
    /**
     * 随机文件读取
     */
    private RandomAccessFile randomAccessFile;

    public SimpleLogStore(String rootDir) {
        this.logDir = rootDir + "log" + File.separator;
        RaftFileUtils.createDir(logDir);
        checkLogFile();
    }

    /**
     * 启动加载日志
     */
    @Override
    public void loadLog() {
        try {
            byte[] bytes = new byte[(int) randomAccessFile.length()];
            randomAccessFile.read(bytes);
            entries = LogEntryUtil.decodeList(bytes);
        } catch (Exception e) {
            throw new RaftException("load simple log error", e);
        }
    }

    @Override
    public long getFirstLogIndex() {
        if (!entries.isEmpty()) {
            return entries.get(0).getIndex();
        }
        return 0;
    }

    @Override
    public long getLastLogIndex() {
        if (!entries.isEmpty()) {
            return entries.get(entries.size() - 1).getIndex();
        }
        return 0;
    }

    @Override
    public LogEntry getEntry(long logIndex) {
        try {
            long firstLogIndex = getFirstLogIndex();
            long lastLogIndex = getLastLogIndex();
            if (logIndex == 0 || logIndex < firstLogIndex || logIndex > lastLogIndex) {
                log.debug("logIndex out of range, logIndex={}, firstLogIndex={}, lastLogIndex={}", logIndex, firstLogIndex, lastLogIndex);
                return null;
            }
            int indexInList = (int) (logIndex - firstLogIndex);
            return entries.get(indexInList);
        } catch (Exception e) {
            log.error("get log entry error logIndex = {}", logIndex, e);
            throw new RaftException("get log entry error logIndex = " + logIndex, e);
        }
    }

    @Override
    public void append(List<LogEntry> logEntryList) {
        try {
            entries.addAll(logEntryList);
            byte[] bytes = LogEntryUtil.encodeList(logEntryList);
            randomAccessFile.write(bytes);
        } catch (Exception e) {
            log.error("append raft log exception", e);
            throw new RaftException("append log error", e);
        }
    }

    @Override
    public void deletePrefix(long logIndex) {
        try {
            long firstLogIndex = getFirstLogIndex();
            if (logIndex <= firstLogIndex) {
                return;
            }

            // 只保留 logIndex及之后的日志
            int start = (int) (logIndex - firstLogIndex);
            List<LogEntry> entryList = CollectionUtil.sub(entries, start, entries.size());
            retainToFile(entryList);
            entries = entryList;
        } catch (Exception e) {
            log.warn("deletePrefix logIndex: {} error", logIndex, e);
        }
    }

    @Override
    public void deleteSuffix(long logIndex) {
        try {
            long firstLogIndex = getFirstLogIndex();
            long lastLogIndex = getLastLogIndex();
            if (logIndex >= lastLogIndex || logIndex <= firstLogIndex - 1) {
                return;
            }

            // 删除logIndex之后的日志
            int start = (int) (logIndex - firstLogIndex) + 1;
            List<LogEntry> entryList = CollectionUtil.sub(entries, start, entries.size());
            entryList.removeAll(entryList);
            byte[] bytes = LogEntryUtil.encodeList(entryList);
            FileChannel channel = randomAccessFile.getChannel();
            channel.truncate(randomAccessFile.length() - bytes.length);
        } catch (Exception e) {
            log.warn("deleteSuffix logIndex: {} error", logIndex, e);
        }
    }

    @Override
    public long getFileSize() {
        try {
            return randomAccessFile.length();
        } catch (Exception e) {
            throw new RaftException("get file size error", e);
        }
    }

    @Override
    public void shutdown() {
        RaftFileUtils.closeFile(randomAccessFile);
    }

    private void retainToFile(List<LogEntry> entryList) throws Exception{
        String logFileName = logDir + logFile;
        String tmpFileName = logDir + tmpLogFile;

        File tmpFile = new File(tmpFileName);
        if (tmpFile.exists()) {
            FileUtil.del(tmpFile);
        }

        // 写入临时文件
        byte[] bytes = LogEntryUtil.encodeList(entryList);
        FileUtil.writeBytes(bytes, tmpFile);
        // 删除旧文件
        randomAccessFile.close();
        FileUtil.del(logFileName);
        // 重命名临时文件
        new File(tmpFileName).renameTo(new File(logFileName));
        randomAccessFile = FileUtil.createRandomAccessFile(new File(logFileName), FileMode.rw);
        randomAccessFile.seek(randomAccessFile.length());
    }

    private void checkLogFile() {
        try {
            String logFileName = logDir + logFile;
            String tmpFileName = logDir + tmpLogFile;
            File logFile = new File(logFileName);
            File tmpFile = new File(tmpFileName);

            if (tmpFile.exists() && !logFile.exists()) {
                tmpFile.renameTo(logFile);
            }
            randomAccessFile = FileUtil.createRandomAccessFile(logFile, FileMode.rw);
        } catch (Exception e) {
            throw new RaftException("checkLogFile", e);
        }
    }
}
