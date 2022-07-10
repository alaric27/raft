package com.yundepot.raft.store;

import com.yundepot.raft.bean.LogEntry;
import com.yundepot.raft.config.RaftConfig;
import com.yundepot.raft.exception.RaftException;
import com.yundepot.raft.util.ByteUtil;
import com.yundepot.raft.util.LogEntryUtil;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 基于rocksdb的日志存储
 * 非线程安全，所有方法的调用都在上层RaftNode的锁中
 * @author zhaiyanan
 * @date 2022/7/5  10:24
 */
@Slf4j
public class RocksDBLogStore implements LogStore{
    static {
        RocksDB.loadLibrary();
    }

    private RocksDB rocksDB;
    private ColumnFamilyHandle defaultHandle;
    private WriteOptions writeOptions;
    private String logDir;

    /**
     * 为了效率额外维护了 firstLogIndex 和 lastLogIndex
     */
    private long firstLogIndex;
    private long lastLogIndex;

    public RocksDBLogStore(RaftConfig config) {
        this.logDir = config.getRootDir() + "log" + File.separator;
        writeOptions = new WriteOptions();
        writeOptions.setSync(config.isSyncWriteLogFile());
    }


    @Override
    public void loadLog() {
        try {
            DBOptions options = new DBOptions();
            options.setCreateIfMissing(true);
            options.setCreateMissingColumnFamilies(true);

            List<ColumnFamilyDescriptor> descriptorList = new ArrayList<>();
            descriptorList.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

            List<ColumnFamilyHandle> handleList = new ArrayList<>();
            rocksDB = RocksDB.open(options, logDir, descriptorList, handleList);
            assert (handleList.size() == 1);

            defaultHandle = handleList.get(0);
        } catch (RocksDBException e) {
            log.error("load log error", e);
            throw new RaftException("load log error", e);
        }
    }

    @Override
    public long getFirstLogIndex() {
        if (firstLogIndex != 0) {
            return firstLogIndex;
        }

        RocksIterator it = rocksDB.newIterator(defaultHandle);
        it.seekToFirst();
        if (it.isValid()) {
            firstLogIndex = ByteUtil.bytesToLong(it.key());
            return firstLogIndex;
        }
        return 0;
    }

    @Override
    public long getLastLogIndex() {
        if (lastLogIndex != 0) {
            return lastLogIndex;
        }

        RocksIterator it = rocksDB.newIterator(defaultHandle);
        it.seekToLast();
        if (it.isValid()) {
            lastLogIndex = ByteUtil.bytesToLong(it.key());
            return lastLogIndex;
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
            byte[] bytes = rocksDB.get(defaultHandle, ByteUtil.longToBytes(logIndex));
            if (bytes == null || bytes.length <= 0) {
                return null;
            }
            return LogEntryUtil.decode(bytes);
        } catch (RocksDBException e) {
            log.error("get log entry error logIndex = {}", logIndex, e);
            throw new RaftException("get log entry error logIndex = " + logIndex, e);
        }
    }

    @Override
    public void append(List<LogEntry> logEntryList) {
        try {
            WriteBatch writeBatch = new WriteBatch();
            for (LogEntry logEntry : logEntryList) {
                byte[] key = ByteUtil.longToBytes(logEntry.getIndex());
                writeBatch.put(defaultHandle, key, LogEntryUtil.encode(logEntry));
            }
            rocksDB.write(writeOptions, writeBatch);
            lastLogIndex = logEntryList.get(logEntryList.size() - 1).getIndex();
        } catch (RocksDBException e) {
            log.error("append raft log exception", e);
            throw new RaftException("append log error", e);
        }
    }

    @Override
    public void deletePrefix(long logIndex) {
        try {
            byte[] begin = ByteUtil.longToBytes(getFirstLogIndex());
            byte[] end = ByteUtil.longToBytes(logIndex);
            rocksDB.deleteRange(defaultHandle, begin, end);
            // 删除sst文件, 释放磁盘空间
            rocksDB.deleteFilesInRanges(defaultHandle, Arrays.asList(begin, end), false);
            firstLogIndex = logIndex;
        } catch (RocksDBException e) {
            log.error("deletePrefix error logIndex: {}", logIndex, e);
            throw new RaftException("deletePrefix error", e);
        }
    }

    @Override
    public void deleteSuffix(long logIndex) {
        try {
            byte[] begin = ByteUtil.longToBytes(logIndex + 1);
            byte[] end = ByteUtil.longToBytes(getLastLogIndex() + 1);
            rocksDB.deleteRange(defaultHandle, begin, end);
            rocksDB.deleteFilesInRanges(defaultHandle, Arrays.asList(begin, end), false);
            lastLogIndex = logIndex;
        } catch (RocksDBException e) {
            log.error("deletePrefix error logIndex: {}", logIndex, e);
            throw new RaftException("deletePrefix error", e);
        }
    }

    @Override
    public long getFileSize() {
        return rocksDB.getColumnFamilyMetaData().size();
    }

    @Override
    public void shutdown() {
        defaultHandle.close();
        rocksDB.close();
    }
}
