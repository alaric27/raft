package com.yundepot.raft.util;

import com.yundepot.oaa.util.CrcUtil;
import com.yundepot.raft.bean.LogEntry;
import com.yundepot.raft.exception.RaftException;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据格式: term -> index -> logType -> dataLen -> data -> crc32
 * @author zhaiyanan
 * @date 2022/6/25  21:13
 */
@Slf4j
public class LogEntryUtil {

    public static byte[] encode(LogEntry logEntry) {
        byte[] bytes = logEntry.getData();
        int dataLen = bytes == null ? 0 : bytes.length;
        int len = 28 + dataLen;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        long term = logEntry.getTerm();
        long index =  logEntry.getIndex();
        int logType = logEntry.getLogType();

        buffer.putLong(term);
        buffer.putLong(index);
        buffer.putInt(logType);
        buffer.putInt(dataLen);
        if (bytes != null) {
            buffer.put(bytes);
        }
        buffer.putInt(getCrc32(term, index, logType, dataLen, bytes));
        return buffer.array();
    }

    /**
     * 编码日志
     * @param logEntryList
     * @return
     */
    public static byte[] encodeList(List<LogEntry> logEntryList) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for (LogEntry entry : logEntryList) {
                outputStream.write(encode(entry));
            }
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new RaftException("encodeList error", e);
        }
    }

    public static LogEntry decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        LogEntry logEntry = new LogEntry();
        while (buffer.hasRemaining()) {
            long term = buffer.getLong();
            long index = buffer.getLong();
            int logType = buffer.getInt();
            int dataLen = buffer.getInt();
            byte[] data = new byte[dataLen];
            buffer.get(data);
            int crc32 = buffer.getInt();
            int newCrc32 = getCrc32(term, index, logType, dataLen, data);
            if (crc32 != newCrc32) {
                throw new RaftException("check crc32 error term = " + term + " index = " + index);
            }
            logEntry.setTerm(term);
            logEntry.setIndex(index);
            logEntry.setLogType(logType);
            logEntry.setData(data);
        }
        return logEntry;
    }

    /**
     * 解码日志
     */
    public static List<LogEntry> decodeList(byte[] bytes) {
        List<LogEntry> entryList = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
            long term = buffer.getLong();
            long index = buffer.getLong();
            int logType = buffer.getInt();
            int dataLen = buffer.getInt();
            byte[] data = new byte[dataLen];
            buffer.get(data);
            int crc32 = buffer.getInt();
            int newCrc32 = getCrc32(term, index, logType, dataLen, data);
            if (crc32 != newCrc32) {
                throw new RaftException("check crc32 error term = " + term + " index = " + index);
            }
            LogEntry logEntry = new LogEntry();
            logEntry.setTerm(term);
            logEntry.setIndex(index);
            logEntry.setLogType(logType);
            logEntry.setData(data);
            entryList.add(logEntry);
        }
        return entryList;
    }

    private static int getCrc32(long term, long index, int logType, int dataLen, byte[] data) {
        int len = 24 + dataLen;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.putLong(term);
        buffer.putLong(index);
        buffer.putInt(logType);
        buffer.putInt(dataLen);
        if (data != null) {
            buffer.put(data);
        }
        return CrcUtil.crc32(buffer.array());
    }
}
