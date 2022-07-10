package com.yundepot.raft.util;

import com.yundepot.raft.bean.Pair;

import java.nio.ByteBuffer;

/**
 * @author zhaiyanan
 * @date 2022/7/1  11:27
 */
public class ByteUtil {

    public static byte[] encode(byte[] key, byte[] value) {
        int len = 4 + key.length + 4 + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putInt(value.length);
        buffer.put(value);
        return buffer.array();
    }

    public static Pair decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] key = new byte[buffer.getInt()];
        buffer.get(key);
        byte[] value = new byte[buffer.getInt()];
        buffer.get(value);
        return new Pair(key, value);
    }

    /**
     * 大端序
     * @param value
     * @return
     */
    public static byte[] longToBytes(long value) {
        byte[] bytes = new byte[Long.BYTES];
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            bytes[i] = (byte) (value & 0xFF);
            value >>= Byte.SIZE;
        }
        return bytes;
    }

    /**
     * bytesToLong 大端序
     * @param bytes
     * @return
     */
    public static long bytesToLong(byte[] bytes) {
        long values = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            values <<= Byte.SIZE;
            values |= (bytes[i] & 0xff);
        }
        return values;
    }
}
