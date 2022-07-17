package com.yundepot.raft.util;

import com.yundepot.raft.bean.Pair;

import java.nio.ByteBuffer;

/**
 * @author zhaiyanan
 * @date 2022/7/1  11:27
 */
public class ByteUtil {

    public static byte[] encode(byte[] key, byte[] value, long timeout) {
        int len = 4 + key.length + 4 + value.length + 8;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putInt(value.length);
        buffer.put(value);
        buffer.putLong(timeout);
        return buffer.array();
    }

    public static Pair decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] key = new byte[buffer.getInt()];
        buffer.get(key);
        byte[] value = new byte[buffer.getInt()];
        buffer.get(value);
        long timeout = buffer.getLong();
        return new Pair(key, value, timeout);
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

    public static byte[] compose(byte[] bytes1, long timeout) {
        byte[] bytes2 = longToBytes(timeout);
        ByteBuffer buffer = ByteBuffer.allocate(bytes1.length + bytes2.length);
        buffer.put(bytes1);
        buffer.put(bytes2);
        return buffer.array();
    }

    public static Pair decompose(byte[] bytes) {
        Pair pair = new Pair();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] value = new byte[bytes.length - 8];
        buffer.get(value);
        pair.setValue(value);
        pair.setTimeout(buffer.getLong());
        return pair;
    }
}
