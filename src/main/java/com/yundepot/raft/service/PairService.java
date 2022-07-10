package com.yundepot.raft.service;


import com.yundepot.raft.bean.Pair;

/**
 * @author zhaiyanan
 * @date 2019/6/20 20:53
 */
public interface PairService {

    /**
     * 写入数据
     * @param pair
     */
    void put(Pair pair);

    /**
     * 写入数据
     */
    void put(byte[] key, byte[] value);

    /**
     * 读取数据
     * @param key
     * @return
     */
    byte[] get(byte[] key);
}
