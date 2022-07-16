package com.yundepot.raft.service;


import com.yundepot.raft.bean.Response;

/**
 * @author zhaiyanan
 * @date 2019/6/20 20:53
 */
public interface PairService {

    /**
     * 写入数据
     */
    Response set(byte[] key, byte[] value);

    /**
     * 读取数据
     * @param key
     * @return
     */
    Response get(byte[] key);

    /**
     * 删除
     * @param key
     * @return
     */
    Response delete(byte[] key);
}
