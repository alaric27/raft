package com.yundepot.raft.service;


import com.yundepot.raft.bean.Pair;
import com.yundepot.raft.bean.Range;
import com.yundepot.raft.bean.Response;

/**
 * @author zhaiyanan
 * @date 2019/6/20 20:53
 */
public interface PairService {

    /**
     * 写入数据，带过期时间
     */
    Response set(Pair pair);

    /**
     * 读取数据
     */
    Response get(byte[] key);

    /**
     * 删除
     */
    Response delete(byte[] key);

    /**
     * 范围删除
     */
    Response deleteRange(Range range);
}
