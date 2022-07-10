package com.yundepot.raft.util;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author zhaiyanan
 * @date 2019/6/21 19:27
 */
public class RandomUtil {

    /**
     * 获取随机数，从range 到 2 * range
     * @param range
     * @return
     */
    public static long getRangeLong(long range) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return random.nextLong(range, 2 * range);
    }
}
