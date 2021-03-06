package com.yundepot.raft.common;

import java.nio.charset.StandardCharsets;

/**
 * @author zhaiyanan
 * @date 2022/6/22  10:37
 */
public class Constant {

    /**
     * 代表没有leader
     */
    public static final int ZERO = 0;
    public static final byte[] CONFIG = "config".getBytes(StandardCharsets.UTF_8);
}
