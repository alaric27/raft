package com.yundepot.raft.common;

/**
 * @author zhaiyanan
 * @date 2022/7/13  13:43
 */
public enum ConsistencyLevel {
    /**
     * 最终一致性
     */
    FINAL(0),

    /**
     * 线性一致性
     */
    LINE(1);

    private final int value;

    ConsistencyLevel(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
