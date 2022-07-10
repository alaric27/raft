package com.yundepot.raft.common;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:18
 */
public enum ResponseCode {
    SUCCESS(1),
    FAIL(2),
    NOT_LEADER(3);

    private final int value;

    ResponseCode(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
