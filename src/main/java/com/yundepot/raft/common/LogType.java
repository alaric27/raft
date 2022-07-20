package com.yundepot.raft.common;

/**
 * @author zhaiyanan
 * @date 2019/6/15 18:11
 */
public enum LogType {
    SET(1),
    CONFIG(2),
    DELETE(3),
    DELETE_RANGE(4);

    private final int value;

    LogType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
