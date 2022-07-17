package com.yundepot.raft.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author zhaiyanan
 * @date 2019/6/17 14:31
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Pair implements Serializable {
    private byte[] key;
    private byte[] value;
    private long timeout;

    public Pair(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }
}
