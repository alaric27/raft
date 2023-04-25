package com.yundepot.raft.bean;

import com.yundepot.raft.common.ConsistencyLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author zhaiyanan
 * @date 2023/4/25  14:27
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GetRequest implements Serializable {
    private byte[] key;

    /**
     * 一致性类型, 默认最终一致性
     */
    private int consistencyLevel = ConsistencyLevel.FINAL.getValue();

    public GetRequest(byte[] key) {
        this.key = key;
    }
}
