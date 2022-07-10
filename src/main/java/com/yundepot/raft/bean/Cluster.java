package com.yundepot.raft.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Set;

/**
 * @author zhaiyanan
 * @date 2019/6/21 16:34
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Cluster implements Serializable {
    private Set<Server> servers;
}
