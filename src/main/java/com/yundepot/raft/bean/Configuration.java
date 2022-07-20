package com.yundepot.raft.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * 集群配置
 * @author zhaiyanan
 * @date 2019/6/21 16:34
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Configuration implements Serializable {

    /**
     * 集群中的节点
     */
    private List<Server> serverList;
}
