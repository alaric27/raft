package com.yundepot.raft.bean;

import com.yundepot.raft.common.ResponseCode;
import lombok.Data;

import java.io.Serializable;

/**
 * @author zhaiyanan
 * @date 2019/6/16 10:22
 */
@Data
public class Response<T> implements Serializable {

    public Response() {
        this(ResponseCode.SUCCESS.getValue(), "成功");
    }

    public Response(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    private int code;
    private String msg;
    private T data;
}
