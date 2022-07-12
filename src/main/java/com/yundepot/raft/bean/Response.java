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
        this(ResponseCode.SUCCESS.getValue());
    }

    private Response(T data) {
        this(ResponseCode.SUCCESS.getValue(), data);
    }

    public Response(int code) {
        this.code = code;
    }

    public Response(int code, T data) {
        this.code = code;
        this.data = data;
    }

    private int code;
    private String msg;
    private T data;

    public static <T> Response<T> success() {
        return new Response();
    }

    public static <T> Response<T> success(T data) {
        return new Response(data);
    }

    public static <T> Response<T> fail(Integer code, T data) {
        return new Response(code, data);
    }
}
