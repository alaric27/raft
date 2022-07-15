package com.yundepot.raft.exception;

/**
 * @author zhaiyanan
 * @date 2019/5/16 11:55
 */
public class RaftException extends RuntimeException{
    private static final long serialVersionUID = -8345704145413873700L;

    public RaftException() {

    }

    public RaftException(String message) {
        super(message);
    }

    public RaftException(String message, Throwable cause) {
        super(message, cause);
    }

    public RaftException(Throwable cause) {
        super(cause);
    }
}
