package com.yundepot.raft.util;

import com.yundepot.raft.exception.RaftException;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

/**
 * @author zhaiyanan
 * @date 2022/7/15  14:03
 */
public class LockUtil {

    public static void runWithLock(Lock lock, Runnable task) {
        lock.lock();
        try {
            task.run();
        } finally {
            lock.unlock();
        }
    }

    public static <T> T getWithLock(Lock lock, Callable<T> task) {
        lock.lock();
        try {
            return task.call();
        } catch(Exception e) {
            throw new RaftException(e);
        } finally {
            lock.unlock();
        }
    }
}
