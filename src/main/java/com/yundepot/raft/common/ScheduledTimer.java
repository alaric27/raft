package com.yundepot.raft.common;

import com.yundepot.oaa.common.NamedThreadFactory;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;


/**
 * @author zhaiyanan
 * @date 2022/6/23  17:13
 */
@Slf4j
public class ScheduledTimer {

    private int STOP_STATE = 0;
    private int RUNNING_STATE = 1;
    private final Lock lock = new ReentrantLock();
    private long time;
    private volatile int state;
    private String taskName;
    private Timeout timeout;
    private final Timer timer;
    private Function<Long, Long> adjustTime;
    private Runnable runnable;


    public ScheduledTimer(String taskName, long time, Runnable runnable) {
        this(taskName, time, runnable, newTime -> newTime);
    }

    public ScheduledTimer(String taskName, long time, Runnable runnable,  Function<Long, Long> adjustTime) {
        this.time = time;
        this.taskName = taskName;
        this.adjustTime = adjustTime;
        this.runnable = runnable;
        timer = new HashedWheelTimer(new NamedThreadFactory(taskName, true), 1, TimeUnit.MILLISECONDS, 2048);
    }

    /**
     * 启动
     */
    public void start() {
        lock.lock();
        try {
            if (state == RUNNING_STATE) {
                return;
            }
            state = RUNNING_STATE;
            schedule();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 重置任务
     */
    public void reset() {
        reset(time);
    }

    /**
     * 重置任务
     * @param time
     */
    public void reset(long time) {
        lock.lock();
        try {
            this.state = RUNNING_STATE;
            this.time = time;
            schedule();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 停止任务
     */
    public void stop() {
        lock.lock();
        try {
            state = STOP_STATE;
            if (timeout != null) {
                timeout.cancel();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 执行业务逻辑，并重置定时器
     */
    public void run() {
        try {
            runnable.run();
        } catch (Exception e) {
            log.error("task error", e);
        }

        lock.lock();
        try {
            timeout = null;
            if (state != RUNNING_STATE) {
                return;
            }
            schedule();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 如果该方法由run或start调用则timeout 为空，正常进行任务创建。
     * 如果由reset调用，则timeout可能不为空，如果已经超时开始调用业务逻辑，则cancel方法取消失败返回false，这时直接返回即可，
     * 由业务逻辑执行完成后run方法再次创建任务
     */
    private void schedule() {
        if (timeout != null && !timeout.isCancelled() && !timeout.cancel()) {
            return;
        }
        TimerTask timerTask = timeout -> {
            try {
                ScheduledTimer.this.run();
            } catch (final Throwable t) {
                log.error("Run timer task failed, taskName={}.", taskName, t);
            }
        };
        timeout = timer.newTimeout(timerTask, adjustTime.apply(time), TimeUnit.MILLISECONDS);
    }
}
