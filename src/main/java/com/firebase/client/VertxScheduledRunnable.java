package com.firebase.client;


import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Inspired by https://github.com/hudak/mod-firebase-connector
 *
 * @author meticoeus
 */
public class VertxScheduledRunnable implements RunnableScheduledFuture<Void> {
    private final Vertx vertx;
    private final long delay;
    private volatile Long timer = null;
    private final FutureTask<Void> task;

    public VertxScheduledRunnable(Vertx vertx, long delay, Runnable runnable) {
        this.vertx = vertx;
        this.delay = delay;
        this.task = new FutureTask<>(runnable, null);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.valueOf(getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS)).intValue();
    }

    @Override
    public void run() {
        if (delay > 0) {
            this.timer = vertx.setTimer(delay, (Long timerId) -> task.run());
        } else {
            vertx.runOnContext((Void nothing) -> task.run());
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        Long timer = this.timer;
        if (timer != null) {
            vertx.cancelTimer(timer);
        }
        return task.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return task.isCancelled();
    }

    @Override
    public boolean isDone() {
        return task.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        return task.get();
    }

    @Override
    public Void get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return task.get(timeout, unit);
    }

    @Override
    public boolean isPeriodic() {
        return false;
    }
}
