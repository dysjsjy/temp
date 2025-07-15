package com.dysjsjy.test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MyEventLoopGroup implements EventLoopGroup {

    private final EventLoop[] chindren;

    private final AtomicInteger index = new AtomicInteger(0);

    public MyEventLoopGroup(int threadNum) {
        this.chindren = new EventLoop[threadNum];
        for (int i = 0; i < threadNum; i++) {
            this.chindren[i] = new MyEventLoop();
        }
    }

    @Override
    public void execute(Runnable runnable) {
        if (runnable instanceof TargetRunnable targetRunnable) {
            chindren[targetRunnable.getIndex()].execute(runnable);
        } else {
            next().execute(runnable);
        }
    }

    @Override
    public void schedule(Runnable task, long delay, TimeUnit unit) {
        if (task instanceof TargetRunnable targetRunnable) {
            chindren[targetRunnable.getIndex()].schedule(task, delay, unit);
        } else {
            next().schedule(task, delay, unit);
        }
    }

    @Override
    public void scheduleAtFixedRate(Runnable task, long initialDelay, long period, TimeUnit unit) {
        if (task instanceof TargetRunnable targetRunnable) {
            chindren[targetRunnable.getIndex()].scheduleAtFixedRate(task, initialDelay, period, unit);
        } else {
            next().scheduleAtFixedRate(task, initialDelay, period, unit);
        }
    }

    @Override
    public EventLoop next() {
        return chindren[index.getAndIncrement() % chindren.length];
    }
}
