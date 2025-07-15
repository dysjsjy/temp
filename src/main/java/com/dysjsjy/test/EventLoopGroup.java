package com.dysjsjy.test;

import java.util.concurrent.TimeUnit;

public interface EventLoopGroup {

    void execute(Runnable runnable);

    void schedule(Runnable task, long delay, TimeUnit unit);

    void scheduleAtFixedRate(Runnable task, long initialDelay, long period, TimeUnit unit);

    EventLoop next();
}