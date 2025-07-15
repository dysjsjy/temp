package com.dysjsjy.test;


import java.util.concurrent.TimeUnit;

public class ScheduleTask implements Runnable, Comparable<ScheduleTask> {

    private final Runnable task;
    private EventLoop eventLoop;
    private long delay;
    private long deadLine;

    public ScheduleTask(Runnable task, EventLoop eventLoop, long deadLine, long delay) {
        this.task = task;
        this.eventLoop = eventLoop;
        this.deadLine = deadLine;
        this.delay = delay;
    }

    @Override
    public void run() {
        try {
            this.task.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (delay > 0) {
                this.deadLine += delay;
                this.eventLoop.getScheduleTaskQueue().offer(this);
            }
        }
    }

    @Override
    public int compareTo(ScheduleTask o) {
        return Long.compare(this.deadLine, o.deadLine);
    }

    public long getDeadLine() {
        return deadLine;
    }

    public void setDeadLine(long deadLine) {
        this.deadLine = deadLine;
    }
}
