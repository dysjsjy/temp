package com.dysjsjy.test;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MyEventLoop implements EventLoop {

    private static final AtomicInteger index = new AtomicInteger(0);

    private static final Runnable WAKE_UP = () -> {
    };

    private final Thread thread;

    private final PriorityQueue<ScheduleTask> scheduleTaskQueue;

    private final BlockingQueue<Runnable> taskQueue;


    public MyEventLoop() {
        this.thread = new EventLoopThread("MyEventThread" + index.getAndIncrement());
        this.scheduleTaskQueue = new PriorityQueue<>(1024);
        this.taskQueue = new ArrayBlockingQueue<>(1024);
        this.thread.start();
    }

    public void execute(Runnable runnable) {
        boolean offer = taskQueue.offer(runnable);
        if (!offer) {
            throw new RuntimeException("任务队列已满");
        }
    }

    public void schedule(Runnable task, long delay, TimeUnit unit) {
        ScheduleTask scheduleTask = new ScheduleTask(task, this, System.currentTimeMillis() + delay, -1);
        boolean offer = scheduleTaskQueue.offer(scheduleTask);
        if (!offer) {
            throw new RuntimeException("任务队列已满");
        }
        execute(WAKE_UP);
    }

    public void scheduleAtFixedRate(Runnable task, long initialDelay, long period, TimeUnit unit) {
        ScheduleTask scheduleTask = new ScheduleTask(task, this, System.currentTimeMillis() + initialDelay, period);
        boolean offer = scheduleTaskQueue.offer(scheduleTask);
        if (!offer) {
            throw new RuntimeException("任务队列已满");
        }
        execute(WAKE_UP);
    }

    private long deadlineMs(long delay, TimeUnit unit) {
        return unit.toMillis(delay) + System.currentTimeMillis();
    }

    @Override
    public Queue<ScheduleTask> getScheduleTaskQueue() {
        return this.scheduleTaskQueue;
    }

    @Override
    public EventLoop next() {
        return this;
    }

    // 为什么这里没有竞争？因为这里从始至终就只有一个线程在运行
    private Runnable getTask() {
        ScheduleTask scheduleTask = scheduleTaskQueue.peek();
        if (scheduleTask == null) {
            Runnable task = null;
            try {
                task = taskQueue.take();
                if (task == WAKE_UP) {
                    task = null;
                }
            } catch (InterruptedException e) {

            }
            return task;
        }

        if (scheduleTask.getDeadLine() <= System.currentTimeMillis()) {
            return scheduleTaskQueue.poll();
        }

        Runnable task = null;
        try {
            task = taskQueue.poll(scheduleTask.getDeadLine() - System.currentTimeMillis(),TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {

        }
        if (task == WAKE_UP) {
            task = null;
        }

        return task;
    }

    class EventLoopThread extends Thread {

        public EventLoopThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            while (true) {
                Runnable task = getTask();
                if (task != null) {
                    task.run();
                }
            }
        }
    }
}
