package com.dysjsjy.test;

public class TargetRunnable implements Runnable{

    private final int index;
    private final Runnable task;

    public TargetRunnable(int index, Runnable task) {
        this.index = index;
        this.task = task;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public void run() {
        this.run();
    }
}
