package com.dysjsjy.test;

import java.util.Queue;

public interface EventLoop extends EventLoopGroup {
    Queue<ScheduleTask> getScheduleTaskQueue();
}
