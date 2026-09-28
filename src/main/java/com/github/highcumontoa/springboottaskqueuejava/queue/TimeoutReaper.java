package com.github.highcumontoa.springboottaskqueuejava.queue;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 周期扫描：租约到期未确认的消息按重试/退避规则重新投递或进入死信。 */
@Component
public class TimeoutReaper {

    private final TaskQueueService service;

    public TimeoutReaper(TaskQueueService service) {
        this.service = service;
    }

    @Scheduled(fixedDelayString = "${taskqueue.reaper-interval-ms:1000}")
    public void tick() {
        service.reapTimeouts();
    }
}
