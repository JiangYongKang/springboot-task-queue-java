package com.github.highcumontoa.springboottaskqueuejava.queue;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** 服务启动后执行恢复：未确认消息重新可拉取，偏移量与死信保持磁盘值。 */
@Component
public class RecoveryRunner implements ApplicationRunner {

    private final TaskQueueService service;

    public RecoveryRunner(TaskQueueService service) {
        this.service = service;
    }

    @Override
    public void run(ApplicationArguments args) {
        service.recover();
    }
}
