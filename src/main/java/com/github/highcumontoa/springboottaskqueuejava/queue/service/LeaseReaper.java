package com.github.highcumontoa.springboottaskqueuejava.queue.service;

import com.github.highcumontoa.springboottaskqueuejava.queue.config.QueueProperties;
import com.github.highcumontoa.springboottaskqueuejava.queue.repository.QueueRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Reclaims deliveries whose ack deadline has passed. The state transition
 * (RETRY_WAIT or DEAD) runs in one transaction per sweep, so a reclaim can
 * never race with a concurrent ack into double processing.
 */
@Component
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final QueueRepository repository;
    private final QueueProperties properties;
    private final QueueService queueService;

    public LeaseReaper(QueueRepository repository, QueueProperties properties,
                       QueueService queueService) {
        this.repository = repository;
        this.properties = properties;
        this.queueService = queueService;
    }

    @Scheduled(fixedDelayString = "${taskqueue.reaper-interval-millis:5000}")
    public void reap() {
        try {
            int reclaimed = repository.reapExpiredLeases(Instant.now(),
                    properties.getMaxAttempts(), queueService.backoffSchedule());
            if (reclaimed > 0) {
                log.warn("reaper reclaimed expired leases count={} state=RETRY_WAIT_or_DEAD", reclaimed);
            }
        } catch (Exception e) {
            log.error("reaper sweep failed", e);
        }
    }
}
