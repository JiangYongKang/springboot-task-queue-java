package com.github.highcumontoa.springboottaskqueuejava.queue.service;

import com.github.highcumontoa.springboottaskqueuejava.queue.config.QueueProperties;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.DeadLetter;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.Delivery;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.GroupOffset;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.RetrySchedule;
import com.github.highcumontoa.springboottaskqueuejava.queue.repository.QueueRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class QueueService {

    private static final Logger log = LoggerFactory.getLogger(QueueService.class);

    private final QueueRepository repository;
    private final QueueProperties properties;

    public QueueService(QueueRepository repository, QueueProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    public PublishResult publish(String dedupKey, String payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        String key = normalizeDedupKey(dedupKey);
        Instant now = Instant.now();
        QueueRepository.PublishRecord record = repository.publish(key, payload, now);
        if (record.duplicated()) {
            log.info("publish duplicated dedupKey={} offset={} state=DEDUP_HIT", key, record.offset());
        } else {
            log.info("publish dedupKey={} offset={} state=STORED", key, record.offset());
        }
        return new PublishResult(record.offset(), key, record.duplicated());
    }

    public Optional<Delivery> pull(String groupName) {
        requireGroup(groupName);
        Instant now = Instant.now();
        Instant deadline = now.plusMillis(properties.getAckTimeoutMillis());
        // A fresh claim can race against a concurrent commit; retry briefly on lock/serialization errors.
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                Optional<Delivery> delivery = repository.claimOne(groupName, now, deadline);
                delivery.ifPresent(d -> log.info(
                        "pull group={} offset={} dedupKey={} deliveryId={} attempt={} state=INFLIGHT leaseUntil={}",
                        groupName, d.getMessageOffset(), d.getDedupKey(), d.getDeliveryId(),
                        d.getAttempt(), deadline));
                return delivery;
            } catch (org.springframework.dao.CannotAcquireLockException
                     | org.springframework.transaction.TransactionSystemException e) {
                log.debug("pull group={} locked, retry={}", groupName, attempt);
                if (attempt == 3) {
                    throw e;
                }
                sleep(5L * attempt);
            }
        }
        return Optional.empty();
    }

    public AckResponse ack(String groupName, String deliveryId) {
        requireGroup(groupName);
        requireDeliveryId(deliveryId);
        Instant now = Instant.now();
        QueueRepository.AckResult result = repository.ack(groupName, deliveryId, now);
        if (result.success()) {
            long committed = committedOffsetAfter(groupName);
            log.info("ack group={} deliveryId={} state=ACKED committedOffset={}",
                    groupName, deliveryId, committed);
            return AckResponse.ok(committed);
        }
        log.info("ack group={} deliveryId={} state=REJECTED reason={}",
                groupName, deliveryId, result.rejection());
        return AckResponse.rejected(result.rejection().name());
    }

    public NackResponse nack(String groupName, String deliveryId, String error) {
        requireGroup(groupName);
        requireDeliveryId(deliveryId);
        Instant now = Instant.now();
        QueueRepository.NackOutcome out = repository.nack(groupName, deliveryId, error,
                properties.getMaxAttempts(), backoffSchedule(), now);
        if (!out.success()) {
            log.info("nack group={} deliveryId={} state=REJECTED reason={}",
                    groupName, deliveryId, out.rejection());
            return new NackResponse(false, out.rejection().name(), out.messageOffset(),
                    null, out.attempt(), null, null);
        }
        if (out.dead()) {
            long committed = committedOffsetAfter(groupName);
            log.info("nack group={} offset={} deliveryId={} attempts={} state=DEAD committedOffset={}",
                    groupName, out.messageOffset(), deliveryId, out.attempt(), committed);
            return new NackResponse(true, null, out.messageOffset(), "DEAD",
                    out.attempt(), null, committed);
        }
        long visibleAt = backoffSchedule()
                .nextVisibleAtEpochMilli(out.attempt() + 1, now.toEpochMilli());
        log.info("nack group={} offset={} deliveryId={} attempt={} state=RETRY_WAIT nextVisibleAt={}",
                groupName, out.messageOffset(), deliveryId, out.attempt(), visibleAt);
        return new NackResponse(true, null, out.messageOffset(), "RETRY_WAIT",
                out.attempt(), visibleAt, null);
    }

    public List<DeadLetter> deadLetters(String groupName, int limit) {
        return repository.findDeadLetters(groupName, limit);
    }

    public List<GroupOffset> offsets() {
        return repository.findAllOffsets();
    }

    public GroupOffset offset(String groupName) {
        return repository.findOffset(groupName).orElseGet(() -> new GroupOffset(groupName, 0L));
    }

    public RetrySchedule backoffSchedule() {
        long base = Math.max(1L, properties.getBackoffMillis());
        double multiplier = properties.getBackoffMultiplier();
        // Delay before delivery attempt n (n >= 2): base * multiplier^(n-2).
        return (nextAttempt, nowMillis) -> {
            long delay = base;
            for (int i = 2; i < nextAttempt; i++) {
                delay = (long) Math.min((double) Long.MAX_VALUE, delay * multiplier);
            }
            return nowMillis + delay;
        };
    }

    private long committedOffsetAfter(String groupName) {
        return repository.findOffset(groupName).map(GroupOffset::getCommittedOffset).orElse(0L);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static String normalizeDedupKey(String dedupKey) {
        if (dedupKey == null || dedupKey.isBlank()) {
            return UUID.randomUUID().toString();
        }
        return dedupKey.trim();
    }

    private static void requireGroup(String groupName) {
        if (groupName == null || groupName.isBlank()) {
            throw new IllegalArgumentException("group must not be blank");
        }
    }

    private static void requireDeliveryId(String deliveryId) {
        if (deliveryId == null || deliveryId.isBlank()) {
            throw new IllegalArgumentException("deliveryId must not be blank");
        }
    }
}
