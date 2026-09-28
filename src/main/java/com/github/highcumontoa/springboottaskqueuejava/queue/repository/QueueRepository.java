package com.github.highcumontoa.springboottaskqueuejava.queue.repository;

import com.github.highcumontoa.springboottaskqueuejava.queue.model.AckRejection;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.DeadLetter;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.Delivery;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.GroupOffset;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.QueueMessage;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.RetrySchedule;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface QueueRepository {

    void initializeSchema();

    /** Insert a new message; on duplicate dedupKey returns the existing offset and duplicated=true. */
    PublishRecord publish(String dedupKey, String payload, Instant now);

    Optional<QueueMessage> findByDedupKey(String dedupKey);

    /** Atomically claim one available message for the group and advance bookkeeping. */
    Optional<Delivery> claimOne(String groupName, Instant now, Instant leaseDeadline);

    /** Acknowledge a delivery; AckRejection is null on success. */
    AckResult ack(String groupName, String deliveryId, Instant now);

    /** Negative acknowledge: schedule retry or move to dead letter, atomically. */
    NackOutcome nack(String groupName, String deliveryId, String error, int maxAttempts,
                     RetrySchedule schedule, Instant now);

    /** Reset expired INFLIGHT leases using retry/dead-letter rules. Returns affected rows. */
    int reapExpiredLeases(Instant now, int maxAttempts, RetrySchedule schedule);

    List<DeadLetter> findDeadLetters(String groupName, int limit);

    List<GroupOffset> findAllOffsets();

    Optional<GroupOffset> findOffset(String groupName);

    record AckResult(boolean success, AckRejection rejection) {
        public static AckResult ok() { return new AckResult(true, null); }
        public static AckResult reject(AckRejection r) { return new AckResult(false, r); }
    }

    record NackOutcome(boolean success, AckRejection rejection, Long messageOffset,
                       boolean dead, int attempt) { }

    record PublishRecord(long offset, boolean duplicated) { }
}
