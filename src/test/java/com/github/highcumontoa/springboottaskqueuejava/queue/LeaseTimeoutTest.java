package com.github.highcumontoa.springboottaskqueuejava.queue;

import com.github.highcumontoa.springboottaskqueuejava.queue.model.Delivery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * When a delivery is not acknowledged before its lease deadline, the reaper
 * returns it to RETRY_WAIT (then DEAD after the cap). A late ack of the stale
 * delivery must be rejected so the message is never processed twice.
 */
class LeaseTimeoutTest extends AbstractQueueTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseTimeoutTest.class);

    @BeforeEach
    void init() {
        // 200ms ack timeout, 30ms retry backoff, max 2 attempts before dead letter.
        buildStack("lease-timeout", 2, 30, 2.0, 200);
    }

    @Test
    void expiredLeaseIsRedeliveredAndLateAckRejected() throws Exception {
        long offset = publish("slow-job", "slow");
        String group = "TIMEOUT_GROUP";

        Delivery first = service.pull(group).orElseThrow();
        log.info("pull group={} offset={} deliveryId={} attempt=1 state=INFLIGHT leaseDeadline=+200ms",
                group, offset, first.getDeliveryId());

        // Consumer crashes / stalls: no ack. Wait for the lease to expire.
        Thread.sleep(230);

        // Drive the same recovery the scheduled reaper performs on restart/interval.
        int reclaimed = repository.reapExpiredLeases(Instant.now(),
                properties.getMaxAttempts(), service.backoffSchedule());
        assertThat(reclaimed).isEqualTo(1);
        log.info("reap group={} offset={} state=RETRY_WAIT attempts=1", group, offset);

        // Before redelivery the stale delivery id still points at a RETRY_WAIT row.
        var staleDuringWait = service.ack(group, first.getDeliveryId());
        assertThat(staleDuringWait.accepted()).isFalse();
        assertThat(staleDuringWait.reason()).isEqualTo("MESSAGE_NOT_INFLIGHT");
        log.info("late-ack group={} staleDeliveryId={} state=REJECTED reason=MESSAGE_NOT_INFLIGHT",
                group, first.getDeliveryId());

        // Immediately still in backoff window...
        Thread.sleep(50);
        Delivery second = service.pull(group).orElseThrow();
        assertThat(second.getAttempt()).isEqualTo(2);
        assertThat(second.getDeliveryId()).isNotEqualTo(first.getDeliveryId());
        log.info("redeliver group={} offset={} deliveryId={} attempt=2 state=INFLIGHT",
                group, offset, second.getDeliveryId());

        // After redelivery the stale id is superseded, so it is reported as not found.
        var lateAck = service.ack(group, first.getDeliveryId());
        assertThat(lateAck.accepted()).isFalse();
        assertThat(lateAck.reason()).isEqualTo("DELIVERY_NOT_FOUND");
        log.info("late-ack group={} staleDeliveryId={} state=REJECTED reason=DELIVERY_NOT_FOUND",
                group, first.getDeliveryId());

        // Second delivery also times out -> reaper sends it to the dead letter queue.
        Thread.sleep(230);
        int reaped2 = repository.reapExpiredLeases(Instant.now(),
                properties.getMaxAttempts(), service.backoffSchedule());
        assertThat(reaped2).isEqualTo(1);
        log.info("reap group={} offset={} state=DEAD attempts=2", group, offset);

        assertThat(service.deadLetters(group, 10)).hasSize(1);
        assertThat(service.pull(group)).as("dead message not redelivered").isEmpty();
        assertThat(committedOffset(group)).as("dead message advances offset").isEqualTo(offset);
    }

    @Test
    void duplicateAndCrossGroupAckAreRejectedWithDistinctReasons() {
        long o1 = publish("a1", "x");
        long o2 = publish("a2", "y");
        Delivery d1 = service.pull("GA").orElseThrow();
        Delivery d2 = service.pull("GB").orElseThrow();

        // Unknown delivery id
        var unknown = service.ack("GA", "does-not-exist");
        assertThat(unknown.accepted()).isFalse();
        assertThat(unknown.reason()).isEqualTo("DELIVERY_NOT_FOUND");

        // Delivery belonging to a different group
        var cross = service.ack("GA", d2.getDeliveryId());
        assertThat(cross.accepted()).isFalse();
        assertThat(cross.reason()).isEqualTo("GROUP_MISMATCH");

        // Valid ack then duplicate ack
        assertThat(service.ack("GA", d1.getDeliveryId()).accepted()).isTrue();
        var dup = service.ack("GA", d1.getDeliveryId());
        assertThat(dup.accepted()).isFalse();
        assertThat(dup.reason()).isEqualTo("DUPLICATE_ACK");
        log.info("ack-rejections group=GA offset={} reasons=[{},{},{}] state=REJECTED",
                o1, unknown.reason(), cross.reason(), dup.reason());
    }
}
