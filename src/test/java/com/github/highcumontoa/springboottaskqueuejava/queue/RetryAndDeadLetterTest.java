package com.github.highcumontoa.springboottaskqueuejava.queue;

import com.github.highcumontoa.springboottaskqueuejava.queue.model.DeadLetter;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.Delivery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Failure retries follow the configured attempt cap and exponential backoff;
 * once the cap is reached the message moves to the dead-letter store carrying
 * its original publish information. The committed offset treats DEAD as terminal.
 */
class RetryAndDeadLetterTest extends AbstractQueueTest {

    private static final Logger log = LoggerFactory.getLogger(RetryAndDeadLetterTest.class);

    @BeforeEach
    void init() {
        // maxAttempts=3, first backoff 100ms x2 multiplier, long ack timeout (we drive retries manually)
        buildStack("retry-dead", 3, 100, 2.0, 60_000);
    }

    @Test
    void retriesWithBackoffThenDeadLettersAfterCap() throws Exception {
        long offset = publish("fail-job", "doomed");
        String group = "WORKERS";

        Delivery d1 = service.pull(group).orElseThrow();
        assertThat(d1.getAttempt()).isEqualTo(1);
        log.info("pull group={} offset={} dedupKey=fail-job attempt=1 state=INFLIGHT", group, offset);

        var nack1 = service.nack(group, d1.getDeliveryId(), "boom-1");
        assertThat(nack1.accepted()).isTrue();
        assertThat(nack1.state()).isEqualTo("RETRY_WAIT");
        assertThat(nack1.attempt()).isEqualTo(1);
        log.info("nack group={} offset={} attempt={} state=RETRY_WAIT nextVisibleAt={}",
                group, offset, nack1.attempt(), nack1.nextVisibleAtEpochMilli());

        // Backoff not elapsed -> not yet redelivered
        assertThat(service.pull(group)).as("message hidden during backoff").isEmpty();
        Thread.sleep(120);

        Delivery d2 = service.pull(group).orElseThrow();
        assertThat(d2.getAttempt()).isEqualTo(2);
        assertThat(d2.getDeliveryId()).isNotEqualTo(d1.getDeliveryId());
        log.info("pull group={} offset={} attempt=2 state=INFLIGHT deliveryId={}",
                group, offset, d2.getDeliveryId());
        service.nack(group, d2.getDeliveryId(), "boom-2");
        Thread.sleep(220);

        Delivery d3 = service.pull(group).orElseThrow();
        assertThat(d3.getAttempt()).isEqualTo(3);
        log.info("pull group={} offset={} attempt=3 state=INFLIGHT deliveryId={}",
                group, offset, d3.getDeliveryId());

        var nack3 = service.nack(group, d3.getDeliveryId(), "boom-3");
        assertThat(nack3.state()).isEqualTo("DEAD");
        log.info("nack group={} offset={} attempt=3 state=DEAD committedOffset={}",
                group, offset, nack3.committedOffset());

        // Never consumable again
        Thread.sleep(300);
        assertThat(service.pull(group)).as("dead message is never redelivered").isEmpty();

        List<DeadLetter> dead = service.deadLetters(group, 10);
        assertThat(dead).hasSize(1);
        DeadLetter dl = dead.get(0);
        assertThat(dl.getMessageOffset()).isEqualTo(offset);
        assertThat(dl.getDedupKey()).isEqualTo("fail-job");
        assertThat(dl.getPayload()).isEqualTo("doomed");
        assertThat(dl.getAttempts()).isEqualTo(3);
        assertThat(dl.getLastError()).isEqualTo("boom-3");
        assertThat(dl.getOriginalPublishedAt()).isNotNull();
        log.info("deadletter group={} offset={} dedupKey={} attempts={} publishedAt={} deadAt={} state=DEAD",
                group, dl.getMessageOffset(), dl.getDedupKey(), dl.getAttempts(),
                dl.getOriginalPublishedAt(), dl.getDeadAt());

        assertThat(committedOffset(group)).as("DEAD is terminal and advances the offset").isEqualTo(offset);
    }
}
