package com.github.highcumontoa.springboottaskqueuejava.queue;

import com.github.highcumontoa.springboottaskqueuejava.queue.model.Delivery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Many consumers in the SAME group pull concurrently: every message must be
 * delivered to exactly one consumer at a time (at-most-one active delivery).
 */
class ConcurrentPullTest extends AbstractQueueTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentPullTest.class);

    @BeforeEach
    void init() {
        buildStack("concurrent-pull");
    }

    @Test
    void sameGroupConsumersNeverShareAMessage() throws Exception {
        int messageCount = 20;
        int consumers = 8;
        for (int i = 1; i <= messageCount; i++) {
            publish("key-" + i, "payload-" + i);
        }

        ExecutorService pool = Executors.newFixedThreadPool(consumers);
        CountDownLatch start = new CountDownLatch(1);
        Set<Long> claimedOffsets = ConcurrentHashMap.newKeySet();
        Set<String> claimedDeliveryIds = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger();
        AtomicInteger emptyPulls = new AtomicInteger();
        AtomicInteger ackFailures = new AtomicInteger();
        List<String> errors = Collections.synchronizedList(new ArrayList<>());
        List<Delivery> allClaims = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch done = new CountDownLatch(consumers);

        for (int c = 0; c < consumers; c++) {
            final String consumer = "consumer-" + c;
            pool.submit(() -> {
                try {
                    start.await();
                    while (true) {
                        Optional<Delivery> d;
                        try {
                            d = service.pull("ORDER_PROCESSORS");
                        } catch (Exception e) {
                            log.warn("{} pull error: {}", consumer, e.toString());
                            continue;
                        }
                        if (d.isEmpty()) {
                            emptyPulls.incrementAndGet();
                            // Two consecutive empty pulls => drained.
                            if (emptyPulls.get() > consumers + 4) {
                                break;
                            }
                            Thread.sleep(5);
                            continue;
                        }
                        Delivery delivery = d.get();
                        allClaims.add(delivery);
                        if (!claimedOffsets.add(delivery.getMessageOffset())
                                || !claimedDeliveryIds.add(delivery.getDeliveryId())) {
                            duplicates.incrementAndGet();
                            log.error("DUPLICATE CONSUMPTION group=ORDER_PROCESSORS consumer={} offset={} deliveryId={}",
                                    consumer, delivery.getMessageOffset(), delivery.getDeliveryId());
                        } else {
                            log.info("claimed group=ORDER_PROCESSORS consumer={} offset={} deliveryId={} attempt={} state=INFLIGHT",
                                    consumer, delivery.getMessageOffset(), delivery.getDeliveryId(),
                                    delivery.getAttempt());
                        }
                        // ack so offset advances and the message is fully done
                        var ackResp = service.ack("ORDER_PROCESSORS", delivery.getDeliveryId());
                        if (!ackResp.accepted()) {
                            ackFailures.incrementAndGet();
                            errors.add("ack rejected offset=" + delivery.getMessageOffset()
                                    + " reason=" + ackResp.reason());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    errors.add(consumer + " fatal: " + e);
                    log.error("consumer fatal", e);
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        log.info("result group=ORDER_PROCESSORS uniqueOffsets={} totalClaims={} duplicates={} ackFailures={} committedOffset={}",
                claimedOffsets.size(), allClaims.size(), duplicates.get(), ackFailures.get(),
                committedOffset("ORDER_PROCESSORS"));
        errors.forEach(e -> log.warn("issue: {}", e));

        assertThat(errors).as("no consumer thread errors").isEmpty();
        assertThat(ackFailures.get()).as("all acks accepted").isZero();
        assertThat(duplicates.get()).as("no message consumed twice in the same group").isZero();
        assertThat(claimedOffsets).hasSize(messageCount);
        assertThat(allClaims).hasSize(messageCount);
        assertThat(committedOffset("ORDER_PROCESSORS")).isEqualTo(messageCount);

        // DB-level invariant: exactly one row per group/message, all ACKED, one delivery each
        List<String> statuses = jdbc.queryForList(
                "SELECT DISTINCT status FROM queue_group_messages WHERE group_name = 'ORDER_PROCESSORS'",
                String.class);
        assertThat(new HashSet<>(statuses)).containsExactly("ACKED");
        Integer rowCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM queue_group_messages WHERE group_name = 'ORDER_PROCESSORS'",
                Integer.class);
        assertThat(rowCount).isEqualTo(messageCount);
    }
}
