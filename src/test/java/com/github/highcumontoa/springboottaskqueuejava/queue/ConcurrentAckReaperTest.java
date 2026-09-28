package com.github.highcumontoa.springboottaskqueuejava.queue;

import com.github.highcumontoa.springboottaskqueuejava.queue.model.Delivery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrent ack vs lease-reaper on the same messages: every message must end
 * terminal exactly once, counters must stay consistent and the committed offset
 * must reach the message count.
 */
class ConcurrentAckReaperTest extends AbstractQueueTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentAckReaperTest.class);

    @BeforeEach
    void init() {
        // very short lease so the reaper races real acks; many attempts before dead
        buildStack("ack-reaper", 100, 5, 2.0, 30);
    }

    @Test
    void ackAndReaperRaceLeavesEveryMessageTerminalOnce() throws Exception {
        int n = 30;
        String group = "RACE";
        List<String> deliveryIds = Collections.synchronizedList(new ArrayList<>());
        for (int i = 1; i <= n; i++) {
            publish("m" + i, "p" + i);
            deliveryIds.add(service.pull(group).orElseThrow().getDeliveryId());
        }

        ExecutorService pool = Executors.newFixedThreadPool(6);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger acked = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        CountDownLatch ackersDone = new CountDownLatch(n);

        for (String deliveryId : deliveryIds) {
            pool.submit(() -> {
                try {
                    start.await();
                    // random small delay to interleave with the reaper and lease expiry
                    Thread.sleep((long) (Math.random() * 40));
                    var r = service.ack(group, deliveryId);
                    if (r.accepted()) {
                        acked.incrementAndGet();
                    } else {
                        rejected.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    ackersDone.countDown();
                }
            });
        }

        // Reaper hammering concurrently
        AtomicInteger reaped = new AtomicInteger();
        pool.submit(() -> {
            try {
                start.await();
                while (ackersDone.getCount() > 0) {
                    int x = repository.reapExpiredLeases(Instant.now(),
                            properties.getMaxAttempts(), service.backoffSchedule());
                    reaped.addAndGet(x);
                    Thread.sleep(5);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        start.countDown();
        assertThat(ackersDone.await(30, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        // Drain any messages the reaper parked in RETRY_WAIT: pull and settle them.
        int guard = 0;
        int redelivered = 0;
        while (guard++ < n * 3) {
            var d = service.pull(group);
            if (d.isEmpty()) {
                // let any remaining backoff/lease expire and run the reaper once more
                int x = repository.reapExpiredLeases(Instant.now().plusSeconds(60),
                        properties.getMaxAttempts(), service.backoffSchedule());
                reaped.addAndGet(x);
                d = service.pull(group);
                if (d.isEmpty()) {
                    break;
                }
            }
            Delivery delivery = d.get();
            service.ack(group, delivery.getDeliveryId());
            redelivered++;
        }

        long committed = committedOffset(group);
        Integer terminal = jdbc.queryForObject(
                "SELECT COUNT(*) FROM queue_group_messages WHERE group_name = 'RACE' "
                        + "AND status IN ('ACKED','DEAD')", Integer.class);
        Integer inflight = jdbc.queryForObject(
                "SELECT COALESCE(SUM(inflight_count),0) FROM queue_group_offsets WHERE group_name = 'RACE'",
                Integer.class);
        log.info("race result group=RACE initialAcks={} rejected={} reaped={} redelivered={} terminal={} inflightCount={} committedOffset={}",
                acked.get(), rejected.get(), reaped.get(), redelivered, terminal, inflight, committed);

        assertThat(terminal).isEqualTo(n);
        assertThat(inflight).isZero();
        assertThat(committed).isEqualTo(n);
        // A message whose original ack won is never reaped; a reaped message's stale ack
        // is rejected. Hence successful original acks plus reclaimed rows never exceed n.
        assertThat(acked.get() + reaped.get()).isLessThanOrEqualTo(n + redelivered);
    }
}
