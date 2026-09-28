package com.github.highcumontoa.springboottaskqueuejava.queue;

import com.github.highcumontoa.springboottaskqueuejava.queue.repository.QueueRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Repeated publishes with the same dedup key must never create more than one
 * consumable message, including under concurrent publishes.
 */
class DuplicatePublishTest extends AbstractQueueTest {

    private static final Logger log = LoggerFactory.getLogger(DuplicatePublishTest.class);

    @BeforeEach
    void init() {
        buildStack("dedup");
    }

    @Test
    void sequentialDuplicatesReturnSameOffsetAndSingleConsumable() {
        long o1 = publish("job-1", "payload-1");
        long o2 = publish("job-1", "payload-1");
        long o3 = publish("job-1", "payload-1");
        log.info("dedupKey=job-1 group=G offsets=[{},{},{}] state=DEDUP_HIT", o1, o2, o3);

        assertThat(o2).isEqualTo(o1);
        assertThat(o3).isEqualTo(o1);

        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM queue_messages WHERE dedup_key = 'job-1'",
                Long.class);
        assertThat(count).isEqualTo(1L);

        var first = service.pull("G");
        assertThat(first).isPresent();
        service.ack("G", first.get().getDeliveryId());
        assertThat(service.pull("G")).as("no second consumable record for the same dedup key").isEmpty();
    }

    @Test
    void concurrentDuplicatePublishesCreateExactlyOneRecord() throws Exception {
        int threads = 12;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger sameOffset = new AtomicInteger();
        java.util.concurrent.atomic.AtomicLong firstOffset =
                new java.util.concurrent.atomic.AtomicLong(-1L);

        Runnable publisher = () -> {
            try {
                start.await();
                QueueRepository.PublishRecord r = repository.publish("race-key", "same",
                        java.time.Instant.now());
                firstOffset.compareAndSet(-1L, r.offset());
                if (r.offset() == firstOffset.get()) {
                    sameOffset.incrementAndGet();
                }
                log.info("concurrent publish dedupKey=race-key offset={} duplicated={} state=STORED_or_DEDUP_HIT",
                        r.offset(), r.duplicated());
            } catch (Exception ignored) {
            } finally {
                done.countDown();
            }
        };

        for (int i = 0; i < threads; i++) {
            new Thread(publisher, "publisher-" + i).start();
        }
        start.countDown();
        assertThat(done.await(20, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM queue_messages WHERE dedup_key = 'race-key'",
                Long.class);
        log.info("concurrent publish final recordCount={} sameOffsetVotes={}", count, sameOffset.get());
        assertThat(count).isEqualTo(1L);
        assertThat(sameOffset.get()).isEqualTo(threads);
    }
}
