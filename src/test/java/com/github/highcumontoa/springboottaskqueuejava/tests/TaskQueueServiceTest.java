package com.github.highcumontoa.springboottaskqueuejava.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.github.highcumontoa.springboottaskqueuejava.queue.AckRejectedException;
import com.github.highcumontoa.springboottaskqueuejava.queue.AckRejectReason;
import com.github.highcumontoa.springboottaskqueuejava.queue.DeadLetterRecord;
import com.github.highcumontoa.springboottaskqueuejava.queue.DuplicateDedupKeyException;
import com.github.highcumontoa.springboottaskqueuejava.queue.MessageRecord;
import com.github.highcumontoa.springboottaskqueuejava.queue.QueueProperties;
import com.github.highcumontoa.springboottaskqueuejava.queue.QueueStorage;
import com.github.highcumontoa.springboottaskqueuejava.queue.TaskQueueService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * 核心语义测试：重复投递、并发拉取、确认超时重投、重试上限进死信、重启恢复、跨组独立性。
 * 关注服务自身日志（TaskQueueService）中打印的 dedupKey / group / offset / 状态迁移。
 */
@SpringBootTest
@TestPropertySource(properties = {
        "taskqueue.data-dir=target/test-queue-data",
        "taskqueue.visibility-timeout-ms=200",
        "taskqueue.max-attempts=3",
        "taskqueue.backoff-base-ms=1",
        "taskqueue.backoff-strategy=FIXED",
        // 测试中关掉自动回收，全部手动 reapTimeouts()，避免时序不确定
        "taskqueue.reaper-interval-ms=3600000"
})
public class TaskQueueServiceTest {

    private static final Logger log = LoggerFactory.getLogger(TaskQueueServiceTest.class);

    @Autowired
    private TaskQueueService service;
    @Autowired
    private QueueProperties properties;
    @Autowired
    private QueueStorage storage;

    @BeforeEach
    void reset() {
        service.resetForTesting();
    }

    /** 等待可拉取消息出现（退避时间通常仅 1ms）。 */
    private List<TaskQueueService.DeliveredMessage> pullEventually(String group, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            List<TaskQueueService.DeliveredMessage> msgs = service.pull(group, 10);
            if (!msgs.isEmpty()) {
                return msgs;
            }
            Thread.sleep(5);
        }
        return List.of();
    }

    /** 等待租约到期后执行一次超时回收。 */
    private void waitForLeaseAndReap() throws InterruptedException {
        Thread.sleep(properties.getVisibilityTimeoutMs() + 50);
        var reaped = service.reapTimeouts();
        log.info("[test] reaped in this round: {}", reaped);
    }

    @Test
    void duplicatePublishDoesNotCreateSecondConsumableRecord() {
        String key = "dedup-dup-1";
        MessageRecord first = service.publish(key, "hello");
        log.info("[test] publish first: group=* offset={} dedupKey={}", first.offset(), key);

        assertThatThrownBy(() -> service.publish(key, "hello-again"))
                .isInstanceOf(DuplicateDedupKeyException.class)
                .satisfies(ex -> assertThat(((DuplicateDedupKeyException) ex).getExistingOffset())
                        .isEqualTo(first.offset()));

        List<TaskQueueService.DeliveredMessage> batch = service.pull("g-dup", 10);
        assertThat(batch).hasSize(1);
        assertThat(batch.get(0).dedupKey()).isEqualTo(key);
        assertThat(batch.get(0).payload()).isEqualTo("hello");
        assertThat(service.pull("g-dup", 10)).isEmpty();
        log.info("[test] duplicate publish verified: only one consumable record at offset={}",
                first.offset());
    }

    @Test
    void concurrentPullersNeverConsumeSameMessageInOneGroup() throws Exception {
        final int messages = 200;
        final int consumers = 20;
        String group = "g-concurrent";
        for (int i = 0; i < messages; i++) {
            service.publish("conc-" + i, "payload-" + i);
        }
        log.info("[test] published {} messages for group={}, starting {} concurrent pullers",
                messages, group, consumers);

        ExecutorService pool = Executors.newFixedThreadPool(consumers);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentHashMap<Long, String> consumedBy = new ConcurrentHashMap<>();
        AtomicInteger duplicateHits = new AtomicInteger();
        for (int c = 0; c < consumers; c++) {
            final String consumer = "consumer-" + c;
            pool.submit(() -> {
                try {
                    start.await();
                    while (true) {
                        List<TaskQueueService.DeliveredMessage> got = service.pull(group, 1);
                        if (got.isEmpty()) {
                            return;
                        }
                        for (TaskQueueService.DeliveredMessage m : got) {
                            if (consumedBy.putIfAbsent(m.offset(), consumer) != null) {
                                duplicateHits.incrementAndGet();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        assertThat(duplicateHits.get()).isZero();
        assertThat(consumedBy).hasSize(messages);
        // 所有消息处于 INFLIGHT，重复拉取不能再拿到任何一条
        assertThat(service.pull(group, messages)).isEmpty();
        log.info("[test] concurrent pull verified: {} unique offsets consumed, duplicateHits={}",
                consumedBy.size(), duplicateHits.get());
    }

    @Test
    void ackTimeoutRedeliversThenAckClosesLoopAndRejectsDuplicateAndCrossGroup() throws Exception {
        String group = "g-timeout";
        MessageRecord msg = service.publish("timeout-1", "task-x");
        long offset = msg.offset();

        TaskQueueService.DeliveredMessage first = service.pull(group, 1).get(0);
        assertThat(first.attempt()).isEqualTo(1);
        log.info("[test] group={} offset={} dedupKey={} delivered attempt=1 leaseExpireAt={}",
                group, offset, first.dedupKey(), first.leaseExpireAt());

        // 不确认 -> 等租约到期 -> 超时回收 -> 重新投递，attempt 递增，位点不变
        Thread.sleep(properties.getVisibilityTimeoutMs() + 50);
        assertThat(service.pull(group, 1)).isEmpty(); // 租约过期但未回收前仍不可拉
        var reaped = service.reapTimeouts();
        assertThat(reaped).hasSize(1);
        assertThat(reaped.get(0).offset()).isEqualTo(offset);
        assertThat(reaped.get(0).deadLettered()).isFalse();

        TaskQueueService.DeliveredMessage second = pullEventually(group, 2000).get(0);
        assertThat(second.offset()).isEqualTo(offset);
        assertThat(second.attempt()).isEqualTo(2);
        log.info("[test] group={} offset={} redelivered attempt=2", group, offset);

        // 新持有者正常确认，闭环
        service.ack(group, offset);

        // 重复确认：必须给出 ALREADY_ACKED，不能静默成功
        assertThatThrownBy(() -> service.ack(group, offset))
                .isInstanceOf(AckRejectedException.class)
                .extracting(ex -> ((AckRejectedException) ex).getReason())
                .isEqualTo(AckRejectReason.ALREADY_ACKED);

        // 跨组/未知位点确认：必须给出 GROUP_OR_MESSAGE_UNKNOWN
        assertThatThrownBy(() -> service.ack(group, offset + 1000))
                .isInstanceOf(AckRejectedException.class)
                .extracting(ex -> ((AckRejectedException) ex).getReason())
                .isEqualTo(AckRejectReason.GROUP_OR_MESSAGE_UNKNOWN);

        // 另一组对同一消息独立消费、独立偏移量
        TaskQueueService.DeliveredMessage other = service.pull("g-timeout-other", 1).get(0);
        assertThat(other.offset()).isEqualTo(offset);
        assertThat(service.committedOffset("g-timeout-other")).isZero();
        service.ack("g-timeout-other", offset);

        assertThat(service.committedOffset(group)).isEqualTo(offset + 1);
        assertThat(service.committedOffset("g-timeout-other")).isEqualTo(offset + 1);
        log.info("[test] ack loop closed: group={} committedOffset={}", group, offset + 1);
    }

    @Test
    void retriesExhaustedMessageEntersDeadLetterWithOriginalInfo() throws Exception {
        String group = "g-dead";
        String key = "dead-1";
        String payload = "payload-must-be-preserved";
        MessageRecord msg = service.publish(key, payload);
        long offset = msg.offset();

        int seenAttempts = 0;
        for (int round = 1; round <= properties.getMaxAttempts(); round++) {
            TaskQueueService.DeliveredMessage m = pullEventually(group, 2000).get(0);
            assertThat(m.offset()).isEqualTo(offset);
            seenAttempts = m.attempt();
            log.info("[test] group={} offset={} dedupKey={} attempt={}",
                    group, offset, key, m.attempt());
            if (round < properties.getMaxAttempts()) {
                waitForLeaseAndReap(); // 超时重投
            } else {
                service.nack(group, offset, "boom-on-last"); // 最后一次显式失败
            }
        }
        assertThat(seenAttempts).isEqualTo(properties.getMaxAttempts());

        List<DeadLetterRecord> dead = service.deadLetters();
        assertThat(dead).anySatisfy(d -> {
            assertThat(d.offset()).isEqualTo(offset);
            assertThat(d.dedupKey()).isEqualTo(key);
            assertThat(d.payload()).isEqualTo(payload);
            assertThat(d.consumerGroup()).isEqualTo(group);
            assertThat(d.attempts()).isEqualTo(properties.getMaxAttempts());
        });
        // 死信后该组不能再消费到这条消息
        assertThat(service.pull(group, 10)).isEmpty();
        DeadLetterRecord record = dead.stream().filter(d -> d.offset() == offset).findFirst().orElseThrow();
        log.info("[test] dead letter: group={} offset={} dedupKey={} attempts={} lastError={}",
                record.consumerGroup(), record.offset(), record.dedupKey(),
                record.attempts(), record.lastError());
    }

    @Test
    void restartKeepsInflightMessagesOffsetAndDeadLetters() throws Exception {
        String group = "g-restart";
        MessageRecord m1 = service.publish("restart-1", "p1");
        MessageRecord m2 = service.publish("restart-2", "p2");
        service.pull(group, 1).get(0); // m1 INFLIGHT，未确认 -> 模拟进程崩溃
        TaskQueueService.DeliveredMessage got2 = service.pull(group, 1).get(0);
        service.ack(group, got2.offset()); // m2 已确认
        long committedBefore = service.committedOffset(group);
        int deadBefore = service.deadLetters().size();
        log.info("[test] before restart group={}: m1(offset={}) INFLIGHT, committedOffset={}",
                group, m1.offset(), committedBefore);

        // 用同一持久化目录构造一个全新的服务实例，等价于进程重启
        TaskQueueService restarted = new TaskQueueService(properties, storage);
        restarted.recover();

        assertThat(restarted.committedOffset(group)).isEqualTo(committedBefore);
        assertThat(restarted.deadLetters()).hasSize(deadBefore);
        // m1 未确认，恢复后必须重新可拉取（断点续拉），且位点不回退
        TaskQueueService.DeliveredMessage redelivered = pullEventuallyFrom(restarted, group, 2000).get(0);
        assertThat(redelivered.offset()).isEqualTo(m1.offset());
        assertThat(redelivered.dedupKey()).isEqualTo("restart-1");
        log.info("[test] after restart: group={} offset={} redelivered, committedOffset={} retained",
                group, m1.offset(), restarted.committedOffset(group));
    }

    private List<TaskQueueService.DeliveredMessage> pullEventuallyFrom(TaskQueueService svc, String group,
                                                                      long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            List<TaskQueueService.DeliveredMessage> msgs = svc.pull(group, 10);
            if (!msgs.isEmpty()) {
                return msgs;
            }
            Thread.sleep(5);
        }
        return List.of();
    }

    @Test
    void differentGroupsConsumeIndependentlyWithIndependentOffsets() {
        String key = "multi-group";
        long offset = service.publish(key, "v").offset();

        Set<Long> g1 = new HashSet<>(service.pull("mg-g1", 10).stream().map(TaskQueueService.DeliveredMessage::offset).toList());
        Set<Long> g2 = new HashSet<>(service.pull("mg-g2", 10).stream().map(TaskQueueService.DeliveredMessage::offset).toList());
        assertThat(g1).containsExactly(offset);
        assertThat(g2).containsExactly(offset);

        service.ack("mg-g1", offset);
        assertThat(service.committedOffset("mg-g1")).isEqualTo(offset + 1);
        assertThat(service.committedOffset("mg-g2")).isZero(); // g2 未确认，偏移量不受 g1 影响

        // g2 确认自己的那份
        assertThatThrownBy(() -> service.ack("mg-g1", offset))
                .isInstanceOf(AckRejectedException.class); // g1 已确认，重复确认拒绝
        service.ack("mg-g2", offset);
        assertThat(service.committedOffset("mg-g2")).isEqualTo(offset + 1);
        log.info("[test] independent groups verified: mg-g1/mg-g2 committedOffset={}", offset + 1);
    }
}
