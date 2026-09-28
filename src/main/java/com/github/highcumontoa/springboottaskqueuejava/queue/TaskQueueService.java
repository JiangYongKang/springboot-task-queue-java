package com.github.highcumontoa.springboottaskqueuejava.queue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 任务队列核心。
 *
 * <p>语义保证：
 * <ul>
 *   <li>所有状态读写都在 {@code lock} 内串行执行：同一消费者组的同一条消息同一时刻
 *       只会被一个消费者拿到（PENDING→INFLIGHT 是持锁的原子检查-翻转），偏移量只增不减，
 *       投递次数不会并发错乱。</li>
 *   <li>每次状态翻转后立即整体快照落盘：重启后未确认消息、偏移量、死信均不丢失。</li>
 *   <li>偏移量按组维护，仅由连续 ACK 推进，是“已连续确认水位”，断点续拉时其后的消息
 *       （包括未确认/重投/死信）仍可继续投递。</li>
 * </ul>
 */
@Service
public class TaskQueueService {

    private static final Logger log = LoggerFactory.getLogger(TaskQueueService.class);

    private final QueueProperties properties;
    private final QueueStorage storage;
    private final Object lock = new Object();

    private long nextOffset;
    private final List<MessageRecord> messages = new ArrayList<>();
    private final Map<String, GroupState> groups = new LinkedHashMap<>();
    private final List<DeadLetterRecord> deadLetters = new ArrayList<>();
    /** dedupKey -> offset，全局去重。 */
    private final Map<String, Long> dedupIndex = new LinkedHashMap<>();

    public TaskQueueService(QueueProperties properties, QueueStorage storage) {
        this.properties = properties;
        this.storage = storage;
    }

    /** 启动恢复：加载磁盘快照，把所有 INFLIGHT（上次进程未确认）消息重新置为可拉取。 */
    public void recover() {
        synchronized (lock) {
            QueueStorage.Snapshot snap = storage.load();
            nextOffset = snap.nextOffset;
            messages.clear();
            messages.addAll(snap.messages);
            groups.clear();
            groups.putAll(snap.groups);
            deadLetters.clear();
            deadLetters.addAll(snap.deadLetters);
            dedupIndex.clear();
            for (MessageRecord m : messages) {
                dedupIndex.put(m.dedupKey(), m.offset());
            }
            long now = System.currentTimeMillis();
            int requeued = 0;
            for (Map.Entry<String, GroupState> ge : groups.entrySet()) {
                GroupState gs = ge.getValue();
                for (Map.Entry<Long, GroupDelivery> de : gs.getDeliveries().entrySet()) {
                    GroupDelivery d = de.getValue();
                    if (d.getStatus() == DeliveryStatus.INFLIGHT) {
                        d.setStatus(DeliveryStatus.PENDING);
                        d.setLeaseExpireAt(0L);
                        // 已投递过的消息恢复后按当前尝试次数做一次退避，避免重启瞬间风暴。
                        d.setNextVisibleAt(now + backoffMs(Math.max(1, d.getAttempts())));
                        d.setLastStatusChangeAt(now);
                        requeued++;
                        log.info("recover group={} offset={} dedupKey={} INFLIGHT->PENDING attempts={} nextVisibleAt={}",
                                ge.getKey(), de.getKey(), dedupKeyOf(de.getKey()),
                                d.getAttempts(), d.getNextVisibleAt());
                    }
                }
            }
            persistLocked();
            log.info("recovery complete: messages={} groups={} deadLetters={} requeuedInflight={} nextOffset={}",
                    messages.size(), groups.size(), deadLetters.size(), requeued, nextOffset);
        }
    }

    /** 投递消息；dedupKey 重复时拒绝，绝不产生第二条可消费记录。 */
    public MessageRecord publish(String dedupKey, String payload) {
        if (dedupKey == null || dedupKey.isBlank()) {
            throw new QueueException("dedupKey must not be blank");
        }
        synchronized (lock) {
            Long existing = dedupIndex.get(dedupKey);
            if (existing != null) {
                log.info("publish rejected duplicate dedupKey={} existingOffset={}", dedupKey, existing);
                throw new DuplicateDedupKeyException(dedupKey, existing);
            }
            long offset = nextOffset++;
            MessageRecord msg = new MessageRecord(offset, dedupKey, payload, System.currentTimeMillis());
            messages.add(msg);
            dedupIndex.put(dedupKey, offset);
            persistLocked();
            log.info("publish dedupKey={} offset={} payloadBytes={}", dedupKey, offset,
                    payload == null ? 0 : payload.length());
            return msg;
        }
    }

    /** 消费者组拉取：只交付该组未确认、未死信、已到可见时间的消息，原子翻转为 INFLIGHT。 */
    public List<DeliveredMessage> pull(String group, int maxMessages) {
        if (group == null || group.isBlank()) {
            throw new QueueException("consumer group must not be blank");
        }
        int limit = maxMessages <= 0 ? 10 : Math.min(maxMessages, 1000);
        long now = System.currentTimeMillis();
        synchronized (lock) {
            GroupState gs = groups.computeIfAbsent(group, k -> {
                GroupState s = new GroupState();
                log.info("group registered group={}", k);
                return s;
            });
            List<DeliveredMessage> result = new ArrayList<>();
            long startFrom = gs.getCommittedOffset();
            for (long off = startFrom; off < messages.size() && result.size() < limit; off++) {
                GroupDelivery d = gs.getDeliveries().get(off);
                if (d != null && d.getStatus() != DeliveryStatus.PENDING) {
                    continue; // INFLIGHT（被别的消费者持有）/ACKED/DEAD 均不可再被本拉取
                }
                if (d != null && d.getNextVisibleAt() > now) {
                    continue; // 退避中
                }
                if (d == null) {
                    d = new GroupDelivery();
                    gs.getDeliveries().put(off, d);
                }
                int attempt = d.getAttempts() + 1;
                d.setAttempts(attempt);
                d.setStatus(DeliveryStatus.INFLIGHT);
                d.setLeaseExpireAt(now + properties.getVisibilityTimeoutMs());
                d.setLastStatusChangeAt(now);
                MessageRecord msg = messages.get((int) off);
                persistLocked();
                log.info("pull group={} offset={} dedupKey={} PENDING->INFLIGHT attempt={} leaseExpireAt={} committedOffset={}",
                        group, off, msg.dedupKey(), attempt, d.getLeaseExpireAt(), gs.getCommittedOffset());
                result.add(new DeliveredMessage(off, msg.dedupKey(), msg.payload(),
                        attempt, d.getLeaseExpireAt()));
            }
            return result;
        }
    }

    /** 确认消费成功；重复/跨组/死信/未在途/租约过期均给出可区分拒绝。 */
    public void ack(String group, long offset) {
        synchronized (lock) {
            GroupState gs = groups.get(group);
            GroupDelivery d = gs == null ? null : gs.getDeliveries().get(offset);
            if (gs == null || d == null) {
                throw reject(AckRejectReason.GROUP_OR_MESSAGE_UNKNOWN, group, offset,
                        "group or message is unknown (possible cross-group ack)");
            }
            switch (d.getStatus()) {
                case ACKED -> throw reject(AckRejectReason.ALREADY_ACKED, group, offset,
                        "message already acknowledged, duplicate ack rejected");
                case DEAD -> throw reject(AckRejectReason.MESSAGE_DEAD, group, offset,
                        "message already moved to dead letter, ack rejected");
                case PENDING -> {
                    if (d.getLeaseExpireAt() > 0L) {
                        // 租约到期被回收重投
                        throw reject(AckRejectReason.LEASE_EXPIRED, group, offset,
                                "lease had expired and message was redelivered");
                    }
                    throw reject(AckRejectReason.NOT_INFLIGHT, group, offset,
                            "message is not inflight (was nacked or never delivered)");
                }
                case INFLIGHT -> {
                    long now = System.currentTimeMillis();
                    if (d.getLeaseExpireAt() <= now) {
                        // 调用方拿到的租约恰好刚过期。交由 reaper 负责重投，此处拒绝以避免双重确认。
                        throw reject(AckRejectReason.LEASE_EXPIRED, group, offset,
                                "lease expired at " + d.getLeaseExpireAt() + ", ack too late");
                    }
                    d.setStatus(DeliveryStatus.ACKED);
                    d.setLeaseExpireAt(0L);
                    d.setLastStatusChangeAt(now);
                    advanceCommittedOffsetLocked(gs, now);
                    persistLocked();
                    log.info("ack group={} offset={} dedupKey={} INFLIGHT->ACKED attempts={} committedOffset={}",
                            group, offset, dedupKeyOf(offset), d.getAttempts(), gs.getCommittedOffset());
                }
            }
        }
    }

    /** 显式上报失败：租约有效时按退避重排队或进死信；租约失效则拒绝（由 reaper 兜底）。 */
    public void nack(String group, long offset, String error) {
        synchronized (lock) {
            GroupState gs = groups.get(group);
            GroupDelivery d = gs == null ? null : gs.getDeliveries().get(offset);
            if (gs == null || d == null) {
                throw reject(AckRejectReason.GROUP_OR_MESSAGE_UNKNOWN, group, offset,
                        "group or message is unknown (possible cross-group nack)");
            }
            if (d.getStatus() == DeliveryStatus.ACKED) {
                throw reject(AckRejectReason.ALREADY_ACKED, group, offset,
                        "message already acknowledged, nack rejected");
            }
            if (d.getStatus() == DeliveryStatus.DEAD) {
                throw reject(AckRejectReason.MESSAGE_DEAD, group, offset,
                        "message already in dead letter, nack rejected");
            }
            long now = System.currentTimeMillis();
            boolean leaseValid = d.getStatus() == DeliveryStatus.INFLIGHT && d.getLeaseExpireAt() > now;
            if (!leaseValid) {
                if (d.getStatus() == DeliveryStatus.INFLIGHT) {
                    throw reject(AckRejectReason.LEASE_EXPIRED, group, offset,
                            "lease expired, timeout reaper owns the redelivery");
                }
                throw reject(AckRejectReason.NOT_INFLIGHT, group, offset,
                        "message is not inflight; nothing to nack");
            }
            d.setLastError(error);
            failDeliveryLocked(group, offset, d, now, "nack: " + error);
        }
    }

    /** 回收所有租约到期仍未确认的在途消息（重试退避或死信）。 */
    public List<ReapedMessage> reapTimeouts() {
        long now = System.currentTimeMillis();
        List<ReapedMessage> reaped = new ArrayList<>();
        synchronized (lock) {
            for (Map.Entry<String, GroupState> ge : groups.entrySet()) {
                String group = ge.getKey();
                for (Map.Entry<Long, GroupDelivery> de : ge.getValue().getDeliveries().entrySet()) {
                    GroupDelivery d = de.getValue();
                    if (d.getStatus() == DeliveryStatus.INFLIGHT && d.getLeaseExpireAt() <= now) {
                        int attempts = d.getAttempts();
                        boolean willDie = attempts >= properties.getMaxAttempts();
                        // failDeliveryLocked 内部已在持锁状态下完成落盘
                        failDeliveryLocked(group, de.getKey(), d, now, "lease timeout");
                        reaped.add(new ReapedMessage(group, de.getKey(), attempts, willDie));
                    }
                }
            }
            return reaped;
        }
    }

    public List<DeadLetterRecord> deadLetters() {
        synchronized (lock) {
            return List.copyOf(deadLetters);
        }
    }

    public long committedOffset(String group) {
        synchronized (lock) {
            GroupState gs = groups.get(group);
            return gs == null ? 0L : gs.getCommittedOffset();
        }
    }

    /** 仅供测试：清空全部内存与持久化状态。 */
    public void resetForTesting() {
        synchronized (lock) {
            nextOffset = 0L;
            messages.clear();
            groups.clear();
            deadLetters.clear();
            dedupIndex.clear();
            persistLocked();
        }
    }

    // ---- internals, lock already held ----

    /** 一次投递失败后的统一状态流转：未超次数则退避重排队，超次数则进死信。 */
    private void failDeliveryLocked(String group, long offset, GroupDelivery d, long now, String cause) {
        if (d.getAttempts() >= properties.getMaxAttempts()) {
            d.setStatus(DeliveryStatus.DEAD);
            d.setLeaseExpireAt(0L);
            d.setNextVisibleAt(0L);
            d.setLastStatusChangeAt(now);
            MessageRecord msg = messages.get((int) offset);
            DeadLetterRecord dl = new DeadLetterRecord(offset, msg.dedupKey(), group, msg.payload(),
                    msg.publishedAt(), d.getAttempts(),
                    cause + (d.getLastError() == null ? "" : " | " + d.getLastError()), now);
            deadLetters.add(dl);
            persistLocked();
            log.warn("dead-letter group={} offset={} dedupKey={} INFLIGHT->DEAD attempts={} maxAttempts={}",
                    group, offset, msg.dedupKey(), d.getAttempts(), properties.getMaxAttempts());
        } else {
            long visibleAt = now + backoffMs(d.getAttempts());
            d.setStatus(DeliveryStatus.PENDING);
            d.setLeaseExpireAt(0L);
            d.setNextVisibleAt(visibleAt);
            d.setLastStatusChangeAt(now);
            persistLocked();
            log.info("retry group={} offset={} dedupKey={} INFLIGHT->PENDING attempts={} nextVisibleAt={} cause={}",
                    group, offset, dedupKeyOf(offset), d.getAttempts(), visibleAt, cause);
        }
    }

    /** 连续 ACK 水位推进：从当前 committedOffset 起，跳过连续已 ACK 的消息。 */
    private void advanceCommittedOffsetLocked(GroupState gs, long now) {
        long off = gs.getCommittedOffset();
        while (off < messages.size()) {
            GroupDelivery d = gs.getDeliveries().get(off);
            if (d != null && d.getStatus() == DeliveryStatus.ACKED) {
                off++;
            } else {
                break;
            }
        }
        if (off != gs.getCommittedOffset()) {
            gs.setCommittedOffset(off);
        }
    }

    private long backoffMs(int failedAttempts) {
        long base = properties.getBackoffBaseMs();
        return switch (properties.getBackoffStrategy()) {
            case FIXED -> base;
            case LINEAR -> base * Math.max(1, failedAttempts);
            case EXPONENTIAL -> base * (1L << Math.min(failedAttempts - 1, 20));
        };
    }

    private String dedupKeyOf(long offset) {
        return offset >= 0 && offset < messages.size() ? messages.get((int) offset).dedupKey() : "?";
    }

    private void persistLocked() {
        storage.save(new QueueStorage.Snapshot(nextOffset,
                List.copyOf(messages), new LinkedHashMap<>(groups), List.copyOf(deadLetters)));
    }

    private AckRejectedException reject(AckRejectReason reason, String group, long offset, String msg) {
        log.info("reject group={} offset={} reason={} detail={}", group, offset, reason, msg);
        return new AckRejectedException(reason, msg + " (group=" + group + ", offset=" + offset + ")");
    }

    /** 拉取返回项。 */
    public record DeliveredMessage(long offset, String dedupKey, String payload,
                                   int attempt, long leaseExpireAt) {
    }

    /** 超时回收返回项。 */
    public record ReapedMessage(String group, long offset, int attempts, boolean deadLettered) {
    }
}
