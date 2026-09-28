package com.github.highcumontoa.springboottaskqueuejava.queue.repository;

import com.github.highcumontoa.springboottaskqueuejava.queue.model.AckRejection;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.DeadLetter;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.Delivery;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.GroupOffset;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.MessageStatus;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.QueueMessage;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.RetrySchedule;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class JdbcQueueRepository implements QueueRepository {

    private final JdbcTemplate jdbc;

    public JdbcQueueRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void initializeSchema() {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS queue_messages (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    dedup_key VARCHAR(128) NOT NULL UNIQUE,
                    payload CLOB NOT NULL,
                    created_at TIMESTAMP(3) NOT NULL
                )
                """);
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS queue_group_messages (
                    group_name VARCHAR(128) NOT NULL,
                    message_id BIGINT NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    attempt_count INT NOT NULL DEFAULT 0,
                    delivery_id VARCHAR(64),
                    leased_at TIMESTAMP(3),
                    lease_deadline TIMESTAMP(3),
                    visible_at BIGINT NOT NULL DEFAULT 0,
                    last_error VARCHAR(1000),
                    PRIMARY KEY (group_name, message_id)
                )
                """);
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_qgm_pull ON queue_group_messages "
                + "(group_name, status, visible_at, message_id)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_qgm_delivery ON queue_group_messages (delivery_id)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_qgm_lease ON queue_group_messages (status, lease_deadline)");
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS queue_group_offsets (
                    group_name VARCHAR(128) PRIMARY KEY,
                    -- Highest offset up to which every message is terminal (ACKED or DEAD).
                    committed_offset BIGINT NOT NULL DEFAULT 0,
                    -- Number of non-terminal rows strictly above committed_offset.
                    inflight_count BIGINT NOT NULL DEFAULT 0,
                    -- Highest offset already observed/claimed for this group (visibility fence).
                    high_watermark BIGINT NOT NULL DEFAULT 0
                )
                """);
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS queue_dead_letters (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    message_id BIGINT NOT NULL,
                    dedup_key VARCHAR(128) NOT NULL,
                    payload CLOB NOT NULL,
                    group_name VARCHAR(128) NOT NULL,
                    attempts INT NOT NULL,
                    last_error VARCHAR(1000),
                    original_published_at TIMESTAMP(3) NOT NULL,
                    dead_at TIMESTAMP(3) NOT NULL
                )
                """);
    }

    @Override
    @Transactional
    public PublishRecord publish(String dedupKey, String payload, Instant now) {
        try {
            jdbc.update("INSERT INTO queue_messages (dedup_key, payload, created_at) VALUES (?, ?, ?)",
                    dedupKey, payload, Timestamp.from(now));
        } catch (org.springframework.dao.DuplicateKeyException e) {
            Long existing = jdbc.queryForObject(
                    "SELECT id FROM queue_messages WHERE dedup_key = ?", Long.class, dedupKey);
            return new PublishRecord(existing, true);
        }
        Long id = jdbc.queryForObject("SELECT id FROM queue_messages WHERE dedup_key = ?",
                Long.class, dedupKey);
        return new PublishRecord(id, false);
    }

    @Override
    public Optional<QueueMessage> findByDedupKey(String dedupKey) {
        return jdbc.query("SELECT id, dedup_key, payload, created_at FROM queue_messages "
                        + "WHERE dedup_key = ?", messageMapper(), dedupKey)
                .stream().findFirst();
    }

    /**
     * Atomically claim one available message for the group.
     *
     * All claims for a group serialize on the group offset row (SELECT ... FOR UPDATE),
     * so two consumers in the same group can never receive the same message. Under that
     * lock we choose the message, write its group row, and maintain the offset
     * bookkeeping counters in the same transaction.
     */
    @Override
    @Transactional
    public Optional<Delivery> claimOne(String groupName, Instant now, Instant leaseDeadline) {
        ensureGroup(groupName);
        long[] counters = lockGroupCounters(groupName);
        long committed = counters[0];

        // 1) A RETRY_WAIT message whose backoff has elapsed (lowest offset first).
        Long messageId = jdbc.query("""
                        SELECT gm.message_id
                        FROM queue_group_messages gm
                        WHERE gm.group_name = ?
                          AND gm.status = 'RETRY_WAIT' AND gm.visible_at <= ?
                          AND gm.message_id > ?
                        ORDER BY gm.message_id ASC
                        LIMIT 1
                        FOR UPDATE
                        """,
                (rs, n) -> rs.getLong(1),
                groupName, now.toEpochMilli(), committed)
                .stream().findFirst().orElse(null);

        boolean existingRow = messageId != null;

        // 2) A brand-new message this group has never materialized (lowest id first).
        if (messageId == null) {
            messageId = jdbc.query("""
                            SELECT m.id
                            FROM queue_messages m
                            WHERE m.id > ?
                              AND NOT EXISTS (
                                  SELECT 1 FROM queue_group_messages gm
                                  WHERE gm.group_name = ? AND gm.message_id = m.id
                              )
                            ORDER BY m.id ASC
                            LIMIT 1
                            FOR UPDATE OF m
                            """,
                    (rs, n) -> rs.getLong(1),
                    committed, groupName)
                    .stream().findFirst().orElse(null);
            if (messageId == null) {
                return Optional.empty();
            }
        }

        QueueMessage msg = jdbc.queryForObject(
                "SELECT id, dedup_key, payload, created_at FROM queue_messages WHERE id = ? FOR UPDATE",
                messageMapper(), messageId);

        String deliveryId = java.util.UUID.randomUUID().toString();
        int attempt;
        if (existingRow) {
            jdbc.update("""
                            UPDATE queue_group_messages
                            SET status = 'INFLIGHT', delivery_id = ?, leased_at = ?,
                                lease_deadline = ?, attempt_count = attempt_count + 1
                            WHERE group_name = ? AND message_id = ?
                            """,
                    deliveryId, Timestamp.from(now), Timestamp.from(leaseDeadline),
                    groupName, messageId);
            attempt = jdbc.queryForObject(
                    "SELECT attempt_count FROM queue_group_messages WHERE group_name = ? AND message_id = ?",
                    Integer.class, groupName, messageId);
            // RETRY_WAIT was already counted in inflight_count; no counter change.
        } else {
            attempt = 1;
            jdbc.update("""
                            INSERT INTO queue_group_messages
                                (group_name, message_id, status, attempt_count, delivery_id,
                                 leased_at, lease_deadline, visible_at)
                            VALUES (?, ?, 'INFLIGHT', 1, ?, ?, ?, 0)
                            """,
                    groupName, messageId, deliveryId, Timestamp.from(now),
                    Timestamp.from(leaseDeadline));
            // A new unconsumed message enters the non-terminal window.
            jdbc.update("UPDATE queue_group_offsets SET inflight_count = inflight_count + 1, "
                    + "high_watermark = CASE WHEN high_watermark < ? THEN ? ELSE high_watermark END "
                    + "WHERE group_name = ?", messageId, messageId, groupName);
        }

        Delivery d = new Delivery();
        d.setDeliveryId(deliveryId);
        d.setMessageOffset(messageId);
        d.setGroupName(groupName);
        d.setPayload(msg.getPayload());
        d.setDedupKey(msg.getDedupKey());
        d.setAttempt(attempt);
        d.setLeasedAt(now);
        return Optional.of(d);
    }

    @Override
    @Transactional
    public AckResult ack(String groupName, String deliveryId, Instant now) {
        ensureGroup(groupName);
        lockGroupCounters(groupName);

        List<long[]> rows = jdbc.query(
                "SELECT message_id FROM queue_group_messages WHERE group_name = ? AND delivery_id = ? FOR UPDATE",
                (rs, i) -> new long[]{rs.getLong(1)}, groupName, deliveryId);

        if (rows.isEmpty()) {
            List<String> otherGroups = jdbc.query(
                    "SELECT group_name FROM queue_group_messages WHERE delivery_id = ? LIMIT 1",
                    (rs, i) -> rs.getString(1), deliveryId);
            return AckResult.reject(otherGroups.isEmpty()
                    ? AckRejection.DELIVERY_NOT_FOUND
                    : AckRejection.GROUP_MISMATCH);
        }

        long messageId = rows.get(0)[0];
        MessageStatus status = loadStatus(groupName, messageId);
        if (status != MessageStatus.INFLIGHT) {
            return AckResult.reject(status == MessageStatus.ACKED
                    ? AckRejection.DUPLICATE_ACK
                    : AckRejection.MESSAGE_NOT_INFLIGHT);
        }

        int updated = jdbc.update("UPDATE queue_group_messages SET status = 'ACKED', lease_deadline = NULL "
                + "WHERE group_name = ? AND message_id = ? AND status = 'INFLIGHT'",
                groupName, messageId);
        if (updated == 0) {
            return AckResult.reject(AckRejection.MESSAGE_NOT_INFLIGHT);
        }
        settleMessage(groupName, messageId);
        return AckResult.ok();
    }

    @Override
    @Transactional
    public NackOutcome nack(String groupName, String deliveryId, String error, int maxAttempts,
                            RetrySchedule schedule, Instant now) {
        ensureGroup(groupName);
        lockGroupCounters(groupName);

        List<long[]> rows = jdbc.query(
                "SELECT message_id FROM queue_group_messages WHERE group_name = ? AND delivery_id = ? FOR UPDATE",
                (rs, i) -> new long[]{rs.getLong(1)}, groupName, deliveryId);

        if (rows.isEmpty()) {
            List<String> otherGroups = jdbc.query(
                    "SELECT group_name FROM queue_group_messages WHERE delivery_id = ? LIMIT 1",
                    (rs, i) -> rs.getString(1), deliveryId);
            return new NackOutcome(false, otherGroups.isEmpty()
                    ? AckRejection.DELIVERY_NOT_FOUND
                    : AckRejection.GROUP_MISMATCH, null, false, 0);
        }

        long messageId = rows.get(0)[0];
        MessageStatus status = loadStatus(groupName, messageId);
        int currentAttempt = jdbc.queryForObject(
                "SELECT attempt_count FROM queue_group_messages WHERE group_name = ? AND message_id = ?",
                Integer.class, groupName, messageId);
        if (status != MessageStatus.INFLIGHT) {
            return new NackOutcome(false, status == MessageStatus.ACKED
                    ? AckRejection.DUPLICATE_ACK
                    : AckRejection.MESSAGE_NOT_INFLIGHT, messageId, false, currentAttempt);
        }

        boolean dead = currentAttempt >= maxAttempts;
        if (dead) {
            moveToDeadLetter(groupName, messageId, currentAttempt, error, now);
            settleMessage(groupName, messageId);
            return new NackOutcome(true, null, messageId, true, currentAttempt);
        }

        long visibleAt = schedule.nextVisibleAtEpochMilli(currentAttempt + 1, now.toEpochMilli());
        int updated = jdbc.update("""
                        UPDATE queue_group_messages
                        SET status = 'RETRY_WAIT', visible_at = ?, last_error = ?, lease_deadline = NULL
                        WHERE group_name = ? AND message_id = ? AND status = 'INFLIGHT'
                        """,
                visibleAt, truncate(error), groupName, messageId);
        if (updated == 0) {
            return new NackOutcome(false, AckRejection.MESSAGE_NOT_INFLIGHT, messageId, false,
                    currentAttempt);
        }
        // RETRY_WAIT remains a non-terminal row: inflight_count unchanged.
        return new NackOutcome(true, null, messageId, false, currentAttempt);
    }

    @Override
    @Transactional
    public int reapExpiredLeases(Instant now, int maxAttempts, RetrySchedule schedule) {
        if (!tableExists("QUEUE_GROUP_OFFSETS")) {
            return 0;
        }
        // Process one group at a time, taking the same group lock used by pull/ack.
        List<String> groups = jdbc.queryForList(
                "SELECT group_name FROM queue_group_offsets ORDER BY group_name", String.class);
        int affected = 0;
        for (String groupName : groups) {
            lockGroupCounters(groupName);
            List<LeaseRow> expired = jdbc.query("""
                            SELECT message_id, attempt_count
                            FROM queue_group_messages
                            WHERE group_name = ? AND status = 'INFLIGHT'
                              AND lease_deadline IS NOT NULL AND lease_deadline <= ?
                            ORDER BY message_id
                            FOR UPDATE
                            """,
                    (rs, i) -> new LeaseRow(rs.getLong(1), rs.getInt(2)),
                    groupName, Timestamp.from(now));

            for (LeaseRow row : expired) {
                boolean dead = row.attemptCount() >= maxAttempts;
                if (dead) {
                    moveToDeadLetter(groupName, row.messageId(), row.attemptCount(),
                            "lease timeout: delivery not acked before deadline", now);
                    settleMessage(groupName, row.messageId());
                } else {
                    long visibleAt = schedule.nextVisibleAtEpochMilli(row.attemptCount() + 1,
                            now.toEpochMilli());
                    jdbc.update("""
                                    UPDATE queue_group_messages
                                    SET status = 'RETRY_WAIT', visible_at = ?,
                                        last_error = 'lease timeout: delivery not acked before deadline',
                                        lease_deadline = NULL
                                    WHERE group_name = ? AND message_id = ? AND status = 'INFLIGHT'
                                    """,
                            visibleAt, groupName, row.messageId());
                }
                affected++;
            }
        }
        return affected;
    }

    @Override
    public List<DeadLetter> findDeadLetters(String groupName, int limit) {
        return jdbc.query("""
                        SELECT id, message_id, dedup_key, payload, group_name, attempts,
                               last_error, original_published_at, dead_at
                        FROM queue_dead_letters
                        WHERE (? IS NULL OR group_name = ?)
                        ORDER BY id ASC LIMIT ?
                        """,
                deadLetterMapper(), groupName, groupName, Math.max(1, Math.min(limit, 500)));
    }

    @Override
    public List<GroupOffset> findAllOffsets() {
        return jdbc.query("SELECT group_name, committed_offset FROM queue_group_offsets ORDER BY group_name",
                (rs, i) -> new GroupOffset(rs.getString(1), rs.getLong(2)));
    }

    @Override
    public Optional<GroupOffset> findOffset(String groupName) {
        return jdbc.query("SELECT group_name, committed_offset FROM queue_group_offsets WHERE group_name = ?",
                (rs, i) -> new GroupOffset(rs.getString(1), rs.getLong(2)), groupName)
                .stream().findFirst();
    }

    // ----- internals -----

    private boolean tableExists(String tableName) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
                Integer.class, tableName);
        return count != null && count > 0;
    }

    private void ensureGroup(String groupName) {
        try {
            jdbc.update("INSERT INTO queue_group_offsets (group_name, committed_offset, inflight_count, "
                    + "high_watermark) SELECT ?, 0, 0, 0 WHERE NOT EXISTS "
                    + "(SELECT 1 FROM queue_group_offsets WHERE group_name = ?)", groupName, groupName);
        } catch (org.springframework.dao.DuplicateKeyException ignored) {
            // Another transaction created the group first; its counters are already valid.
        }
    }

    /** Locks the per-group counter row and returns [committedOffset, inflightCount, highWatermark]. */
    private long[] lockGroupCounters(String groupName) {
        return jdbc.queryForObject("""
                        SELECT committed_offset, inflight_count, high_watermark
                        FROM queue_group_offsets WHERE group_name = ? FOR UPDATE
                        """,
                (rs, n) -> new long[]{rs.getLong(1), rs.getLong(2), rs.getLong(3)},
                groupName);
    }

    /**
     * Mark one message as settled (ACKED or DEAD): decrement the non-terminal counter and,
     * if the window is fully drained, fast-forward the committed offset to the high
     * watermark. Runs under the caller's per-group lock, so concurrent settlements cannot
     * lose increments or roll the offset back.
     */
    private void settleMessage(String groupName, long messageId) {
        long[] counters = lockGroupCounters(groupName);
        long committed = counters[0];
        long inflight = counters[1];
        long watermark = counters[2];

        long remaining = inflight - 1;
        long newCommitted = committed;
        if (remaining == 0) {
            newCommitted = Math.max(committed, watermark);
        }
        jdbc.update("UPDATE queue_group_offsets SET inflight_count = ?, committed_offset = ? "
                + "WHERE group_name = ?", Math.max(0, remaining), newCommitted, groupName);
    }

    private MessageStatus loadStatus(String groupName, long messageId) {
        String s = jdbc.queryForObject(
                "SELECT status FROM queue_group_messages WHERE group_name = ? AND message_id = ?",
                String.class, groupName, messageId);
        return MessageStatus.valueOf(s);
    }

    private void moveToDeadLetter(String groupName, long messageId, int attempt,
                                  String error, Instant now) {
        QueueMessage msg = jdbc.queryForObject(
                "SELECT id, dedup_key, payload, created_at FROM queue_messages WHERE id = ?",
                messageMapper(), messageId);
        jdbc.update("""
                        INSERT INTO queue_dead_letters
                            (message_id, dedup_key, payload, group_name, attempts,
                             last_error, original_published_at, dead_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                messageId, msg.getDedupKey(), msg.getPayload(), groupName, attempt,
                truncate(error), Timestamp.from(msg.getCreatedAt()), Timestamp.from(now));
        jdbc.update("UPDATE queue_group_messages SET status = 'DEAD', lease_deadline = NULL, "
                + "last_error = ? WHERE group_name = ? AND message_id = ?",
                truncate(error), groupName, messageId);
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= 1000 ? s : s.substring(0, 1000);
    }

    private RowMapper<QueueMessage> messageMapper() {
        return (rs, n) -> new QueueMessage(
                rs.getLong("id"),
                rs.getString("dedup_key"),
                rs.getString("payload"),
                rs.getTimestamp("created_at").toInstant());
    }

    private RowMapper<DeadLetter> deadLetterMapper() {
        return new RowMapper<>() {
            @Override
            public DeadLetter mapRow(ResultSet rs, int n) throws SQLException {
                DeadLetter d = new DeadLetter();
                d.setId(rs.getLong("id"));
                d.setMessageOffset(rs.getLong("message_id"));
                d.setDedupKey(rs.getString("dedup_key"));
                d.setPayload(rs.getString("payload"));
                d.setGroupName(rs.getString("group_name"));
                d.setAttempts(rs.getInt("attempts"));
                d.setLastError(rs.getString("last_error"));
                d.setOriginalPublishedAt(rs.getTimestamp("original_published_at").toInstant());
                d.setDeadAt(rs.getTimestamp("dead_at").toInstant());
                return d;
            }
        };
    }

    private record LeaseRow(long messageId, int attemptCount) {}
}
