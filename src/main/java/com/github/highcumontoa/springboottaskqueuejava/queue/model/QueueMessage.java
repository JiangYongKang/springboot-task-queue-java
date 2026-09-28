package com.github.highcumontoa.springboottaskqueuejava.queue.model;

import java.time.Instant;

public class QueueMessage {
    private long offset;
    private String dedupKey;
    private String payload;
    private Instant createdAt;

    public QueueMessage() {}

    public QueueMessage(long offset, String dedupKey, String payload, Instant createdAt) {
        this.offset = offset;
        this.dedupKey = dedupKey;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }
    public String getDedupKey() { return dedupKey; }
    public void setDedupKey(String dedupKey) { this.dedupKey = dedupKey; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
