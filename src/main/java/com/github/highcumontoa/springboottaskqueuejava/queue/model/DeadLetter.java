package com.github.highcumontoa.springboottaskqueuejava.queue.model;

import java.time.Instant;

public class DeadLetter {
    private long id;
    private long messageOffset;
    private String dedupKey;
    private String payload;
    private String groupName;
    private int attempts;
    private String lastError;
    private Instant deadAt;
    private Instant originalPublishedAt;

    public long getId() { return id; }
    public void setId(long id) { this.id = id; }
    public long getMessageOffset() { return messageOffset; }
    public void setMessageOffset(long messageOffset) { this.messageOffset = messageOffset; }
    public String getDedupKey() { return dedupKey; }
    public void setDedupKey(String dedupKey) { this.dedupKey = dedupKey; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public String getGroupName() { return groupName; }
    public void setGroupName(String groupName) { this.groupName = groupName; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
    public Instant getDeadAt() { return deadAt; }
    public void setDeadAt(Instant deadAt) { this.deadAt = deadAt; }
    public Instant getOriginalPublishedAt() { return originalPublishedAt; }
    public void setOriginalPublishedAt(Instant originalPublishedAt) { this.originalPublishedAt = originalPublishedAt; }
}
