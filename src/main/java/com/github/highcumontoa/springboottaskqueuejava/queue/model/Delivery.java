package com.github.highcumontoa.springboottaskqueuejava.queue.model;

import java.time.Instant;

public class Delivery {
    private String deliveryId;
    private long messageOffset;
    private String groupName;
    private String payload;
    private String dedupKey;
    private int attempt;
    private Instant leasedAt;

    public Delivery() {}

    public String getDeliveryId() { return deliveryId; }
    public void setDeliveryId(String deliveryId) { this.deliveryId = deliveryId; }
    public long getMessageOffset() { return messageOffset; }
    public void setMessageOffset(long messageOffset) { this.messageOffset = messageOffset; }
    public String getGroupName() { return groupName; }
    public void setGroupName(String groupName) { this.groupName = groupName; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public String getDedupKey() { return dedupKey; }
    public void setDedupKey(String dedupKey) { this.dedupKey = dedupKey; }
    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public Instant getLeasedAt() { return leasedAt; }
    public void setLeasedAt(Instant leasedAt) { this.leasedAt = leasedAt; }
}
