package com.github.highcumontoa.springboottaskqueuejava.queue;

/** 单个消费者组对单条消息的投递状态（可变，随持久化快照读写）。 */
public class GroupDelivery {

    private DeliveryStatus status = DeliveryStatus.PENDING;
    private int attempts;
    private long leaseExpireAt;
    private long nextVisibleAt;
    private String lastError;
    private long lastStatusChangeAt;

    public DeliveryStatus getStatus() { return status; }
    public void setStatus(DeliveryStatus status) { this.status = status; }

    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }

    public long getLeaseExpireAt() { return leaseExpireAt; }
    public void setLeaseExpireAt(long leaseExpireAt) { this.leaseExpireAt = leaseExpireAt; }

    public long getNextVisibleAt() { return nextVisibleAt; }
    public void setNextVisibleAt(long nextVisibleAt) { this.nextVisibleAt = nextVisibleAt; }

    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }

    public long getLastStatusChangeAt() { return lastStatusChangeAt; }
    public void setLastStatusChangeAt(long lastStatusChangeAt) { this.lastStatusChangeAt = lastStatusChangeAt; }
}
