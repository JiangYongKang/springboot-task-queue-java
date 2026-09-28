package com.github.highcumontoa.springboottaskqueuejava.queue.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "taskqueue")
public class QueueProperties {

    private int maxAttempts = 3;
    private long backoffMillis = 1000;
    private double backoffMultiplier = 2.0;
    private long ackTimeoutMillis = 30000;
    private long reaperIntervalMillis = 5000;

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public long getBackoffMillis() { return backoffMillis; }
    public void setBackoffMillis(long backoffMillis) { this.backoffMillis = backoffMillis; }

    public double getBackoffMultiplier() { return backoffMultiplier; }
    public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }

    public long getAckTimeoutMillis() { return ackTimeoutMillis; }
    public void setAckTimeoutMillis(long ackTimeoutMillis) { this.ackTimeoutMillis = ackTimeoutMillis; }

    public long getReaperIntervalMillis() { return reaperIntervalMillis; }
    public void setReaperIntervalMillis(long reaperIntervalMillis) { this.reaperIntervalMillis = reaperIntervalMillis; }
}
