package com.github.highcumontoa.springboottaskqueuejava.queue;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 队列可配置参数，前缀 taskqueue。 */
@ConfigurationProperties(prefix = "taskqueue")
public class QueueProperties {

    /** 持久化数据目录。 */
    private String dataDir = "data/taskqueue";

    /** 拉取后确认租约时长（毫秒）。 */
    private long visibilityTimeoutMs = 10_000;

    /** 最大投递次数（含首次），超过后进入死信。 */
    private int maxAttempts = 5;

    /** 重试基础退避（毫秒）。 */
    private long backoffBaseMs = 1_000;

    /** 退避策略。 */
    private BackoffStrategy backoffStrategy = BackoffStrategy.EXPONENTIAL;

    /** 租约超时回收扫描间隔（毫秒）。 */
    private long reaperIntervalMs = 1_000;

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }

    public long getVisibilityTimeoutMs() { return visibilityTimeoutMs; }
    public void setVisibilityTimeoutMs(long visibilityTimeoutMs) { this.visibilityTimeoutMs = visibilityTimeoutMs; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public long getBackoffBaseMs() { return backoffBaseMs; }
    public void setBackoffBaseMs(long backoffBaseMs) { this.backoffBaseMs = backoffBaseMs; }

    public BackoffStrategy getBackoffStrategy() { return backoffStrategy; }
    public void setBackoffStrategy(BackoffStrategy backoffStrategy) { this.backoffStrategy = backoffStrategy; }

    public long getReaperIntervalMs() { return reaperIntervalMs; }
    public void setReaperIntervalMs(long reaperIntervalMs) { this.reaperIntervalMs = reaperIntervalMs; }
}
