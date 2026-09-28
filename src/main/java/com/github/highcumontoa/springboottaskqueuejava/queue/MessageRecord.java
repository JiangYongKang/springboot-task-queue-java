package com.github.highcumontoa.springboottaskqueuejava.queue;

/**
 * 消息日志中的一条原始消息。offset 为组内共享的单调递增位点，dedupKey 为全局唯一去重标识。
 */
public record MessageRecord(
        long offset,
        String dedupKey,
        String payload,
        long publishedAt
) {
}
