package com.github.highcumontoa.springboottaskqueuejava.queue;

/** 死信记录，保留消息的原始投递信息。 */
public record DeadLetterRecord(
        long offset,
        String dedupKey,
        String consumerGroup,
        String payload,
        long publishedAt,
        int attempts,
        String lastError,
        long deadAt
) {
}
