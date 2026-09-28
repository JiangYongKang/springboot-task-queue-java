package com.github.highcumontoa.springboottaskqueuejava.queue.model;

/**
 * Computes the earliest epoch-milli time at which a retry attempt becomes visible.
 */
@FunctionalInterface
public interface RetrySchedule {
    long nextVisibleAtEpochMilli(int nextAttempt, long nowEpochMilli);
}
