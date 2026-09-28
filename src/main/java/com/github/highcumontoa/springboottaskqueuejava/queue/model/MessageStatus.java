package com.github.highcumontoa.springboottaskqueuejava.queue.model;

public enum MessageStatus {
    PENDING,
    INFLIGHT,
    RETRY_WAIT,
    ACKED,
    DEAD
}
