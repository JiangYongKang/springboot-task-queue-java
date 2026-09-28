package com.github.highcumontoa.springboottaskqueuejava.queue.model;

public enum AckRejection {
    DELIVERY_NOT_FOUND,
    GROUP_MISMATCH,
    MESSAGE_NOT_INFLIGHT,
    DUPLICATE_ACK
}
