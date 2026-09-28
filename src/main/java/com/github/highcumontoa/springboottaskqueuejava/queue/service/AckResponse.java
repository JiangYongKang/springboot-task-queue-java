package com.github.highcumontoa.springboottaskqueuejava.queue.service;

public record AckResponse(boolean accepted, String reason, Long committedOffset) {

    public static AckResponse ok(long committedOffset) {
        return new AckResponse(true, null, committedOffset);
    }

    public static AckResponse rejected(String reason) {
        return new AckResponse(false, reason, null);
    }
}
