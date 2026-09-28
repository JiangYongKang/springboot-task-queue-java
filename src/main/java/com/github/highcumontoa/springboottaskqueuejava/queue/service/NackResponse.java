package com.github.highcumontoa.springboottaskqueuejava.queue.service;

public record NackResponse(boolean accepted, String reason, Long offset, String state,
                          int attempt, Long nextVisibleAtEpochMilli, Long committedOffset) {
}
