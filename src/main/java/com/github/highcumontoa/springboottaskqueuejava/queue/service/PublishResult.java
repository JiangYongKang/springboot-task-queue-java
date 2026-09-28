package com.github.highcumontoa.springboottaskqueuejava.queue.service;

public record PublishResult(long offset, String dedupKey, boolean duplicated) {
}
