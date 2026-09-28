package com.github.highcumontoa.springboottaskqueuejava.queue;

/** 投递时去重标识已存在。 */
public class DuplicateDedupKeyException extends QueueException {

    private final String dedupKey;
    private final long existingOffset;

    public DuplicateDedupKeyException(String dedupKey, long existingOffset) {
        super("dedupKey already exists: " + dedupKey + " at offset " + existingOffset);
        this.dedupKey = dedupKey;
        this.existingOffset = existingOffset;
    }

    public String getDedupKey() { return dedupKey; }
    public long getExistingOffset() { return existingOffset; }
}
