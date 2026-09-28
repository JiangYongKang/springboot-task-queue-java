package com.github.highcumontoa.springboottaskqueuejava.queue.model;

public class GroupOffset {
    private String groupName;
    private long committedOffset;

    public GroupOffset() {}

    public GroupOffset(String groupName, long committedOffset) {
        this.groupName = groupName;
        this.committedOffset = committedOffset;
    }

    public String getGroupName() { return groupName; }
    public void setGroupName(String groupName) { this.groupName = groupName; }
    public long getCommittedOffset() { return committedOffset; }
    public void setCommittedOffset(long committedOffset) { this.committedOffset = committedOffset; }
}
