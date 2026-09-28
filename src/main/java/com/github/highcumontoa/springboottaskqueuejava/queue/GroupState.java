package com.github.highcumontoa.springboottaskqueuejava.queue;

import java.util.LinkedHashMap;
import java.util.Map;

/** 一个消费者组的全部状态：已提交偏移量 + 每条消息的投递状态。 */
public class GroupState {

    private long committedOffset;
    private Map<Long, GroupDelivery> deliveries = new LinkedHashMap<>();

    public long getCommittedOffset() { return committedOffset; }
    public void setCommittedOffset(long committedOffset) { this.committedOffset = committedOffset; }

    public Map<Long, GroupDelivery> getDeliveries() { return deliveries; }
    public void setDeliveries(Map<Long, GroupDelivery> deliveries) { this.deliveries = deliveries; }
}
