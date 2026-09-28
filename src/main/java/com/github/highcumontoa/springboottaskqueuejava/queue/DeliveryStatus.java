package com.github.highcumontoa.springboottaskqueuejava.queue;

/** 某消费者组内一条消息的投递状态。 */
public enum DeliveryStatus {
    /** 可被拉取（含等待退避结束的情形，退避时间另由 nextVisibleAt 控制）。 */
    PENDING,
    /** 已投递给某个消费者，处于租约确认窗口内。 */
    INFLIGHT,
    /** 已确认，终态。 */
    ACKED,
    /** 超过重试上限进入死信，终态。 */
    DEAD
}
