package com.github.highcumontoa.springboottaskqueuejava.queue;

/** 确认/否定确认被拒绝时的可区分原因。 */
public enum AckRejectReason {
    /** 该组不存在或该位点不属于该组投递记录（跨组确认）。 */
    GROUP_OR_MESSAGE_UNKNOWN,
    /** 当前已被确认，重复确认。 */
    ALREADY_ACKED,
    /** 当前已进死信，不能再确认。 */
    MESSAGE_DEAD,
    /** 消息处于可拉取状态，没有在途投递可确认（租约已超时回收或被显式否定）。 */
    NOT_INFLIGHT,
    /** 租约已过期且消息已被重新投递。 */
    LEASE_EXPIRED
}
