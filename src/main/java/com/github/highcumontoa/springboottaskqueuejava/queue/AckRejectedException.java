package com.github.highcumontoa.springboottaskqueuejava.queue;

/** 确认/否定确认被拒绝，带可区分原因。 */
public class AckRejectedException extends QueueException {

    private final AckRejectReason reason;

    public AckRejectedException(AckRejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public AckRejectReason getReason() { return reason; }
}
