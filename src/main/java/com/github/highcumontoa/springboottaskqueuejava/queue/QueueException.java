package com.github.highcumontoa.springboottaskqueuejava.queue;

/** 队列业务异常基类。 */
public class QueueException extends RuntimeException {

    public QueueException(String message) {
        super(message);
    }
}
