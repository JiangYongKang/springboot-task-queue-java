package com.github.highcumontoa.springboottaskqueuejava.queue;

/** 失败重试的退避策略。 */
public enum BackoffStrategy {
    /** 固定退避：base。 */
    FIXED,
    /** 线性退避：base * attempt。 */
    LINEAR,
    /** 指数退避：base * 2^(attempt-1)。 */
    EXPONENTIAL
}
