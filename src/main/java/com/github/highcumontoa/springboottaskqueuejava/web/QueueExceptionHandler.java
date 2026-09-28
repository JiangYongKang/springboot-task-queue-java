package com.github.highcumontoa.springboottaskqueuejava.web;

import java.util.Map;
import com.github.highcumontoa.springboottaskqueuejava.queue.AckRejectedException;
import com.github.highcumontoa.springboottaskqueuejava.queue.DuplicateDedupKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** 把队列业务异常映射为可区分的 HTTP 响应。 */
@RestControllerAdvice
public class QueueExceptionHandler {

    @ExceptionHandler(DuplicateDedupKeyException.class)
    public ResponseEntity<Map<String, Object>> duplicate(DuplicateDedupKeyException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "error", "DUPLICATE_DEDUP_KEY",
                "dedupKey", ex.getDedupKey(),
                "existingOffset", ex.getExistingOffset(),
                "message", ex.getMessage()));
    }

    @ExceptionHandler(AckRejectedException.class)
    public ResponseEntity<Map<String, Object>> ackRejected(AckRejectedException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "error", "ACK_REJECTED",
                "reason", ex.getReason().name(),
                "message", ex.getMessage()));
    }
}
