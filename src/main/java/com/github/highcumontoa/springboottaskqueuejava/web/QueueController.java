package com.github.highcumontoa.springboottaskqueuejava.web;

import java.util.List;
import java.util.Map;
import com.github.highcumontoa.springboottaskqueuejava.queue.DeadLetterRecord;
import com.github.highcumontoa.springboottaskqueuejava.queue.MessageRecord;
import com.github.highcumontoa.springboottaskqueuejava.queue.TaskQueueService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 任务队列对外 HTTP 接口。 */
@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final TaskQueueService service;

    public QueueController(TaskQueueService service) {
        this.service = service;
    }

    public record PublishRequest(String dedupKey, String payload) {
    }

    public record AckRequest(String error) {
    }

    @PostMapping("/publish")
    public Map<String, Object> publish(@RequestBody PublishRequest req) {
        MessageRecord msg = service.publish(req.dedupKey(), req.payload());
        return Map.of("offset", msg.offset(), "dedupKey", msg.dedupKey(),
                "publishedAt", msg.publishedAt());
    }

    @GetMapping("/groups/{group}/pull")
    public List<TaskQueueService.DeliveredMessage> pull(@PathVariable String group,
                                                        @RequestParam(defaultValue = "10") int maxMessages) {
        return service.pull(group, maxMessages);
    }

    @PostMapping("/groups/{group}/messages/{offset}/ack")
    public Map<String, Object> ack(@PathVariable String group, @PathVariable long offset) {
        service.ack(group, offset);
        return Map.of("group", group, "offset", offset, "status", "ACKED");
    }

    @PostMapping("/groups/{group}/messages/{offset}/nack")
    public Map<String, Object> nack(@PathVariable String group, @PathVariable long offset,
                                    @RequestBody(required = false) AckRequest req) {
        service.nack(group, offset, req == null ? null : req.error());
        return Map.of("group", group, "offset", offset, "status", "REQUEUED");
    }

    @GetMapping("/dead-letters")
    public List<DeadLetterRecord> deadLetters() {
        return service.deadLetters();
    }

    @GetMapping("/groups/{group}/offset")
    public Map<String, Object> offset(@PathVariable String group) {
        return Map.of("group", group, "committedOffset", service.committedOffset(group));
    }
}
