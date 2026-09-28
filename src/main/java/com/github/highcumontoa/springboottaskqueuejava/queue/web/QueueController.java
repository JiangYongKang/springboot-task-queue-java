package com.github.highcumontoa.springboottaskqueuejava.queue.web;

import com.github.highcumontoa.springboottaskqueuejava.queue.model.DeadLetter;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.Delivery;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.GroupOffset;
import com.github.highcumontoa.springboottaskqueuejava.queue.service.AckResponse;
import com.github.highcumontoa.springboottaskqueuejava.queue.service.NackResponse;
import com.github.highcumontoa.springboottaskqueuejava.queue.service.PublishResult;
import com.github.highcumontoa.springboottaskqueuejava.queue.service.QueueService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final QueueService queueService;

    public QueueController(QueueService queueService) {
        this.queueService = queueService;
    }

    public record PublishRequest(String dedupKey, String payload) {}
    public record PullResponse(boolean found, Delivery delivery) {}
    public record AckRequest(String deliveryId) {}
    public record NackRequest(String deliveryId, String error) {}

    @PostMapping("/messages")
    public PublishResult publish(@RequestBody PublishRequest request) {
        return queueService.publish(request.dedupKey(), request.payload());
    }

    @PostMapping("/groups/{group}/pull")
    public PullResponse pull(@PathVariable String group) {
        return queueService.pull(group)
                .map(d -> new PullResponse(true, d))
                .orElseGet(() -> new PullResponse(false, null));
    }

    @PostMapping("/groups/{group}/ack")
    public AckResponse ack(@PathVariable String group, @RequestBody AckRequest request) {
        return queueService.ack(group, request.deliveryId());
    }

    @PostMapping("/groups/{group}/nack")
    public NackResponse nack(@PathVariable String group, @RequestBody NackRequest request) {
        return queueService.nack(group, request.deliveryId(), request.error());
    }

    @GetMapping("/groups/{group}/offset")
    public GroupOffset offset(@PathVariable String group) {
        return queueService.offset(group);
    }

    @GetMapping("/offsets")
    public List<GroupOffset> offsets() {
        return queueService.offsets();
    }

    @GetMapping("/groups/{group}/dead-letters")
    public List<DeadLetter> deadLetters(@PathVariable String group,
                                        @RequestParam(defaultValue = "50") int limit) {
        return queueService.deadLetters(group, limit);
    }
}
