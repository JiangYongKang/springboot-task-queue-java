package com.github.highcumontoa.springboottaskqueuejava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.springboottaskqueuejava.queue.TaskQueueService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 对外 HTTP 接口集成测试：投递、拉取、确认、重复去重冲突、确认拒绝原因、死信查询。 */
@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "taskqueue.data-dir=target/test-queue-api-data",
        "taskqueue.visibility-timeout-ms=200",
        "taskqueue.max-attempts=2",
        "taskqueue.backoff-base-ms=1",
        "taskqueue.reaper-interval-ms=3600000"
})
public class QueueApiTest {

    private static final Logger log = LoggerFactory.getLogger(QueueApiTest.class);
    private final ObjectMapper json = new ObjectMapper();

    @Autowired
    private MockMvc mvc;
    @Autowired
    private TaskQueueService service;

    @BeforeEach
    void reset() {
        service.resetForTesting();
    }

    private long publish(String key, String payload) throws Exception {
        MvcResult res = mvc.perform(post("/api/queue/publish")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(new PublishBody(key, payload))))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode node = json.readTree(res.getResponse().getContentAsString());
        long offset = node.get("offset").asLong();
        log.info("[api-test] publish group=* offset={} dedupKey={}", offset, key);
        return offset;
    }

    private record PublishBody(String dedupKey, String payload) {
    }

    @Test
    void publishPullAckHappyPathAndOffsetMoves() throws Exception {
        long o1 = publish("api-1", "a");
        publish("api-2", "b");

        mvc.perform(get("/api/queue/groups/api-g1/pull").param("maxMessages", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));

        mvc.perform(post("/api/queue/groups/api-g1/messages/{offset}/ack", o1))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACKED"));

        mvc.perform(get("/api/queue/groups/api-g1/offset"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.committedOffset").value(o1 + 1));
        log.info("[api-test] happy path: committedOffset={}", o1 + 1);
    }

    @Test
    void duplicatePublishReturns409WithExistingOffset() throws Exception {
        long offset = publish("api-dup", "x");
        mvc.perform(post("/api/queue/publish")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(new PublishBody("api-dup", "x2"))))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("DUPLICATE_DEDUP_KEY"))
                .andExpect(jsonPath("$.existingOffset").value(offset));
        log.info("[api-test] duplicate publish rejected with existingOffset={}", offset);
    }

    @Test
    void duplicateAckAndCrossGroupAckReturnDistinctReasons() throws Exception {
        long offset = publish("api-ack", "x");
        mvc.perform(get("/api/queue/groups/api-g2/pull")).andExpect(status().isOk());

        mvc.perform(post("/api/queue/groups/api-g2/messages/{offset}/ack", offset))
                .andExpect(status().isOk());

        mvc.perform(post("/api/queue/groups/api-g2/messages/{offset}/ack", offset))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("ALREADY_ACKED"));

        mvc.perform(post("/api/queue/groups/api-g2/messages/{offset}/ack", offset + 999))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("GROUP_OR_MESSAGE_UNKNOWN"));
        log.info("[api-test] ack rejection reasons are distinct");
    }

    @Test
    void exhaustedRetriesAppearInDeadLetterEndpoint() throws Exception {
        long offset = publish("api-dead", "original-payload");
        mvc.perform(get("/api/queue/groups/api-g3/pull")).andExpect(status().isOk());
        Thread.sleep(260);
        service.reapTimeouts(); // attempt1 超时 -> 重投
        mvc.perform(get("/api/queue/groups/api-g3/pull")).andExpect(status().isOk());
        mvc.perform(post("/api/queue/groups/api-g3/messages/{offset}/nack", offset)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"error\":\"final failure\"}"))
                .andExpect(status().isOk());

        mvc.perform(get("/api/queue/dead-letters"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].offset").value(offset))
                .andExpect(jsonPath("$[0].dedupKey").value("api-dead"))
                .andExpect(jsonPath("$[0].payload").value("original-payload"))
                .andExpect(jsonPath("$[0].attempts").value(2))
                .andExpect(jsonPath("$[0].consumerGroup").value("api-g3"));
        // nack 进入死信后再 ack 必须被拒绝
        mvc.perform(post("/api/queue/groups/api-g3/messages/{offset}/ack", offset))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("MESSAGE_DEAD"));
        log.info("[api-test] dead letter visible via API at offset={}", offset);
    }

    @Test
    void twoGroupsGetIndependentDeliveryOfSameMessage() throws Exception {
        long offset = publish("api-mg", "z");
        MvcResult r1 = mvc.perform(get("/api/queue/groups/a1/pull")).andReturn();
        MvcResult r2 = mvc.perform(get("/api/queue/groups/a2/pull")).andReturn();
        assertThat(r1.getResponse().getContentAsString()).contains(String.valueOf(offset));
        assertThat(r2.getResponse().getContentAsString()).contains(String.valueOf(offset));
        // 同一组第二次拉取不能再拿到（INFLIGHT 互斥）
        mvc.perform(get("/api/queue/groups/a1/pull"))
                .andExpect(jsonPath("$.length()").value(0));
    }
}
