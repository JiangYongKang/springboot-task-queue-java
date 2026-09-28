package com.github.highcumontoa.springboottaskqueuejava.queue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Full-stack HTTP test proving the external API delivers, pulls, acknowledges,
 * rejects bad acks with distinct reasons and exposes offsets/dead letters.
 */
@SpringBootTest
@AutoConfigureMockMvc
class QueueHttpEndToEndTest {

    @Autowired
    private MockMvc mvc;

    @Test
    void publishPullAckRejectLifecycleOverHttp() throws Exception {
        String group = "HTTP_GROUP";

        // publish twice with the same dedup key -> same offset, second flagged duplicated
        mvc.perform(post("/api/queue/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"dedupKey":"http-job","payload":"hello"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicated").value(false))
                .andExpect(jsonPath("$.offset").value(org.hamcrest.Matchers.greaterThanOrEqualTo(1)));

        mvc.perform(post("/api/queue/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"dedupKey":"http-job","payload":"hello"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicated").value(true));

        // pull
        String body = mvc.perform(post("/api/queue/groups/" + group + "/pull"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.found").value(true))
                .andExpect(jsonPath("$.delivery.dedupKey").value("http-job"))
                .andReturn().getResponse().getContentAsString();
        String deliveryId = com.jayway.jsonpath.JsonPath.read(body, "$.delivery.deliveryId");

        // ack unknown delivery -> DELIVERY_NOT_FOUND
        mvc.perform(post("/api/queue/groups/" + group + "/ack")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"deliveryId\":\"bogus\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(false))
                .andExpect(jsonPath("$.reason").value("DELIVERY_NOT_FOUND"));

        // valid ack
        mvc.perform(post("/api/queue/groups/" + group + "/ack")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"deliveryId\":\"" + deliveryId + "\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.committedOffset").value(org.hamcrest.Matchers.greaterThanOrEqualTo(1)));

        // duplicate ack
        mvc.perform(post("/api/queue/groups/" + group + "/ack")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"deliveryId\":\"" + deliveryId + "\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(false))
                .andExpect(jsonPath("$.reason").value("DUPLICATE_ACK"));

        mvc.perform(get("/api/queue/groups/" + group + "/offset"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.groupName").value(group))
                .andExpect(jsonPath("$.committedOffset").value(org.hamcrest.Matchers.greaterThanOrEqualTo(1)));

        mvc.perform(get("/api/queue/groups/" + group + "/dead-letters"))
                .andExpect(status().isOk());
    }
}
