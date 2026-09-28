package com.github.highcumontoa.springboottaskqueuejava.queue;

import com.github.highcumontoa.springboottaskqueuejava.queue.model.Delivery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetAdvanceTest extends AbstractQueueTest {

    @BeforeEach
    void init() {
        buildStack("offset");
    }

    @Test
    void offsetAdvancesInOrderEvenWhenAcksArriveOutOfOrder() {
        for (int i = 1; i <= 5; i++) {
            publish("k" + i, "p" + i);
        }
        // pull all 5 into INFLIGHT
        List<Delivery> deliveries = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            deliveries.add(service.pull("G").orElseThrow());
        }
        assertThat(service.pull("G")).isEmpty();

        // ack in reverse order: 5,4,3 ... committed must stay 0 until 1 is acked
        for (int i = 4; i >= 1; i--) {
            var resp = service.ack("G", deliveries.get(i).getDeliveryId());
            assertThat(resp.accepted()).isTrue();
        }
        assertThat(committedOffset("G")).as("offset stays at 0 while offset 1 is INFLIGHT").isZero();

        var resp = service.ack("G", deliveries.get(0).getDeliveryId());
        assertThat(resp.accepted()).isTrue();
        assertThat(committedOffset("G")).as("offset jumps to 5 after the head is acked").isEqualTo(5);
    }
}
