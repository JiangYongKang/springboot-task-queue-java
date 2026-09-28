package com.github.highcumontoa.springboottaskqueuejava.queue;

import com.github.highcumontoa.springboottaskqueuejava.queue.config.QueueProperties;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.Delivery;
import com.github.highcumontoa.springboottaskqueuejava.queue.repository.JdbcQueueRepository;
import com.github.highcumontoa.springboottaskqueuejava.queue.repository.QueueRepository;
import com.github.highcumontoa.springboottaskqueuejava.queue.service.QueueService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcQueueRepositorySmokeTest {

    private DataSource dataSource;
    private JdbcQueueRepository repository;
    private QueueService service;
    private QueueProperties properties;

    @BeforeEach
    void setUp() {
        dataSource = new DriverManagerDataSource("jdbc:h2:mem:smoke;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000",
                "sa", "");
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        repository = new JdbcQueueRepository(jdbc);
        repository.initializeSchema();
        properties = new QueueProperties();
        properties.setMaxAttempts(3);
        properties.setBackoffMillis(50);
        properties.setBackoffMultiplier(2.0);
        properties.setAckTimeoutMillis(30000);
        service = new QueueService(repository, properties);
    }

    @AfterEach
    void tearDown() throws Exception {
        try (java.sql.Connection c = dataSource.getConnection();
             java.sql.Statement st = c.createStatement()) {
            st.execute("SHUTDOWN");
        }
    }

    @Test
    void publishPullAckAdvancesOffset() {
        QueueRepository.PublishRecord r1 = repository.publish("k1", "p1", Instant.now());
        assertThat(r1.duplicated()).isFalse();
        QueueRepository.PublishRecord r1Again = repository.publish("k1", "p1", Instant.now());
        assertThat(r1Again.duplicated()).isTrue();
        assertThat(r1Again.offset()).isEqualTo(r1.offset());

        Optional<Delivery> d = repository.claimOne("g1", Instant.now(),
                Instant.now().plus(30, ChronoUnit.SECONDS));
        assertThat(d).isPresent();
        assertThat(d.get().getAttempt()).isEqualTo(1);

        QueueRepository.AckResult ack = repository.ack("g1", d.get().getDeliveryId(), Instant.now());
        assertThat(ack.success()).isTrue();
        assertThat(repository.findOffset("g1")).get()
                .extracting(o -> o.getCommittedOffset()).isEqualTo(1L);

        Optional<Delivery> second = repository.claimOne("g1", Instant.now(),
                Instant.now().plus(30, ChronoUnit.SECONDS));
        assertThat(second).isEmpty();
    }
}
