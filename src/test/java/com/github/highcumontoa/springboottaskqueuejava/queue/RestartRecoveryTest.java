package com.github.highcumontoa.springboottaskqueuejava.queue;

import com.github.highcumontoa.springboottaskqueuejava.queue.config.QueueProperties;
import com.github.highcumontoa.springboottaskqueuejava.queue.model.Delivery;
import com.github.highcumontoa.springboottaskqueuejava.queue.repository.JdbcQueueRepository;
import com.github.highcumontoa.springboottaskqueuejava.queue.service.QueueService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * After a process restart (new DataSource on the same file database) unacked
 * messages, per-group offsets and dead letters must survive. The unacked message
 * keeps its lease; once the deadline passes the reaper makes it consumable again.
 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

    private record Stack(DataSource ds, JdbcQueueRepository repo, QueueService svc,
                         QueueProperties props) { }

    private Stack open(String url) {
        DataSource ds = new DriverManagerDataSource(url, "sa", "");
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
        JdbcQueueRepository raw = new JdbcQueueRepository(jdbc);
        raw.initializeSchema();
        ProxyFactory pf = new ProxyFactory(raw);
        pf.setProxyTargetClass(true);
        pf.addAdvice(new TransactionInterceptor(tm, new AnnotationTransactionAttributeSource()));
        JdbcQueueRepository repo = (JdbcQueueRepository) pf.getProxy();
        QueueProperties props = new QueueProperties();
        props.setMaxAttempts(2);
        props.setBackoffMillis(20);
        props.setAckTimeoutMillis(200);
        return new Stack(ds, repo, new QueueService(repo, props), props);
    }

    private void close(Stack s) throws Exception {
        try (var c = s.ds().getConnection(); var st = c.createStatement()) {
            st.execute("SHUTDOWN");
        }
    }

    @Test
    void stateSurvivesRestartAndUnackedMessageIsRecoverable() throws Exception {
        Path dir = Files.createTempDirectory("taskqueue-restart-");
        String url = "jdbc:h2:file:" + dir.resolve("q") + ";LOCK_TIMEOUT=10000";

        // ---- first run ----
        Stack first = open(url);
        first.svc().publish("unacked", "u");   // offset 1
        first.svc().publish("acked", "a");     // offset 2
        first.svc().publish("doomed", "d");    // offset 3

        Delivery unacked = first.svc().pull("G").orElseThrow(); // msg 1, INFLIGHT
        Delivery acked = first.svc().pull("G").orElseThrow();   // msg 2
        first.svc().ack("G", acked.getDeliveryId());            // offset stuck at 0 (msg1 pending)

        // force msg 3 to dead letter: pull, nack twice
        Delivery d3a = first.svc().pull("G").orElseThrow();
        first.svc().nack("G", d3a.getDeliveryId(), "e1");
        Thread.sleep(40);
        Delivery d3b = first.svc().pull("G").orElseThrow();
        first.svc().nack("G", d3b.getDeliveryId(), "e2");

        long offsetBefore = first.svc().offset("G").getCommittedOffset();
        int deadBefore = first.svc().deadLetters("G", 10).size();
        log.info("before-restart group=G committedOffset={} deadCount={} inflightOffset=1",
                offsetBefore, deadBefore);
        close(first);

        // ---- second run on the same file ----
        Stack second = open(url);
        long offsetAfter = second.svc().offset("G").getCommittedOffset();
        int deadAfter = second.svc().deadLetters("G", 10).size();
        log.info("after-restart group=G committedOffset={} deadCount={} state=PERSISTED",
                offsetAfter, deadAfter);

        assertThat(offsetAfter).isEqualTo(offsetBefore);
        assertThat(deadAfter).isEqualTo(deadBefore).isEqualTo(1);
        var dl = second.svc().deadLetters("G", 10).get(0);
        assertThat(dl.getDedupKey()).isEqualTo("doomed");
        assertThat(dl.getPayload()).isEqualTo("d");
        assertThat(dl.getOriginalPublishedAt()).isNotNull();

        // The old lease survived; it cannot be consumed while still valid.
        assertThat(second.svc().pull("G")).as("still within lease: not redelivered").isEmpty();
        // The stale delivery id still exists after restart.
        var staleAck = second.svc().ack("G", unacked.getDeliveryId());
        assertThat(staleAck.accepted()).isTrue();
        log.info("after-restart ack of surviving lease deliveryId={} state=ACKED",
                unacked.getDeliveryId());

        close(second);

        // ---- third run: if the consumer had crashed without acking, the lease expires ----
        Path dir2 = Files.createTempDirectory("taskqueue-restart2-");
        String url2 = "jdbc:h2:file:" + dir2.resolve("q") + ";LOCK_TIMEOUT=10000";
        Stack a = open(url2);
        a.svc().publish("lost", "l");
        Delivery lost = a.svc().pull("G").orElseThrow();
        close(a);

        Thread.sleep(220); // lease (200ms) expires while process is down

        Stack b = open(url2);
        int reclaimed = b.repo().reapExpiredLeases(Instant.now(), b.props().getMaxAttempts(),
                b.svc().backoffSchedule());
        assertThat(reclaimed).isEqualTo(1);
        Thread.sleep(40);
        Delivery redelivered = b.svc().pull("G").orElseThrow();
        assertThat(redelivered.getMessageOffset()).isEqualTo(lost.getMessageOffset());
        assertThat(redelivered.getAttempt()).isEqualTo(2);
        log.info("after-restart group=G offset={} state=RETRY_WAIT->INFLIGHT attempt=2 recovered=true",
                redelivered.getMessageOffset());
        close(b);
    }
}
