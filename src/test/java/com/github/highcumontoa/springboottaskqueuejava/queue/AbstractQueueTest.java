package com.github.highcumontoa.springboottaskqueuejava.queue;

import com.github.highcumontoa.springboottaskqueuejava.queue.config.QueueProperties;
import com.github.highcumontoa.springboottaskqueuejava.queue.repository.JdbcQueueRepository;
import com.github.highcumontoa.springboottaskqueuejava.queue.repository.QueueRepository;
import com.github.highcumontoa.springboottaskqueuejava.queue.service.QueueService;
import org.junit.jupiter.api.AfterEach;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

/**
 * Builds the queue stack against an isolated in-memory H2 database with real
 * Spring transaction management (required for FOR UPDATE row locking tests).
 */
public abstract class AbstractQueueTest {

    protected DataSource dataSource;
    protected JdbcTemplate jdbc;
    protected JdbcQueueRepository repository;
    protected QueueProperties properties;
    protected QueueService service;
    protected TransactionTemplate tx;

    protected void buildStack(String dbName) {
        buildStack(dbName, 3, 100, 2.0, 30000);
    }

    protected void buildStack(String dbName, int maxAttempts, long backoffMillis,
                              double multiplier, long ackTimeoutMillis) {
        dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000", "sa", "");
        jdbc = new JdbcTemplate(dataSource);
        DataSourceTransactionManager tm = new DataSourceTransactionManager(dataSource);
        tx = new TransactionTemplate(tm);
        JdbcQueueRepository raw = new JdbcQueueRepository(jdbc);
        raw.initializeSchema();
        // Apply real @Transactional semantics (FOR UPDATE locks span the whole method).
        ProxyFactory pf = new ProxyFactory(raw);
        pf.setProxyTargetClass(true);
        pf.addAdvice(new TransactionInterceptor(tm, new AnnotationTransactionAttributeSource()));
        repository = (JdbcQueueRepository) pf.getProxy();
        properties = new QueueProperties();
        properties.setMaxAttempts(maxAttempts);
        properties.setBackoffMillis(backoffMillis);
        properties.setBackoffMultiplier(multiplier);
        properties.setAckTimeoutMillis(ackTimeoutMillis);
        service = new QueueService(repository, properties);
    }

    protected long publish(String key, String payload) {
        return service.publish(key, payload).offset();
    }

    protected long committedOffset(String group) {
        return service.offset(group).getCommittedOffset();
    }

    @AfterEach
    void shutdownDb() throws Exception {
        try (java.sql.Connection c = dataSource.getConnection();
             java.sql.Statement st = c.createStatement()) {
            st.execute("SHUTDOWN");
        }
    }
}
