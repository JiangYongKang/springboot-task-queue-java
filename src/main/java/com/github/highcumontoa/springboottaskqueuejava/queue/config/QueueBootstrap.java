package com.github.highcumontoa.springboottaskqueuejava.queue.config;

import com.github.highcumontoa.springboottaskqueuejava.queue.repository.QueueRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(QueueProperties.class)
public class QueueBootstrap implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(QueueBootstrap.class);

    private final QueueRepository repository;

    public QueueBootstrap(QueueRepository repository) {
        this.repository = repository;
    }

    @Override
    public void run(ApplicationArguments args) {
        repository.initializeSchema();
        log.info("Task queue schema initialized");
    }
}
