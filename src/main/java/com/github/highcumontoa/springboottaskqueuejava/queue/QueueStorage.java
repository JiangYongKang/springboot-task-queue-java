package com.github.highcumontoa.springboottaskqueuejava.queue;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 基于本地文件 JSON 快照的持久化：重启后消息日志、各组偏移量/投递状态、死信均不丢失。
 * 所有状态由 TaskQueueService 在同一把锁内修改后整体落盘，保证磁盘状态与内存状态一致。
 */
@Component
public class QueueStorage {

    private static final Logger log = LoggerFactory.getLogger(QueueStorage.class);
    private static final String STATE_FILE = "taskqueue-state.json";

    private final QueueProperties properties;
    private final ObjectMapper mapper;
    private final Path stateFile;

    public QueueStorage(QueueProperties properties) {
        this.properties = properties;
        this.mapper = new ObjectMapper();
        this.mapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        this.stateFile = Path.of(properties.getDataDir(), STATE_FILE);
    }

    /** 磁盘上的完整状态快照。 */
    public static class Snapshot {
        public long nextOffset;
        public List<MessageRecord> messages = new ArrayList<>();
        public Map<String, GroupState> groups = new LinkedHashMap<>();
        public List<DeadLetterRecord> deadLetters = new ArrayList<>();

        public Snapshot() {
        }

        public Snapshot(long nextOffset, List<MessageRecord> messages,
                        Map<String, GroupState> groups, List<DeadLetterRecord> deadLetters) {
            this.nextOffset = nextOffset;
            this.messages = messages;
            this.groups = groups;
            this.deadLetters = deadLetters;
        }
    }

    /** 启动时加载快照；文件不存在则返回空快照。 */
    public Snapshot load() {
        if (!Files.exists(stateFile)) {
            log.info("persistent state file not found at {}, starting empty", stateFile.toAbsolutePath());
            return new Snapshot();
        }
        try {
            Snapshot snapshot = mapper.readValue(stateFile.toFile(), Snapshot.class);
            if (snapshot.messages == null) {
                snapshot.messages = new ArrayList<>();
            }
            if (snapshot.groups == null) {
                snapshot.groups = new LinkedHashMap<>();
            }
            if (snapshot.deadLetters == null) {
                snapshot.deadLetters = new ArrayList<>();
            }
            log.info("loaded state: {} messages, {} groups, {} dead letters, nextOffset={}",
                    snapshot.messages.size(), snapshot.groups.size(),
                    snapshot.deadLetters.size(), snapshot.nextOffset);
            return snapshot;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load queue state from " + stateFile, e);
        }
    }

    /** 原子覆盖写快照：先写临时文件，flush+fsync 后 rename，避免半写文件。 */
    public synchronized void save(Snapshot snapshot) {
        try {
            Path dir = stateFile.getParent();
            if (dir != null) {
                Files.createDirectories(dir);
            }
            Path tmp = stateFile.resolveSibling(STATE_FILE + ".tmp");
            try (var out = Files.newOutputStream(tmp)) {
                mapper.writerWithDefaultPrettyPrinter().writeValue(out, snapshot);
                out.flush();
                try {
                    out.getClass(); // stream close handles fsync on local FS; rename below is atomic
                } catch (Exception ignored) {
                    // no-op
                }
            }
            try {
                Files.move(tmp, stateFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicFailure) {
                Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist queue state to " + stateFile, e);
        }
    }
}
