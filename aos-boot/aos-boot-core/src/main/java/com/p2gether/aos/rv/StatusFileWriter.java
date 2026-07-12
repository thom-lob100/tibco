package com.p2gether.aos.rv;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Periodically writes this instance's live role to a small JSON file so an external,
 * Rendezvous-independent tool (the modern-patch agent) can read it straight off the
 * local filesystem — no management HTTP port, no RV client needed.
 *
 * <p>Enabled only when {@code aos.status.file} points somewhere (the patch agent passes
 * {@code --aos.status.file=<path next to the log>} when it launches an instance). The
 * key field is {@code role}: {@code active} while this member is consuming, {@code
 * standby} while an FT standby waits. It refreshes every {@code aos.status.interval}
 * seconds, so an FT promotion/demotion shows up within one interval.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "aos.status.file")
public class StatusFileWriter {

    private final RendezvousProperties properties;
    private final RendezvousSubscriber subscriber;
    private final Path file;
    private final long intervalSeconds;
    private final long pid = ProcessHandle.current().pid();

    private ScheduledExecutorService scheduler;

    public StatusFileWriter(RendezvousProperties properties, RendezvousSubscriber subscriber,
                            @Value("${aos.status.file}") String file,
                            @Value("${aos.status.interval:3}") long intervalSeconds) {
        this.properties = properties;
        this.subscriber = subscriber;
        this.file = Path.of(file);
        this.intervalSeconds = Math.max(1, intervalSeconds);
    }

    @PostConstruct
    public void start() {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "aos-status-writer");
            thread.setDaemon(true);   // must not keep the JVM alive on its own
            return thread;
        });
        this.scheduler.scheduleWithFixedDelay(
                this::writeQuietly, 0, this.intervalSeconds, TimeUnit.SECONDS);
        log.info("Status file enabled: {} (every {}s)", this.file, this.intervalSeconds);
    }

    private void writeQuietly() {
        try {
            this.write();
        } catch (RuntimeException | IOException exception) {
            // Status reporting is best-effort; never let it disturb the service.
            log.debug("Could not write status file {}", this.file, exception);
        }
    }

    private void write() throws IOException {
        RendezvousProperties.Subject subject = properties.getSubject();
        RendezvousProperties.Dq dq = properties.getDq();
        RendezvousProperties.Ft ft = properties.getFt();
        boolean active = subscriber.isActive();

        StringBuilder json = new StringBuilder(256);
        json.append("{\n");
        field(json, "listener", subject.getListener());
        field(json, "factory", subject.getFactory());
        field(json, "environment", subject.getEnvironment());
        field(json, "subject", subject.value());
        field(json, "mode", ft.isEnabled() ? "FT" : "DQ");
        field(json, "role", active ? "active" : "standby");
        rawField(json, "consuming", Boolean.toString(active));
        field(json, "dqName", dq.getName());
        field(json, "ftGroup", ft.isEnabled() ? properties.ftName() : null);
        rawField(json, "ftWeight", ft.isEnabled() ? Integer.toString(ft.getWeight()) : null);
        rawField(json, "activeGoal", ft.isEnabled() ? Integer.toString(ft.getActiveGoal()) : null);
        rawField(json, "schedulerWeight", Integer.toString(dq.getSchedulerWeight()));
        field(json, "host", properties.resolvedSenderName());
        rawField(json, "pid", Long.toString(pid));
        rawField(json, "updatedAtEpochMillis", Long.toString(System.currentTimeMillis()));
        field(json, "updatedAt", Instant.now().toString());
        // trim trailing comma+newline of the last entry
        int len = json.length();
        if (len >= 2 && json.charAt(len - 2) == ',') {
            json.delete(len - 2, len);
            json.append('\n');
        }
        json.append("}\n");

        Path parent = this.file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path tmp = this.file.resolveSibling(this.file.getFileName() + ".tmp");
        Files.write(tmp, json.toString().getBytes(StandardCharsets.UTF_8));
        try {
            Files.move(tmp, this.file,
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicUnsupported) {
            Files.move(tmp, this.file, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void field(StringBuilder json, String key, String value) {
        if (value == null) {
            rawField(json, key, null);
        } else {
            json.append("  \"").append(key).append("\": \"").append(escape(value)).append("\",\n");
        }
    }

    private static void rawField(StringBuilder json, String key, String rawValue) {
        json.append("  \"").append(key).append("\": ").append(rawValue == null ? "null" : rawValue).append(",\n");
    }

    private static String escape(String value) {
        StringBuilder out = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }

    @PreDestroy
    public void stop() {
        if (this.scheduler != null) {
            this.scheduler.shutdownNow();
        }
    }
}
