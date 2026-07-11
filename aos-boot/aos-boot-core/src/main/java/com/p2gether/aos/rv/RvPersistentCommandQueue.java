package com.p2gether.aos.rv;

import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvMsg;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

/**
 * Database-backed reprocessing queue for {@code @RvCommand(persistent = true)} commands.
 *
 * <p>The dispatcher persists the message (status PROCESSING) before the inline first
 * attempt; success deletes the row, failure parks it as PENDING with a due time. This
 * poller claims due PENDING rows — plus PROCESSING rows whose worker apparently died
 * (older than {@code processing-timeout}) — re-invokes the handler, and either deletes
 * the row or reschedules it until {@code max-attempts}, after which the row is kept as
 * FAILED (dead letter; requeue by resetting status to PENDING). Claiming uses a
 * conditional UPDATE, so multiple instances can share one queue table safely.
 *
 * <p>Retries never send replies: the original requester already received a
 * {@code {status=QUEUED}} reply and its inbox is gone by the time a retry runs.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "aos.rendezvous.persistent-queue.enabled", havingValue = "true", matchIfMissing = true)
public class RvPersistentCommandQueue {

    private final JdbcTemplate jdbc;
    private final ObjectProvider<RendezvousProperties> propertiesProvider;
    private final ObjectProvider<RvCommandDispatcher> dispatcherProvider;
    private ScheduledExecutorService poller;

    public RvPersistentCommandQueue(JdbcTemplate jdbc,
                                 ObjectProvider<RendezvousProperties> propertiesProvider,
                                 ObjectProvider<RvCommandDispatcher> dispatcherProvider) {
        this.jdbc = jdbc;
        this.propertiesProvider = propertiesProvider;
        this.dispatcherProvider = dispatcherProvider;
    }

    @PostConstruct
    public void start() {
        long intervalMillis = (long) (config().getPollInterval() * 1000);
        poller = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "rv-persistent-queue");
            thread.setDaemon(true);
            return thread;
        });
        poller.scheduleWithFixedDelay(this::poll, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        log.info("Persistent command queue polling every {}s (max attempts {}, backoff {}s)",
                config().getPollInterval(), config().getMaxAttempts(), config().getBackoff());
    }

    /** Persists an incoming persistent message as PROCESSING (the inline attempt is underway). */
    public long enqueue(String command, TibrvMsg message) throws TibrvException {
        return insert(command, message, "PROCESSING");
    }

    /**
     * Submits a follow-up persistent command straight to the queue as PENDING (no RV round
     * trip); the poller executes it like any other due row. Use this to chain persistent
     * steps — e.g. settle once, then notify — so a failure in a later step retries only
     * that step and never re-runs the earlier ones.
     */
    public long submit(String command, Object payload) throws TibrvException {
        TibrvMsg message = RvMessages.toMsg(payload);
        message.setSendSubject(selfSubject(command));
        long id = insert(command, message, "PENDING");
        log.info("Submitted persistent command '{}' (row {})", command, id);
        return id;
    }

    private long insert(String command, TibrvMsg message, String status) throws TibrvException {
        String payload = RvMessages.serialize(message);
        Timestamp now = Timestamp.from(Instant.now());
        int maxAttempts = Math.max(1, config().getMaxAttempts());
        KeyHolder key = new GeneratedKeyHolder();
        jdbc.update(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO rv_command_queue (command, subject, payload, status, attempts,"
                            + " max_attempts, next_attempt_at, created_at, updated_at)"
                            + " VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, command);
            statement.setString(2, message.getSendSubject());
            statement.setString(3, payload);
            statement.setString(4, status);
            statement.setInt(5, maxAttempts);
            statement.setTimestamp(6, now);
            statement.setTimestamp(7, now);
            statement.setTimestamp(8, now);
            return statement;
        }, key);
        return Objects.requireNonNull(key.getKey(), "no generated id").longValue();
    }

    /** Subject recorded for self-submitted rows: this instance addressing itself. */
    private String selfSubject(String command) {
        RendezvousProperties properties = propertiesProvider.getObject();
        RendezvousProperties.Subject subject = properties.getSubject();
        String joined = String.join(".",
                subject.getFactory(), subject.getEnvironment(),
                subject.getListener(), properties.resolvedSenderName(),
                subject.getListener(), command);
        return subject.isLocal() ? "_LOCAL." + joined : joined;
    }

    /** Removes a row whose handler succeeded. */
    public void complete(long id) {
        jdbc.update("DELETE FROM rv_command_queue WHERE id = ?", id);
    }

    /** Records a failed attempt: back to PENDING with a due time, or FAILED when exhausted. */
    public void failAttempt(long id, String error) {
        Timestamp now = Timestamp.from(Instant.now());
        Timestamp due = Timestamp.from(Instant.now().plusMillis((long) (config().getBackoff() * 1000)));
        int updated = jdbc.update(
                "UPDATE rv_command_queue SET attempts = attempts + 1,"
                        + " status = CASE WHEN attempts + 1 >= max_attempts THEN 'FAILED' ELSE 'PENDING' END,"
                        + " next_attempt_at = ?, last_error = ?, updated_at = ? WHERE id = ?",
                due, abbreviate(error), now, id);
        if (updated == 1 && Boolean.TRUE.equals(jdbc.queryForObject(
                "SELECT status = 'FAILED' FROM rv_command_queue WHERE id = ?", Boolean.class, id))) {
            log.error("Persistent command row {} moved to FAILED (dead letter): {}", id, error);
        }
    }

    private void poll() {
        try {
            Timestamp now = Timestamp.from(Instant.now());
            Timestamp staleBefore = Timestamp.from(
                    Instant.now().minusMillis((long) (config().getProcessingTimeout() * 1000)));
            List<Long> due = jdbc.queryForList(
                    "SELECT id FROM rv_command_queue"
                            + " WHERE (status = 'PENDING' AND next_attempt_at <= ?)"
                            + " OR (status = 'PROCESSING' AND updated_at < ?)"
                            + " ORDER BY id LIMIT ?",
                    Long.class, now, staleBefore, config().getBatchSize());
            due.forEach(id -> claimAndProcess(id, now, staleBefore));
        } catch (Exception exception) {
            log.error("Persistent queue poll failed", exception);
        }
    }

    private void claimAndProcess(long id, Timestamp now, Timestamp staleBefore) {
        int claimed = jdbc.update(
                "UPDATE rv_command_queue SET status = 'PROCESSING', updated_at = ?"
                        + " WHERE id = ? AND ((status = 'PENDING' AND next_attempt_at <= ?)"
                        + " OR (status = 'PROCESSING' AND updated_at < ?))",
                Timestamp.from(Instant.now()), id, now, staleBefore);
        if (claimed != 1) {
            return; // another instance took it
        }
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT command, subject, payload, attempts, max_attempts FROM rv_command_queue WHERE id = ?", id);
        String command = (String) row.get("command");
        try {
            TibrvMsg message = RvMessages.deserialize((String) row.get("payload"));
            message.setSendSubject((String) row.get("subject"));
            dispatcherProvider.getObject().invokeOnce(command, message);
            complete(id);
            log.info("Persistent command '{}' (row {}) succeeded on attempt {}/{}",
                    command, id, ((Number) row.get("attempts")).intValue() + 1, row.get("max_attempts"));
        } catch (Exception exception) {
            Throwable failure = exception.getCause() != null ? exception.getCause() : exception;
            log.warn("Persistent command '{}' (row {}) failed on attempt {}/{}: {}",
                    command, id, ((Number) row.get("attempts")).intValue() + 1,
                    row.get("max_attempts"), String.valueOf(failure));
            failAttempt(id, String.valueOf(failure));
        }
    }

    private RendezvousProperties.PersistentQueue config() {
        return propertiesProvider.getObject().getPersistentQueue();
    }

    private static String abbreviate(String error) {
        return error != null && error.length() > 2000 ? error.substring(0, 2000) : error;
    }

    @PreDestroy
    public void stop() {
        if (poller != null) {
            poller.shutdownNow();
        }
    }
}
