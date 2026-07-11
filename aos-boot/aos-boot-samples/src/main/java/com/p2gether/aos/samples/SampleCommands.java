package com.p2gether.aos.samples;

import com.p2gether.aos.rv.RendezvousPublisher;
import com.p2gether.aos.rv.RvCommand;
import com.p2gether.aos.rv.RvPersistentCommandQueue;
import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvMsg;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Example command handlers: any bean method annotated with {@link RvCommand} is invoked
 * when a message whose subject ends with that command is assigned to this DQ member.
 * Records are bound from/to message fields by name; raw {@link TibrvMsg} also works
 * (e.g. when the payload is a single full-document field).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SampleCommands {

    private final RendezvousPublisher publisher;
    private final ObjectProvider<RvPersistentCommandQueue> persistentQueue;
    private final JdbcTemplate jdbc;

    /** Destination notified when a settlement succeeds. */
    @Value("${sample.settle.notify-destination:messo}")
    private String settleNotifyDestination;

    public record OrderCreateRequest(String orderId, int quantity) {
    }

    public record OrderCreateReply(String status, String handledBy, String orderId) {
    }

    /** Handles command {@code ORDER_CREATE} (derived from the method name) and replies. */
    @RvCommand
    public OrderCreateReply orderCreate(OrderCreateRequest request) {
        log.info("orderCreate executed for {}", request);
        return new OrderCreateReply("OK", "orderCreate", request.orderId());
    }

    /** Handles command {@code ORDER_CANCEL}; throws (and is retried) when orderId is missing. */
    @RvCommand
    public TibrvMsg orderCancel(TibrvMsg request) throws TibrvException {
        if (request.get("orderId") == null) {
            throw new IllegalArgumentException("orderId is required");
        }
        log.info("orderCancel executed for {}", request);
        TibrvMsg reply = new TibrvMsg();
        reply.add("status", "OK");
        reply.add("handledBy", "orderCancel");
        return reply;
    }

    /** Handles command {@code PING} (explicit name), one-way: no reply. */
    @RvCommand("PING")
    public void ping(TibrvMsg request) {
        log.info("ping executed for {}", request);
    }

    public record OrderSettleRequest(String orderId) {
    }

    public record OrderSettledEvent(String orderId, String status, String settledBy) {
    }

    /**
     * Handles command {@code ORDER_SETTLE}; persistent, so a failure parks the message
     * in the {@code rv_command_queue} table and it is retried across restarts. The
     * settlement write and the chained {@code NOTIFY_SETTLED} submit run in ONE
     * transaction (both share the datasource), so a failure rolls back both — the
     * settlement is never recorded without its notification row and vice versa.
     * {@code rollbackFor = Exception.class} matters: {@code submit} throws the checked
     * {@code TibrvException}, which the default rollback rule would ignore. The
     * {@code simulate.settle.failure} system property forces a failure AFTER the writes
     * to prove the rollback.
     */
    @Transactional(rollbackFor = Exception.class)
    @RvCommand(persistent = true)
    public TibrvMsg orderSettle(OrderSettleRequest request) throws TibrvException {
        // Idempotent settlement write (H2 MERGE = upsert; use the real database's
        // equivalent in production) - safe if a crash-reclaim re-runs this attempt.
        jdbc.update("MERGE INTO sample_settlement (order_id, settled_at) KEY (order_id)"
                + " VALUES (?, CURRENT_TIMESTAMP)", request.orderId());
        log.info("orderSettle executed for {}", request);
        OrderSettledEvent event = new OrderSettledEvent(request.orderId(), "SETTLED", "orderSettle");
        RvPersistentCommandQueue queue = persistentQueue.getIfAvailable();
        if (queue != null) {
            queue.submit("NOTIFY_SETTLED", event);
        } else {
            // Persistent queue disabled: notify best-effort inline.
            publisher.publish(settleNotifyDestination, "ORDER_SETTLED", event);
        }
        if (Boolean.getBoolean("simulate.settle.failure")) {
            throw new IllegalStateException("settlement backend unavailable (simulated)");
        }
        TibrvMsg reply = new TibrvMsg();
        reply.add("status", "OK");
        reply.add("handledBy", "orderSettle");
        return reply;
    }

    /**
     * Publish-only step chained from {@code orderSettle}; retried independently of the
     * settlement. A publish with no listeners does NOT fail (RV is fire-and-forget) —
     * this throws only on transport/config errors, e.g. an undefined destination.
     */
    @RvCommand(value = "NOTIFY_SETTLED", persistent = true)
    public void notifySettled(OrderSettledEvent event) throws TibrvException {
        publisher.publish(settleNotifyDestination, "ORDER_SETTLED", event);
        log.info("notifySettled published {} to '{}'", event, settleNotifyDestination);
    }
}
