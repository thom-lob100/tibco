package com.p2gether.aos.rv;

import com.tibco.tibrv.Tibrv;
import com.tibco.tibrv.TibrvCmListener;
import com.tibco.tibrv.TibrvCmQueueTransport;
import com.tibco.tibrv.TibrvDispatcher;
import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvListener;
import com.tibco.tibrv.TibrvMsg;
import com.tibco.tibrv.TibrvMsgCallback;
import com.tibco.tibrv.TibrvQueue;
import com.tibco.tibrv.TibrvRvdTransport;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Joins a Rendezvous distributed queue (DQ) on startup and logs every message assigned
 * to this member.
 *
 * <p>All instances started with the same {@code aos.rendezvous.dq.name} form one queue
 * group: each message is processed by exactly one member, the member with the highest
 * {@code scheduler-weight} is elected scheduler, and if it dies another member takes
 * over. The dispatcher is a non-daemon thread, so it also keeps the JVM alive.
 *
 * <p>Each assigned message is routed by its command element to the matching
 * {@link RvCommand} handler method; a reply the handler returns is sent back when the
 * request carries a reply subject.
 *
 * <p>With {@code aos.rendezvous.ft.enabled=true} the listener is NOT attached at
 * startup: {@link RendezvousFtCoordinator} calls {@link #activate()} /
 * {@link #deactivate()} as this instance wins or loses the fault-tolerance election,
 * so standby instances stay connected but consume nothing until promoted.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RendezvousSubscriber implements TibrvMsgCallback {

    private final RendezvousProperties properties;
    private final RvCommandDispatcher commandDispatcher;

    private TibrvRvdTransport transport;
    private TibrvCmQueueTransport dqTransport;
    private TibrvQueue queue;
    private TibrvDispatcher dispatcher;
    private TibrvCmListener listener;

    @PostConstruct
    public void start() {
        RendezvousProperties.Dq dq = properties.getDq();
        try {
            Tibrv.open(Tibrv.IMPL_NATIVE);
            transport = new TibrvRvdTransport(
                    properties.getService(), properties.getNetwork(), properties.getDaemon());
            dqTransport = new TibrvCmQueueTransport(
                    transport, dq.getName(),
                    dq.getWorkerWeight(), dq.getWorkerTasks(), dq.getSchedulerWeight(),
                    dq.getSchedulerHeartbeat(), dq.getSchedulerActivation());
            queue = new TibrvQueue();
            dispatcher = new TibrvDispatcher(queue);
            if (properties.getFt().isEnabled()) {
                log.info("FT mode: standing by in group '{}' until activated (weight={})",
                        properties.ftName(), properties.getFt().getWeight());
            } else {
                activate();
            }
        } catch (TibrvException exception) {
            throw new IllegalStateException("Failed to join the Rendezvous distributed queue", exception);
        }
    }

    /** Attaches the DQ listener; idempotent. Called at startup, or by the FT coordinator. */
    public synchronized void activate() throws TibrvException {
        if (listener != null) {
            return;
        }
        RendezvousProperties.Dq dq = properties.getDq();
        String subject = properties.getSubject().value();
        listener = new TibrvCmListener(queue, this, dqTransport, subject, null);
        log.info("Joined DQ '{}' on subject '{}' (workerWeight={}, schedulerWeight={}, "
                        + "service={}, network={}, daemon={})",
                dq.getName(), subject,
                dq.getWorkerWeight(), dq.getSchedulerWeight(),
                properties.getService(), properties.getNetwork(), properties.getDaemon());
    }

    /**
     * Whether this instance is currently consuming: always true after startup without
     * FT; with FT, true only while this member holds an active slot. Periodic jobs
     * ({@code @Scheduled}) must check this, because Spring scheduling keeps running on
     * FT standbys — only RV consumption is toggled by the coordinator.
     */
    public synchronized boolean isActive() {
        return listener != null;
    }

    /** Detaches the DQ listener (stop consuming); idempotent. Called by the FT coordinator. */
    public synchronized void deactivate() {
        if (listener == null) {
            return;
        }
        listener.destroy();
        listener = null;
        log.info("Left DQ '{}' — standing by", properties.getDq().getName());
    }

    @Override
    public void onMsg(TibrvListener source, TibrvMsg message) {
        // Returning normally from this callback confirms the assignment to the scheduler;
        // handler exceptions are contained inside the dispatcher, so a failing handler
        // does not push the message back to the queue. On handler failure the future
        // completes later from the async retry pool, so this callback never blocks on
        // retries and the member keeps consuming.
        log.info("Received [{}] {}", message.getSendSubject(), message);
        commandDispatcher.dispatch(message).thenAccept(reply -> reply.ifPresent(replyMessage -> {
            if (message.getReplySubject() == null) {
                return; // one-way publish, the handler's reply has nowhere to go
            }
            try {
                transport.sendReply(replyMessage, message);
            } catch (TibrvException exception) {
                log.error("Failed to reply to [{}]", message.getSendSubject(), exception);
            }
        }));
    }

    @PreDestroy
    public void stop() {
        if (listener != null) {
            listener.destroy();
        }
        if (dispatcher != null) {
            dispatcher.destroy();
        }
        if (queue != null) {
            queue.destroy();
        }
        if (dqTransport != null) {
            dqTransport.destroy();
        }
        if (transport != null) {
            transport.destroy();
        }
        if (Tibrv.isValid()) {
            try {
                Tibrv.close();
            } catch (TibrvException exception) {
                log.warn("Error while closing Rendezvous", exception);
            }
        }
        log.info("Rendezvous DQ member stopped");
    }
}
