package com.p2gether.aos.rv;

import com.tibco.tibrv.Tibrv;
import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvMsg;
import com.tibco.tibrv.TibrvRvdTransport;
import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Sends messages to destinations predefined under {@code aos.rendezvous.destinations}
 * (e.g. MESSO), each of which may live on its own Rendezvous service/network.
 *
 * <p>One {@link TibrvRvdTransport} is created lazily per destination and reused for
 * every call. Payloads may be a {@link TibrvMsg}, a {@code Map}, or a record (see
 * {@link RvMessages}). {@link #publish} is one-way (fire-and-forget); {@link #request}
 * is request/reply and retries on timeout per {@code aos.rendezvous.request.*} —
 * retries resend the same request, so target handlers should be idempotent.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RendezvousPublisher {

    private final RendezvousProperties properties;

    private final Map<String, TibrvRvdTransport> transports = new ConcurrentHashMap<>();
    private volatile boolean openedRv;

    /** One-way send: composes the subject and publishes without waiting for a reply. */
    public void publish(String destinationName, String command, Object payload) throws TibrvException {
        String subject = properties.subjectFor(destinationName, command);
        TibrvMsg message = RvMessages.toMsg(payload);
        message.setSendSubject(subject);
        transport(destinationName).send(message);
        log.debug("Published to '{}' via destination '{}'", subject, destinationName);
    }

    /**
     * Request/reply with the configured default timeout and timeout retries.
     *
     * @return the reply message, or {@code null} when every attempt times out
     */
    public TibrvMsg request(String destinationName, String command, Object payload) throws TibrvException {
        return request(destinationName, command, payload, properties.getRequest().getTimeout());
    }

    /**
     * Request/reply binding the reply into {@code replyType} (a record or TibrvMsg).
     *
     * @return the bound reply, or {@code null} when every attempt times out
     */
    public <T> T request(String destinationName, String command, Object payload, Class<T> replyType)
            throws TibrvException {
        TibrvMsg reply = request(destinationName, command, payload);
        return reply == null ? null : RvMessages.fromMsg(reply, replyType);
    }

    /**
     * Single-attempt request/reply with no timeout retries, for interactive callers
     * (e.g. the REST gateway) that must fail fast instead of holding their thread
     * through the retry policy.
     *
     * @return the reply message, or {@code null} on timeout
     */
    public TibrvMsg requestOnce(String destinationName, String command, Object payload, double timeoutSeconds)
            throws TibrvException {
        String subject = properties.subjectFor(destinationName, command);
        TibrvMsg message = RvMessages.toMsg(payload);
        message.setSendSubject(subject);
        TibrvMsg reply = transport(destinationName).sendRequest(message, timeoutSeconds);
        if (reply == null) {
            log.warn("Request to '{}' timed out after {}s (single attempt)", subject, timeoutSeconds);
        }
        return reply;
    }

    /**
     * Request/reply with an explicit per-attempt timeout; retries on timeout per
     * {@code aos.rendezvous.request.retries}/{@code backoff}.
     *
     * @return the reply message, or {@code null} when every attempt times out
     */
    public TibrvMsg request(String destinationName, String command, Object payload, double timeoutSeconds)
            throws TibrvException {
        String subject = properties.subjectFor(destinationName, command);
        TibrvMsg message = RvMessages.toMsg(payload);
        message.setSendSubject(subject);
        RendezvousProperties.Request policy = properties.getRequest();
        int attempts = Math.max(1, policy.getRetries() + 1);
        TibrvRvdTransport transport = transport(destinationName);
        for (int attempt = 1; attempt <= attempts; attempt++) {
            TibrvMsg reply = transport.sendRequest(message, timeoutSeconds);
            if (reply != null) {
                log.debug("Request to '{}' replied on attempt {}/{}", subject, attempt, attempts);
                return reply;
            }
            log.warn("Request to '{}' timed out after {}s (attempt {}/{})",
                    subject, timeoutSeconds, attempt, attempts);
            if (attempt < attempts) {
                pause(policy.getBackoff());
            }
        }
        log.error("Request to '{}' gave up after {} attempts", subject, attempts);
        return null;
    }

    private static void pause(double seconds) {
        if (seconds <= 0) {
            return;
        }
        try {
            Thread.sleep((long) (seconds * 1000));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private synchronized TibrvRvdTransport transport(String destinationName) throws TibrvException {
        TibrvRvdTransport cached = transports.get(destinationName);
        if (cached != null) {
            return cached;
        }
        RendezvousProperties.Destination destination = properties.destination(destinationName);
        if (!openedRv) {
            // Tibrv.open/close are reference counted, so this coexists with the subscriber's open.
            Tibrv.open(Tibrv.IMPL_NATIVE);
            openedRv = true;
        }
        String service = destination.getService() != null ? destination.getService() : properties.getService();
        String network = destination.getNetwork() != null ? destination.getNetwork() : properties.getNetwork();
        String daemon = destination.getDaemon() != null ? destination.getDaemon() : properties.getDaemon();
        TibrvRvdTransport transport = new TibrvRvdTransport(service, network, daemon);
        transports.put(destinationName, transport);
        log.info("Created transport for destination '{}' (listener={}, service={}, network={}, daemon={})",
                destinationName, destination.getListener(), service, network, daemon);
        return transport;
    }

    @PreDestroy
    public void stop() {
        transports.values().forEach(TibrvRvdTransport::destroy);
        transports.clear();
        if (openedRv && Tibrv.isValid()) {
            try {
                Tibrv.close();
            } catch (TibrvException exception) {
                log.warn("Error while closing Rendezvous", exception);
            }
        }
    }
}
