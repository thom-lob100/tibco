package com.p2gether.aos.rv;

import com.tibco.tibrv.Tibrv;
import com.tibco.tibrv.TibrvDispatcher;
import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvListener;
import com.tibco.tibrv.TibrvMsg;
import com.tibco.tibrv.TibrvMsgCallback;
import com.tibco.tibrv.TibrvQueue;
import com.tibco.tibrv.TibrvRvdTransport;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Test aid, disabled by default: impersonates the target of a configured destination
 * (e.g. MESSO) by listening on that destination's connection and replying to every
 * request. Enable with {@code --aos.rendezvous.destination-simulator.enabled=true};
 * pick the destination with {@code --aos.rendezvous.destination-simulator.destination=<name>}.
 * Works against the in-memory stub or a real rvd, so request/reply can be exercised
 * before the real target system is available.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "aos.rendezvous.destination-simulator.enabled", havingValue = "true")
public class DestinationSimulator implements TibrvMsgCallback {

    private final RendezvousProperties properties;
    private final String destinationName;

    private TibrvRvdTransport transport;
    private TibrvQueue queue;
    private TibrvDispatcher dispatcher;
    private TibrvListener listener;
    private String simulatedListener;

    public DestinationSimulator(
            RendezvousProperties properties,
            @Value("${aos.rendezvous.destination-simulator.destination:messo}") String destinationName) {
        this.properties = properties;
        this.destinationName = destinationName;
    }

    @PostConstruct
    public void start() throws TibrvException {
        RendezvousProperties.Destination destination = properties.destination(destinationName);
        simulatedListener = destination.getListener();
        Tibrv.open(Tibrv.IMPL_NATIVE);
        String service = destination.getService() != null ? destination.getService() : properties.getService();
        String network = destination.getNetwork() != null ? destination.getNetwork() : properties.getNetwork();
        String daemon = destination.getDaemon() != null ? destination.getDaemon() : properties.getDaemon();
        transport = new TibrvRvdTransport(service, network, daemon);
        RendezvousProperties.Subject own = properties.getSubject();
        String pattern = String.join(".",
                own.getFactory(), own.getEnvironment(), "*", "*", simulatedListener, ">");
        if (own.isLocal()) {
            pattern = "_LOCAL." + pattern;
        }
        queue = new TibrvQueue();
        listener = new TibrvListener(queue, this, transport, pattern, null);
        dispatcher = new TibrvDispatcher(queue);
        log.info("Destination simulator for '{}' listening on '{}' (service={}, network={})",
                destinationName, pattern, service, network);
    }

    @Override
    public void onMsg(TibrvListener source, TibrvMsg request) {
        log.info("Simulator '{}' received [{}] {}", simulatedListener, request.getSendSubject(), request);
        if (request.getReplySubject() == null) {
            return; // one-way publish, nothing to reply to
        }
        try {
            TibrvMsg reply = new TibrvMsg();
            reply.add("status", "OK");
            reply.add("respondedBy", simulatedListener + "-SIMULATOR");
            reply.add("requestSubject", request.getSendSubject());
            transport.sendReply(reply, request);
        } catch (TibrvException exception) {
            log.error("Simulator '{}' failed to reply", simulatedListener, exception);
        }
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
    }
}
