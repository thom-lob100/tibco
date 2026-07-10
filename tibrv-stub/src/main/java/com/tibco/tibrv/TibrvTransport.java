package com.tibco.tibrv;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Stub of {@code com.tibco.tibrv.TibrvTransport}: sends onto the JVM-local bus. */
public abstract class TibrvTransport {

    /** Transports with the same key (service/network) see each other's messages. */
    abstract String busKey();

    public void send(TibrvMsg message) throws TibrvException {
        StubBus.send(busKey(), message);
    }

    /**
     * Request/reply over a temporary {@code _INBOX} subject. A negative timeout waits
     * forever; returns {@code null} when the timeout elapses without a reply.
     */
    public TibrvMsg sendRequest(TibrvMsg message, double timeout) throws TibrvException {
        String inbox = "_INBOX." + UUID.randomUUID();
        CompletableFuture<TibrvMsg> future = new CompletableFuture<>();
        TibrvListener inboxListener = new TibrvListener(this, inbox, future::complete);
        try {
            message.setReplySubject(inbox);
            StubBus.send(busKey(), message);
            return timeout < 0
                    ? future.get()
                    : future.get((long) (timeout * 1000), TimeUnit.MILLISECONDS);
        } catch (TimeoutException timedOut) {
            return null;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new TibrvException("interrupted while waiting for reply");
        } catch (ExecutionException impossible) {
            throw new TibrvException("reply delivery failed: " + impossible.getMessage());
        } finally {
            inboxListener.destroy();
        }
    }

    public void sendReply(TibrvMsg reply, TibrvMsg request) throws TibrvException {
        String replySubject = request.getReplySubject();
        if (replySubject == null) {
            throw new TibrvException("request carries no reply subject");
        }
        reply.setSendSubject(replySubject);
        StubBus.send(busKey(), reply);
    }

    public void destroy() {
        // Listeners hold the bus registrations; nothing to release here.
    }
}
