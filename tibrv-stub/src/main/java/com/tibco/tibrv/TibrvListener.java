package com.tibco.tibrv;

import java.util.function.Consumer;

/** Stub of {@code com.tibco.tibrv.TibrvListener}: a subject subscription on the bus. */
public class TibrvListener {

    private final TibrvQueue queue;
    private final TibrvMsgCallback callback;
    private final Consumer<TibrvMsg> direct;
    private final String busKey;
    private final String subject;
    private final String dqName;

    public TibrvListener(TibrvQueue queue, TibrvMsgCallback callback, TibrvTransport transport,
                         String subject, Object closure) throws TibrvException {
        this.queue = queue;
        this.callback = callback;
        this.direct = null;
        this.busKey = transport.busKey();
        this.subject = subject;
        this.dqName = transport instanceof TibrvCmQueueTransport cmQueue ? cmQueue.cmName() : null;
        StubBus.register(busKey, this);
    }

    /** Internal inbox listener used by {@link TibrvTransport#sendRequest}: bypasses the queue. */
    TibrvListener(TibrvTransport transport, String subject, Consumer<TibrvMsg> direct) {
        this.queue = null;
        this.callback = null;
        this.direct = direct;
        this.busKey = transport.busKey();
        this.subject = subject;
        this.dqName = null;
        StubBus.register(busKey, this);
    }

    void post(TibrvMsg message) {
        if (direct != null) {
            direct.accept(message);
        } else {
            queue.post(() -> callback.onMsg(this, message));
        }
    }

    String subjectPattern() {
        return subject;
    }

    String dqName() {
        return dqName;
    }

    public String getSubject() {
        return subject;
    }

    public void destroy() {
        StubBus.unregister(busKey, this);
    }
}
