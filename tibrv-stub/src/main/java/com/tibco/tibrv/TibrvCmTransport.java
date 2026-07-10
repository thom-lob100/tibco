package com.tibco.tibrv;

/** Stub of {@code com.tibco.tibrv.TibrvCmTransport} (certified-messaging transport base). */
public class TibrvCmTransport extends TibrvTransport {

    TibrvTransport inner;

    public TibrvCmTransport(TibrvTransport transport) throws TibrvException {
        this.inner = transport;
    }

    /** For subclasses only; the real class exposes no public no-arg constructor. */
    protected TibrvCmTransport() {
    }

    @Override
    String busKey() {
        return inner.busKey();
    }
}
