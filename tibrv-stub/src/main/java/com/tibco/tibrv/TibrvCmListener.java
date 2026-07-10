package com.tibco.tibrv;

/** Stub of {@code com.tibco.tibrv.TibrvCmListener} (listener for certified/distributed-queue delivery). */
public class TibrvCmListener extends TibrvListener {

    public TibrvCmListener(TibrvQueue queue, TibrvMsgCallback callback, TibrvCmTransport transport,
                           String subject, Object closure) throws TibrvException {
        super(queue, callback, transport, subject, closure);
    }
}
