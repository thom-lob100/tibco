package com.tibco.tibrv;

import java.util.concurrent.LinkedBlockingQueue;

/** Stub of {@code com.tibco.tibrv.TibrvQueue}: a FIFO of pending callback invocations. */
public class TibrvQueue implements TibrvDispatchable {

    private final LinkedBlockingQueue<Runnable> events = new LinkedBlockingQueue<>();

    public TibrvQueue() throws TibrvException {
    }

    void post(Runnable event) {
        events.offer(event);
    }

    @Override
    public void dispatch() throws TibrvException, InterruptedException {
        events.take().run();
    }

    public void destroy() {
        events.clear();
    }
}
