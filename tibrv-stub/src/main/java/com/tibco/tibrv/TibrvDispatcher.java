package com.tibco.tibrv;

/**
 * Stub of {@code com.tibco.tibrv.TibrvDispatcher}. Like the real class it extends
 * {@link Thread}, starts itself on construction, and runs non-daemon (keeping the JVM
 * alive); {@code destroy()} is the TIBCO method, not the removed {@code Thread.destroy()}.
 */
public class TibrvDispatcher extends Thread {

    private final TibrvDispatchable dispatchable;
    private volatile boolean destroyed;

    public TibrvDispatcher(TibrvDispatchable dispatchable) {
        this.dispatchable = dispatchable;
        setName("Dispatcher");
        start();
    }

    @Override
    public void run() {
        while (!destroyed) {
            try {
                dispatchable.dispatch();
            } catch (InterruptedException | TibrvException exception) {
                return;
            }
        }
    }

    public void destroy() {
        destroyed = true;
        interrupt();
    }
}
