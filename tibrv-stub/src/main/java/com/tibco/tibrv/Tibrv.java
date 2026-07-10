package com.tibco.tibrv;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stub of {@code com.tibco.tibrv.Tibrv}. Open/close are reference counted like the
 * real library; "opening" only enables the JVM-local {@link StubBus}, no native code.
 */
public final class Tibrv {

    public static final int IMPL_NATIVE = 1;

    private static final AtomicInteger OPEN_COUNT = new AtomicInteger();

    private Tibrv() {
    }

    public static void open(int implementation) throws TibrvException {
        if (OPEN_COUNT.getAndIncrement() == 0) {
            System.err.println("[tibrvj-stub] In-memory Rendezvous stub active - "
                    + "JVM-local delivery only, NOT the real TIBCO library");
        }
    }

    public static boolean isValid() {
        return OPEN_COUNT.get() > 0;
    }

    public static void close() throws TibrvException {
        OPEN_COUNT.updateAndGet(count -> Math.max(0, count - 1));
    }
}
