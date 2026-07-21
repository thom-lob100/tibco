package com.tibco.tibrv;

/** Stub of {@code com.tibco.tibrv.TibrvCmQueueTransport} (distributed-queue member transport). */
public class TibrvCmQueueTransport extends TibrvCmTransport {

    private final String cmName;

    public TibrvCmQueueTransport(TibrvRvdTransport transport, String cmName) throws TibrvException {
        this.inner = transport;
        this.cmName = cmName;
    }

    public TibrvCmQueueTransport(TibrvRvdTransport transport, String cmName,
                                 int workerWeight, int workerTasks, int schedulerWeight,
                                 double schedulerHeartbeat, double schedulerActivation)
            throws TibrvException {
        this.inner = transport;
        this.cmName = cmName;
    }

    String cmName() {
        return cmName;
    }
}
