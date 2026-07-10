package com.tibco.tibrv;

/** Stub of {@code com.tibco.tibrv.TibrvRvdTransport}. */
public class TibrvRvdTransport extends TibrvTransport {

    private final String service;
    private final String network;
    private final String daemon;

    public TibrvRvdTransport(String service, String network, String daemon) throws TibrvException {
        this.service = service;
        this.network = network;
        this.daemon = daemon;
    }

    public String getService() {
        return service;
    }

    public String getNetwork() {
        return network;
    }

    public String getDaemon() {
        return daemon;
    }

    @Override
    String busKey() {
        return (service == null ? "" : service) + "|" + (network == null ? "" : network);
    }
}
