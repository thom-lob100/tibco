package com.tibco.tibrv;

/** Stub of {@code com.tibco.tibrv.TibrvException}. */
public class TibrvException extends Exception {

    private static final long serialVersionUID = 1L;

    public TibrvException(String message) {
        super(message);
    }

    public TibrvException(int errorCode, String message) {
        super(message);
    }
}
