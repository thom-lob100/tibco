package com.tibco.tibrv;

/** Stub of {@code com.tibco.tibrv.TibrvMsgField}: one named field of a message. */
public class TibrvMsgField {

    public String name;
    public Object data;

    public TibrvMsgField() {
    }

    TibrvMsgField(String name, Object data) {
        this.name = name;
        this.data = data;
    }
}
