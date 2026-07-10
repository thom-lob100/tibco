package com.tibco.tibrv;

import java.util.LinkedHashMap;
import java.util.Map;

/** Stub of {@code com.tibco.tibrv.TibrvMsg}: an ordered field map plus subjects. */
public class TibrvMsg {

    private final Map<String, Object> fields = new LinkedHashMap<>();
    private String sendSubject;
    private String replySubject;

    public TibrvMsg() throws TibrvException {
    }

    private TibrvMsg(Map<String, Object> fields, String sendSubject, String replySubject) {
        this.fields.putAll(fields);
        this.sendSubject = sendSubject;
        this.replySubject = replySubject;
    }

    public String getSendSubject() {
        return sendSubject;
    }

    public void setSendSubject(String subject) throws TibrvException {
        this.sendSubject = subject;
    }

    public String getReplySubject() {
        return replySubject;
    }

    public void setReplySubject(String subject) throws TibrvException {
        this.replySubject = subject;
    }

    public Object get(String fieldName) throws TibrvException {
        return fields.get(fieldName);
    }

    public int getNumFields() throws TibrvException {
        return fields.size();
    }

    public TibrvMsgField getFieldByIndex(int fieldIndex) throws TibrvException {
        int index = 0;
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            if (index++ == fieldIndex) {
                return new TibrvMsgField(entry.getKey(), entry.getValue());
            }
        }
        throw new TibrvException("field index out of range: " + fieldIndex);
    }

    public void add(String fieldName, String value) throws TibrvException {
        fields.put(fieldName, value);
    }

    public void add(String fieldName, Object value) throws TibrvException {
        fields.put(fieldName, value);
    }

    TibrvMsg copy() {
        return new TibrvMsg(fields, sendSubject, replySubject);
    }

    @Override
    public String toString() {
        StringBuilder text = new StringBuilder("{");
        fields.forEach((name, value) -> {
            if (text.length() > 1) {
                text.append(", ");
            }
            text.append(name).append('=').append(value);
        });
        return text.append('}').toString();
    }
}
