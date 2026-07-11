package com.p2gether.aos.rv;

import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvMsg;
import com.tibco.tibrv.TibrvMsgField;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Converts between {@link TibrvMsg} field maps and request/reply models so business
 * code can work with typed messages instead of raw field access. Supported payloads:
 * {@code TibrvMsg} (as-is), {@code Map} (entries become fields), and records (each
 * component becomes a field / is bound from the field of the same name).
 */
public final class RvMessages {

    private RvMessages() {
    }

    /** Builds the wire message for a payload; null becomes an empty message. */
    public static TibrvMsg toMsg(Object payload) throws TibrvException {
        if (payload instanceof TibrvMsg message) {
            return message;
        }
        TibrvMsg message = new TibrvMsg();
        if (payload == null) {
            return message;
        }
        if (payload instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                message.add(String.valueOf(entry.getKey()), (Object) entry.getValue());
            }
            return message;
        }
        if (payload.getClass().isRecord()) {
            try {
                for (RecordComponent component : payload.getClass().getRecordComponents()) {
                    Object value = component.getAccessor().invoke(payload);
                    if (value != null) {
                        message.add(component.getName(), value);
                    }
                }
            } catch (ReflectiveOperationException exception) {
                throw new IllegalArgumentException(
                        "Cannot read record payload " + payload.getClass().getName(), exception);
            }
            return message;
        }
        throw new IllegalArgumentException("Unsupported payload type "
                + payload.getClass().getName() + "; use TibrvMsg, Map, or a record");
    }

    /** Binds a message into the given type: {@code TibrvMsg} as-is, records by field name. */
    public static <T> T fromMsg(TibrvMsg message, Class<T> type) throws TibrvException {
        if (type == TibrvMsg.class) {
            return type.cast(message);
        }
        if (!type.isRecord()) {
            throw new IllegalArgumentException("Unsupported message type "
                    + type.getName() + "; use TibrvMsg or a record");
        }
        RecordComponent[] components = type.getRecordComponents();
        Object[] arguments = new Object[components.length];
        for (int i = 0; i < components.length; i++) {
            arguments[i] = convert(message.get(components[i].getName()), components[i].getType());
        }
        try {
            Constructor<T> constructor = type.getDeclaredConstructor(
                    Arrays.stream(components).map(RecordComponent::getType).toArray(Class[]::new));
            constructor.setAccessible(true);
            return constructor.newInstance(arguments);
        } catch (ReflectiveOperationException exception) {
            throw new IllegalArgumentException("Cannot bind message " + message
                    + " into " + type.getName(), exception);
        }
    }

    /** Copies a message's fields into an ordered map (e.g. for a JSON response body). */
    public static Map<String, Object> toMap(TibrvMsg message) throws TibrvException {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (int i = 0; i < message.getNumFields(); i++) {
            TibrvMsgField field = message.getFieldByIndex(i);
            fields.put(field.name, field.data);
        }
        return fields;
    }

    /**
     * Serializes a message's fields to a text payload for the persistent queue. Values
     * are stored as strings; record binding converts them back on deserialization.
     */
    public static String serialize(TibrvMsg message) throws TibrvException {
        Properties fields = new Properties();
        for (int i = 0; i < message.getNumFields(); i++) {
            TibrvMsgField field = message.getFieldByIndex(i);
            if (field.data != null) {
                fields.setProperty(field.name, String.valueOf(field.data));
            }
        }
        StringWriter payload = new StringWriter();
        try {
            fields.store(payload, null);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return payload.toString();
    }

    /** Rebuilds a message from a payload produced by {@link #serialize}. */
    public static TibrvMsg deserialize(String payload) throws TibrvException {
        Properties fields = new Properties();
        try {
            fields.load(new StringReader(payload));
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        TibrvMsg message = new TibrvMsg();
        for (String name : fields.stringPropertyNames()) {
            message.add(name, fields.getProperty(name));
        }
        return message;
    }

    private static Object convert(Object value, Class<?> target) {
        if (value == null) {
            if (target == int.class || target == long.class || target == double.class) {
                return target == int.class ? 0 : target == long.class ? 0L : 0.0;
            }
            return target == boolean.class ? Boolean.FALSE : null;
        }
        if (target.isInstance(value)) {
            return value;
        }
        if (target == String.class) {
            return String.valueOf(value);
        }
        if (target == int.class || target == Integer.class) {
            return value instanceof Number number ? number.intValue() : Integer.parseInt(value.toString());
        }
        if (target == long.class || target == Long.class) {
            return value instanceof Number number ? number.longValue() : Long.parseLong(value.toString());
        }
        if (target == double.class || target == Double.class) {
            return value instanceof Number number ? number.doubleValue() : Double.parseDouble(value.toString());
        }
        if (target == boolean.class || target == Boolean.class) {
            return Boolean.parseBoolean(value.toString());
        }
        return value;
    }
}
