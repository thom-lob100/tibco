package com.p2gether.aos.rv;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a Spring bean method as the handler for one command element: when a message
 * arrives whose subject ends with that command, the method is invoked with the message.
 *
 * <p>The method may take a single {@link com.tibco.tibrv.TibrvMsg} parameter (or none),
 * and may return a {@code TibrvMsg}, which is sent back automatically when the request
 * carries a reply subject (request/reply); a {@code void} handler never replies.
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RvCommand {

    /**
     * The command element to handle, e.g. {@code ORDER_CREATE}. Empty derives it from
     * the method name in UPPER_SNAKE form ({@code orderCreate} → {@code ORDER_CREATE}).
     */
    String value() default "";

    /**
     * Marks a command that must not be lost: the message is persisted to the
     * {@code rv_command_queue} table before processing, and on failure it is retried
     * from the database — surviving JVM restarts — until it succeeds or exhausts
     * {@code aos.rendezvous.persistent-queue.max-attempts} (then kept as FAILED).
     * Request/reply callers receive a {@code {status=QUEUED}} reply on failure.
     */
    boolean persistent() default false;
}
