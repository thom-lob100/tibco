package com.p2gether.aos.rv;

import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvMsg;
import jakarta.annotation.PreDestroy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

/**
 * Registry of {@link RvCommand} handler methods, collected from every Spring bean at
 * startup. {@link #dispatch} maps an incoming message's command element (the last
 * subject element) to its handler and invokes it, binding the message into the
 * handler's parameter type (raw {@code TibrvMsg} or a record, see {@link RvMessages}).
 *
 * <p><b>Failure policy</b> ({@code aos.rendezvous.handler-retry.*}): the first attempt
 * runs in the caller's (DQ callback) thread; if it throws, dispatch returns immediately
 * and retries run on the async retry pool, so the DQ member keeps consuming while a
 * failed message is retried. When a retry succeeds its reply completes the returned
 * future; after the last failure — or when no handler exists — the future completes
 * with a {@code {status=ERROR}} reply so request/reply callers fail fast. Exceptions
 * never escape to the Rendezvous callback, so the DQ assignment stays confirmed and the
 * message is not redelivered (a JVM crash mid-retry therefore loses the retries).
 *
 * <p>Retries may run concurrently with newer messages' handlers; handlers of retried
 * commands must be safe to run out of order and in parallel.
 */
@Slf4j
@Component
public class RvCommandDispatcher implements BeanPostProcessor {

    private record Handler(Object bean, Method method, boolean persistent) {
    }

    private final ObjectProvider<RendezvousProperties> propertiesProvider;
    private final ObjectProvider<RvPersistentCommandQueue> persistentQueueProvider;
    private final Map<String, Handler> handlers = new ConcurrentHashMap<>();
    private ScheduledExecutorService retryExecutor;

    public RvCommandDispatcher(ObjectProvider<RendezvousProperties> propertiesProvider,
                               ObjectProvider<RvPersistentCommandQueue> persistentQueueProvider) {
        this.propertiesProvider = propertiesProvider;
        this.persistentQueueProvider = persistentQueueProvider;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        for (Method method : ClassUtils.getUserClass(bean).getDeclaredMethods()) {
            RvCommand annotation = method.getAnnotation(RvCommand.class);
            if (annotation == null) {
                continue;
            }
            validateSignature(method);
            String command = annotation.value().isEmpty()
                    ? toUpperSnake(method.getName()) : annotation.value();
            Handler previous = handlers.putIfAbsent(command, new Handler(bean, method, annotation.persistent()));
            if (previous != null) {
                throw new IllegalStateException("Duplicate @RvCommand handler for '" + command
                        + "': " + previous.method() + " and " + method);
            }
            method.setAccessible(true);
            log.info("Registered command '{}' -> {}.{}{}", command,
                    ClassUtils.getUserClass(bean).getSimpleName(), method.getName(),
                    annotation.persistent() ? " (persistent)" : "");
        }
        return bean;
    }

    /**
     * Invokes the handler for the message's command element.
     *
     * @return a future completing with the reply to send back (handler result, or an
     *         ERROR reply on final failure); already completed unless retries run
     */
    public CompletableFuture<Optional<TibrvMsg>> dispatch(TibrvMsg message) {
        String subject = message.getSendSubject();
        String command = subject.substring(subject.lastIndexOf('.') + 1);
        Handler handler = handlers.get(command);
        if (handler == null) {
            log.warn("No @RvCommand handler for command '{}' (subject '{}')", command, subject);
            return CompletableFuture.completedFuture(
                    errorReply(command, "no handler for command '" + command + "'"));
        }
        if (handler.persistent()) {
            CompletableFuture<Optional<TibrvMsg>> persistentResult = dispatchPersistent(handler, command, message);
            if (persistentResult != null) {
                return persistentResult;
            }
            // Persistent queue unavailable: fall through to the in-memory retry path.
        }
        RendezvousProperties.HandlerRetry policy = propertiesProvider.getObject().getHandlerRetry();
        int attempts = Math.max(1, policy.getRetries() + 1);
        try {
            return CompletableFuture.completedFuture(toReply(invoke(handler, message)));
        } catch (Exception exception) {
            Throwable failure = unwrap(exception);
            log.warn("Handler for '{}' failed (attempt 1/{}): {}",
                    command, attempts, String.valueOf(failure));
            if (attempts == 1) {
                log.error("Handler for '{}' failed after 1 attempt", command, failure);
                return CompletableFuture.completedFuture(errorReply(command, String.valueOf(failure)));
            }
            CompletableFuture<Optional<TibrvMsg>> pending = new CompletableFuture<>();
            scheduleRetry(handler, message, command, policy, 2, attempts, pending);
            return pending;
        }
    }

    /**
     * Persistent path: persist first, then attempt inline. Success deletes the row and
     * replies normally; failure parks the row for the queue poller and immediately
     * replies {@code {status=QUEUED}} so callers know the message is safe and retrying.
     * Returns null when the queue is unavailable (bean disabled or database down).
     */
    private CompletableFuture<Optional<TibrvMsg>> dispatchPersistent(
            Handler handler, String command, TibrvMsg message) {
        RvPersistentCommandQueue queue = persistentQueueProvider.getIfAvailable();
        if (queue == null) {
            log.warn("Persistent command '{}' has no queue bean (persistent-queue disabled);"
                    + " falling back to in-memory retry", command);
            return null;
        }
        long id;
        try {
            id = queue.enqueue(command, message);
        } catch (Exception databaseFailure) {
            log.error("Failed to enqueue persistent command '{}'; falling back to in-memory retry",
                    command, databaseFailure);
            return null;
        }
        try {
            Optional<TibrvMsg> reply = toReply(invoke(handler, message));
            queue.complete(id);
            return CompletableFuture.completedFuture(reply);
        } catch (Exception exception) {
            Throwable failure = unwrap(exception);
            log.warn("Persistent command '{}' (row {}) failed inline, parked for the queue poller: {}",
                    command, id, String.valueOf(failure));
            queue.failAttempt(id, String.valueOf(failure));
            return CompletableFuture.completedFuture(queuedReply(command, id, String.valueOf(failure)));
        }
    }

    /** Single handler invocation without retries; used by the persistent queue poller. */
    Optional<TibrvMsg> invokeOnce(String command, TibrvMsg message) throws Exception {
        Handler handler = handlers.get(command);
        if (handler == null) {
            throw new IllegalStateException("no handler for command '" + command + "'");
        }
        return toReply(invoke(handler, message));
    }

    private void scheduleRetry(Handler handler, TibrvMsg message, String command,
                               RendezvousProperties.HandlerRetry policy,
                               int attempt, int attempts,
                               CompletableFuture<Optional<TibrvMsg>> pending) {
        retryExecutor().schedule(() -> {
            try {
                pending.complete(toReply(invoke(handler, message)));
                log.info("Handler for '{}' succeeded on retry (attempt {}/{})",
                        command, attempt, attempts);
            } catch (Exception exception) {
                Throwable failure = unwrap(exception);
                log.warn("Handler for '{}' failed (attempt {}/{}): {}",
                        command, attempt, attempts, String.valueOf(failure));
                if (attempt < attempts) {
                    scheduleRetry(handler, message, command, policy, attempt + 1, attempts, pending);
                } else {
                    log.error("Handler for '{}' failed after {} attempts", command, attempts, failure);
                    pending.complete(errorReply(command, String.valueOf(failure)));
                }
            }
        }, (long) (policy.getBackoff() * 1000), TimeUnit.MILLISECONDS);
    }

    private synchronized ScheduledExecutorService retryExecutor() {
        if (retryExecutor == null) {
            int threads = Math.max(1, propertiesProvider.getObject().getHandlerRetry().getThreads());
            AtomicInteger threadNumber = new AtomicInteger();
            retryExecutor = Executors.newScheduledThreadPool(threads, task -> {
                Thread thread = new Thread(task, "rv-handler-retry-" + threadNumber.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            });
        }
        return retryExecutor;
    }

    @PreDestroy
    public void stop() {
        synchronized (this) {
            if (retryExecutor != null) {
                retryExecutor.shutdownNow();
            }
        }
    }

    private static Object invoke(Handler handler, TibrvMsg message) throws Exception {
        Class<?>[] parameters = handler.method().getParameterTypes();
        Object[] arguments = parameters.length == 0
                ? new Object[0] : new Object[] {RvMessages.fromMsg(message, parameters[0])};
        return handler.method().invoke(handler.bean(), arguments);
    }

    private static Optional<TibrvMsg> toReply(Object result) throws TibrvException {
        return result == null ? Optional.empty() : Optional.of(RvMessages.toMsg(result));
    }

    private static Throwable unwrap(Exception exception) {
        return exception instanceof InvocationTargetException invocation
                ? invocation.getCause() : exception;
    }

    private static Optional<TibrvMsg> queuedReply(String command, long queueId, String error) {
        try {
            TibrvMsg reply = new TibrvMsg();
            reply.add("status", "QUEUED");
            reply.add("command", command);
            reply.add("queueId", String.valueOf(queueId));
            reply.add("error", error);
            return Optional.of(reply);
        } catch (TibrvException exception) {
            log.error("Failed to build QUEUED reply for command '{}'", command, exception);
            return Optional.empty();
        }
    }

    private static Optional<TibrvMsg> errorReply(String command, String error) {
        try {
            TibrvMsg reply = new TibrvMsg();
            reply.add("status", "ERROR");
            reply.add("command", command);
            reply.add("error", error);
            return Optional.of(reply);
        } catch (TibrvException exception) {
            log.error("Failed to build ERROR reply for command '{}'", command, exception);
            return Optional.empty();
        }
    }

    private static void validateSignature(Method method) {
        Class<?>[] parameters = method.getParameterTypes();
        boolean parametersOk = parameters.length == 0
                || (parameters.length == 1
                        && (parameters[0] == TibrvMsg.class || parameters[0].isRecord()));
        Class<?> returnType = method.getReturnType();
        boolean returnOk = returnType == void.class || returnType == TibrvMsg.class || returnType.isRecord();
        if (!parametersOk || !returnOk) {
            throw new IllegalStateException("@RvCommand method " + method
                    + " must take (TibrvMsg), (a record), or no arguments"
                    + " and return void, TibrvMsg, or a record");
        }
    }

    private static String toUpperSnake(String methodName) {
        return methodName.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toUpperCase();
    }
}
