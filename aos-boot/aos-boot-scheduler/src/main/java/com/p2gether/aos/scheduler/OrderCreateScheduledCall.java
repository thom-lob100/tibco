package com.p2gether.aos.scheduler;

import com.p2gether.aos.rv.RendezvousPublisher;
import com.p2gether.aos.rv.RendezvousSubscriber;
import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvMsg;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Example of driving a samples handler on a schedule: every
 * {@code aos.scheduler.order-create.interval} seconds (default 300 = 5 minutes) the
 * FT-active SCH instance sends {@code ORDER_CREATE} — handled by the samples module's
 * {@code SampleCommands.orderCreate} — to the
 * {@code aos.scheduler.order-create.destination} destination and logs the reply.
 *
 * <p>The scheduler module never calls samples code directly (samples are excluded
 * from the production artifact); the only link is the command name on the subject.
 * {@code request()} retries resend the SAME message, so the target handler must be
 * idempotent — {@code orderCreate} is.
 *
 * <p>Demo in one JVM on the stub bus (which cannot cross processes): run the samples
 * module AS the SCH role, so the samples handlers and this schedule share the
 * instance and the default destination {@code self} loops back through RV:
 * <pre>
 * mvn -pl aos-boot-samples spring-boot:run -Dspring-boot.run.arguments="\
 *   --aos.rendezvous.subject.listener=SCH \
 *   --aos.scheduler.order-create.enabled=true \
 *   --aos.scheduler.order-create.interval=10"
 * </pre>
 * Against a real rvd with the handlers in another role, point
 * {@code aos.scheduler.order-create.destination} at that role's destination instead.
 *
 * <p>Registered by {@link SchedulerConfiguration} only in the SCH role, and disabled
 * by default; enable with {@code aos.scheduler.order-create.enabled=true}.
 */
@Slf4j
@RequiredArgsConstructor
public class OrderCreateScheduledCall {

    private final RendezvousSubscriber subscriber;
    private final RendezvousPublisher publisher;
    private final String destination;

    @Scheduled(fixedDelayString = "${aos.scheduler.order-create.interval:300}", timeUnit = TimeUnit.SECONDS)
    public void call() {
        if (!subscriber.isActive()) {
            log.debug("FT standby — skipping scheduled ORDER_CREATE");
            return;
        }
        String orderId = "SCH-" + System.currentTimeMillis();
        try {
            TibrvMsg reply = publisher.request(destination, "ORDER_CREATE",
                    Map.of("orderId", orderId, "quantity", 1));
            if (reply == null) {
                log.warn("Scheduled ORDER_CREATE({}) to '{}' timed out on every attempt",
                        orderId, destination);
            } else {
                log.info("Scheduled ORDER_CREATE({}) to '{}' replied: {}", orderId, destination, reply);
            }
        } catch (TibrvException exception) {
            log.error("Scheduled ORDER_CREATE({}) to '{}' failed", orderId, destination, exception);
        }
    }
}
