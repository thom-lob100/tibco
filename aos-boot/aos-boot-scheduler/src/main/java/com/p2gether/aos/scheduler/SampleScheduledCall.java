package com.p2gether.aos.scheduler;

import com.p2gether.aos.rv.RendezvousPublisher;
import com.p2gether.aos.rv.RendezvousSubscriber;
import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvMsg;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Example of the pattern this module exists for: every
 * {@code aos.scheduler.sample.interval} seconds the FT-active instance sends a command
 * to a destination and logs the reply.
 *
 * <p>Spring's scheduler runs on every instance, FT standbys included — the
 * {@link RendezvousSubscriber#isActive()} guard is what restricts the actual call to
 * the active member, so every periodic job must start with it.
 *
 * <p>Disabled by default; enable with {@code aos.scheduler.sample.enabled=true} (pair
 * with {@code aos.rendezvous.destination-simulator.enabled=true} to get replies
 * without a real MESSO).
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "aos.scheduler.sample.enabled", havingValue = "true")
public class SampleScheduledCall {

    private final RendezvousSubscriber subscriber;
    private final RendezvousPublisher publisher;

    @Scheduled(fixedDelayString = "${aos.scheduler.sample.interval:60}", timeUnit = TimeUnit.SECONDS)
    public void call() {
        if (!subscriber.isActive()) {
            log.debug("FT standby — skipping scheduled call");
            return;
        }
        try {
            TibrvMsg reply = publisher.request("messo", "ORDER_SYNC", Map.of("trigger", "SCHEDULED"));
            if (reply == null) {
                log.warn("Scheduled ORDER_SYNC to 'messo' timed out on every attempt");
            } else {
                log.info("Scheduled ORDER_SYNC to 'messo' replied: {}", reply);
            }
        } catch (TibrvException exception) {
            log.error("Scheduled ORDER_SYNC to 'messo' failed", exception);
        }
    }
}
