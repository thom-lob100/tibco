package com.p2gether.aos.scheduler;

import com.p2gether.aos.rv.RendezvousProperties;
import com.p2gether.aos.rv.RendezvousPublisher;
import com.p2gether.aos.rv.RendezvousSubscriber;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Activates the scheduler role. The whole module is a no-op unless the instance is
 * launched as SCH ({@code --aos.rendezvous.subject.listener=SCH}) — only then do the
 * scheduling infrastructure and these beans exist, so the single
 * aos-boot-application jar can be started per role (one folder per listener) without
 * scheduled work leaking into other roles.
 *
 * <p>Launch the SCH role with FT enabled so exactly one instance fires the schedule,
 * and typically without the web server (the family's REST port is the BOOT role):
 * {@code --aos.rendezvous.subject.listener=SCH --aos.rendezvous.ft.enabled=true
 * --spring.main.web-application-type=none}
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(name = "aos.rendezvous.subject.listener", havingValue = "SCH")
@RequiredArgsConstructor
public class SchedulerConfiguration {

    private final RendezvousProperties properties;

    @PostConstruct
    void warnWhenNotFaultTolerant() {
        if (!properties.getFt().isEnabled()) {
            log.warn("SCH role without FT (aos.rendezvous.ft.enabled=false): "
                    + "more than one instance would duplicate every scheduled call");
        }
    }

    @Bean
    SchedulerCommands schedulerCommands(
            @Value("${aos.scheduler.sample.enabled:false}") boolean sampleEnabled,
            @Value("${aos.scheduler.sample.interval:60}") long sampleInterval) {
        return new SchedulerCommands(sampleEnabled, sampleInterval);
    }

    @Bean
    @ConditionalOnProperty(name = "aos.scheduler.sample.enabled", havingValue = "true")
    SampleScheduledCall sampleScheduledCall(RendezvousSubscriber subscriber,
                                            RendezvousPublisher publisher) {
        return new SampleScheduledCall(subscriber, publisher);
    }

    @Bean
    @ConditionalOnProperty(name = "aos.scheduler.order-create.enabled", havingValue = "true")
    OrderCreateScheduledCall orderCreateScheduledCall(
            RendezvousSubscriber subscriber,
            RendezvousPublisher publisher,
            @Value("${aos.scheduler.order-create.destination:self}") String destination) {
        return new OrderCreateScheduledCall(subscriber, publisher, destination);
    }
}
