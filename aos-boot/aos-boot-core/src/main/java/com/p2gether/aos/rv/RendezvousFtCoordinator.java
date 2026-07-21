package com.p2gether.aos.rv;

import com.tibco.tibrv.Tibrv;
import com.tibco.tibrv.TibrvDispatcher;
import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvFtMember;
import com.tibco.tibrv.TibrvFtMemberCallback;
import com.tibco.tibrv.TibrvQueue;
import com.tibco.tibrv.TibrvRvdTransport;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Fault-tolerance (active/standby) coordination, enabled with
 * {@code aos.rendezvous.ft.enabled=true}: joins the FT group
 * ({@code AOS.<listener>.FT} by default) and turns the {@link RendezvousSubscriber}'s
 * consumption on/off as this instance is promoted or demoted. Instances share the
 * group; the highest-weight {@code active-goal} members consume, the rest stand by
 * and take over automatically when an active member dies.
 *
 * <p>Combining FT with the DQ subscriber means {@code active-goal > 1} yields "N
 * active members load-balancing via the DQ, the rest on standby".
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "aos.rendezvous.ft.enabled", havingValue = "true")
public class RendezvousFtCoordinator implements TibrvFtMemberCallback {

    private final RendezvousProperties properties;
    private final RendezvousSubscriber subscriber;

    private TibrvRvdTransport transport;
    private TibrvQueue queue;
    private TibrvDispatcher dispatcher;
    private TibrvFtMember member;

    @PostConstruct
    public void start() throws TibrvException {
        RendezvousProperties.Ft ft = properties.getFt();
        Tibrv.open(Tibrv.IMPL_NATIVE);
        transport = new TibrvRvdTransport(
                properties.getService(), properties.getNetwork(), properties.getDaemon());
        queue = new TibrvQueue();
        member = new TibrvFtMember(queue, this, transport, properties.ftName(),
                ft.getWeight(), ft.getActiveGoal(),
                ft.getHeartbeat(), ft.getPreparation(), ft.getActivation(), null);
        dispatcher = new TibrvDispatcher(queue);
        log.info("Joined FT group '{}' (weight={}, activeGoal={})",
                properties.ftName(), ft.getWeight(), ft.getActiveGoal());
    }

    @Override
    public void onFtAction(TibrvFtMember source, String groupName, int action) {
        try {
            switch (action) {
                case TibrvFtMember.PREPARE_TO_ACTIVATE ->
                        log.info("FT '{}' (weight={}): PREPARE_TO_ACTIVATE", groupName, source.getWeight());
                case TibrvFtMember.ACTIVATE -> {
                    log.info("FT '{}' (weight={}): ACTIVATE — starting to consume",
                            groupName, source.getWeight());
                    subscriber.activate();
                }
                case TibrvFtMember.DEACTIVATE -> {
                    log.info("FT '{}' (weight={}): DEACTIVATE — standing by",
                            groupName, source.getWeight());
                    subscriber.deactivate();
                }
                default -> log.warn("FT '{}': unknown action {}", groupName, action);
            }
        } catch (TibrvException exception) {
            log.error("FT '{}' action {} failed", groupName, action, exception);
        }
    }

    @PreDestroy
    public void stop() {
        if (member != null) {
            member.destroy();
        }
        if (dispatcher != null) {
            dispatcher.destroy();
        }
        if (queue != null) {
            queue.destroy();
        }
        if (transport != null) {
            transport.destroy();
        }
        if (Tibrv.isValid()) {
            try {
                Tibrv.close();
            } catch (TibrvException exception) {
                log.warn("Error while closing Rendezvous", exception);
            }
        }
    }
}
