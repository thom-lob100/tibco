package com.p2gether.aos.scheduler;

import com.p2gether.aos.rv.RvCommand;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Control commands of this service, reached over RV — e.g. from the aos-boot-app
 * REST gateway (the family's single HTTP port) through its {@code sch} destination.
 * In FT mode only the active instance consumes, so the replying member is by
 * definition the active one.
 */
@Component
public class SchedulerCommands {

    public record SchStatusReply(String status, String service, String host,
                                 boolean sampleEnabled, long sampleInterval) {}

    private final boolean sampleEnabled;
    private final long sampleInterval;

    public SchedulerCommands(@Value("${aos.scheduler.sample.enabled:false}") boolean sampleEnabled,
                             @Value("${aos.scheduler.sample.interval:60}") long sampleInterval) {
        this.sampleEnabled = sampleEnabled;
        this.sampleInterval = sampleInterval;
    }

    /** Reports which instance is active and how the sample job is configured. */
    @RvCommand("SCH_STATUS")
    public SchStatusReply schStatus() {
        return new SchStatusReply("OK", "SCH", hostName(), sampleEnabled, sampleInterval);
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException exception) {
            return "unknown";
        }
    }
}
