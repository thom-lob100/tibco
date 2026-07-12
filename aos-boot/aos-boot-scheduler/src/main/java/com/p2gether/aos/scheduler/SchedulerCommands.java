package com.p2gether.aos.scheduler;

import com.p2gether.aos.rv.RvCommand;
import java.net.InetAddress;
import java.net.UnknownHostException;
import lombok.RequiredArgsConstructor;

/**
 * Control commands of this role, reached over RV — e.g. from the BOOT role's REST
 * gateway (the family's single HTTP port) through its {@code sch} destination.
 * In FT mode only the active instance consumes, so the replying member is by
 * definition the active one. Registered by {@link SchedulerConfiguration} only in
 * the SCH role.
 */
@RequiredArgsConstructor
public class SchedulerCommands {

    public record SchStatusReply(String status, String service, String host,
                                 boolean sampleEnabled, long sampleInterval) {}

    private final boolean sampleEnabled;
    private final long sampleInterval;

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
