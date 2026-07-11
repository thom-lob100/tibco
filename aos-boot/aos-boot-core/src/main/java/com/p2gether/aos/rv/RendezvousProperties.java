package com.p2gether.aos.rv;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Rendezvous connection parameters bound from {@code aos.rendezvous.*} in application.yml. */
@Getter
@Setter
@ConfigurationProperties(prefix = "aos.rendezvous")
public class RendezvousProperties {

    private String service;
    private String network;
    private String daemon;

    /**
     * Sender element stamped on outbound subjects; null auto-detects the machine name
     * (per team convention the sender identifies the sending host). Override per
     * instance with {@code --aos.rendezvous.sender-name=...} when several instances
     * share a host.
     */
    private String senderName;

    private final Subject subject = new Subject();
    private final Dq dq = new Dq();
    private final Ft ft = new Ft();
    private final Request request = new Request();
    private final HandlerRetry handlerRetry = new HandlerRetry();
    private final PersistentQueue persistentQueue = new PersistentQueue();

    /** Outbound targets keyed by a short name, e.g. {@code destinations.messo.listener=MESSO}. */
    private final Map<String, Destination> destinations = new LinkedHashMap<>();

    /**
     * Fault-tolerance (active/standby) mode: instances sharing the group name elect
     * the highest-weight {@code active-goal} members as active consumers; the rest
     * stand by and take over when an active member dies. Off by default — plain DQ
     * mode (all members consuming, load-balanced) is the normal deployment.
     */
    @Getter
    @Setter
    public static class Ft {

        private boolean enabled = false;
        /** FT group name; null derives {@code AOS.<listener>.FT}. */
        private String name;
        /** Higher weight is preferred as the active member; override per instance. */
        private int weight = 1;
        /** How many members should be active at once. */
        private int activeGoal = 1;
        private double heartbeat = 1.5;
        private double preparation = 0.0;
        private double activation = 4.8;
    }

    /** FT group name, derived from the listener element unless set explicitly. */
    public String ftName() {
        return ft.getName() != null ? ft.getName() : "AOS." + subject.getListener() + ".FT";
    }

    /**
     * Database-backed reprocessing queue for {@code @RvCommand(persistent = true)}
     * commands ({@code rv_command_queue} table): failed messages are retried from the
     * database and survive JVM restarts.
     */
    @Getter
    @Setter
    public static class PersistentQueue {

        /** Disables the queue bean entirely; persistent commands then fall back to in-memory retry. */
        private boolean enabled = true;
        /** Seconds between polls for due PENDING rows. */
        private double pollInterval = 5.0;
        /** Total attempts (including the inline first one) before a row is marked FAILED. */
        private int maxAttempts = 5;
        /** Seconds until a failed row becomes due again. */
        private double backoff = 10.0;
        /** Rows claimed per poll. */
        private int batchSize = 10;
        /** Seconds after which a PROCESSING row (crashed worker) is reclaimed. */
        private double processingTimeout = 300.0;
    }

    /** Outbound request/reply behaviour: per-attempt timeout and retries on timeout. */
    @Getter
    @Setter
    public static class Request {

        /** Seconds to wait for a reply, per attempt. */
        private double timeout = 5.0;
        /** Additional attempts after the first one times out (0 disables retry). */
        private int retries = 2;
        /** Seconds to pause between attempts. */
        private double backoff = 0.5;
    }

    /**
     * Inbound {@code @RvCommand} handler retry policy when a handler throws. The first
     * attempt runs in the DQ callback; retries run asynchronously on a dedicated pool
     * so the member keeps consuming new messages while a failed one is retried.
     */
    @Getter
    @Setter
    public static class HandlerRetry {

        /** Additional attempts after the first failure (0 disables retry). */
        private int retries = 2;
        /** Seconds between attempts (scheduler delay, does not block the DQ callback). */
        private double backoff = 0.5;
        /** Threads in the async retry pool. */
        private int threads = 2;
    }

    public String resolvedSenderName() {
        return senderName != null ? senderName : HostName.VALUE;
    }

    /**
     * The six dot-separated subject elements; {@link #value()} joins them into the
     * Rendezvous subject, e.g. {@code P2.TEST.*.*.BOOT.*}. When {@code local} is true
     * the reserved {@code _LOCAL} element is prepended, restricting delivery to the
     * local rvd daemon (never forwarded to the network).
     */
    @Getter
    @Setter
    public static class Subject {

        private boolean local = false;
        private String factory = "P2";
        private String environment = "TEST";
        private String sendSystem = "*";
        private String sender = "*";
        private String listener = "BOOT";
        private String command = "*";

        public String value() {
            String subject = String.join(".", factory, environment, sendSystem, sender, listener, command);
            return local ? "_LOCAL." + subject : subject;
        }
    }

    /**
     * A predefined outbound target (another TIBCO service such as BIZ). Connection
     * values left null fall back to this application's own {@code service} /
     * {@code network} / {@code daemon}.
     */
    @Getter
    @Setter
    public static class Destination {

        private String service;
        private String network;
        private String daemon;

        /** The target service's listener element, e.g. {@code MESSO}. */
        private String listener;
        /** Send-system element; null means this application's own listener element (e.g. BOOT). */
        private String sendSystem;
        /** Sender element; null means {@code aos.rendezvous.sender-name} / the machine name. */
        private String sender;
    }

    /** Returns the destination registered under {@code name}, or throws if undefined. */
    public Destination destination(String name) {
        Destination destination = destinations.get(name);
        if (destination == null) {
            throw new IllegalArgumentException(
                    "Unknown Rendezvous destination '" + name + "'; define it under aos.rendezvous.destinations");
        }
        return destination;
    }

    /**
     * Composes the outbound subject for a destination:
     * {@code factory.environment.<send-system>.<sender>.<target listener>.<command>},
     * sharing this application's factory / environment / {@code _LOCAL} settings so the
     * call automatically follows the active profile. Send-system defaults to this
     * application's listener element (the calling service), sender to the machine name.
     */
    public String subjectFor(String destinationName, String command) {
        Destination destination = destination(destinationName);
        String sendSystem = destination.getSendSystem() != null
                ? destination.getSendSystem() : subject.getListener();
        String sender = destination.getSender() != null
                ? destination.getSender() : resolvedSenderName();
        String joined = String.join(".",
                subject.getFactory(), subject.getEnvironment(),
                sendSystem, sender, destination.getListener(), command);
        return subject.isLocal() ? "_LOCAL." + joined : joined;
    }

    /** Lazily detected machine name, sanitized to a single valid subject element. */
    private static final class HostName {

        private static final String VALUE = detect();

        private static String detect() {
            String name = System.getenv("COMPUTERNAME");
            if (name == null || name.isBlank()) {
                name = System.getenv("HOSTNAME");
            }
            if (name == null || name.isBlank()) {
                try {
                    name = java.net.InetAddress.getLocalHost().getHostName();
                } catch (java.net.UnknownHostException unknown) {
                    name = "UNKNOWN";
                }
            }
            int domainStart = name.indexOf('.');
            if (domainStart > 0) {
                name = name.substring(0, domainStart);
            }
            return name.replaceAll("[^A-Za-z0-9_-]", "_");
        }
    }

    /** Distributed-queue membership settings; every instance of the group must share {@code name}. */
    @Getter
    @Setter
    public static class Dq {

        private String name;
        private int workerWeight = 1;
        private int workerTasks = 1;
        private int schedulerWeight = 1;
        private double schedulerHeartbeat = 1.0;
        private double schedulerActivation = 3.5;
    }
}
