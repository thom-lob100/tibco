package com.p2gether.aos.api;

import com.p2gether.aos.rv.RendezvousPublisher;
import com.p2gether.aos.rv.RvMessages;
import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvMsg;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

/**
 * REST → RV bridge shared by every gateway controller. aos-boot-app is the single
 * HTTP entry point for the whole service family: child services stay headless,
 * expose their operations as {@code @RvCommand} handlers, and a controller here
 * bridges each use case to the service's destination.
 *
 * <p>Uses {@link RendezvousPublisher#requestOnce} — one attempt with a short timeout
 * ({@code aos.api.rv-timeout}) — so Tomcat threads never wait out the RV retry
 * policy, and maps the reply convention to HTTP: OK 200, QUEUED 202, NOT_FOUND 404,
 * ERROR 502, timeout 504.
 */
@Component
public class RvApiBridge {

    private final RendezvousPublisher publisher;
    private final double rvTimeoutSeconds;

    public RvApiBridge(RendezvousPublisher publisher,
                       @Value("${aos.api.rv-timeout:2.0}") double rvTimeoutSeconds) {
        this.publisher = publisher;
        this.rvTimeoutSeconds = rvTimeoutSeconds;
    }

    /** Sends one command to a destination and maps the reply to an HTTP response. */
    public ResponseEntity<Map<String, Object>> call(String destinationName, String command, Object payload)
            throws TibrvException {
        TibrvMsg reply = publisher.requestOnce(destinationName, command, payload, rvTimeoutSeconds);
        if (reply == null) {
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                    .body(Map.of("status", "TIMEOUT"));
        }
        Map<String, Object> body = RvMessages.toMap(reply);
        HttpStatus status = switch (String.valueOf(body.get("status"))) {
            case "QUEUED" -> HttpStatus.ACCEPTED;
            case "NOT_FOUND" -> HttpStatus.NOT_FOUND;
            case "ERROR" -> HttpStatus.BAD_GATEWAY;
            default -> HttpStatus.OK;
        };
        return ResponseEntity.status(status).body(body);
    }
}
