package com.p2gether.aos.samples;

import com.p2gether.aos.rv.RendezvousPublisher;
import com.p2gether.aos.rv.RvMessages;
import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvMsg;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Sample REST gateway endpoint bridging HTTP into Rendezvous: the request is sent to
 * the {@code self} destination (this service's own DQ group), so any member — possibly
 * this instance — handles {@code EQP_STATUS} and the reply is mapped back to HTTP.
 *
 * <p>Endpoints are defined explicitly per use case (never a generic
 * {@code /rv/&#123;command&#125;} proxy, which would expose the whole internal command
 * space), and the bridge uses {@link RendezvousPublisher#requestOnce} — one attempt
 * with a short timeout — so Tomcat threads never wait out the retry policy.
 */
@RestController
@RequestMapping("/api")
public class EqpApiController {

    private final RendezvousPublisher publisher;
    private final double rvTimeoutSeconds;

    public EqpApiController(RendezvousPublisher publisher,
                            @Value("${aos.api.rv-timeout:2.0}") double rvTimeoutSeconds) {
        this.publisher = publisher;
        this.rvTimeoutSeconds = rvTimeoutSeconds;
    }

    /** Requests an equipment-port status change; body-less POST, parameters in the path. */
    @PostMapping("/eqp/{eqpId}/ports/{portId}/status-request")
    public ResponseEntity<Map<String, Object>> requestEqpStatus(
            @PathVariable String eqpId, @PathVariable String portId) throws TibrvException {
        TibrvMsg reply = publisher.requestOnce("self", "EQP_STATUS",
                Map.of("eqpId", eqpId, "portId", portId), rvTimeoutSeconds);
        return toHttp(reply);
    }

    /** RV reply convention → HTTP: OK 200, QUEUED 202, NOT_FOUND 404, ERROR 502, timeout 504. */
    private static ResponseEntity<Map<String, Object>> toHttp(TibrvMsg reply) throws TibrvException {
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
