package com.p2gether.aos.samples;

import com.p2gether.aos.api.RvApiBridge;
import com.tibco.tibrv.TibrvException;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Sample REST gateway endpoint bridging HTTP into Rendezvous: the request is sent to
 * the {@code self} destination (this service's own DQ group), so any member — possibly
 * this instance — handles {@code EQP_STATUS} and the reply is mapped back to HTTP by
 * {@link RvApiBridge}.
 *
 * <p>Endpoints are defined explicitly per use case (never a generic
 * {@code /rv/&#123;command&#125;} proxy, which would expose the whole internal command
 * space).
 */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class EqpApiController {

    private final RvApiBridge bridge;

    /** Requests an equipment-port status change; body-less POST, parameters in the path. */
    @PostMapping("/eqp/{eqpId}/ports/{portId}/status-request")
    public ResponseEntity<Map<String, Object>> requestEqpStatus(
            @PathVariable String eqpId, @PathVariable String portId) throws TibrvException {
        return bridge.call("self", "EQP_STATUS", Map.of("eqpId", eqpId, "portId", portId));
    }
}
