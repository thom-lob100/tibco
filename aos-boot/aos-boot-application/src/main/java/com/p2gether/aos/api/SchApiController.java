package com.p2gether.aos.api;

import com.tibco.tibrv.TibrvException;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Gateway endpoints for the scheduler service (aos-boot-scheduler, listener
 * {@code SCH}): the scheduler itself is headless — its control commands are
 * {@code @RvCommand} handlers reached through the {@code sch} destination, and this
 * app's single HTTP port fronts them. In FT mode only the active scheduler instance
 * consumes, so whichever member replies is the active one.
 */
@RestController
@RequestMapping("/api/sch")
@RequiredArgsConstructor
public class SchApiController {

    private final RvApiBridge bridge;

    /** Status of the FT-active scheduler instance (host, sample-job settings). */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() throws TibrvException {
        return bridge.call("sch", "SCH_STATUS", null);
    }
}
