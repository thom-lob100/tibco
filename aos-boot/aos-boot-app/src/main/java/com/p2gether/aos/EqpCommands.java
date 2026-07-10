package com.p2gether.aos;

import com.p2gether.aos.rv.RvCommand;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Equipment commands called by other systems (e.g. EAP) over subjects like
 * {@code P2.TEST.EAP.<host>.BOOT.EQP_STATUS}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EqpCommands {

    private final JdbcTemplate jdbc;

    public record EqpStatusRequest(String eqpId, String portId) {
    }

    public record EqpStatusReply(String status, String eqpId, String portId, String previousStatus) {
    }

    /**
     * Handles command {@code EQP_STATUS} (derived from the method name): looks up the
     * equipment master by eqpId + portId and moves the port status to {@code REQUEST}.
     * An unknown equipment is a business reply ({@code NOT_FOUND}), not an exception —
     * retrying it could never succeed. Persistent, and naturally idempotent (setting
     * the same status twice yields the same state), so at-least-once redelivery is safe.
     */
    @Transactional(rollbackFor = Exception.class)
    @RvCommand(persistent = true)
    public EqpStatusReply eqpStatus(EqpStatusRequest request) {
        List<String> statuses = jdbc.queryForList(
                "SELECT status FROM eqp_master WHERE eqp_id = ? AND port_id = ?",
                String.class, request.eqpId(), request.portId());
        if (statuses.isEmpty()) {
            log.warn("eqpStatus: unknown equipment {}/{}", request.eqpId(), request.portId());
            return new EqpStatusReply("NOT_FOUND", request.eqpId(), request.portId(), null);
        }
        String previous = statuses.get(0);
        jdbc.update("UPDATE eqp_master SET status = 'REQUEST', updated_at = CURRENT_TIMESTAMP"
                        + " WHERE eqp_id = ? AND port_id = ?",
                request.eqpId(), request.portId());
        log.info("eqpStatus: {}/{} {} -> REQUEST", request.eqpId(), request.portId(), previous);
        return new EqpStatusReply("OK", request.eqpId(), request.portId(), previous);
    }
}
