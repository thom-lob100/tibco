-- Sample seed data for local runs (H2 MERGE = idempotent upsert).
-- Remove together with the sample commands in production.
MERGE INTO eqp_master (eqp_id, port_id, status, updated_at) KEY (eqp_id, port_id)
    VALUES ('EQ-4711', 'P-03', 'IDLE', CURRENT_TIMESTAMP);
MERGE INTO eqp_master (eqp_id, port_id, status, updated_at) KEY (eqp_id, port_id)
    VALUES ('EQ-4711', 'P-04', 'RUN', CURRENT_TIMESTAMP);
