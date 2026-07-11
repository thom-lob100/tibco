-- Sample tables and seed data for the demo commands in aos-boot-samples.
-- Runs after aos-boot-app's schema.sql (H2 syntax, idempotent). The production
-- artifact (aos-boot-app -exec jar) does not contain this module.

-- Written by SampleCommands.orderSettle inside the same transaction as the
-- chained NOTIFY_SETTLED submit.
CREATE TABLE IF NOT EXISTS sample_settlement (
    order_id   VARCHAR(100) PRIMARY KEY,
    settled_at TIMESTAMP    NOT NULL
);

-- Equipment master read/updated by EqpCommands.eqpStatus (EQP_STATUS).
CREATE TABLE IF NOT EXISTS eqp_master (
    eqp_id     VARCHAR(100) NOT NULL,
    port_id    VARCHAR(100) NOT NULL,
    status     VARCHAR(20)  NOT NULL,
    updated_at TIMESTAMP    NOT NULL,
    PRIMARY KEY (eqp_id, port_id)
);

MERGE INTO eqp_master (eqp_id, port_id, status, updated_at) KEY (eqp_id, port_id)
    VALUES ('EQ-4711', 'P-03', 'IDLE', CURRENT_TIMESTAMP);
MERGE INTO eqp_master (eqp_id, port_id, status, updated_at) KEY (eqp_id, port_id)
    VALUES ('EQ-4711', 'P-04', 'RUN', CURRENT_TIMESTAMP);
