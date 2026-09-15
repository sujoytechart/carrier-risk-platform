-- Run once in the explicitly selected disposable database before any writers.
-- IF NOT EXISTS preserves an existing claim; never CREATE OR REPLACE this table.
CREATE SCHEMA IF NOT EXISTS CARRIER_RISK_CONTROL;

CREATE TABLE IF NOT EXISTS CARRIER_RISK_CONTROL.BUILD_GUARD AS
SELECT
    1::INTEGER AS singleton,
    NULL::VARCHAR AS owner_run,
    0::INTEGER AS revision;
