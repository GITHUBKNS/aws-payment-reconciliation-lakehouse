-- Run statements individually in the dedicated Athena workgroup.
SHOW TABLES IN payment_demo;

SELECT status, COUNT(*) AS records,
       SUM(COALESCE(expected_cents, 0)) / 100.0 AS expected_amount,
       SUM(COALESCE(settled_cents, 0)) / 100.0 AS settled_amount,
       SUM(variance_cents) / 100.0 AS variance_amount
FROM payment_demo.reconciliation GROUP BY status ORDER BY status;

SELECT committed_at, snapshot_id, parent_id, operation
FROM payment_demo."payments$snapshots" ORDER BY committed_at;

SELECT * FROM payment_demo.payments WHERE payment_id = 'P000201';

-- Version travel template: replace FIRST_SNAPSHOT_ID with the actual integer
-- snapshot ID for the initial ingestion in YOUR deployment before executing.
-- SELECT 'BEFORE_DELETE' AS record_state, payment_id, version, operation, amount_cents
-- FROM payment_demo.payments FOR VERSION AS OF FIRST_SNAPSHOT_ID
-- WHERE payment_id = 'P000201'
-- UNION ALL
-- SELECT 'AFTER_DELETE', payment_id, version, operation, amount_cents
-- FROM payment_demo.payments WHERE payment_id = 'P000201' ORDER BY version;
