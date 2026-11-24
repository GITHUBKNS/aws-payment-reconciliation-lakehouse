# Observed execution results — November 24, 2026

## Evidence and scope

A live AWS deployment in us-east-1 completed successfully. The sanitized audit is derived from the uploaded run JSON; original query UUIDs were replaced with sequential query numbers and the S3 bucket name was replaced with `YOUR_BUCKET`. All other recorded metrics, SQL logic and summaries are preserved. Manual Athena verification results below are from the console screenshots; they are not part of the 31-statement runner log. Local test results are from the user's terminal output. All data is synthetic.

## Data processing

| Metric | Batch 1 | Batch 2 | Batch 2 replay |
|---|---:|---:|---:|
| Raw events | 20,000 | 247 | 247 |
| Invalid events | 0 | 2 | 2 |
| Exact duplicates | 0 | 2 | 2 |
| Superseded in batch | 0 | 3 | 3 |
| Candidate keys | 20,000 | 240 | 240 |
| Changed keys | 20,000 | 240 | 0 |

Replay input is not counted again in the 20,247 distinct-batch input total. The two streams are independently versioned. Changed keys are pre-MERGE candidate counts checked against the local reference, not Athena's physical write count. The replay still executed two MERGE statements, but no business keys qualified for change.

## Financial reconciliation

| Status | Initial | Incremental | Replay |
|---|---:|---:|---:|
| MATCHED | 9,980 | 9,970 | 9,970 |
| MISSING_SETTLEMENT | 10 | 0 | 0 |
| AMOUNT_MISMATCH | 10 | 10 | 10 |
| ORPHAN_SETTLEMENT | 10 | 20 | 20 |

| Final status | Records | Expected USD | Settled USD | Variance USD |
|---|---:|---:|---:|---:|
| AMOUNT_MISMATCH | 10 | 773.35 | 823.35 | 50.00 |
| MATCHED | 9,970 | 2,466,548.95 | 2,466,548.95 | 0.00 |
| ORPHAN_SETTLEMENT | 20 | 0.00 | 947.35 | 947.35 |
| Total | 10,000 | 2,467,322.30 | 2,468,319.65 | 997.35 |

Variance means settled minus expected; a positive value is not necessarily a financial loss. Internal money values use integer cents.

## Query-level metrics

| Metric | Value |
|---|---:|
| Successful statements | 31 of 31 |
| CREATE statements (including repeated IF NOT EXISTS) | 10 |
| SELECT statements | 15 |
| MERGE statements | 6 |
| Total bytes scanned | 1,521,833 |
| Cumulative engine time | 30.109 seconds |
| Mean engine time | 971.26 ms |
| Minimum engine time | 248 ms |
| Maximum engine time | 2,653 ms |
| Maximum individual scan | 188,097 bytes |

Engine time excludes orchestration overhead, polling and queue time; not a wall-clock or scale benchmark.

## Independent Athena verification

| Query | Result | Console runtime | Displayed scan |
|---|---|---:|---:|
| SHOW TABLES | 6 tables, 1 view | 148 ms | not displayed |
| Reconciliation by status | 3 categories, totals above | 1.34 s | 127.99 KB |
| Payment snapshot history | 2 committed snapshots, linked parent | 617 ms | 1.38 KB |
| Historical vs current P000201 | version 1 U 8437 cents; version 2 D 0 cents | 871 ms | 62.12 KB |

The two snapshots are consistent with the zero-change replay. Snapshot IDs vary across deployments; use your own first snapshot ID for the version-travel query. No restore or cross-table atomicity was demonstrated.

## Tests

Seven tests passed on Windows (0.714 s) and in CloudShell under the dedicated IAM user (1.279 s). Tests cover baseline, batch immutability, two conflict scenarios, incremental/replay behavior, stale-event protection, and local rollback/recovery. These are local Python/SQLite tests; cloud parity was checked separately by the runner. Hosted GitHub Actions execution is not yet verified.

## Estimated query cost

Assuming standard Athena on-demand SQL at $5/TB and a 10 MB minimum for applicable statements, 19 nonzero-scan queries at the minimum produce a decimal-unit estimate of $0.00095. Allowing for the other two zero-scan SELECTs yields $0.00105. Ten CREATE/DDL statements are excluded from the scan-charge estimate. This is roughly $0.001 for the recorded deployment, not an invoice. Additional manual queries, S3 storage/requests, Glue catalog charges and other AWS services are excluded. AWS billing may lag. Check the account's billing console for actual charges.

References: [Athena pricing](https://aws.amazon.com/athena/pricing/), [AWS Athena pricing explanation](https://docs.aws.amazon.com/whitepapers/latest/big-data-analytics-options/amazon-athena.html), [MERGE](https://docs.aws.amazon.com/athena/latest/ug/merge-into-statement.html), [version travel](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-time-travel-and-version-travel-queries.html).
