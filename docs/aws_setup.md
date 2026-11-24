# Deploy to AWS after local tests pass

## 1. Account and spending controls

Use a personal AWS sandbox, never an employer account. Check Billing > Free Tier/credits and supported services for your account plan before running. Use AWS Budgets with a low alert threshold (alerts are not a hard cap). Athena, S3 and Glue catalog usage may be billable depending on plan and credits. No paid Spark cluster or managed Airflow is required. Do not upgrade an account solely for this lab without reviewing charges.

## 2. Prepare resources in the same supported region (e.g. us-east-1)

- S3 > Create bucket: choose a unique general-purpose bucket name, keep all public access blocked, ACLs disabled, and SSE-S3 encryption enabled. Use it only for this demo.
- Athena > Workgroups: select or create a workgroup using Athena SQL engine version 3. Set the query-results location to `s3://YOUR_BUCKET/athena-results/` or allow the client to supply it; choose SSE-S3 and optional per-query bytes scanned limits. Keep the same region as your bucket. Do not enable provisioned capacity.
- Use an authorized IAM role/user session with S3 list/get/put and delete where needed for Iceberg, Athena start/get/stop/query results and Glue catalog create/read/update for the demo database/tables, plus view permissions; restrict scope to the project bucket/prefix and workgroup/catalog where supported. Lake Formation governance can require additional grants. Avoid root credentials and never paste access keys into notebooks or the repository. Prefer IAM Identity Center or an existing temporary AWS session.

## 3. Execute

Option A: AWS CloudShell in the same region (if available to your account), upload the ZIP through Actions > Upload file. Unzip it, `cd payment_lakehouse`, then check `python3 -c 'import boto3'`. Install boto3 if needed (`python3 -m pip install --user boto3`). Check `aws sts get-caller-identity` privately; do not publish its output.

Option B: a local terminal with Python and AWS CLI configured using a permitted session, e.g. `aws configure sso` then `aws sso login --profile YOUR_PROFILE`; set AWS_PROFILE to that profile for the Python process. Install boto3 with `python -m pip install -r requirements-aws.txt`.

From project root, replace YOUR_BUCKET with your real bucket name and use your actual region/workgroup. This command creates catalog metadata and writes S3 objects; it issues billable AWS queries if your plan/credits do not cover them.

```sh
python3 src/aws_runner.py --bucket YOUR_BUCKET --region us-east-1 --database payment_demo --prefix payment-demo --workgroup payment-lakehouse-wg
```

Use a fresh dedicated database and prefix for the first full demo. If a run fails after partially applying data, inspect the local `output/aws_run_*.json` or S3 audit report and Athena query history. Correct the error, then repeat the same command with `--resume` (only on the same dedicated database/prefix and unchanged fixture). It replays both batches, checks the final reconciliation, and verifies zero changed keys on the last replay; it does not claim cross-table atomicity. Never run simultaneous writers. If setup fails due to permissions or unsupported account features, stop and diagnose rather than disabling bucket security.

## 4. Verify independently in Athena

Select the dedicated `payment-lakehouse-wg` workgroup, `AwsDataCatalog` and database `payment_demo`. Run:

```sql
SELECT status, count(*) AS records,
       sum(coalesce(expected_cents,0)) AS expected_cents,
       sum(coalesce(settled_cents,0)) AS settled_cents,
       sum(variance_cents) AS variance_cents
FROM reconciliation GROUP BY status ORDER BY status;
```

```sql
SELECT * FROM payments WHERE payment_id = 'P000201';
SELECT * FROM reconciliation WHERE status <> 'MATCHED' ORDER BY payment_id;
SELECT * FROM "payments$snapshots" ORDER BY committed_at DESC;
```

Expect 9,970 matched, 10 mismatches, 20 orphan settlements; P000201 should retain version 2 and operation D. The final runner line should be `PASS: AWS results match local reference; replay changes zero keys.` Capture your own query results, Iceberg table metadata/snapshots, and replay audit, redacting identifiers and bucket names before publishing. Local results are not evidence of AWS execution.

## 5. Clean up after collecting evidence

Export sanitized results first. In the dedicated demo database only, drop the reconciliation view, external stage tables and Iceberg state tables when no longer needed (Athena DROP TABLE on Iceberg can delete table data). Delete the dedicated database after its contents are removed. Delete the demo S3 objects and bucket if unused; if bucket versioning is enabled, remove object versions/delete markers too. Remove dedicated workgroup/resources if created, and review Billing. Never delete shared or production resources. AWS billing data may lag.

For the validated deployment, the workgroup overrode client-side settings, the scan limit was 100 MB, and budget alerts were configured at $1. The original runner requests its own output location, but the workgroup override takes precedence. See [cleanup checklist](cleanup.md) before removing resources.
