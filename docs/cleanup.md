# Cleanup checklist (only after evidence is saved)

Use the original dedicated project resources in us-east-1. Verify the database, bucket and workgroup names before deleting anything. Do not delete shared resources or the default `primary` workgroup. Cleanup is optional but recommended when the lab is finished to avoid ongoing S3 storage charges.

1. Download and verify all desired screenshots and the audit report. Keep private originals offline; publish sanitized copies only.
2. Using an authorized identity with deletion rights (the project IAM policy did not grant Glue deletion or workgroup deletion), select the dedicated workgroup in Athena. Drop the `payment_demo.reconciliation` view, then the four external staging tables, then the two Iceberg tables. Athena DROP TABLE for Iceberg deletes the underlying table data; do not do this until done with time travel. Run each statement separately, confirm success and then drop the empty `payment_demo` database.
3. In S3, verify that the dedicated bucket is not used by anything else, then empty its remaining objects (including raw, staged, quarantine, manifests, audit and query-result prefixes). If versioning was enabled later, remove prior versions and delete markers as well. Delete the empty dedicated bucket if no longer needed.
4. Delete the dedicated Athena workgroup after queries finish, leaving the default workgroup unchanged. Review any CloudWatch alarms or other resources created for this lab.
5. If the IAM user is no longer needed, remove its project permissions and console access, unassign its MFA device and delete the user using an authorized administrative identity. Do not delete the root MFA device. Keep the budget while verifying final charges.
6. Review AWS Billing after usage has posted. Budget alerts are not spending caps. Sign out of administrative sessions.

Official references: [Iceberg DROP TABLE](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-drop-table.html), [empty a bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/empty-bucket.html).
