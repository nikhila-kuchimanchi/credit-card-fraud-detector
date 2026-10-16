# Local validation, 2026-10-16

Authoring runtime: Python 3.12. No AWS credentials were used and no AWS resources
were created. Target deployment uses Amazon Linux 2023 with Python 3.11.

Passed:
- 9 standard-library unittest tests: label/identity exclusion, stable/replay-unique
  event IDs, partial-batch retry, transport retry with unchanged payload, bounded
  failure, invalid money rejection, atomic checkpoint write, CLI checkpoint resume,
  and selected infrastructure settings.
- Parsed/converted all 555,719 fraudTest rows into v1 events without validation errors.
- Inspected both source CSVs for header, row/label counts and timestamp range.
- Python syntax compilation for project scripts.
- bash -n for the generated EC2 bootstrap script.

The first known-fraud source row is index 1,685 (zero-based). At 5 records/sec it
appears after approximately 337 seconds plus setup/network overhead, if starting at
row zero. A known label does not guarantee a model prediction.

Not performed here:
- AWS CloudFormation service-side validation or stack deployment.
- Real IAM permissions, package installation on AL2023, S3 downloads, SSM registration,
  live Kinesis write/read, or AWS quota checks.
- Model training/accuracy, SageMaker deployment, Lambda event source mappings,
  persistence, notifications, API or dashboard tests.

Run scripts/manage.py deploy in the authorized AWS account; it invokes service-side
CloudFormation validation before deployment. Then inspect logs and run verify_stream.py.
A successful infrastructure deployment alone is not end-to-end pipeline verification.
