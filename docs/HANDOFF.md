# Team handoff: ingestion contract v1

## What this repository implements

EC2 continuously replays a CSV and publishes JSON to Kinesis Data Streams.
CloudFormation creates networking, EC2, its instance role (or uses a lab profile),
S3 staging and one provisioned Kinesis shard. A systemd service survives logout,
restarts after failure and starts after EC2 reboot. No downstream services are deployed.

## Who should do what

- Nikhila: producer, stream, replay controls, event schema, ingestion evidence.
- Model owner: fit on fraudTrain, validation within training data, preprocessing artifact,
  feature contract agreement, deploy SageMaker endpoint and document request/response JSON.
- Backend owner: Kinesis-triggered Lambda -> SageMaker Runtime -> DynamoDB -> SNS.
- Frontend/API owner: authenticated read API through API Gateway -> read Lambda -> DynamoDB;
  dashboard calls the API, never embeds AWS access keys.

SageMaker returns a score. It does not autonomously mark the database: the backend
Lambda must invoke the endpoint, interpret its response, apply the agreed threshold,
and write predicted results to DynamoDB. SNS is a side effect managed by the backend.

## Read the Kinesis event

See examples/event.json and examples/lambda_event.json. Lambda gets a BASE64 string:

```python
import base64
import json

for record in event['Records']:
    transaction = json.loads(base64.b64decode(record['kinesis']['data']))
    assert transaction['schema_version'] == '1.0'
    model_inputs = transaction['features']  # only these fields go to the endpoint
    evaluation_label = transaction['ground_truth']['is_fraud']
    # evaluation_label is NEVER sent to the model or used to trigger an alert.
```

## Model request agreement

The producer supplies this exact feature set (not a universal pretrained-model format):

| Feature | Type | Meaning |
|---|---|---|
| amount | float | Source amt; currency is not specified in the CSV |
| category | string | Source merchant category |
| merchant | string | Source merchant name |
| state | string | Source state |
| city_population | int | Source city_pop |
| customer_lat, customer_lon | float | Source customer coordinates |
| merchant_lat, merchant_lon | float | Source merchant coordinates |
| age_at_transaction | int | Age derived using source transaction date and dob |
| hour | int | 0..23 from original timestamp |
| weekday | int | Monday=0 .. Sunday=6 from original timestamp |
| month | int | 1..12 from original timestamp |

Use src/producer.py::row_to_features in training too. Model owner must fit encoders,
scalers and other preprocessing on training/validation data only, save the complete
pipeline and use that identical pipeline in inference. Handle unseen categories.
If another feature set is necessary, agree a new feature_version before merging.
Do not use a generic pretrained model that expects unrelated anonymized V1..V28 fields.
A model file is not included here; ML training/deployment is a teammate dependency.

## Identity and time semantics

- event_id: idempotency key for one row in one replay cycle. Stable on producer retry.
- replay_id: persists in local checkpoint across service/instance restarts on the same disk.
- replay_cycle: increases at end of file so a new replay is a NEW simulated transaction.
- source_transaction_id: original trans_num. Repeats across cycles; don't use alone as DB key.
- account_id: deterministic SHA-256 prefix of cc_num. A demo pseudonym, NOT PCI tokenization.
- source_timestamp: original naive timestamp. Dataset has no declared timezone.
- emitted_at: real current UTC publish time. Use for dashboard arrival age.
- source_row: zero-based position in the CSV. Needed when presentation sampling changes order.

Source timestamps drive model calendar features. Do not replace them with current
clock features unless retraining for that distribution. This is historical replay,
not a live payment authorization feed and not a real-time transaction blocker.

## Reliability requirements

Producer retries individual PutRecords failures. A transport timeout can produce
ambiguous success and duplicates. It saves a checkpoint only after the entire batch
is acknowledged. Crash after publish but before checkpoint can replay that batch.
If EC2 is replaced/deleted, its local checkpoint disappears and a new replay_id begins.
Data is not backed up by this producer. Kinesis retains events for 24 hours in this demo.

PutRecords does NOT guarantee ordering. Account partitioning does not fix failed-record
retry ordering. Stateful velocity features require a deliberate event-time/order design.
For this starter the inference feature set is per-transaction and stateless.

Kinesis-triggered Lambda also processes at least once. Use conditional/idempotent DB
writes keyed by event_id; make alert delivery retry-safe too. Avoid a naive
'write DB then send SNS' implementation that loses an alert on retry or sends duplicates.
Use an explicit alert state/outbox pattern; SNS standard delivery can duplicate.
Configure ReportBatchItemFailures, bounded retries/record age and a failure destination.

## Suggested downstream result contract (to agree with teammates)

DynamoDB: event_id partition key plus replay_id, emitted_at, source_timestamp,
account_id, features, ground_truth (evaluation only), fraud_score (number),
predicted_fraud (boolean), model_version, threshold and scored_at. Store a distinct
PENDING/ERROR state if inference fails; never label an inference failure as non-fraud.
Suggested endpoint response: {"fraud_score": 0.93, "model_version": "v1"}.
Neither this response nor DB table is implemented by this repository.

SNS: start with confirmed EMAIL subscribers; use SMS only if the team's account,
region, sandbox and cost constraints permit. Ground-truth labels must never trigger alerts.
API: choose pagination and authorization; dashboard polls the read API or uses an
explicit push design. API Gateway by itself does not read DynamoDB.

## Same account versus separate accounts

Recommended: ONE shared stack in the team's approved AWS account and region.
Share StreamName, StreamArn, Region, schema and fixtures (not credentials).
Each teammate uses their own authorized AWS identity. Producer IAM role cannot read
Kinesis, invoke SageMaker, or write DynamoDB by design.

A teammate with Kinesis read permission can clone the repo, install dependencies and run
verify_stream.py --stream NAME --region REGION against that shared stream.
They must not deploy another producer unless an independent stack is intended.
Cross-account consumers need explicit IAM/resource-policy work; cloning is not permission.

## Model evaluation warning

Original test data: 555,719 rows, 2,145 fraud labels (0.3860%). An all-nonfraud model
would already have 99.6140% accuracy. Report precision, recall, PR-AUC, confusion matrix
and operational latency, and select a threshold using validation, not the test labels.
Use untouched fraudTest for final evaluation. Repeated online tuning from test labels
invalidates the holdout. The optional 10%-fraud demo changes prevalence and order;
never report it as unbiased performance. Dataset provenance/license must be confirmed
from the team's original download source before redistributing or claiming real-bank data.
