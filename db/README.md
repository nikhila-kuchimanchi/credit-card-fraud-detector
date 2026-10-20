# DB part: DynamoDB storage for scored transactions

Owners: Eunsoo, Mario. Input contract: `docs/event_schema.json` (v1).

## Deployed table

| Item | Value |
|---|---|
| Table | `FraudTransactions` |
| ARN | `arn:aws:dynamodb:us-east-1:658980432945:table/FraudTransactions` |
| Region | `us-east-1` |
| Billing | On-demand (PAY_PER_REQUEST) |
| TTL | `expires_at` = `emitted_at` + 30 days (items auto-delete; change `DEFAULT_TTL_DAYS` in `db/schema.py`) |

## Keys and indexes

| Name | Partition key | Sort key | Used for |
|---|---|---|---|
| Table | `event_id` | – | Get / idempotent write of one transaction |
| GSI `by-day` | `emitted_date` (YYYY-MM-DD, UTC) | `emitted_at` | Dashboard: newest transactions today |
| GSI `flagged` (sparse) | `flag_status` = `FLAGGED` | `emitted_at` | Dashboard: newest suspicious transactions |

`flag_status` is written only when `predicted_fraud` is true, so the `flagged`
index contains only suspicious items (cheap to query, no scan needed).
`event_id` is the key, not `source_transaction_id`, because the latter repeats
across replay cycles.

## Item lifecycle

```
put_pending      -> status=PENDING, alert_status=NONE
record_score     -> status=SCORED, fraud_score, predicted_fraud, threshold, model_version
                    (+ flag_status=FLAGGED, alert_status=PENDING if fraud)
record_error     -> status=ERROR, error_message, attempts += 1  (never "non-fraud")
mark_alert_sent  -> alert_status=SENT, alert_message_id
```

All writes are conditional, so a retried Kinesis batch cannot create duplicates,
overwrite a score, or mark an alert twice. `ground_truth` is stored for evaluation
only and must never trigger alerts.

## Code

| File | Purpose |
|---|---|
| `db/schema.py` | Table/index definition and constants |
| `db/repository.py` | Write path (scoring Lambda) and read path (API Lambda) |
| `db/create_table.py` | Create/delete the table: `python -m db.create_table --region us-east-1` |
| `tests/test_db.py` | Offline tests using `examples/event.json` |

Scoring Lambda usage sketch:

```python
import boto3
from db import repository as repo

ddb = boto3.client("dynamodb")
repo.put_pending(ddb, transaction)                      # False = duplicate, safe to continue
updated, flagged = repo.record_score(ddb, transaction["event_id"], score, THRESHOLD, "v1")
if updated and flagged:
    msg_id = sns.publish(...)["MessageId"]
    repo.mark_alert_sent(ddb, transaction["event_id"], msg_id)
```

Read API usage: `repo.list_recent(ddb, day="2026-10-20", limit=50)` and
`repo.list_flagged(ddb, limit=50)`; both return `(items, next_key)` for pagination.

## Verified

- Offline: `python -m unittest discover -s tests -v` (23 tests incl. producer).
- Live (2026-10-19): put, duplicate put rejected, score, duplicate score rejected,
  alert sent, duplicate alert rejected, both GSIs return the item, TTL enabled.
  Test item deleted afterwards.

## TODO

- [ ] Mario: IAM policies as JSON in `db/iam/` (writer Lambda: PutItem/UpdateItem on
      this table; reader Lambda: GetItem/Query on table and indexes). Eunsoo applies.
- [ ] Agree threshold and endpoint response format with model owner.
- [ ] Alerting is at-least-once: if SNS publish succeeds but `mark_alert_sent` fails,
      a retry can send one duplicate email (acceptable for the demo).

## Try it with the fake 20-row CSV

```bash
python -m db.load_sample --region us-east-1           # 20 items: 18 scored (3 flagged), 1 ERROR, 1 PENDING
python -m db.load_sample --region us-east-1 --delete  # clean up
```
Scores are random placeholders (`model_version = demo-fake-v0`), not model output.
