"""Kinesis-triggered writer Lambda: score each transaction and store it.

Flow per record: decode -> put_pending -> score -> record_score
                 -> (if flagged and SNS configured) begin_alert -> publish -> mark_alert_sent
Kinesis delivery is at-least-once; every DB write is conditional (db/repository.py),
so a retried batch never duplicates or overwrites results.
Transient failures are returned in batchItemFailures so only those records retry.
Malformed records are logged and skipped (retrying them can never succeed).
"""

import base64
import json
import logging
import os

import scorer
from db import repository as repo

log = logging.getLogger()
log.setLevel(logging.INFO)

TABLE = os.environ.get("TABLE_NAME", "FraudTransactions")
THRESHOLD = float(os.environ.get("THRESHOLD", "0.5"))
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")

_ddb = None
_sns = None


def _clients():
    global _ddb, _sns
    if _ddb is None:
        import boto3
        _ddb = boto3.client("dynamodb")
        _sns = boto3.client("sns") if ALERT_TOPIC_ARN else None
    return _ddb, _sns


def _alert_message(tx, fraud_score):
    f = tx["features"]
    return (
        "Suspicious transaction detected (simulated replay data)\n\n"
        f"event_id: {tx['event_id']}\n"
        f"fraud_score: {fraud_score:.3f} (threshold {THRESHOLD})\n"
        f"amount: {f['amount']}  category: {f['category']}  merchant: {f['merchant']}\n"
        f"state: {f['state']}  source_timestamp: {tx['source_timestamp']}\n"
        f"account_id (pseudonym): {tx['account_id']}\n"
    )


def process(tx, ddb, sns):
    """Store and score one transaction. Returns a short outcome label."""
    event_id = tx["event_id"]
    repo.put_pending(ddb, tx, table=TABLE)  # False on redelivery; continue anyway
    try:
        fraud_score, model_version = scorer.score(tx["features"])
    except Exception as exc:  # inference failure is never treated as non-fraud
        log.warning("scoring failed for %s: %s", event_id, exc)
        repo.record_error(ddb, event_id, f"scoring failed: {exc}", table=TABLE)
        return "error"
    _, flagged = repo.record_score(ddb, event_id, fraud_score, THRESHOLD, model_version,
                                   table=TABLE)
    if flagged and sns is not None and repo.begin_alert(ddb, event_id, table=TABLE):
        message_id = sns.publish(
            TopicArn=ALERT_TOPIC_ARN,
            Subject="Fraud alert (demo)",
            Message=_alert_message(tx, fraud_score),
        )["MessageId"]
        repo.mark_alert_sent(ddb, event_id, message_id, table=TABLE)
    return "flagged" if flagged else "scored"


def lambda_handler(event, context):
    ddb, sns = _clients()
    failures, counts = [], {}
    for record in event.get("Records", []):
        seq = record["kinesis"]["sequenceNumber"]
        try:
            tx = json.loads(base64.b64decode(record["kinesis"]["data"]))
            if tx.get("schema_version") != "1.0":
                raise ValueError(f"unsupported schema_version {tx.get('schema_version')!r}")
        except (ValueError, KeyError, TypeError) as exc:
            log.error("skipping malformed record %s: %s", seq, exc)
            counts["malformed"] = counts.get("malformed", 0) + 1
            continue
        try:
            outcome = process(tx, ddb, sns)
        except ValueError as exc:  # missing required event fields
            log.error("skipping invalid event %s: %s", seq, exc)
            outcome = "malformed"
        except Exception:
            log.exception("transient failure on %s; will retry", seq)
            failures.append({"itemIdentifier": seq})
            outcome = "retry"
        counts[outcome] = counts.get(outcome, 0) + 1
    log.info("batch summary: %s", json.dumps(counts))
    return {"batchItemFailures": failures}
