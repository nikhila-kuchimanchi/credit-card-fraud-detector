"""Idempotent DynamoDB reads/writes for the scoring and read Lambdas.

Works with a low-level boto3 DynamoDB client (boto3.client("dynamodb")) but
does not import boto3 itself, so it can be unit-tested without AWS.

Kinesis -> Lambda delivery is at-least-once, so every write is conditional:
  * put_pending   creates the item only if event_id is new
  * record_score  never overwrites an item that is already SCORED
  * record_error  never downgrades a SCORED item to ERROR
  * mark_alert_sent moves alert_status PENDING -> SENT exactly once
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import math

from . import schema

_EVENT_FIELDS = (
    "schema_version", "feature_version", "event_id", "replay_id", "replay_cycle",
    "source_row", "source_dataset", "source_transaction_id", "source_timestamp",
    "emitted_at", "account_id", "features", "ground_truth",
)


# ---------------------------------------------------------------- serialization
def to_attr(value):
    """Python value -> DynamoDB AttributeValue."""
    if value is None:
        return {"NULL": True}
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, (int, Decimal)):
        return {"N": str(value)}
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"DynamoDB cannot store non-finite number: {value}")
        return {"N": repr(value)}
    if isinstance(value, str):
        return {"S": value}
    if isinstance(value, dict):
        return {"M": {str(k): to_attr(v) for k, v in value.items()}}
    if isinstance(value, (list, tuple)):
        return {"L": [to_attr(v) for v in value]}
    raise TypeError(f"Unsupported type for DynamoDB: {type(value).__name__}")


def from_attr(attr):
    """DynamoDB AttributeValue -> Python value (numbers become int or float)."""
    (kind, value), = attr.items()
    if kind == "NULL":
        return None
    if kind in ("S", "BOOL"):
        return value
    if kind == "N":
        return int(value) if value.lstrip("-").isdigit() else float(value)
    if kind == "M":
        return {k: from_attr(v) for k, v in value.items()}
    if kind == "L":
        return [from_attr(v) for v in value]
    raise TypeError(f"Unsupported DynamoDB type: {kind}")


def from_item(item):
    return {k: from_attr(v) for k, v in item.items()}


def _values(**kwargs):
    return {f":{k}": to_attr(v) for k, v in kwargs.items()}


def _now_iso(now=None):
    return (now or datetime.now(timezone.utc)).isoformat()


def _is_conditional_failure(exc):
    response = getattr(exc, "response", None) or {}
    return response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


# ---------------------------------------------------------------- write path
def build_pending_item(event, now=None, ttl_days=schema.DEFAULT_TTL_DAYS):
    """Kinesis event (docs/event_schema.json) -> new PENDING item."""
    missing = [f for f in _EVENT_FIELDS if f not in event]
    if missing:
        raise ValueError(f"event is missing required fields: {missing}")
    now = now or datetime.now(timezone.utc)
    emitted = datetime.fromisoformat(event["emitted_at"])
    if emitted.tzinfo is None:
        emitted = emitted.replace(tzinfo=timezone.utc)
    item = {f: event[f] for f in _EVENT_FIELDS}
    item.update({
        "emitted_date": emitted.astimezone(timezone.utc).date().isoformat(),
        "status": schema.STATUS_PENDING,
        "alert_status": schema.ALERT_NONE,
        "created_at": _now_iso(now),
        "updated_at": _now_iso(now),
        schema.TTL_ATTRIBUTE: int((emitted + timedelta(days=ttl_days)).timestamp()),
    })
    return item


def put_pending(client, event, table=schema.TABLE_NAME, now=None):
    """Store a new transaction. Returns False if event_id already exists (retry)."""
    item = build_pending_item(event, now)
    try:
        client.put_item(
            TableName=table,
            Item={k: to_attr(v) for k, v in item.items()},
            ConditionExpression="attribute_not_exists(event_id)",
        )
        return True
    except Exception as exc:
        if _is_conditional_failure(exc):
            return False
        raise


def record_score(client, event_id, fraud_score, threshold, model_version,
                 table=schema.TABLE_NAME, now=None):
    """Save the model result. Returns (updated, predicted_fraud).

    updated is False when the item was already SCORED (duplicate delivery),
    so the caller must not send a second alert.
    """
    fraud_score = float(fraud_score)
    predicted = fraud_score >= float(threshold)
    sets = [
        "#st = :scored", "fraud_score = :score", "predicted_fraud = :pred",
        "threshold = :thr", "model_version = :mv", "scored_at = :ts", "updated_at = :ts",
    ]
    values = _values(scored=schema.STATUS_SCORED, score=fraud_score, pred=predicted,
                     thr=float(threshold), mv=str(model_version), ts=_now_iso(now))
    if predicted:
        sets += ["flag_status = :flag", "alert_status = :alert"]
        values.update(_values(flag=schema.FLAGGED_VALUE, alert=schema.ALERT_PENDING))
    try:
        client.update_item(
            TableName=table,
            Key={"event_id": to_attr(event_id)},
            UpdateExpression="SET " + ", ".join(sets) + " REMOVE error_message",
            ConditionExpression="attribute_exists(event_id) AND #st <> :scored",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=values,
        )
        return True, predicted
    except Exception as exc:
        if _is_conditional_failure(exc):
            return False, predicted
        raise


def record_error(client, event_id, message, table=schema.TABLE_NAME, now=None):
    """Mark inference failure. Never treated as non-fraud; never overwrites SCORED."""
    try:
        client.update_item(
            TableName=table,
            Key={"event_id": to_attr(event_id)},
            UpdateExpression="SET #st = :error, error_message = :msg, updated_at = :ts "
                             "ADD attempts :one",
            ConditionExpression="attribute_exists(event_id) AND #st <> :scored",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=_values(error=schema.STATUS_ERROR, msg=str(message)[:1000],
                                              ts=_now_iso(now), one=1,
                                              scored=schema.STATUS_SCORED),
        )
        return True
    except Exception as exc:
        if _is_conditional_failure(exc):
            return False
        raise


def mark_alert_sent(client, event_id, message_id, table=schema.TABLE_NAME, now=None):
    """PENDING -> SENT once. Returns False if already sent."""
    try:
        client.update_item(
            TableName=table,
            Key={"event_id": to_attr(event_id)},
            UpdateExpression="SET alert_status = :sent, alert_message_id = :mid, "
                             "alerted_at = :ts, updated_at = :ts",
            ConditionExpression="alert_status = :pending",
            ExpressionAttributeValues=_values(sent=schema.ALERT_SENT, pending=schema.ALERT_PENDING,
                                              mid=str(message_id), ts=_now_iso(now)),
        )
        return True
    except Exception as exc:
        if _is_conditional_failure(exc):
            return False
        raise


# ---------------------------------------------------------------- read path
def get_transaction(client, event_id, table=schema.TABLE_NAME):
    response = client.get_item(TableName=table, Key={"event_id": to_attr(event_id)})
    item = response.get("Item")
    return from_item(item) if item else None


def _query(client, table, index, key_name, key_value, limit, start_key):
    params = {
        "TableName": table,
        "IndexName": index,
        "KeyConditionExpression": "#k = :v",
        "ExpressionAttributeNames": {"#k": key_name},
        "ExpressionAttributeValues": {":v": to_attr(key_value)},
        "ScanIndexForward": False,  # newest first
        "Limit": int(limit),
    }
    if start_key:
        params["ExclusiveStartKey"] = start_key
    response = client.query(**params)
    return [from_item(i) for i in response.get("Items", [])], response.get("LastEvaluatedKey")


def list_recent(client, day=None, limit=50, start_key=None, table=schema.TABLE_NAME):
    """Newest transactions for one UTC day (YYYY-MM-DD). Returns (items, next_key)."""
    day = day or datetime.now(timezone.utc).date().isoformat()
    return _query(client, table, schema.GSI_BY_DAY, "emitted_date", day, limit, start_key)


def list_flagged(client, limit=50, start_key=None, table=schema.TABLE_NAME):
    """Newest predicted-fraud transactions. Returns (items, next_key)."""
    return _query(client, table, schema.GSI_FLAGGED, "flag_status", schema.FLAGGED_VALUE,
                  limit, start_key)
