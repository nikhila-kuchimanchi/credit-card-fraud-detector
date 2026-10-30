"""
Kinesis -> SageMaker fraud scorer.

This Lambda is responsible for EVERYTHING between the stream and the model:
  1. decode each Kinesis record (base64 -> JSON)
  2. turn the raw transaction into the 8 model features
  3. call the SageMaker endpoint
  4. apply the alert threshold and report results

Environment variables:
  ENDPOINT_NAME    SageMaker endpoint name (e.g. fraud-detector-serverless)
  ALERT_THRESHOLD  score at or above which a transaction is an alert (default 0.5)

IMPORTANT: build_instance() assumes the stream sends the raw fields listed in
RAW_FIELDS. If your stream uses different names, change ONLY build_instance().
"""
import base64
import json
import logging
import math
import os
from datetime import datetime

import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENDPOINT_NAME = os.environ["ENDPOINT_NAME"]
ALERT_THRESHOLD = float(os.environ.get("ALERT_THRESHOLD", "0.5"))

# Created once per container and reused across invocations.
runtime = boto3.client(
    "sagemaker-runtime",
    config=Config(retries={"max_attempts": 3, "mode": "standard"}, read_timeout=30),
)

# Raw fields expected in each stream record.
RAW_FIELDS = [
    "trans_date_trans_time",  # "2020-06-21 12:14:25" (same timezone semantics as training data)
    "dob",                    # "1988-03-09"
    "lat", "long",            # cardholder home coordinates
    "merch_lat", "merch_long",  # merchant coordinates
    "amt",                    # raw amount, NOT log-transformed
    "category", "gender", "city_pop",
]


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    a = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return 6371.0 * 2 * math.asin(math.sqrt(a))


def build_instance(rec):
    """Raw transaction -> the 8 features the model was trained on.

    Must match training exactly (hour, weekday, age, distance_km, amt,
    category, gender, city_pop). Raises ValueError on missing/invalid input.
    """
    missing = [f for f in RAW_FIELDS if f not in rec or rec[f] in (None, "")]
    if missing:
        raise ValueError(f"missing fields: {missing}")

    t = datetime.fromisoformat(str(rec["trans_date_trans_time"]))
    dob = datetime.fromisoformat(str(rec["dob"]))

    return {
        "hour": t.hour,
        "weekday": t.weekday(),  # Monday = 0, same as pandas dayofweek
        "age": int((t - dob).days / 365.25),
        "distance_km": haversine_km(
            float(rec["lat"]), float(rec["long"]),
            float(rec["merch_lat"]), float(rec["merch_long"]),
        ),
        "amt": float(rec["amt"]),
        "category": str(rec["category"]),
        "gender": str(rec["gender"]),
        "city_pop": int(rec["city_pop"]),
    }


def lambda_handler(event, context):
    instances, meta = [], []

    # 1) Decode and build features. A bad record is logged and skipped:
    #    retrying a malformed record would block the shard forever.
    for r in event["Records"]:
        seq = r["kinesis"]["sequenceNumber"]
        try:
            rec = json.loads(base64.b64decode(r["kinesis"]["data"]))
            instances.append(build_instance(rec))
            meta.append({"seq": seq, "id": rec.get("trans_num")})
        except (KeyError, ValueError, TypeError) as e:
            logger.error(json.dumps({"event": "bad_record", "seq": seq, "error": str(e)}))

    if not instances:
        return {"batchItemFailures": []}

    # 2) Score the whole batch in one endpoint call.
    #    On an endpoint error, report every valid record as failed so Kinesis retries them.
    try:
        resp = runtime.invoke_endpoint(
            EndpointName=ENDPOINT_NAME,
            ContentType="application/json",
            Body=json.dumps({"instances": instances}),
        )
        scores = json.loads(resp["Body"].read())["scores"]
        if len(scores) != len(instances):
            raise ValueError(f"expected {len(instances)} scores, got {len(scores)}")
    except Exception:
        logger.exception("endpoint call failed")
        return {"batchItemFailures": [{"itemIdentifier": m["seq"]} for m in meta]}

    # 3) Apply the threshold and report. Log ids and scores only, never raw card data.
    alerts = 0
    for m, s in zip(meta, scores):
        is_alert = s >= ALERT_THRESHOLD
        alerts += is_alert
        logger.info(json.dumps({
            "event": "scored", "trans_id": m["id"], "seq": m["seq"],
            "score": round(float(s), 4), "alert": bool(is_alert),
        }))
        # TODO (team decision): send alerts onward, e.g. SNS, SQS, DynamoDB.

    logger.info(json.dumps({"event": "batch_done", "scored": len(scores), "alerts": int(alerts)}))
    return {"batchItemFailures": []}