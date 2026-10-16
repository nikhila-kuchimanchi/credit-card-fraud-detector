"""Replay a CSV as versioned transaction events. No model or consumer is deployed."""
import argparse
import csv
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import random
import time
import uuid
from datetime import datetime, timezone

LOG = logging.getLogger("fraud-producer")
FEATURES = ["amount", "category", "merchant", "state", "city_population",
            "customer_lat", "customer_lon", "merchant_lat", "merchant_lon",
            "age_at_transaction", "hour", "weekday", "month"]
REQUIRED = {"trans_date_trans_time", "cc_num", "merchant", "category", "amt", "state",
            "city_pop", "lat", "long", "merch_lat", "merch_long", "dob", "trans_num", "is_fraud"}


def number(row, key):
    value = float(row[key])
    if not math.isfinite(value):
        raise ValueError(f"{key} is not finite")
    return value


def row_to_features(row):
    """Use this exact transformation for TRAINING as well as inference.

    Source timestamps have no declared timezone. Derive source-calendar features
    without falsely converting those timestamps to UTC.
    """
    when = datetime.strptime(row["trans_date_trans_time"], "%Y-%m-%d %H:%M:%S")
    born = datetime.strptime(row["dob"], "%Y-%m-%d").date()
    age = when.year - born.year - ((when.month, when.day) < (born.month, born.day))
    amount = number(row, "amt")
    if amount < 0 or not 0 <= age <= 120:
        raise ValueError("Invalid amount or age")
    latitude, longitude = number(row, "lat"), number(row, "long")
    mlat, mlon = number(row, "merch_lat"), number(row, "merch_long")
    if not (-90 <= latitude <= 90 and -90 <= mlat <= 90 and
            -180 <= longitude <= 180 and -180 <= mlon <= 180):
        raise ValueError("Invalid coordinates")
    population = int(row["city_pop"])
    if population < 0:
        raise ValueError("Invalid population")
    return dict(zip(FEATURES, [amount, row["category"], row["merchant"], row["state"],
        population, latitude, longitude, mlat, mlon, age, when.hour,
        when.weekday(), when.month]))


def make_event(row, replay_id, cycle, row_index, dataset):
    features = row_to_features(row)
    if row["is_fraud"] not in ("0", "1"):
        raise ValueError("is_fraud must be 0 or 1")
    account = hashlib.sha256(row["cc_num"].encode()).hexdigest()[:24]
    event_id = str(uuid.uuid5(uuid.UUID(replay_id),
                           f"{cycle}:{row_index}:{row['trans_num']}"))
    return {
        "schema_version": "1.0", "feature_version": "v1", "event_id": event_id,
        "replay_id": replay_id, "replay_cycle": cycle, "source_row": row_index,
        "source_dataset": dataset, "source_transaction_id": row["trans_num"],
        "source_timestamp": row["trans_date_trans_time"],
        "emitted_at": datetime.now(timezone.utc).isoformat(),
        "account_id": account, "features": features,
        "ground_truth": {"is_fraud": int(row["is_fraud"])},
    }


def send_batch(client, stream, events, attempts=8, sleep=time.sleep):
    """Retry failed PutRecords entries; preserve event IDs for idempotency.

    An ambiguous transport error may cause a duplicate. PutRecords does not
    guarantee order, even with the same partition key. Consumers must deduplicate.
    """
    pending = [{"PartitionKey": event["account_id"],
                "Data": json.dumps(event, allow_nan=False).encode()}
               for event in events]
    for attempt in range(attempts):
        try:
            response = client.put_records(StreamName=stream, Records=pending)
        except Exception:
            if attempt == attempts - 1:
                raise
            LOG.warning("Kinesis request error; retrying", exc_info=True)
        else:
            entries = response["Records"]
            if len(entries) != len(pending):
                raise RuntimeError("Unexpected PutRecords response length")
            pending = [record for record, result in zip(pending, entries)
                       if result.get("ErrorCode")]
            if not pending:
                return
            LOG.warning("Retrying %d failed records", len(pending))
        if attempt < attempts - 1:
            sleep(min(30, 0.5 * 2 ** attempt) + random.uniform(0, 0.25))
    raise RuntimeError(f"Failed to publish {len(pending)} records; checkpoint not advanced")


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def save_state(path, state):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state))
    temporary.replace(path)


def run(args):
    if not args.csv.is_file():
        raise FileNotFoundError(args.csv)
    if not math.isfinite(args.rate) or args.rate <= 0 or args.rate > 100:
        raise ValueError("rate must be > 0 and <= 100 records/second for this demo")
    if not 1 <= args.batch_size <= 100 or args.max_records < 0:
        raise ValueError("batch-size must be 1..100; max-records must be >= 0")
    file_hash = fingerprint(args.csv)
    state = {"replay_id": str(uuid.uuid4()), "cycle": 0, "next_row": 0,
             "dataset_sha256": file_hash, "dataset": args.dataset}
    if args.checkpoint and args.checkpoint.exists():
        state = json.loads(args.checkpoint.read_text())
        if state["dataset_sha256"] != file_hash or state["dataset"] != args.dataset:
            raise ValueError("Dataset changed. Use a new checkpoint or intentionally reset it.")
    if args.checkpoint:
        save_state(args.checkpoint, state)
    client = None
    if not args.dry_run:
        import boto3
        from botocore.config import Config
        client = boto3.client("kinesis", region_name=args.region,
                              config=Config(retries={"mode": "standard", "max_attempts": 5},
                                            connect_timeout=5, read_timeout=30))
    sent = 0
    while True:
        batch, seen = [], 0
        with args.csv.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not REQUIRED.issubset(reader.fieldnames or []):
                raise ValueError(f"Missing CSV columns: {sorted(REQUIRED - set(reader.fieldnames or []))}")
            for index, row in enumerate(reader):
                seen += 1
                if index < state["next_row"]:
                    continue
                # Fail loudly on invalid data: don't silently drop transactions.
                batch.append(make_event(row, state["replay_id"], state["cycle"],
                                        index, args.dataset))
                last = args.max_records and sent + len(batch) >= args.max_records
                if len(batch) < args.batch_size and not last:
                    continue
                publish(args, client, batch)
                sent += len(batch)
                state["next_row"] = index + 1
                if args.checkpoint:
                    save_state(args.checkpoint, state)
                LOG.info("published=%d cycle=%d next_row=%d", sent,
                         state["cycle"], state["next_row"])
                count = len(batch)
                batch = []
                if args.max_records and sent >= args.max_records:
                    return
                time.sleep(count / args.rate)
            if batch:
                publish(args, client, batch)
                sent += len(batch)
                state["next_row"] = seen
                if args.checkpoint:
                    save_state(args.checkpoint, state)
                time.sleep(len(batch) / args.rate)
        if not seen:
            raise ValueError("CSV contains no transactions")
        if not args.loop:
            return
        state["cycle"] += 1
        state["next_row"] = 0
        if args.checkpoint:
            save_state(args.checkpoint, state)


def publish(args, client, batch):
    if args.dry_run:
        for event in batch:
            print(json.dumps(event, allow_nan=False), flush=True)
    else:
        send_batch(client, args.stream, batch)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--csv", type=Path, required=True)
    p.add_argument("--dataset", default="fraudTest")
    p.add_argument("--region", default=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    p.add_argument("--stream", default=os.getenv("STREAM_NAME"))
    p.add_argument("--rate", type=float, default=float(os.getenv("RATE", "5")))
    p.add_argument("--batch-size", type=int, default=5)
    p.add_argument("--max-records", type=int, default=0, help="0 means no limit")
    p.add_argument("--checkpoint", type=Path)
    p.add_argument("--loop", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if not args.dry_run and not args.stream:
        p.error("--stream is required unless --dry-run")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(args)


if __name__ == "__main__":
    main()
