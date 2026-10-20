"""Load the 20-row fake CSV into DynamoDB to see what stored items look like.

Scores are FAKE (random, seeded) - there is no model yet. model_version is
"demo-fake-v0" so these rows are easy to recognise and delete.

Usage (from the repository root):
    python -m db.load_sample --region us-east-1            # load 20 items
    python -m db.load_sample --region us-east-1 --delete   # remove them again
"""

import argparse
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from producer import make_event  # noqa: E402

from . import repository as repo, schema  # noqa: E402

DEMO_REPLAY_ID = "00000000-0000-0000-0000-00000000de00"
THRESHOLD = 0.5


def demo_events(csv_path):
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    start = datetime.now(timezone.utc) - timedelta(minutes=len(rows))
    for i, row in enumerate(rows):
        event = make_event(row, DEMO_REPLAY_ID, 0, i, "synthetic_sample")
        event["emitted_at"] = (start + timedelta(minutes=i)).isoformat(timespec="seconds")
        yield i, event


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", default=str(ROOT / "examples/synthetic_sample.csv"))
    parser.add_argument("--table", default=schema.TABLE_NAME)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--delete", action="store_true")
    args = parser.parse_args()

    import boto3
    ddb = boto3.client("dynamodb", region_name=args.region)
    rng = random.Random(7)
    events = list(demo_events(args.csv))

    if args.delete:
        for _, event in events:
            ddb.delete_item(TableName=args.table, Key={"event_id": repo.to_attr(event["event_id"])})
        print(f"Deleted {len(events)} demo items")
        return

    counts = {"SCORED": 0, "FLAGGED": 0, "ERROR": 0, "PENDING": 0, "duplicate": 0}
    for i, event in events:
        if not repo.put_pending(ddb, event, table=args.table):
            counts["duplicate"] += 1
            continue
        eid = event["event_id"]
        if i == len(events) - 1:            # leave one PENDING (model not called yet)
            counts["PENDING"] += 1
            continue
        if i == len(events) - 2:            # simulate an inference failure
            repo.record_error(ddb, eid, "demo: endpoint timeout", table=args.table)
            counts["ERROR"] += 1
            continue
        fraud = event["ground_truth"]["is_fraud"] == 1
        score = round(rng.uniform(0.8, 0.97) if fraud else rng.uniform(0.01, 0.3), 3)
        if i == 3:                          # one false positive, like a real model
            score = 0.62
        _, flagged = repo.record_score(ddb, eid, score, THRESHOLD, "demo-fake-v0", table=args.table)
        counts["SCORED"] += 1
        if flagged:
            repo.mark_alert_sent(ddb, eid, f"demo-msg-{i}", table=args.table)
            counts["FLAGGED"] += 1

    print(f"Loaded into {args.table}: {counts}")
    flagged, _ = repo.list_flagged(ddb, limit=10, table=args.table)
    print("Flagged (newest first):")
    for item in flagged:
        print(f"  {item['source_transaction_id']}  score={item['fraud_score']}  "
              f"true_label={item['ground_truth']['is_fraud']}  alert={item['alert_status']}")


if __name__ == "__main__":
    main()
