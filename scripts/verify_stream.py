"""Read new events for a short smoke test. Does not consume/delete records for Lambda."""
import argparse
import base64
import json
from pathlib import Path
import time
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from producer import FEATURES


def main():
    import boto3
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--stack", default="fraud-ingestion")
    p.add_argument("--stream")
    p.add_argument("--count", type=int, default=10)
    p.add_argument("--timeout", type=int, default=60)
    args = p.parse_args()
    if args.count < 1 or args.timeout < 1:
        p.error("count and timeout must be positive")
    if not args.stream:
        cfn = boto3.client("cloudformation", region_name=args.region)
        stack = cfn.describe_stacks(StackName=args.stack)["Stacks"][0]
        args.stream = next(x["OutputValue"] for x in stack["Outputs"]
                           if x["OutputKey"] == "StreamName")
    client = boto3.client("kinesis", region_name=args.region)
    shards = []
    for page in client.get_paginator("list_shards").paginate(StreamName=args.stream):
        shards.extend(page["Shards"])
    iterators = [client.get_shard_iterator(StreamName=args.stream,
        ShardId=shard["ShardId"], ShardIteratorType="LATEST")["ShardIterator"]
        for shard in shards]
    deadline, count, labels = time.monotonic() + args.timeout, 0, 0
    while count < args.count and time.monotonic() < deadline:
        for index, iterator in enumerate(iterators):
            if not iterator:
                continue
            response = client.get_records(ShardIterator=iterator, Limit=100)
            iterators[index] = response.get("NextShardIterator")
            for record in response["Records"]:
                event = json.loads(record["Data"])
                assert event["schema_version"] == "1.0"
                assert set(event["features"]) == set(FEATURES)
                assert "is_fraud" not in event["features"]
                assert "cc_num" not in event
                count += 1
                labels += event["ground_truth"]["is_fraud"]
                print(json.dumps({"event_id": event["event_id"],
                    "source_timestamp": event["source_timestamp"],
                    "emitted_at": event["emitted_at"],
                    "amount": event["features"]["amount"],
                    "ground_truth": event["ground_truth"]}))
                if count == 1:
                    target = Path(".local")
                    target.mkdir(exist_ok=True)
                    fixture = {"Records": [{"eventID": "example:1", "eventSource": "aws:kinesis",
                        "awsRegion": args.region, "kinesis": {
                            "data": base64.b64encode(record["Data"]).decode(),
                            "partitionKey": event["account_id"],
                            "sequenceNumber": record["SequenceNumber"]}}]}
                    (target / "live-lambda-fixture.json").write_text(json.dumps(fixture, indent=2))
                if count >= args.count:
                    break
            if count >= args.count:
                break
        time.sleep(0.5)
    print(f"Read {count} new records; {labels} source fraud labels, not model predictions.")
    if count < args.count:
        raise SystemExit("Not enough new events arrived. Check the producer service and logs.")


if __name__ == "__main__":
    main()
