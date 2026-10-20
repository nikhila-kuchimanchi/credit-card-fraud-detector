"""Create (or delete) the FraudTransactions DynamoDB table.

Usage:
    python -m db.create_table --region us-east-1
    python -m db.create_table --region us-east-1 --delete
    python -m db.create_table --print   # show definition, no AWS calls
"""

import argparse
import json

from . import schema


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--table", default=schema.TABLE_NAME)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--delete", action="store_true", help="delete the table")
    parser.add_argument("--print", action="store_true", help="print definition only")
    args = parser.parse_args()

    params = schema.create_table_params(args.table)
    if args.print:
        print(json.dumps(params, indent=2))
        return

    import boto3  # only needed for real AWS calls
    client = boto3.client("dynamodb", region_name=args.region)

    if args.delete:
        client.delete_table(TableName=args.table)
        client.get_waiter("table_not_exists").wait(TableName=args.table)
        print(f"Deleted {args.table}")
        return

    try:
        client.create_table(**params)
        print(f"Creating {args.table} ...")
    except client.exceptions.ResourceInUseException:
        print(f"{args.table} already exists")
    client.get_waiter("table_exists").wait(TableName=args.table)
    ttl = client.describe_time_to_live(TableName=args.table)["TimeToLiveDescription"]
    if ttl["TimeToLiveStatus"] == "DISABLED":
        client.update_time_to_live(
            TableName=args.table,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": schema.TTL_ATTRIBUTE},
        )
    arn = client.describe_table(TableName=args.table)["Table"]["TableArn"]
    print(f"Ready: {arn}")


if __name__ == "__main__":
    main()
