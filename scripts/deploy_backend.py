"""Deploy the backend: writer Lambda (Kinesis -> DynamoDB), reader Lambda + HTTP API.

Requires an authenticated AWS identity (aws login / aws configure) and boto3.

    python scripts/deploy_backend.py deploy  --region us-east-1 [--alert-email you@umd.edu]
    python scripts/deploy_backend.py status  --region us-east-1
    python scripts/deploy_backend.py destroy --region us-east-1 --yes

The Kinesis stream comes from the ingestion stack (scripts/manage.py deploy):
--stream-arn, else .local/outputs.json, else the "fraud-ingestion" stack outputs.
deploy is idempotent: rerun it after code changes to update the functions.
destroy removes Lambdas, trigger, API, roles and SNS topic; the DynamoDB table is
kept unless --delete-table is given.
"""

import argparse
import io
import json
from pathlib import Path
import sys
import time
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db import schema  # noqa: E402

PREFIX = "fraud"
WRITER = f"{PREFIX}-writer"
READER = f"{PREFIX}-reader"
API_NAME = f"{PREFIX}-api"
TOPIC_NAME = f"{PREFIX}-alerts"
RUNTIME = "python3.12"
BASIC_EXEC = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
TRUST = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},
                   "Action": "sts:AssumeRole"}],
}
STATE_FILE = ROOT / ".local" / "backend.json"


# ------------------------------------------------------------------ helpers
def find_stream_arn(args, session):
    if args.stream_arn:
        return args.stream_arn
    local = ROOT / ".local" / "outputs.json"
    if local.is_file():
        arn = json.loads(local.read_text()).get("StreamArn")
        if arn:
            return arn
    cfn = session.client("cloudformation")
    try:
        stack = cfn.describe_stacks(StackName=args.stack)["Stacks"][0]
    except Exception:
        sys.exit("No Kinesis stream found. Deploy ingestion first "
                 "(scripts/manage.py deploy) or pass --stream-arn.")
    return next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "StreamArn")


def build_zip(handler_dir, extra_files=()):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(handler_dir.glob("*.py")):
            archive.write(path, path.name)
        for name in ("__init__.py", "schema.py", "repository.py"):
            archive.write(ROOT / "db" / name, f"db/{name}")
        for path, arcname in extra_files:
            archive.write(path, arcname)
    return buffer.getvalue()


def ensure_role(iam, name, inline_policies):
    try:
        role = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(TRUST),
                               Description="fraud detector Lambda role",
                               Tags=[{"Key": "project", "Value": "credit-card-fraud-detector"}])
        created = True
        print(f"  created role {name}")
    except iam.exceptions.EntityAlreadyExistsException:
        role = iam.get_role(RoleName=name)
        created = False
    iam.attach_role_policy(RoleName=name, PolicyArn=BASIC_EXEC)
    for policy_name, document in inline_policies.items():
        iam.put_role_policy(RoleName=name, PolicyName=policy_name,
                            PolicyDocument=json.dumps(document))
    return role["Role"]["Arn"], created


def ensure_function(lam, name, role_arn, code, env, timeout, memory, description):
    config = dict(Role=role_arn, Handler="handler.lambda_handler", Runtime=RUNTIME,
                  Timeout=timeout, MemorySize=memory, Description=description,
                  Environment={"Variables": env})
    try:
        lam.get_function(FunctionName=name)
        lam.update_function_code(FunctionName=name, ZipFile=code)
        lam.get_waiter("function_updated_v2").wait(FunctionName=name)
        lam.update_function_configuration(FunctionName=name, **config)
        print(f"  updated function {name}")
    except lam.exceptions.ResourceNotFoundException:
        for attempt in range(10):  # new IAM roles take a few seconds to propagate
            try:
                lam.create_function(FunctionName=name, Code={"ZipFile": code},
                                    Tags={"project": "credit-card-fraud-detector"}, **config)
                break
            except lam.exceptions.InvalidParameterValueException as exc:
                if "role" not in str(exc).lower() or attempt == 9:
                    raise
                time.sleep(5)
        print(f"  created function {name}")
    lam.get_waiter("function_active_v2").wait(FunctionName=name)
    lam.get_waiter("function_updated_v2").wait(FunctionName=name)
    return lam.get_function(FunctionName=name)["Configuration"]["FunctionArn"]


def ensure_table(session, table):
    ddb = session.client("dynamodb")
    try:
        return ddb.describe_table(TableName=table)["Table"]["TableArn"]
    except ddb.exceptions.ResourceNotFoundException:
        print(f"  creating table {table}")
        ddb.create_table(**schema.create_table_params(table))
        ddb.get_waiter("table_exists").wait(TableName=table)
        ddb.update_time_to_live(TableName=table, TimeToLiveSpecification={
            "Enabled": True, "AttributeName": schema.TTL_ATTRIBUTE})
        return ddb.describe_table(TableName=table)["Table"]["TableArn"]


def load_policy(name, table_arn):
    """Mario's least-privilege policies, re-targeted at the actual table ARN."""
    text = (ROOT / "db" / "iam" / name).read_text()
    default_arn = "arn:aws:dynamodb:us-east-1:658980432945:table/FraudTransactions"
    return json.loads(text.replace(default_arn, table_arn))


# ------------------------------------------------------------------ actions
def deploy(args, session):
    iam, lam = session.client("iam"), session.client("lambda")
    stream_arn = find_stream_arn(args, session)
    print(f"Kinesis stream: {stream_arn}")
    table_arn = ensure_table(session, args.table)
    print(f"DynamoDB table: {table_arn}")

    topic_arn = ""
    if args.alert_email:
        sns = session.client("sns")
        topic_arn = sns.create_topic(Name=TOPIC_NAME)["TopicArn"]
        subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
        if not any(s["Endpoint"] == args.alert_email for s in subs):
            sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=args.alert_email)
            print(f"  SNS: confirm the subscription email sent to {args.alert_email}")
        print(f"SNS topic: {topic_arn}")

    print("IAM roles")
    writer_policies = {
        "dynamodb-write": load_policy("writer-policy.json", table_arn),
        "kinesis-read": {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow",
            "Action": ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary",
                       "kinesis:GetRecords", "kinesis:GetShardIterator",
                       "kinesis:ListShards", "kinesis:SubscribeToShard"],
            "Resource": stream_arn}]},
    }
    if topic_arn:
        writer_policies["sns-publish"] = {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": "sns:Publish", "Resource": topic_arn}]}
    writer_role, new_w = ensure_role(iam, f"{WRITER}-role", writer_policies)
    reader_role, new_r = ensure_role(iam, f"{READER}-role",
                                     {"dynamodb-read": load_policy("reader-policy.json", table_arn)})
    if new_w or new_r:
        time.sleep(10)

    print("Lambda functions")
    writer_arn = ensure_function(
        lam, WRITER, writer_role, build_zip(ROOT / "lambdas" / "writer"),
        {"TABLE_NAME": args.table, "THRESHOLD": str(args.threshold),
         "ALERT_TOPIC_ARN": topic_arn, "SCORER_MODE": "placeholder"},
        timeout=60, memory=256,
        description="Kinesis -> score -> DynamoDB (-> SNS) for fraud detector")
    reader_arn = ensure_function(
        lam, READER, reader_role, build_zip(ROOT / "lambdas" / "reader"),
        {"TABLE_NAME": args.table}, timeout=10, memory=128,
        description="Read-only API for fraud detector dashboard")

    print("Kinesis trigger")
    mappings = lam.list_event_source_mappings(EventSourceArn=stream_arn,
                                              FunctionName=WRITER)["EventSourceMappings"]
    mapping_config = dict(BatchSize=100, MaximumBatchingWindowInSeconds=1,
                          FunctionResponseTypes=["ReportBatchItemFailures"],
                          BisectBatchOnFunctionError=True, MaximumRetryAttempts=3,
                          MaximumRecordAgeInSeconds=3600, Enabled=True)
    if mappings:
        lam.update_event_source_mapping(UUID=mappings[0]["UUID"], **mapping_config)
        mapping_uuid = mappings[0]["UUID"]
    else:
        mapping_uuid = lam.create_event_source_mapping(
            EventSourceArn=stream_arn, FunctionName=WRITER,
            StartingPosition="LATEST", **mapping_config)["UUID"]
    print(f"  event source mapping {mapping_uuid}")

    print("HTTP API")
    api_gw = session.client("apigatewayv2")
    api = next((a for a in api_gw.get_apis()["Items"] if a["Name"] == API_NAME), None)
    if api is None:
        api = api_gw.create_api(
            Name=API_NAME, ProtocolType="HTTP", Target=reader_arn,
            CorsConfiguration={"AllowOrigins": ["*"], "AllowMethods": ["GET", "OPTIONS"],
                               "AllowHeaders": ["content-type"]},
            Tags={"project": "credit-card-fraud-detector"})
        print(f"  created API {api['ApiId']}")
    api_gw.update_stage(ApiId=api["ApiId"], StageName="$default", DefaultRouteSettings={
        "ThrottlingBurstLimit": 20, "ThrottlingRateLimit": 10})
    account = session.client("sts").get_caller_identity()["Account"]
    try:
        lam.add_permission(
            FunctionName=READER, StatementId="apigateway-invoke",
            Action="lambda:InvokeFunction", Principal="apigateway.amazonaws.com",
            SourceArn=f"arn:aws:execute-api:{args.region}:{account}:{api['ApiId']}/*")
    except lam.exceptions.ResourceConflictException:
        pass
    url = api["ApiEndpoint"]

    state = {"Region": args.region, "Table": args.table, "StreamArn": stream_arn,
             "WriterFunction": WRITER, "ReaderFunction": READER, "MappingUUID": mapping_uuid,
             "ApiId": api["ApiId"], "ApiUrl": url, "AlertTopicArn": topic_arn,
             "Threshold": args.threshold}
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2) + "\n")
    print(json.dumps(state, indent=2))
    print(f"\nTry: curl {url}/transactions/flagged?limit=5")


def status(args, session):
    lam, ddb = session.client("lambda"), session.client("dynamodb")
    for name in (WRITER, READER):
        try:
            c = lam.get_function(FunctionName=name)["Configuration"]
            print(f"{name}: {c['State']} / last update {c.get('LastUpdateStatus')}")
        except lam.exceptions.ResourceNotFoundException:
            print(f"{name}: not deployed")
    for m in lam.list_event_source_mappings(FunctionName=WRITER).get("EventSourceMappings", []):
        print(f"trigger: {m['State']} / last result: {m.get('LastProcessingResult')}")
    table = ddb.describe_table(TableName=args.table)["Table"]
    print(f"table {args.table}: ~{table['ItemCount']} items (updated every ~6 hours)")
    if STATE_FILE.is_file():
        print("API:", json.loads(STATE_FILE.read_text()).get("ApiUrl"))


def destroy(args, session):
    if not args.yes:
        sys.exit("destroy requires --yes")
    lam, iam = session.client("lambda"), session.client("iam")
    api_gw, sns = session.client("apigatewayv2"), session.client("sns")
    for m in lam.list_event_source_mappings(FunctionName=WRITER).get("EventSourceMappings", []):
        lam.delete_event_source_mapping(UUID=m["UUID"])
        print("deleted Kinesis trigger")
    for api in api_gw.get_apis()["Items"]:
        if api["Name"] == API_NAME:
            api_gw.delete_api(ApiId=api["ApiId"])
            print("deleted API")
    for name in (WRITER, READER):
        try:
            lam.delete_function(FunctionName=name)
            print(f"deleted {name}")
        except lam.exceptions.ResourceNotFoundException:
            pass
        role = f"{name}-role"
        try:
            for p in iam.list_role_policies(RoleName=role)["PolicyNames"]:
                iam.delete_role_policy(RoleName=role, PolicyName=p)
            for p in iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"]:
                iam.detach_role_policy(RoleName=role, PolicyArn=p["PolicyArn"])
            iam.delete_role(RoleName=role)
            print(f"deleted {role}")
        except iam.exceptions.NoSuchEntityException:
            pass
    for topic in sns.list_topics()["Topics"]:
        if topic["TopicArn"].endswith(f":{TOPIC_NAME}"):
            sns.delete_topic(TopicArn=topic["TopicArn"])
            print("deleted SNS topic")
    if args.delete_table:
        session.client("dynamodb").delete_table(TableName=args.table)
        print(f"deleting table {args.table}")
    else:
        print(f"kept table {args.table} (use --delete-table to remove it)")
    print("Backend removed. The ingestion stack is separate: scripts/manage.py destroy --yes")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=["deploy", "status", "destroy"])
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--stack", default="fraud-ingestion", help="ingestion stack name")
    parser.add_argument("--stream-arn")
    parser.add_argument("--table", default=schema.TABLE_NAME)
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--alert-email", help="optional: create SNS topic + email subscription")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--delete-table", action="store_true")
    args = parser.parse_args()

    import boto3
    session = boto3.Session(region_name=args.region)
    print("Using identity:", session.client("sts").get_caller_identity()["Arn"])
    {"deploy": deploy, "status": status, "destroy": destroy}[args.action](args, session)


if __name__ == "__main__":
    main()
