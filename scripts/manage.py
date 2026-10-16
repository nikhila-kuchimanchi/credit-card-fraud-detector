"""Deploy or manage only the ingestion stack using an authenticated AWS CLI."""
import argparse
import csv
import json
from pathlib import Path
import subprocess
import sys
import zipfile
ROOT = Path(__file__).resolve().parents[1]


def aws(args, region, capture=True):
    command = ["aws", "--region", region, "--no-cli-pager"] + args
    result = subprocess.run(command, check=True, text=True,
                            stdout=subprocess.PIPE if capture else None)
    return result.stdout.strip() if capture else ""


def outputs(stack, region):
    result = json.loads(aws(["cloudformation", "describe-stacks", "--stack-name", stack,
                            "--output", "json"], region))
    return {item["OutputKey"]: item["OutputValue"]
            for item in result["Stacks"][0].get("Outputs", [])}


def remote(instance, command, region):
    response = json.loads(aws(["ssm", "send-command", "--instance-ids", instance,
        "--document-name", "AWS-RunShellScript", "--parameters",
        json.dumps({"commands": [command]}), "--output", "json"], region))
    command_id = response["Command"]["CommandId"]
    print("SSM command:", command_id)
    # A failing command still needs its stderr shown.
    try:
        aws(["ssm", "wait", "command-executed", "--command-id", command_id,
             "--instance-id", instance], region)
    except subprocess.CalledProcessError:
        pass
    invocation = json.loads(aws(["ssm", "get-command-invocation", "--command-id",
        command_id, "--instance-id", instance, "--output", "json"], region))
    print(invocation.get("StandardOutputContent", ""))
    print(invocation.get("StandardErrorContent", ""))
    if invocation["Status"] != "Success":
        raise RuntimeError("Remote command status: " + invocation["Status"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["deploy", "outputs", "status", "logs", "start", "stop", "restart", "destroy"])
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--stack", default="fraud-ingestion")
    parser.add_argument("--data", type=Path)
    parser.add_argument("--rate", type=int, default=5)
    parser.add_argument("--dataset-name", default="fraudTest")
    parser.add_argument("--instance-profile", default="")
    parser.add_argument("--yes", action="store_true", help="Confirm deletion of this ingestion stack and its S3 data")
    args = parser.parse_args()
    if args.action == "deploy":
        if not args.data or not args.data.is_file():
            parser.error("deploy requires --data /path/to/fraudTest.csv")
        sys.path.insert(0, str(ROOT / "src"))
        from producer import REQUIRED, make_event
        with args.data.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not REQUIRED.issubset(reader.fieldnames or []):
                parser.error("Dataset columns do not match the supplied fraud CSV")
            first = next(reader, None)
            if first is None:
                parser.error("Dataset is empty")
            make_event(first, "00000000-0000-0000-0000-000000000001", 0, 0, args.dataset_name)
        identity = aws(["sts", "get-caller-identity", "--output", "json"], args.region)
        print("Deploying using identity:", identity)
        aws(["cloudformation", "validate-template", "--template-body",
             "file://" + str(ROOT / "infra/template.json")], args.region)
        aws(["cloudformation", "deploy", "--stack-name", args.stack,
             "--template-file", str(ROOT / "infra/template.json"), "--capabilities", "CAPABILITY_IAM",
             "--parameter-overrides", f"Rate={args.rate}", f"DatasetName={args.dataset_name}",
             f"ExistingInstanceProfileName={args.instance_profile}",
             "--no-fail-on-empty-changeset"], args.region, capture=False)
        out = outputs(args.stack, args.region)
        bucket = out["DataBucketName"]
        local = ROOT / ".local"
        local.mkdir(exist_ok=True)
        artifact = local / "producer.zip"
        with zipfile.ZipFile(artifact, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.write(ROOT / "src/producer.py", "src/producer.py")
            archive.write(ROOT / "requirements.txt", "requirements.txt")
        aws(["s3", "cp", str(args.data), f"s3://{bucket}/data/replay.csv"], args.region, capture=False)
        aws(["s3", "cp", str(artifact), f"s3://{bucket}/artifacts/producer.zip"], args.region, capture=False)
        out.update({"StackName": args.stack, "DatasetName": args.dataset_name})
        (local / "outputs.json").write_text(json.dumps(out, indent=2) + "\n")
        print(json.dumps(out, indent=2))
        print("Allow several minutes for EC2 setup, then run status and verify_stream.py.")
        print("For a code update on an existing stack, run restart after deploy.")
        print("Rate/dataset parameter changes require EC2 replacement; see README before updates.")
    else:
        out = outputs(args.stack, args.region)
        if args.action == "outputs":
            print(json.dumps(out, indent=2))
        elif args.action == "destroy":
            if not args.yes:
                parser.error("destroy requires --yes; it removes this stack and its dataset/code bucket")
            aws(["s3", "rm", f"s3://{out['DataBucketName']}", "--recursive"], args.region, capture=False)
            aws(["cloudformation", "delete-stack", "--stack-name", args.stack], args.region)
            aws(["cloudformation", "wait", "stack-delete-complete", "--stack-name", args.stack], args.region, capture=False)
            print("Ingestion stack deleted. Delete downstream teammate resources separately.")
        else:
            commands = {
                "status": "systemctl status fraud-producer --no-pager; tail -n 15 /var/log/cloud-init-output.log",
                "logs": "journalctl -u fraud-producer -n 40 --no-pager",
                "start": "systemctl start fraud-producer",
                "stop": "systemctl stop fraud-producer",
                "restart": "systemctl restart fraud-producer",
            }
            remote(out["InstanceId"], commands[args.action], args.region)


if __name__ == "__main__":
    main()
