"""Generate the committed CloudFormation template using only the standard library."""
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]

def ref(name): return {"Ref": name}
def sub(value): return {"Fn::Sub": value}
def att(name, attribute): return {"Fn::GetAtt": [name, attribute]}

userdata = r'''#!/bin/bash
set -euxo pipefail
dnf install -y python3.11 python3.11-pip unzip awscli-2
systemctl enable --now amazon-ssm-agent
mkdir -p /opt/fraud
cat > /opt/fraud/config.env <<'ENV'
AWS_DEFAULT_REGION=${AWS::Region}
AWS_REGION=${AWS::Region}
DATA_BUCKET=${DataBucket}
STREAM_NAME=${Stream}
RATE=${Rate}
DATASET_NAME=${DatasetName}
ENV
cat > /opt/fraud/start.sh <<'SCRIPT'
#!/bin/bash
set -euo pipefail
cd /opt/fraud
# A failed download/setup exits; systemd retries without hiding the failure.
aws s3 cp "s3://$DATA_BUCKET/artifacts/producer.zip" code.zip
unzip -oq code.zip -d code
if [ ! -f transactions.csv ]; then
    aws s3 cp "s3://$DATA_BUCKET/data/replay.csv" transactions.csv.part
    mv transactions.csv.part transactions.csv
fi
if [ ! -x venv/bin/python ]; then python3.11 -m venv venv; fi
venv/bin/python -m pip install -q -r code/requirements.txt
exec venv/bin/python code/src/producer.py --csv transactions.csv \
    --dataset "$DATASET_NAME" --region "$AWS_DEFAULT_REGION" \
    --stream "$STREAM_NAME" --rate "$RATE" --batch-size 5 \
    --checkpoint /opt/fraud/state/checkpoint.json --loop
SCRIPT
chmod 750 /opt/fraud/start.sh
chown -R ec2-user:ec2-user /opt/fraud
cat > /etc/systemd/system/fraud-producer.service <<'SERVICE'
[Unit]
Description=Dataset replay to Kinesis
Wants=network-online.target
After=network-online.target
StartLimitIntervalSec=0
[Service]
Type=simple
User=ec2-user
WorkingDirectory=/opt/fraud
EnvironmentFile=/opt/fraud/config.env
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/fraud/start.sh
Restart=on-failure
RestartSec=30
NoNewPrivileges=true
[Install]
WantedBy=multi-user.target
SERVICE
systemctl daemon-reload
systemctl enable --now fraud-producer
'''
resources = {
    "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.42.0.0/16", "EnableDnsSupport": True, "EnableDnsHostnames": True}},
    "Gateway": {"Type": "AWS::EC2::InternetGateway"},
    "GatewayAttachment": {"Type": "AWS::EC2::VPCGatewayAttachment", "Properties": {"VpcId": ref("Vpc"), "InternetGatewayId": ref("Gateway")}},
    "Subnet": {"Type": "AWS::EC2::Subnet", "Properties": {"VpcId": ref("Vpc"), "CidrBlock": "10.42.1.0/24", "MapPublicIpOnLaunch": True, "AvailabilityZone": {"Fn::Select": [0, {"Fn::GetAZs": ""}]}}},
    "RouteTable": {"Type": "AWS::EC2::RouteTable", "Properties": {"VpcId": ref("Vpc")}},
    "Route": {"Type": "AWS::EC2::Route", "DependsOn": "GatewayAttachment", "Properties": {"RouteTableId": ref("RouteTable"), "DestinationCidrBlock": "0.0.0.0/0", "GatewayId": ref("Gateway")}},
    "SubnetRoute": {"Type": "AWS::EC2::SubnetRouteTableAssociation", "Properties": {"SubnetId": ref("Subnet"), "RouteTableId": ref("RouteTable")}},
    "SecurityGroup": {"Type": "AWS::EC2::SecurityGroup", "Properties": {"GroupDescription": "Producer outbound only; manage with Session Manager", "VpcId": ref("Vpc"), "SecurityGroupEgress": [{"IpProtocol": "-1", "CidrIp": "0.0.0.0/0"}]}},
    "DataBucket": {"Type": "AWS::S3::Bucket", "Properties": {"PublicAccessBlockConfiguration": {"BlockPublicAcls": True, "BlockPublicPolicy": True, "IgnorePublicAcls": True, "RestrictPublicBuckets": True}, "BucketEncryption": {"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}}},
    "BucketPolicy": {"Type": "AWS::S3::BucketPolicy", "Properties": {"Bucket": ref("DataBucket"), "PolicyDocument": {"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Principal": "*", "Action": "s3:*", "Resource": [att("DataBucket", "Arn"), sub("${DataBucket.Arn}/*")], "Condition": {"Bool": {"aws:SecureTransport": "false"}}}]}}},
    "Stream": {"Type": "AWS::Kinesis::Stream", "Properties": {"RetentionPeriodHours": 24, "ShardCount": 1, "StreamModeDetails": {"StreamMode": "PROVISIONED"}}},
    "ProducerRole": {"Type": "AWS::IAM::Role", "Condition": "CreateRole", "Properties": {"AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}]}, "ManagedPolicyArns": [sub("arn:${AWS::Partition}:iam::aws:policy/AmazonSSMManagedInstanceCore")], "Policies": [{"PolicyName": "ProducerOnly", "PolicyDocument": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["kinesis:PutRecords"], "Resource": att("Stream", "Arn")}, {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": [sub("${DataBucket.Arn}/data/*"), sub("${DataBucket.Arn}/artifacts/*")]}]}}]}},
    "ProducerProfile": {"Type": "AWS::IAM::InstanceProfile", "Condition": "CreateRole", "Properties": {"Roles": [ref("ProducerRole")]}},
    "ProducerInstance": {"Type": "AWS::EC2::Instance", "DependsOn": ["Route", "SubnetRoute"], "Properties": {"ImageId": ref("AmiId"), "InstanceType": ref("InstanceType"), "IamInstanceProfile": {"Fn::If": ["CreateRole", ref("ProducerProfile"), ref("ExistingInstanceProfileName")]}, "MetadataOptions": {"HttpTokens": "required", "HttpEndpoint": "enabled"}, "NetworkInterfaces": [{"DeviceIndex": "0", "AssociatePublicIpAddress": True, "SubnetId": ref("Subnet"), "GroupSet": [ref("SecurityGroup")]}], "BlockDeviceMappings": [{"DeviceName": "/dev/xvda", "Ebs": {"VolumeSize": 12, "VolumeType": "gp3", "Encrypted": True, "DeleteOnTermination": True}}], "Tags": [{"Key": "Name", "Value": sub("${AWS::StackName}-producer")}], "UserData": {"Fn::Base64": sub(userdata)}}},
}
template = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "Credit card fraud classroom demo: EC2 replay producer, Kinesis, S3, isolated public subnet. No inference/backend deployed.",
    "Parameters": {
        "AmiId": {"Type": "AWS::SSM::Parameter::Value<AWS::EC2::Image::Id>", "Default": "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"},
        "InstanceType": {"Type": "String", "Default": "t3.micro", "AllowedValues": ["t3.micro", "t3.small"]},
        "Rate": {"Type": "Number", "Default": 5, "MinValue": 1, "MaxValue": 100},
        "DatasetName": {"Type": "String", "Default": "fraudTest", "AllowedPattern": "[a-zA-Z0-9_-]+"},
        "ExistingInstanceProfileName": {"Type": "String", "Default": "", "Description": "Optional lab-provided INSTANCE PROFILE name, not role ARN. Must permit S3 reads, Kinesis writes and SSM."},
    },
    "Conditions": {"CreateRole": {"Fn::Equals": [ref("ExistingInstanceProfileName"), ""]}},
    "Resources": resources,
    "Outputs": {
        "StreamName": {"Value": ref("Stream")}, "StreamArn": {"Value": att("Stream", "Arn")},
        "DataBucketName": {"Value": ref("DataBucket")}, "InstanceId": {"Value": ref("ProducerInstance")},
        "Region": {"Value": ref("AWS::Region")}, "EventSchemaVersion": {"Value": "1.0"},
    },
}
(ROOT / "infra" / "template.json").write_text(json.dumps(template, indent=2) + "\n")
print("Generated infra/template.json")
