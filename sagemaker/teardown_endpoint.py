import boto3
from botocore.exceptions import ClientError

sm = boto3.client("sagemaker")
name = "fraud-detector-serverless"

for label, fn, kwargs in [
    ("endpoint", sm.delete_endpoint, {"EndpointName": name}),
    ("endpoint config", sm.delete_endpoint_config, {"EndpointConfigName": name}),
]:
    try:
        fn(**kwargs)
        print("deleted", label, name)
    except ClientError as e:
        print(label, "->", e.response["Error"]["Message"])