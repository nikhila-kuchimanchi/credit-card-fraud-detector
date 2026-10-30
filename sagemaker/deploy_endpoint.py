import os
from dotenv import load_dotenv
import sagemaker
from sagemaker.sklearn.model import SKLearnModel
from sagemaker.serverless import ServerlessInferenceConfig

load_dotenv()
BUCKET = os.environ["FRAUD_BUCKET"]
ROLE = os.environ["FRAUD_SM_ROLE_ARN"]
MODEL_DATA = os.environ["FRAUD_MODEL_DATA"]   # the s3://.../output/model.tar.gz URI
ENDPOINT_NAME = "fraud-detector-serverless"

session = sagemaker.Session(default_bucket=BUCKET)

model = SKLearnModel(
    model_data=MODEL_DATA,
    role=ROLE,
    entry_point="inference.py",
    source_dir="src",
    framework_version="1.2-1",
    py_version="py3",
    code_location=f"s3://{BUCKET}/code",
    sagemaker_session=session,
)

model.deploy(
    serverless_inference_config=ServerlessInferenceConfig(
        memory_size_in_mb=2048, max_concurrency=5
    ),
    endpoint_name=ENDPOINT_NAME,
)
print("deployed:", ENDPOINT_NAME)