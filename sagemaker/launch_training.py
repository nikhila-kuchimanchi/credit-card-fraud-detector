import os
import sagemaker
from sagemaker.sklearn.estimator import SKLearn
from dotenv import load_dotenv

load_dotenv()
BUCKET = os.environ["FRAUD_BUCKET"]
ROLE = os.environ["FRAUD_SM_ROLE_ARN"]


session = sagemaker.Session(default_bucket=BUCKET)

est = SKLearn(
    entry_point="train.py",
    source_dir="src",
    role=ROLE,
    instance_type="ml.m5.large",
    instance_count=1,
    framework_version="1.2-1",
    py_version="py3",
    hyperparameters={"n-estimators": 500, "learning-rate": 0.05, "num-leaves": 63},
    code_location=f"s3://{BUCKET}/code",
    output_path=f"s3://{BUCKET}/model_artifacts",
    max_run=1800,  # hard stop after 30 min, so a bug can't run up the bill
    sagemaker_session=session,
)

est.fit({"train": f"s3://{BUCKET}/training_data/"})