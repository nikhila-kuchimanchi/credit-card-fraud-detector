# Fraud Model on AWS SageMaker

A LightGBM fraud classifier, trained as a **SageMaker training job** (script mode) and served from a **SageMaker serverless endpoint**, plus the **Lambda function that scores Kinesis stream events through it**.

```
 Kinesis stream  ──►  Lambda (fraud-scorer)  ──►  SageMaker serverless endpoint  ──►  score 0..1
 raw transactions     ★ builds the features       fraud-detector-serverless
                      ★ calls the endpoint
                      ★ applies the threshold
```

> ## ★ The Lambda does the feature work
>
> **The endpoint does not accept raw transactions.** It accepts exactly **eight model features**. Whoever feeds the stream only has to send *raw* transaction fields. **The Lambda function turns each raw record into the eight features and calls the endpoint.** If the Lambda sends anything else, the endpoint returns an error.
>
> Jump to [Step 6: The Lambda (Kinesis → SageMaker)](#9-step-6-the-lambda-kinesis--sagemaker). The code is in `lambda_function.py`.

---

## Table of contents

1. [Results](#1-results)
2. [Integration contract](#2-integration-contract)
3. [Prerequisites](#3-prerequisites)
4. [Step 1: Local setup](#4-step-1-local-setup)
5. [Step 2: AWS setup](#5-step-2-aws-setup)
6. [Step 3: Train on SageMaker](#6-step-3-train-on-sagemaker)
7. [Step 4: Deploy the endpoint](#7-step-4-deploy-the-endpoint)
8. [Step 5: Test and measure latency](#8-step-5-test-and-measure-latency)
9. [Step 6: The Lambda (Kinesis → SageMaker)](#9-step-6-the-lambda-kinesis--sagemaker)
10. [Cleanup (do not skip)](#10-cleanup-do-not-skip)
11. [Troubleshooting](#11-troubleshooting)
12. [Security notes](#12-security-notes)

---

## 1. Results

All numbers are on a **time-held-out test set** (trained on earlier transactions, evaluated on later ones, test set used once). The data is simulated, so these numbers are optimistic and do not predict real-world performance. Fraud is rare (about 0.4% of test rows), so accuracy is meaningless: an "everything is legit" model scores over 99% accuracy and catches no fraud. We use PR-AUC and precision at a fixed alert budget instead.

### LightGBM vs. baseline

| Metric (test set) | Logistic regression baseline | LightGBM (deployed) |
|---|---|---|
| PR-AUC | 0.307 | **0.878** |
| Precision @ top 100 alerts | 0.55 | **1.00** |
| Precision @ top 500 alerts | not measured | **0.998** |
| Precision @ top 1,000 alerts | not measured | **0.995** |

A random model's PR-AUC equals the fraud rate (about 0.004). The baseline is logistic regression with class weights. Validation PR-AUC was 0.913, so expect a small drop on newer data.

### Threshold choice

The endpoint returns a score between 0 and 1, and the Lambda decides what counts as an alert.

| Threshold | Test precision | Test recall | Test F1 |
|---|---|---|---|
| 0.50 (default) | 0.895 | 0.748 | 0.815 |
| 0.339 (best F1 on validation) | 0.862 | 0.778 | 0.817 |

Tuning gave no real gain, so **0.5 is a fine default**. The Lambda reads it from the `ALERT_THRESHOLD` environment variable, so it can change without redeploying the model.

### Training and serving

| Item | Value |
|---|---|
| Training job | 1x `ml.m5.large`, **159 billable seconds** (well under 1 cent) |
| Container | SageMaker scikit-learn `1.2-1` (Python 3.9) |
| LightGBM version | **4.6.0** (must match between training and serving) |
| Endpoint | Serverless, 2048 MB memory, max concurrency 5 |
| First call after deploy | 381 ms |
| Warm latency (single row, from a laptop, includes network) | p50 = 81 ms |
| Cold start after idle | not measured yet |

### Not done

- The Lambda is written and passes a local test with a mocked endpoint, but has **not been deployed or run against the real stream yet**.
- Probability calibration, card-level history features, sequence models.
- Cold-start and p95 latency measurements.
- Variance across several time splits (one split gives one estimate).

---

## 2. Integration contract

**Endpoint:** `fraud-detector-serverless` (region `us-east-1`)

**Permission the caller (the Lambda) needs:** `sagemaker:InvokeEndpoint` on this endpoint's ARN only (least privilege).

**Request** (`ContentType: application/json`). One request can score several transactions:

```json
{
  "instances": [
    {"hour": 23, "weekday": 4, "age": 41, "distance_km": 83.2,
     "amt": 912.50, "category": "shopping_net", "gender": "F", "city_pop": 12000}
  ]
}
```

**Response:**

```json
{"scores": [0.9894]}
```

One score per instance, in the same order. Higher means more likely fraud.

**The eight model features** (exactly these, any order, extra keys are ignored):

| Feature | Type | How the Lambda computes it |
|---|---|---|
| `hour` | int | hour (0 to 23) of the transaction timestamp |
| `weekday` | int | day of week, Monday = 0 |
| `age` | int | years between date of birth and the transaction date |
| `distance_km` | float | haversine distance between home and merchant coordinates |
| `amt` | float | **raw** amount, not log-transformed |
| `category` | string | merchant category, sent as plain text |
| `gender` | string | sent as plain text |
| `city_pop` | int | city population |

Behavior to know about:
- A **missing feature** returns an error naming it.
- A **category value never seen in training** is treated as missing, which LightGBM handles. It does not crash.
- Only `application/json` is accepted.
- **Features must be computed exactly as in training.** A different timezone for `hour`, or a log-transformed `amt`, silently produces wrong scores without any error.

**Training data format** (if you retrain): `train.parquet` and `val.parquet` with the eight columns above plus an integer label `is_fraud`. `category` and `gender` are pandas `category` columns. The last 20% of the training period by time is used as `val.parquet` for early stopping.

---

## 3. Prerequisites

- An AWS account. Use an **IAM user**, never root credentials.
- Python 3.9 to 3.11 and the AWS CLI v2.
- macOS only: `brew install libomp` (LightGBM needs it for local runs).
- Region used here: **us-east-1**. Use one region everywhere.

---

## 4. Step 1: Local setup

Project layout:

```
fraud-sagemaker/
├── src/
│   ├── train.py              # training script (SageMaker script mode)
│   ├── inference.py          # model_fn / input_fn / predict_fn / output_fn
│   └── requirements.txt      # installed INSIDE the SageMaker containers
├── lambda_function.py        # ★ Kinesis -> features -> SageMaker (deployed to Lambda)
├── launch_training.py        # starts the training job
├── deploy_endpoint.py        # creates the serverless endpoint
├── teardown_endpoint.py      # deletes the endpoint
├── invoke_test.py            # sends test requests, measures latency
├── requirements-local.txt    # installed on YOUR machine
├── .env                      # your private values (never commit)
├── .env.example              # fake values, safe to commit
└── .gitignore
```

**Two dependency files, on purpose.**

`requirements-local.txt` (your machine):

```
sagemaker<3
boto3
python-dotenv
scikit-learn
pandas
numpy
pyarrow
lightgbm
joblib
```

`src/requirements.txt` (installed inside the SageMaker containers):

```
lightgbm==4.6.0
pyarrow
joblib
```

Important:
- Pin `sagemaker<3`. The scripts use the v2 API (`SKLearn`, `SKLearnModel`).
- **Do not** add `scikit-learn`, `pandas` or `numpy` to `src/requirements.txt`. The container already has them, and installing newer versions over the top can break loading the model.
- **Pin `lightgbm` to the version the training job installed (4.6.0)** and make sure it exists for the container's Python 3.9. A version copied from a laptop (for example 4.7.0) may not exist for 3.9, and the endpoint then crashes at startup.
- The Lambda needs **no extra packages**. `boto3` is already included in the Lambda Python runtime.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-local.txt
```

`.gitignore`:

```
.venv/
.env
data/
model_test/
*.parquet
*.tar.gz
__pycache__/
.ipynb_checkpoints/
```

`.env.example` (commit this one; copy to `.env` and fill in real values):

```
FRAUD_BUCKET=your-bucket-name
FRAUD_SM_ROLE_ARN=arn:aws:iam::123456789012:role/your-role-name
FRAUD_MODEL_DATA=s3://your-bucket-name/model_artifacts/<job-name>/output/model.tar.gz
```

`FRAUD_MODEL_DATA` is filled in after training (Step 3).

Configure credentials for your IAM user (never paste keys into code or chat):

```bash
aws configure                    # region: us-east-1
aws sts get-caller-identity      # should show your user ARN, not ":root"
```

Your user needs permission to create SageMaker training jobs, models and endpoints, to read and write the bucket, and `iam:PassRole` on the training role below.

---

## 5. Step 2: AWS setup

### 2a. Budget alert (before creating anything)

Open **AWS Budgets** and create a monthly cost budget (we used $10) with email alerts: **actual** at 50% and 80%, **forecasted** at 100%. Billing data lags by hours, and AWS has no hard spending cap, so a budget can only react.

### 2b. S3 bucket

Create a bucket with **Block all public access ON**, and these folders:

```
training_data/
model_artifacts/
```

(`code/` is created automatically when the SDK uploads your script.)

### 2c. IAM role for SageMaker

A role is like a hat. Its **permissions** say what it can do, and its **trust policy** says who may put it on. SageMaker puts this hat on the temporary training machine so it can use S3 without any access keys in your code.

1. **IAM → Policies → Create policy → JSON**, named `fraud-sagemaker-training-policy`. Replace `YOUR-BUCKET`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET/training_data/*",
        "arn:aws:s3:::YOUR-BUCKET/code/*",
        "arn:aws:s3:::YOUR-BUCKET/model_artifacts/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::YOUR-BUCKET"
    },
    {
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::YOUR-BUCKET/model_artifacts/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/sagemaker/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchCheckLayerAvailability"
      ],
      "Resource": "*"
    }
  ]
}
```

Why each part exists:
- `GetObject` on `code/*`: the SDK uploads your script there and the job downloads it. Missing it gives `AccessDenied`.
- `GetObject` on `model_artifacts/*`: the endpoint loads `model.tar.gz` from there.
- `ListBucket` applies to the **bucket itself** (no `/*`). It is a separate action, and forgetting it also gives `AccessDenied`.
- `PutObject` is limited to `model_artifacts/*`.
- ECR actions let SageMaker pull its container image.

2. **IAM → Roles → Create role → AWS service → SageMaker.** Attach **your** policy, not `AmazonSageMakerFullAccess` (far too broad). Name it `fraud-sagemaker-training-role`.
3. Open the role → **Trust relationships** and confirm:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "sagemaker.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

4. Copy the role's full ARN into `FRAUD_SM_ROLE_ARN` in `.env`.

### 2d. Service quotas

New AWS accounts can have a quota of **0** for some instance types, which fails the job with `ResourceLimitExceeded`. In **Service Quotas → Amazon SageMaker**, check:

- `ml.m5.large for training job usage` (needs at least 1)
- endpoint or serverless concurrency quotas, if the deploy later complains

If a quota is 0, use **Request increase at account level**. Small requests are often approved within minutes, but it can take a day.

---

## 6. Step 3: Train on SageMaker

**How script mode works.** You write a normal `train.py`. SageMaker starts a temporary machine, runs your script in its own container, and shuts the machine down when the script ends, so you pay only for the minutes it ran. The script follows three conventions:

- hyperparameters arrive as command-line arguments
- data is read from the folder in `SM_CHANNEL_TRAIN`
- the model is saved to `SM_MODEL_DIR`, and everything saved there is packed into `model.tar.gz` and uploaded to S3

`train.py` also saves `features.json` (column order and category levels) next to the model, and the endpoint uses it to encode requests exactly like training.

### 3a. Upload the training data

```bash
aws s3 cp data/train.parquet s3://YOUR-BUCKET/training_data/train.parquet
aws s3 cp data/val.parquet   s3://YOUR-BUCKET/training_data/val.parquet
aws s3 ls s3://YOUR-BUCKET/training_data/
```

The listing should show both files with non-zero sizes. (The data format is described in the [Integration contract](#2-integration-contract).)

### 3b. Free local test first

```bash
python src/train.py --train-dir data --model-dir model_test
```

Expect a validation PR-AUC of about 0.91 and P@100 of 1.0. Testing locally costs nothing and catches bugs before you pay for a machine.

### 3c. Launch the job

`launch_training.py` reads `FRAUD_BUCKET` and `FRAUD_SM_ROLE_ARN` from `.env` and starts an `SKLearn` estimator with:

- `entry_point="train.py"`, `source_dir="src"` (so `src/requirements.txt` is installed in the container)
- `framework_version="1.2-1"`, `py_version="py3"`
- `instance_type="ml.m5.large"`, `instance_count=1`
- `code_location=s3://<bucket>/code` and `output_path=s3://<bucket>/model_artifacts`
- `max_run=1800` (hard stop at 30 minutes, so a bug cannot run up the bill)
- hyperparameters `n-estimators=500`, `learning-rate=0.05`, `num-leaves=63` (early stopping usually ends training near iteration 177)

```bash
python launch_training.py
```

The container's output streams into your terminal. Success looks like:

```
val_pr_auc=0.9128
val_p_at_100=1.0000
Training seconds: 159
Billable seconds: 159
```

### 3d. Find the model artifact

```bash
aws s3 ls s3://YOUR-BUCKET/model_artifacts/ --recursive
```

Find the line ending in `output/model.tar.gz` (about 530 KB) and put its full S3 URI into `FRAUD_MODEL_DATA` in `.env`.

---

## 7. Step 4: Deploy the endpoint

### Serverless or real-time?

| | Serverless | Real-time |
|---|---|---|
| Billing | per request (plus a little storage) | **every hour it is up**, even with zero traffic |
| Latency | possible **cold start** after idle time | consistently fast |
| Fits | bursty, low-concurrency traffic | steady traffic with strict latency needs |

We chose **serverless**: traffic here is bursty and low, and a forgotten real-time endpoint is the easiest way to get a surprise bill. The tradeoff is a possible cold-start delay on the first request after idle time.

### Deploy

```bash
python deploy_endpoint.py
```

This creates a model, an endpoint config and an endpoint named `fraud-detector-serverless`. It takes a few minutes. The progress bar ends with `!` and then it prints `deployed: fraud-detector-serverless`.

The endpoint runs `src/inference.py`, which implements four functions:

| Function | Job |
|---|---|
| `model_fn` | loads `model.joblib` and `features.json` from the unpacked archive |
| `input_fn` | parses the JSON request body into a DataFrame |
| `predict_fn` | checks the columns, fixes the order, applies the saved category levels, returns scores |
| `output_fn` | returns `{"scores": [...]}` as JSON |

Check the status at any time:

```bash
aws sagemaker describe-endpoint --endpoint-name fraud-detector-serverless \
  --query "[EndpointStatus,FailureReason]"
```

---

## 8. Step 5: Test and measure latency

```bash
python invoke_test.py
```

It sends five known rows (three fraud, two legit) and then 30 single-row calls. Expected result: fraud rows score high (we saw 0.989, 0.956, 0.999) and legit rows score about 0, plus a latency summary (we saw p50 = 81 ms from a laptop).

To measure a **cold start**, wait 10 to 15 minutes without calling the endpoint and run it again. The first-call time is the cold-start cost.

### Logs

Endpoint logs are in CloudWatch under `/aws/sagemaker/Endpoints/fraud-detector-serverless`, training logs under `/aws/sagemaker/TrainingJobs`:

```bash
aws logs tail /aws/sagemaker/Endpoints/fraud-detector-serverless --since 1h
```

When something fails, the real reason is almost always in these logs, not in the terminal.

---

## 9. Step 6: The Lambda (Kinesis → SageMaker)

> **★ This Lambda is the bridge between the stream and the model, and it owns the feature computation.** The stream carries raw transactions. The endpoint only understands the eight features in the [Integration contract](#2-integration-contract). **`lambda_function.py` converts one into the other.** Nothing else in the pipeline does this.

### What it does, in order

1. **Decodes** each Kinesis record. Records arrive as a **list**, and each record's `data` is **base64-encoded JSON**.
2. **Builds the eight features** from the raw fields (`hour`, `weekday`, `age`, `distance_km`, `amt`, `category`, `gender`, `city_pop`), using exactly the logic used in training.
3. **Calls the endpoint once per batch** with `{"instances": [...]}`.
4. **Applies `ALERT_THRESHOLD`** and logs one JSON line per transaction (`trans_id`, `score`, `alert`), never the raw card data.
5. **Handles errors** so one bad record can't stall the stream (see below).

### ⚠ The one assumption you must check

`lambda_function.py` assumes each stream record is JSON containing these **raw fields**:

| Raw field | Example | Used for |
|---|---|---|
| `trans_date_trans_time` | `"2020-06-21 22:14:25"` | `hour`, `weekday`, `age` |
| `dob` | `"1988-03-09"` | `age` |
| `lat`, `long` | `36.0788`, `-81.1781` | `distance_km` (cardholder home) |
| `merch_lat`, `merch_long` | `36.0113`, `-82.0483` | `distance_km` (merchant) |
| `amt` | `912.5` | `amt` (**raw**, never log-transformed) |
| `category`, `gender` | `"shopping_net"`, `"F"` | passed as plain text |
| `city_pop` | `3495` | `city_pop` |
| `trans_num` (optional) | `"abc123"` | only used to label log lines |

**Agree on this record format with whoever builds the stream.** If their field names differ, change **only** the `build_instance()` function in `lambda_function.py`. Two things there can silently ruin the scores without raising any error:

- **Timezone:** `hour` must use the same timezone convention as the training data (naive local timestamps).
- **Amount:** send the raw `amt`.

### Error handling (already in the code)

| Situation | What the Lambda does |
|---|---|
| Record is not valid base64/JSON, or a field is missing or malformed | Logs `bad_record` with the sequence number and **skips it**. Retrying a malformed record would block the shard forever. |
| The endpoint call fails (throttling, cold-start timeout, 5xx) | Logs the error and returns **all valid records as failed**, so Kinesis retries them. |
| The endpoint returns the wrong number of scores | Treated as an endpoint failure and retried. |

> Returning failed records only works if the trigger has **"Report batch item failures"** turned on (see below). If it is off, Kinesis ignores the list and treats the batch as successful, so failed records are silently lost.

### 6a. Create the Lambda's own IAM role

Do **not** reuse the SageMaker training role. Different service, different powers.

1. **IAM → Roles → Create role → AWS service → Lambda.** Name it `fraud-lambda-role`. Its trust policy names `lambda.amazonaws.com` (not `sagemaker.amazonaws.com`).
2. Attach the AWS-managed policy **`AWSLambdaKinesisExecutionRole`**. It allows reading the Kinesis stream and writing CloudWatch logs.
3. Add an **inline policy** that lets it call this endpoint and nothing else:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sagemaker:InvokeEndpoint",
      "Resource": "arn:aws:sagemaker:us-east-1:YOUR-ACCOUNT-ID:endpoint/fraud-detector-serverless"
    }
  ]
}
```

### 6b. Create the function

In **Lambda → Create function → Author from scratch**:

| Setting | Value |
|---|---|
| Name | `fraud-scorer` |
| Runtime | Python 3.12 |
| Execution role | existing role `fraud-lambda-role` |
| Code | paste `lambda_function.py` and click **Deploy** |
| Handler | `lambda_function.lambda_handler` |
| Timeout | **60 seconds** (a cold-start call can take several seconds) |
| Memory | 256 MB |

**Configuration → Environment variables:**

| Key | Value |
|---|---|
| `ENDPOINT_NAME` | `fraud-detector-serverless` |
| `ALERT_THRESHOLD` | `0.5` |

### 6c. Connect the Kinesis stream

**Function → Add trigger → Kinesis**, select the stream your teammates created, and set:

| Setting | Value | Why |
|---|---|---|
| Batch size | 100 | one endpoint call scores up to 100 transactions |
| Batch window | 1 to 5 seconds | lets small batches fill up |
| Starting position | Latest | only new events |
| **Report batch item failures** | **ON** | required for the retry behavior above |
| Split batch on error | ON | isolates a problematic record |
| Retry attempts | 3 | stops infinite retries |
| Maximum record age | 1 hour | drops events that are too old |

The Lambda's role needs read access to that stream. `AWSLambdaKinesisExecutionRole` grants it for streams in the account. If your teammates' stream is in another account, ask them for cross-account access.

### 6d. Test it before connecting the real stream

Use the Lambda console's **Test** button with a hand-made Kinesis event. This script prints one, with a correctly base64-encoded record:

```python
import base64, json

rec = {
    "trans_date_trans_time": "2020-06-21 22:14:25",
    "dob": "1988-03-09",
    "lat": 36.0788, "long": -81.1781,
    "merch_lat": 36.011293, "merch_long": -82.048315,
    "amt": 912.5, "category": "shopping_net", "gender": "F",
    "city_pop": 3495, "trans_num": "test-001",
}
event = {"Records": [{"kinesis": {
    "sequenceNumber": "1",
    "data": base64.b64encode(json.dumps(rec).encode()).decode(),
}}]}
print(json.dumps(event, indent=2))
```

Paste the printed JSON as the test event. Expected result: `{"batchItemFailures": []}`, and a `scored` log line with a score between 0 and 1.

Then check **CloudWatch → `/aws/lambda/fraud-scorer`**:
- `scored` and `batch_done` lines mean success.
- The `REPORT` line shows duration and memory. The first call after idle includes `Init Duration` (Lambda cold start) and may include the endpoint's own cold start.
- `bad_record` means the record format doesn't match the table above.
- `endpoint call failed` includes a traceback with the reason (most often a missing `sagemaker:InvokeEndpoint` permission, a wrong endpoint name, or a throttle).

### 6e. Concurrency: the endpoint has a limit

The endpoint was deployed with **max concurrency 5**. Kinesis runs one Lambda at a time per shard (more if you raise the parallelization factor), and each one calls the endpoint. If the stream has more concurrent Lambdas than the endpoint can serve, calls are throttled. The Lambda retries them, but latency suffers. If you see throttling, raise `max_concurrency` in `deploy_endpoint.py`, redeploy, and check the serverless concurrency quota in Service Quotas.

### 6f. What happens to alerts?

The Lambda currently **logs** every score and marks alerts with `"alert": true`. Where alerts should go next (SNS notification, SQS, a DynamoDB table, a dashboard) is a team decision. The place to add it is marked with a `TODO` in the loop at the end of `lambda_handler`.

---

## 10. Cleanup (do not skip)

A real-time endpoint bills every hour. A serverless one bills per request, but still delete what you are not using. `teardown_endpoint.py`:

```python
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
```

```bash
python teardown_endpoint.py
aws sagemaker list-endpoints --name-contains fraud-detector
aws sagemaker list-endpoint-configs --name-contains fraud-detector
```

Both lists should come back empty. Deleting the endpoint does **not** delete the model object, the S3 files, the CloudWatch logs, or the Lambda. If you only want to pause, **disable the Kinesis trigger** on the Lambda before deleting the endpoint. Otherwise the Lambda keeps calling a deleted endpoint, fails, and Kinesis keeps retrying.

---

## 11. Troubleshooting

Errors we actually hit, and what fixed them.

| Error | Cause | Fix |
|---|---|---|
| `OSError: Library not loaded: libomp.dylib` (macOS, local run) | LightGBM needs the OpenMP library | `brew install libomp` (system-wide, not inside the venv). SageMaker is unaffected. |
| `ModuleNotFoundError: No module named 'sklearn'` | The venv lacks local-only packages | `pip install -r requirements-local.txt` |
| `ResourceLimitExceeded: ... 'ml.m5.large for training job usage' is 0` | New-account quota is 0 | Service Quotas → SageMaker → request an increase, or use an instance type that has quota |
| `Could not assume role ... allows principal 'sagemaker.amazonaws.com'` | Wrong role ARN in `.env`, or the trust policy does not name SageMaker | `aws iam get-role --role-name ...`, then fix the ARN or the trust policy |
| Training job fails with `AccessDenied` on S3 | Role cannot read `code/*` or `training_data/*`, or `ListBucket` is missing | Use the policy in Step 2c |
| Endpoint fails with "model process exited"; log says `No matching distribution found for lightgbm==4.7.0` | The pin came from a laptop. The container runs Python 3.9 and that version is not available for it | Pin the **version the training job installed** (see the `/aws/sagemaker/TrainingJobs` log, "Successfully installed lightgbm-4.6.0") |
| Endpoint fails with `Invalid requirement: 'lightgbm==X.Y.Z'` | A placeholder was left in `src/requirements.txt` | Use a real version number |
| `Cannot create already existing endpoint configuration` | A failed deploy leaves its config behind | Run `teardown_endpoint.py`, confirm both lists are empty, redeploy |

Lambda problems (not hit yet, the likely causes):

| Symptom | Likely cause |
|---|---|
| `AccessDeniedException ... InvokeEndpoint` in the Lambda log | The inline policy is missing, or its endpoint ARN has the wrong account ID, region or name |
| `bad_record ... missing fields` | The stream's field names differ from the table in Step 6. Edit `build_instance()`. |
| Lambda times out | Timeout too low for an endpoint cold start. Use 60 seconds or more. |
| `KeyError: 'ENDPOINT_NAME'` at startup | The environment variable is not set on the function |
| Failed records are never retried | "Report batch item failures" is off on the Kinesis trigger |
| Scores look wrong but nothing errors | A feature is computed differently from training (timezone for `hour`, or `amt` not raw) |

---

## 12. Security notes

- **Never commit** access keys, `.env`, data files or model files. Keys live in `~/.aws/credentials`, outside the project.
- An account ID or role ARN is not a credential, but do not publish yours. Keep them in `.env`.
- Use an IAM user, not root. For a real project, give the dev user a narrower policy than the broad permissions that make a learning project easy.
- The Lambda's role can invoke **only this endpoint**. Keep it that way.
- The Lambda logs transaction ids and scores, not raw card data. Keep it that way too.
- If a key is ever committed, **rotate it** (delete and recreate it). Removing it from the repo is not enough, because it stays in the git history.
- Keep Block Public Access ON for the bucket.
- Replace every `YOUR-BUCKET`, `YOUR-ACCOUNT-ID` and ARN in this guide with your own values.