"""Fraud scoring for the writer Lambda.

SCORER_MODE=placeholder (default): a simple rule-based score so the pipeline can
run end to end before the SageMaker model exists. It uses ONLY model features,
never ground_truth. It is not a trained model; do not report its accuracy.

SCORER_MODE=sagemaker: calls the endpoint in SAGEMAKER_ENDPOINT. Agreed contract
(to confirm with the model owner):
    request  {"features": {...13 feature fields...}}
    response {"fraud_score": 0.93, "model_version": "v1"}
"""

import json
import os

PLACEHOLDER_VERSION = "placeholder-rules-v0"
_NIGHT_HOURS = {22, 23, 0, 1, 2, 3}
_ONLINE_CATEGORIES = {"shopping_net", "misc_net", "grocery_pos"}
_runtime = None


def placeholder_score(features):
    score = 0.02
    amount = float(features["amount"])
    if amount > 1000:
        score += 0.5
    elif amount > 500:
        score += 0.35
    elif amount > 200:
        score += 0.1
    if int(features["hour"]) in _NIGHT_HOURS:
        score += 0.3
    if features["category"] in _ONLINE_CATEGORIES:
        score += 0.1
    return round(min(score, 0.99), 4)


def _sagemaker_score(features, endpoint):
    global _runtime
    if _runtime is None:
        import boto3
        _runtime = boto3.client("sagemaker-runtime")
    response = _runtime.invoke_endpoint(
        EndpointName=endpoint,
        ContentType="application/json",
        Body=json.dumps({"features": features}),
    )
    body = json.loads(response["Body"].read())
    return float(body["fraud_score"]), str(body.get("model_version", endpoint))


def score(features):
    """Return (fraud_score in [0, 1], model_version)."""
    if os.environ.get("SCORER_MODE", "placeholder") == "sagemaker":
        return _sagemaker_score(features, os.environ["SAGEMAKER_ENDPOINT"])
    return placeholder_score(features), PLACEHOLDER_VERSION
