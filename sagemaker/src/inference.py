import json, os
import joblib
import pandas as pd

def model_fn(model_dir):
    model = joblib.load(os.path.join(model_dir, "model.joblib"))
    with open(os.path.join(model_dir, "features.json")) as f:
        meta = json.load(f)
    return {"model": model, "features": meta["features"], "categories": meta["categories"]}

def input_fn(body, content_type):
    if content_type != "application/json":
        raise ValueError(f"Unsupported content type: {content_type}")
    payload = json.loads(body)
    return pd.DataFrame(payload["instances"])

def predict_fn(df, artifacts):
    feats = artifacts["features"]
    missing = [c for c in feats if c not in df.columns]
    if missing:
        raise ValueError(f"Missing features: {missing}")
    df = df[feats].copy()
    for c, levels in artifacts["categories"].items():
        df[c] = pd.Categorical(df[c].astype(str), categories=levels)
    return artifacts["model"].predict_proba(df)[:, 1]

def output_fn(scores, accept):
    return json.dumps({"scores": [float(s) for s in scores]}), "application/json"