import argparse, json, os
import joblib
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import average_precision_score

LABEL = "is_fraud"

def p_at_k(y, s, k):
    return np.asarray(y)[np.argsort(-s)[:k]].mean()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--n-estimators", type=int, default=500)
    p.add_argument("--learning-rate", type=float, default=0.05)
    p.add_argument("--num-leaves", type=int, default=63)
    p.add_argument("--train-dir", default=os.environ.get("SM_CHANNEL_TRAIN", "data"))
    p.add_argument("--model-dir", default=os.environ.get("SM_MODEL_DIR", "model"))
    a = p.parse_args()

    train = pd.read_parquet(os.path.join(a.train_dir, "train.parquet"))
    val = pd.read_parquet(os.path.join(a.train_dir, "val.parquet"))
    feats = [c for c in train.columns if c != LABEL]

    # Text columns become pandas categoricals; remember the levels for serving.
    cat_levels = {}
    for c in feats:
        if train[c].dtype == "object" or str(train[c].dtype) == "category":
            levels = sorted(train[c].astype(str).unique())
            cat_levels[c] = levels
            for d in (train, val):
                d[c] = pd.Categorical(d[c].astype(str), categories=levels)

    model = lgb.LGBMClassifier(
        n_estimators=a.n_estimators,
        learning_rate=a.learning_rate,
        num_leaves=a.num_leaves,
        subsample=0.8,
        subsample_freq=1,
        colsample_bytree=0.8,
        verbose=-1,
    )
    model.fit(
        train[feats], train[LABEL],
        eval_set=[(val[feats], val[LABEL])],
        eval_metric="average_precision",
        callbacks=[lgb.early_stopping(50), lgb.log_evaluation(50)],
    )

    s = model.predict_proba(val[feats])[:, 1]
    print(f"val_pr_auc={average_precision_score(val[LABEL], s):.4f}")
    print(f"val_p_at_100={p_at_k(val[LABEL], s, 100):.4f}")

    os.makedirs(a.model_dir, exist_ok=True)
    joblib.dump(model, os.path.join(a.model_dir, "model.joblib"))
    with open(os.path.join(a.model_dir, "features.json"), "w") as f:
        json.dump({"features": feats, "categories": cat_levels}, f)

if __name__ == "__main__":
    main()