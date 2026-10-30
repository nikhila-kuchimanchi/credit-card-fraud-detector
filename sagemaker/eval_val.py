import json, sys
import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score, roc_auc_score, log_loss, brier_score_loss,
    precision_recall_curve, confusion_matrix,
)

sys.path.insert(0, "src")
import inference

LABEL = "is_fraud"

art = inference.model_fn("model_test")


def score(df):
    # Same path the endpoint uses: JSON body -> input_fn -> predict_fn
    rows = df.drop(columns=[LABEL]).astype(object).to_dict(orient="records")
    X = inference.input_fn(json.dumps({"instances": rows}, default=str), "application/json")
    return inference.predict_fn(X, art)


def confusion_row(y, s, t):
    pred = (s >= t).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, pred).ravel()
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn)
    f1 = 2 * p * r / (p + r) if p + r else 0.0
    return p, r, f1, tn, fp, fn, tp


def evaluate(name, y, s, thresholds):
    n, pos = len(y), int(y.sum())
    print(f"\n{'=' * 20} {name.upper()} {'=' * 20}")
    print(f"rows={n}  fraud={pos}  base_rate={pos / n:.4%}")

    print("\n-- Threshold-free --")
    print(f"PR-AUC (avg precision): {average_precision_score(y, s):.4f}  (random ~ {pos / n:.4f})")
    print(f"ROC-AUC:                {roc_auc_score(y, s):.4f}")
    print(f"Log loss:               {log_loss(y, s):.5f}")
    print(f"Brier score:            {brier_score_loss(y, s):.5f}")

    print("\n-- Precision / recall @ top-k --")
    order = np.argsort(-s)
    for k in (100, 500, 1000, pos):
        top = y[order[:k]]
        print(f"top {k:>5}: precision={top.mean():.4f}  recall={top.sum() / pos:.4f}")

    print("\n-- Confusion matrix at thresholds --")
    print(f"{'thr':>8} {'prec':>7} {'recall':>7} {'F1':>7} {'TN':>8} {'FP':>6} {'FN':>6} {'TP':>6}")
    for label, t in thresholds:
        p, r, f1, tn, fp, fn, tp = confusion_row(y, s, t)
        print(f"{t:8.4f} {p:7.4f} {r:7.4f} {f1:7.4f} {tn:8d} {fp:6d} {fn:6d} {tp:6d}  <- {label}")

    print("\n-- Recall at fixed precision --")
    prec, rec, _ = precision_recall_curve(y, s)
    for target in (0.5, 0.8, 0.9, 0.95):
        ok = prec[:-1] >= target
        print(f"precision>={target:.2f}: recall={rec[:-1][ok].max() if ok.any() else 0:.4f}")


val = pd.read_parquet("data/val.parquet")
test = pd.read_parquet("data/test.parquet")
y_val, y_test = val[LABEL].to_numpy(), test[LABEL].to_numpy()
s_val, s_test = score(val), score(test)

# Pick the best-F1 threshold on val only, then apply it unchanged to test.
prec, rec, thr = precision_recall_curve(y_val, s_val)
f1s = 2 * prec * rec / np.clip(prec + rec, 1e-12, None)
t_val = float(thr[np.argmax(f1s[:-1])])
thresholds = [("0.5", 0.5), ("best-F1 on val", t_val)]

evaluate("val", y_val, s_val, thresholds)
evaluate("test", y_test, s_test, thresholds)
