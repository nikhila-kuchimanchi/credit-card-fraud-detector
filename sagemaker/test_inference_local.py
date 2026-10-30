import json, pandas as pd, sys
sys.path.insert(0, "src")
import inference

art = inference.model_fn("model_test")
val = pd.read_parquet("data/val.parquet").tail(5)
rows = val.drop(columns=["is_fraud"]).astype(object).to_dict(orient="records")
body = json.dumps({"instances": rows}, default=str)

df = inference.input_fn(body, "application/json")
scores = inference.predict_fn(df, art)
print(scores, val["is_fraud"].tolist())

val = pd.read_parquet("data/val.parquet")
val = pd.concat([val[val.is_fraud == 1].head(3), val[val.is_fraud == 0].head(2)])