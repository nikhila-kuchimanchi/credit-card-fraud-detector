import json, time
import boto3
import numpy as np
import pandas as pd

runtime = boto3.client("sagemaker-runtime", region_name="us-east-1")
NAME = "fraud-detector-serverless"

val = pd.read_parquet("data/val.parquet")
sample = pd.concat([val[val.is_fraud == 1].head(3), val[val.is_fraud == 0].head(2)])
rows = sample.drop(columns=["is_fraud"]).astype(object).to_dict(orient="records")

def invoke(instances):
    t0 = time.perf_counter()
    resp = runtime.invoke_endpoint(
        EndpointName=NAME,
        ContentType="application/json",
        Body=json.dumps({"instances": instances}, default=str),
    )
    ms = (time.perf_counter() - t0) * 1000
    return json.loads(resp["Body"].read())["scores"], ms

# 1) Correctness: 3 fraud rows + 2 legit rows
scores, ms = invoke(rows)
print("scores:", [round(s, 4) for s in scores])
print("labels:", sample["is_fraud"].tolist())
print(f"first call: {ms:.0f} ms")

# 2) Latency: 30 single-row calls
times = [invoke(rows[:1])[1] for _ in range(30)]
print(f"single row  p50={np.percentile(times, 50):.0f} ms")