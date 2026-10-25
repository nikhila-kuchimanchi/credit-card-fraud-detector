# Backend: writer Lambda, reader Lambda, HTTP API

```
EC2 producer -> Kinesis -> fraud-writer (Lambda) -> DynamoDB FraudTransactions -> fraud-reader (Lambda) -> HTTP API -> dashboard
                                 |  (placeholder score now, SageMaker later)
                                 +-> SNS email (optional)
```

| Piece | Code | Notes |
|---|---|---|
| Writer Lambda `fraud-writer` | `lambdas/writer/handler.py` | Kinesis trigger, batch 100, ReportBatchItemFailures, 3 retries |
| Scoring | `lambdas/writer/scorer.py` | `SCORER_MODE=placeholder` rule-based score (features only, never labels); `sagemaker` mode calls the endpoint |
| Reader Lambda `fraud-reader` | `lambdas/reader/handler.py` | GET only, uses `db/iam/reader-policy.json` |
| HTTP API `fraud-api` | created by the deploy script | CORS `*`, throttled to 10 req/s (burst 20) |
| IAM | `db/iam/*.json` + scoped Kinesis/SNS statements | Lambda roles `fraud-writer-role`, `fraud-reader-role` |

## API

| Request | Returns |
|---|---|
| `GET /transactions/recent?day=YYYY-MM-DD&limit=50` | newest transactions of that UTC day (default today) |
| `GET /transactions/flagged?limit=50` | newest predicted-fraud transactions |
| `GET /transactions/{event_id}` | one transaction |
| `GET /health` | `{"status": "ok"}` |

List responses are `{"items": [...], "count": n, "next": token}`; pass `next=token` for the next page.

## Run the whole pipeline (Git Bash, from the repo root)

```bash
python -m unittest discover -s tests                       # 35 offline tests

# 1) ingestion: EC2 + Kinesis (+ S3 upload of the 150 MB CSV), a few minutes
python scripts/manage.py deploy --data data/fraudTest.csv --stack fraud-ingestion --region us-east-1 --rate 2
python scripts/manage.py status --stack fraud-ingestion --region us-east-1   # wait until active
python scripts/verify_stream.py --stack fraud-ingestion --region us-east-1 --count 5

# 2) backend: roles, Lambdas, Kinesis trigger, API
python scripts/deploy_backend.py deploy --region us-east-1
python scripts/deploy_backend.py status --region us-east-1

# 3) look at results
curl "$(python -c "import json;print(json.load(open('.local/backend.json'))['ApiUrl'])")/transactions/flagged?limit=5"

# 4) clean up (ingestion is what costs money while running)
python scripts/manage.py destroy --stack fraud-ingestion --region us-east-1 --yes
python scripts/deploy_backend.py destroy --region us-east-1 --yes            # keeps the table
```

Placeholder score on the first 150,000 rows of fraudTest.csv: ~1.1% flagged,
recall ~61%, precision ~25%. It is a stand-in, not a model; do not report it as results.

`--alert-email you@umd.edu` adds SNS email alerts. At ~1% flagged and 2 events/s that is
roughly 70 emails per hour, so enable it only briefly for a demo (or raise `--threshold`).

## Cost while running (us-east-1, approximate)

Kinesis 1 shard ~$0.015/h, EC2 t3.micro + public IPv4 ~$0.015/h, plus DynamoDB writes
(~3 per event) and Lambda requests. A 1-2 hour test is well under $1. The ingestion
stack keeps billing until `manage.py destroy`; Lambdas/API cost nothing when idle.
