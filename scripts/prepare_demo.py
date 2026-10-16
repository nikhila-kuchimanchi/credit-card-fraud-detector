"""Make a deliberately enriched presentation sample; never use for unbiased evaluation."""
import argparse
import csv
from pathlib import Path
import random


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("source", type=Path)
    p.add_argument("--output", type=Path, default=Path(".local/fraudDemo.csv"))
    args = p.parse_args()
    normal, fraud = [], []
    with args.source.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames
        for row in reader:
            if row["is_fraud"] == "1" and len(fraud) < 20:
                fraud.append(row)
            elif row["is_fraud"] == "0" and len(normal) < 180:
                normal.append(row)
            if len(normal) == 180 and len(fraud) == 20:
                break
    if len(normal) != 180 or len(fraud) != 20:
        raise ValueError("Need at least 180 non-fraud and 20 fraud rows")
    rows = normal + fraud
    random.Random(17).shuffle(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {args.output}: 200 rows, 10% known fraud, shuffled for a presentation.")
    print("This changes prevalence and temporal order. Do not use it for model evaluation.")


if __name__ == "__main__":
    main()
