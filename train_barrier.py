import argparse
import json
import os
from typing import Dict, List

import numpy as np

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pd = None

try:
    import pyarrow.parquet as pq  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pq = None

from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import classification_report, confusion_matrix
import joblib


EXCLUDE_COLS = {
    "target",
    "label",
    "horizonMs",
    "entryTime",
    "tpBps",
    "slBps",
    "eventType",
    "returnPct",
    "midReturnPct",
    "longReturnPct",
    "shortReturnPct",
    "absReturnPct",
    "lagMs",
    "targetMode",
    "targetField",
}


def iter_batches(path: str, chunk_rows: int):
    lower = path.lower()
    if lower.endswith(".csv"):
        yield from pd.read_csv(path, chunksize=chunk_rows)
        return
    if lower.endswith(".jsonl") or lower.endswith(".json"):
        yield from pd.read_json(path, lines=True, chunksize=chunk_rows)
        return
    if lower.endswith(".parquet"):
        if pq is None:
            raise SystemExit("pyarrow is required for parquet streaming.")
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=chunk_rows):
            yield batch.to_pandas()
        return
    raise SystemExit(f"Unsupported dataset format: {path}")


def load_dataset(path: str):
    if pd is None:
        raise SystemExit("pandas is required for training. Install requirements.txt first.")
    lower = path.lower()
    if lower.endswith(".parquet"):
        return pd.read_parquet(path)
    if lower.endswith(".csv"):
        return pd.read_csv(path)
    if lower.endswith(".jsonl") or lower.endswith(".json"):
        return pd.read_json(path, lines=True)
    raise SystemExit(f"Unsupported dataset format: {path}")


def load_dataset_sampled(path: str, sample_frac: float, max_rows: int, random_state: int, chunk_rows: int):
    if pd is None:
        raise SystemExit("pandas is required for training. Install requirements.txt first.")
    if sample_frac >= 1.0 and max_rows <= 0:
        return load_dataset(path)

    rng = np.random.RandomState(random_state)
    sampled = []
    total = 0

    for chunk in iter_batches(path, chunk_rows):
        if sample_frac < 1.0:
            chunk = chunk.sample(frac=sample_frac, random_state=rng)
        if chunk.empty:
            continue
        if max_rows > 0:
            remaining = max_rows - total
            if remaining <= 0:
                break
            if len(chunk) > remaining:
                chunk = chunk.sample(n=remaining, random_state=rng)
        sampled.append(chunk)
        total += len(chunk)
        if max_rows > 0 and total >= max_rows:
            break

    if not sampled:
        raise SystemExit("No rows loaded from dataset.")
    return pd.concat(sampled, ignore_index=True)


def prepare_matrix(df, target_column: str):
    if "entryTime" in df.columns:
        df = df.sort_values("entryTime").reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    df[target_column] = pd.to_numeric(df[target_column], errors="coerce")
    df = df[df[target_column].notna()].reset_index(drop=True)
    df = df.replace([np.inf, -np.inf], np.nan)

    exclude = set(EXCLUDE_COLS)
    exclude.add(target_column)
    exclude.add("symbol")
    numeric_cols = list(df.select_dtypes(include=[np.number]).columns)
    feature_cols = [col for col in numeric_cols if col not in exclude]
    if not feature_cols:
        raise SystemExit("No numeric feature columns found after exclusions.")

    X = df[feature_cols].astype(float)
    y = df[target_column].astype(int)
    return df, X, y, feature_cols


def main() -> None:
    parser = argparse.ArgumentParser(description="Train barrier classifier.")
    parser.add_argument("--data", default="barrier_dataset.parquet", help="Path to dataset file")
    parser.add_argument("--target-column", default="target", help="Target column")
    parser.add_argument("--model-out", default="barrier_model.joblib", help="Output model path")
    parser.add_argument("--meta-out", default="barrier_meta.json", help="Output metadata path")
    parser.add_argument("--test-frac", type=float, default=0.2)
    parser.add_argument("--max-depth", type=int, default=6)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--sample-frac", type=float, default=1.0, help="Sample fraction for large datasets")
    parser.add_argument("--max-rows", type=int, default=0, help="Max rows to load (0 = no limit)")
    parser.add_argument("--chunk-rows", type=int, default=200000, help="Chunk size when streaming input")
    args = parser.parse_args()

    df = load_dataset_sampled(
        args.data,
        sample_frac=max(0.0, min(float(args.sample_frac), 1.0)),
        max_rows=max(0, int(args.max_rows)),
        random_state=int(args.random_state),
        chunk_rows=max(1000, int(args.chunk_rows)),
    )
    if args.target_column not in df.columns:
        raise SystemExit(f"Target column not found: {args.target_column}")

    df, X, y, feature_cols = prepare_matrix(df, args.target_column)
    n = len(y)
    if n < 50:
        raise SystemExit("Dataset too small to train reliably.")

    test_frac = min(max(float(args.test_frac), 0.05), 0.5)
    split_idx = int(n * (1.0 - test_frac))
    split_idx = min(split_idx, n - 5)
    if split_idx <= 10:
        raise SystemExit("Not enough data after train/test split.")

    X_train = X.iloc[:split_idx]
    y_train = y.iloc[:split_idx]
    X_test = X.iloc[split_idx:]
    y_test = y.iloc[split_idx:]

    model = HistGradientBoostingClassifier(
        max_depth=args.max_depth,
        learning_rate=args.learning_rate,
        random_state=args.random_state,
    )
    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    report = classification_report(y_test, preds, output_dict=True, zero_division=0)
    matrix = confusion_matrix(y_test, preds).tolist()
    print(json.dumps({"classificationReport": report, "confusionMatrix": matrix}, indent=2))

    os.makedirs(os.path.dirname(args.model_out) or ".", exist_ok=True)
    joblib.dump(model, args.model_out)

    meta = {
        "dataPath": args.data,
        "targetColumn": args.target_column,
        "featureColumns": feature_cols,
        "testFraction": test_frac,
        "classificationReport": report,
        "confusionMatrix": matrix,
    }
    with open(args.meta_out, "w", encoding="utf-8") as handle:
        json.dump(meta, handle)
    print(f"Saved model to {args.model_out} and metadata to {args.meta_out}")


if __name__ == "__main__":
    main()
