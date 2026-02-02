import argparse
import json
import os
from typing import Dict, List, Tuple

import numpy as np

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pd = None

try:
    import pyarrow.parquet as pq  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pq = None

from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score
import joblib


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


BASE_EXCLUDE_COLS = {
    "target",
    "targetField",
    "returnPct",
    "midReturnPct",
    "longReturnPct",
    "shortReturnPct",
    "lagMs",
    "horizonMs",
    "entryTime",
}


def encode_symbol(df):
    if "symbol" not in df.columns:
        return df, []
    categories = sorted(str(s) for s in df["symbol"].dropna().unique())
    cat_dtype = pd.api.types.CategoricalDtype(categories=categories)
    df = df.copy()
    df["symbolCode"] = df["symbol"].astype(cat_dtype).cat.codes
    return df, categories


def prepare_matrix(df, target_column: str):
    if "entryTime" in df.columns:
        df = df.sort_values("entryTime").reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    df, symbol_categories = encode_symbol(df)
    df[target_column] = pd.to_numeric(df[target_column], errors="coerce")
    df = df[df[target_column].notna()].reset_index(drop=True)

    df = df.replace([np.inf, -np.inf], np.nan)

    exclude = set(BASE_EXCLUDE_COLS)
    exclude.add(target_column)
    exclude.add("symbol")

    numeric_cols = list(df.select_dtypes(include=[np.number]).columns)
    feature_cols = [col for col in numeric_cols if col not in exclude]
    if not feature_cols:
        raise SystemExit("No numeric feature columns found after exclusions.")

    X = df[feature_cols].astype(float)
    y = df[target_column].astype(float)
    return df, X, y, feature_cols, symbol_categories


def safe_corr(a: np.ndarray, b: np.ndarray) -> float:
    if a.size < 2 or b.size < 2:
        return 0.0
    if np.nanstd(a) == 0 or np.nanstd(b) == 0:
        return 0.0
    corr = np.corrcoef(a, b)[0, 1]
    return float(corr) if np.isfinite(corr) else 0.0


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    mae = float(mean_absolute_error(y_true, y_pred))
    r2 = float(r2_score(y_true, y_pred))
    corr = safe_corr(y_true, y_pred)
    sign_acc = float(np.mean(np.sign(y_true) == np.sign(y_pred)))

    pos_mask = y_pred > 0
    neg_mask = y_pred < 0
    mean_pos = float(np.mean(y_true[pos_mask])) if np.any(pos_mask) else 0.0
    mean_neg = float(np.mean(y_true[neg_mask])) if np.any(neg_mask) else 0.0

    return {
        "mae": mae,
        "r2": r2,
        "corr": corr,
        "signAccuracy": sign_acc,
        "meanTargetWhenPredPos": mean_pos,
        "meanTargetWhenPredNeg": mean_neg,
    }


def walk_forward_eval(
    X,
    y,
    create_model,
    min_train_frac: float,
    folds: int,
) -> List[Dict[str, float]]:
    n = len(y)
    if n < 100 or folds <= 0:
        return []
    min_train_end = max(10, int(n * min_train_frac))
    if min_train_end >= n - 5:
        return []
    remaining = n - min_train_end
    step = max(5, remaining // folds)

    scores: List[Dict[str, float]] = []
    for fold in range(folds):
        train_end = min_train_end + fold * step
        test_end = min(train_end + step, n)
        if test_end - train_end < 5:
            break

        model = create_model()
        model.fit(X.iloc[:train_end], y.iloc[:train_end])
        preds = model.predict(X.iloc[train_end:test_end])
        metrics = compute_metrics(y.iloc[train_end:test_end].to_numpy(), np.asarray(preds))
        metrics.update(
            {
                "fold": float(fold),
                "trainEnd": float(train_end),
                "testSize": float(test_end - train_end),
            }
        )
        scores.append(metrics)
    return scores


def main() -> None:
    parser = argparse.ArgumentParser(description="Train time-aware models on trading datasets.")
    parser.add_argument("--data", default="dataset.parquet", help="Path to dataset file")
    parser.add_argument("--target-column", default="target", help="Target column name")
    parser.add_argument("--model-out", default="model.joblib", help="Output model path")
    parser.add_argument("--meta-out", default="model_meta.json", help="Output metadata path")
    parser.add_argument("--test-frac", type=float, default=0.2, help="Fraction reserved for test")
    parser.add_argument("--min-train-frac", type=float, default=0.5, help="Min train fraction")
    parser.add_argument("--walk-forward-folds", type=int, default=4, help="Walk-forward folds")
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--max-depth", type=int, default=6)
    parser.add_argument("--learning-rate", type=float, default=0.05)
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

    df, X, y, feature_cols, symbol_categories = prepare_matrix(df, args.target_column)
    n = len(y)
    if n < 50:
        raise SystemExit("Dataset too small to train reliably.")

    test_frac = min(max(float(args.test_frac), 0.05), 0.5)
    min_train_frac = min(max(float(args.min_train_frac), 0.1), 0.95)

    split_idx = int(n * (1.0 - test_frac))
    min_train_idx = int(n * min_train_frac)
    split_idx = max(split_idx, min_train_idx)
    split_idx = min(split_idx, n - 5)
    if split_idx <= 10:
        raise SystemExit("Not enough data after train/test split.")

    X_train = X.iloc[:split_idx]
    y_train = y.iloc[:split_idx]
    X_test = X.iloc[split_idx:]
    y_test = y.iloc[split_idx:]

    def create_model():
        return HistGradientBoostingRegressor(
            random_state=args.random_state,
            max_depth=args.max_depth,
            learning_rate=args.learning_rate,
        )

    model = create_model()
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    metrics = compute_metrics(y_test.to_numpy(), np.asarray(preds))

    walk_scores = walk_forward_eval(
        X, y, create_model, min_train_frac=min_train_frac, folds=int(args.walk_forward_folds)
    )

    print(
        f"Train rows={len(X_train)} | Test rows={len(X_test)} | Features={len(feature_cols)}"
    )
    print(json.dumps({"testMetrics": metrics, "walkForward": walk_scores[:3]}, indent=2))

    os.makedirs(os.path.dirname(args.model_out) or ".", exist_ok=True)
    joblib.dump(model, args.model_out)

    meta = {
        "dataPath": args.data,
        "targetColumn": args.target_column,
        "featureColumns": feature_cols,
        "symbolCategories": symbol_categories,
        "testFraction": test_frac,
        "minTrainFraction": min_train_frac,
        "testMetrics": metrics,
        "walkForwardScores": walk_scores,
    }
    with open(args.meta_out, "w", encoding="utf-8") as handle:
        json.dump(meta, handle)
    print(f"Saved model to {args.model_out} and metadata to {args.meta_out}")


if __name__ == "__main__":
    main()
