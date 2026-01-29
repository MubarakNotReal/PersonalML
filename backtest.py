import argparse
import json
import math
import os
from typing import Dict, List

import numpy as np
import joblib

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None


def load_dataset(path: str):
    if pd is None:
        raise SystemExit("pandas is required for backtesting. Install requirements.txt first.")
    lower = path.lower()
    if lower.endswith(".parquet"):
        return pd.read_parquet(path)
    if lower.endswith(".csv"):
        return pd.read_csv(path)
    if lower.endswith(".jsonl") or lower.endswith(".json"):
        return pd.read_json(path, lines=True)
    raise SystemExit(f"Unsupported dataset format: {path}")


def load_meta(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def encode_symbol_with_categories(df, categories: List[str]):
    if pd is None or "symbol" not in df.columns or not categories:
        return df
    dtype = pd.api.types.CategoricalDtype(categories=categories)
    df = df.copy()
    df["symbolCode"] = df["symbol"].astype(dtype).cat.codes
    return df


def build_feature_matrix(df, feature_cols: List[str]):
    df = df.replace([np.inf, -np.inf], np.nan)
    for col in feature_cols:
        if col not in df.columns:
            df[col] = np.nan
    return df[feature_cols].astype(float)


def compute_max_drawdown(equity: np.ndarray) -> float:
    if equity.size == 0:
        return 0.0
    running_max = np.maximum.accumulate(equity)
    drawdowns = equity - running_max
    return float(np.min(drawdowns))


def is_finite(value) -> bool:
    try:
        return value is not None and math.isfinite(float(value))
    except Exception:
        return False


def select_return(row, action: str):
    base = getattr(row, "returnPct", None)
    if action == "long":
        candidate = getattr(row, "longReturnPct", None)
        return float(candidate) if is_finite(candidate) else (float(base) if is_finite(base) else None)
    candidate = getattr(row, "shortReturnPct", None)
    if is_finite(candidate):
        return float(candidate)
    if is_finite(base):
        return -float(base)
    return None


def run_backtest(
    df,
    long_threshold: float,
    short_threshold: float,
    extra_cost_bps: float,
    short_enabled: bool,
):
    cost_pct = float(extra_cost_bps) / 100.0
    last_exit: Dict[str, int] = {}
    trade_returns: List[float] = []
    trades: List[dict] = []

    for row in df.itertuples(index=False):
        symbol = getattr(row, "symbol", None)
        entry_time = getattr(row, "entryTime", None)
        horizon_ms = getattr(row, "horizonMs", None)
        pred = getattr(row, "prediction", None)
        if not symbol or entry_time is None or horizon_ms is None or not is_finite(pred):
            continue
        entry_time = int(entry_time)
        horizon_ms = int(horizon_ms)
        if horizon_ms <= 0:
            continue
        if symbol in last_exit and entry_time < last_exit[symbol]:
            continue

        pred_val = float(pred)
        action = None
        if pred_val >= long_threshold:
            action = "long"
        elif short_enabled and pred_val <= -short_threshold:
            action = "short"
        if action is None:
            continue

        realized = select_return(row, action)
        if realized is None or not is_finite(realized):
            continue
        realized -= cost_pct

        exit_time = entry_time + horizon_ms
        last_exit[symbol] = exit_time
        trade_returns.append(float(realized))
        trades.append(
            {
                "symbol": symbol,
                "entryTime": entry_time,
                "exitTime": exit_time,
                "action": action,
                "prediction": pred_val,
                "realizedPct": float(realized),
            }
        )

    return np.asarray(trade_returns, dtype=float), trades


def summarize_returns(returns: np.ndarray) -> Dict[str, float]:
    if returns.size == 0:
        return {
            "trades": 0.0,
            "hitRate": 0.0,
            "meanPct": 0.0,
            "medianPct": 0.0,
            "stdPct": 0.0,
            "sharpeLike": 0.0,
            "cumPct": 0.0,
            "maxDrawdownPct": 0.0,
        }
    trades = float(returns.size)
    hit_rate = float(np.mean(returns > 0))
    mean_pct = float(np.mean(returns))
    median_pct = float(np.median(returns))
    std_pct = float(np.std(returns))
    sharpe_like = float(mean_pct / std_pct * math.sqrt(trades)) if std_pct > 0 else 0.0
    equity = np.cumsum(returns)
    max_dd = compute_max_drawdown(equity)
    return {
        "trades": trades,
        "hitRate": hit_rate,
        "meanPct": mean_pct,
        "medianPct": median_pct,
        "stdPct": std_pct,
        "sharpeLike": sharpe_like,
        "cumPct": float(equity[-1]),
        "maxDrawdownPct": float(max_dd),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest model signals on labeled datasets.")
    parser.add_argument("--data", default="dataset.parquet")
    parser.add_argument("--model", default="model.joblib")
    parser.add_argument("--meta", default="model_meta.json")
    parser.add_argument("--long-threshold", type=float, default=0.0)
    parser.add_argument("--short-threshold", type=float, default=0.0)
    parser.add_argument("--extra-cost-bps", type=float, default=0.0)
    parser.add_argument("--disable-short", action="store_true")
    parser.add_argument("--trades-out", default="", help="Optional path to write trades JSONL")
    args = parser.parse_args()

    df = load_dataset(args.data)
    meta = load_meta(args.meta)
    model = joblib.load(args.model)

    feature_cols = list(meta.get("featureColumns") or [])
    symbol_categories = list(meta.get("symbolCategories") or [])
    if not feature_cols:
        raise SystemExit("No featureColumns found in metadata.")

    df = encode_symbol_with_categories(df, symbol_categories)
    if "entryTime" in df.columns:
        df = df.sort_values("entryTime").reset_index(drop=True)

    X = build_feature_matrix(df, feature_cols)
    preds = model.predict(X)
    df = df.copy()
    df["prediction"] = preds

    long_threshold = float(args.long_threshold)
    short_threshold = float(args.short_threshold)
    short_enabled = not args.disable_short

    returns, trades = run_backtest(
        df,
        long_threshold=long_threshold,
        short_threshold=short_threshold,
        extra_cost_bps=float(args.extra_cost_bps),
        short_enabled=short_enabled,
    )
    summary = summarize_returns(returns)
    print(json.dumps(summary, indent=2))

    if args.trades_out:
        os.makedirs(os.path.dirname(args.trades_out) or ".", exist_ok=True)
        with open(args.trades_out, "w", encoding="utf-8") as handle:
            for trade in trades:
                handle.write(json.dumps(trade) + "\n")
        print(f"Wrote {len(trades)} trades to {args.trades_out}")


if __name__ == "__main__":
    main()
