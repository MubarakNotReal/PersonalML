import argparse
import bisect
import glob
import json
import os
import math
from typing import Dict, Iterable, List, Tuple


try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pd = None


def parse_int_list(text: str) -> List[int]:
    values: List[int] = []
    for part in str(text or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            values.append(int(part))
        except Exception:
            continue
    return sorted(set(v for v in values if v > 0))


def parse_symbol_list(text: str) -> List[str]:
    values: List[str] = []
    for part in str(text or "").split(","):
        sym = part.strip().upper()
        if sym.endswith("USDT") and sym:
            values.append(sym)
    return sorted(set(values))


def iter_snapshot_files(data_dir: str) -> List[str]:
    pattern = os.path.join(data_dir, "**", "snapshots_*.jsonl")
    return sorted(glob.glob(pattern, recursive=True))


def iter_snapshots(data_dir: str) -> Iterable[dict]:
    for path in iter_snapshot_files(data_dir):
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if obj.get("type") != "snapshot":
                    continue
                yield obj


def load_labels(labels_path: str, horizon_ms: int) -> Dict[Tuple[str, int, int], dict]:
    out: Dict[Tuple[str, int, int], dict] = {}
    with open(labels_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("type") != "return":
                continue
            if int(obj.get("horizonMs") or 0) != horizon_ms:
                continue
            symbol = obj.get("symbol")
            entry_time = obj.get("entryTime")
            if not symbol or entry_time is None:
                continue
            key = (symbol, int(entry_time), horizon_ms)
            out[key] = obj
    return out


def is_finite_num(value) -> bool:
    try:
        return value is not None and float(value) == float(value) and abs(float(value)) != float("inf")
    except Exception:
        return False


def is_micro_feature(key: str) -> bool:
    if key in {
        "bestBid",
        "bestAsk",
        "spreadPct",
        "imbalance",
        "depthBidQty",
        "depthAskQty",
        "depthImbalance",
        "microCompleteness",
    }:
        return True
    if key.startswith("depth"):
        return True
    if key.startswith("flow") or key.startswith("liq") or key.startswith("agg"):
        return True
    return False


def compute_micro_completeness(features: dict) -> float:
    existing = features.get("microCompleteness")
    if is_finite_num(existing):
        return float(existing)
    micro_keys = [k for k in features.keys() if is_micro_feature(k)]
    if not micro_keys:
        return 0.0
    present = sum(1 for k in micro_keys if is_finite_num(features.get(k)))
    return present / float(len(micro_keys))


def drop_micro_features(features: dict) -> dict:
    return {k: v for k, v in features.items() if not is_micro_feature(k)}


def build_context_index(data_dir: str, context_symbols: List[str]) -> Dict[str, Tuple[List[int], List[float]]]:
    context_pairs: Dict[str, List[Tuple[int, float]]] = {sym: [] for sym in context_symbols}
    if not context_symbols:
        return {}
    for obj in iter_snapshots(data_dir):
        symbol = obj.get("symbol")
        if symbol not in context_pairs:
            continue
        try:
            time = int(obj.get("time"))
            price = float(obj.get("price"))
        except Exception:
            continue
        if not is_finite_num(price):
            continue
        context_pairs[symbol].append((time, price))

    context_index: Dict[str, Tuple[List[int], List[float]]] = {}
    for sym, pairs in context_pairs.items():
        pairs.sort(key=lambda row: row[0])
        if not pairs:
            continue
        times = [row[0] for row in pairs]
        prices = [row[1] for row in pairs]
        context_index[sym] = (times, prices)
    return context_index


def compute_context_features(
    context_index: Dict[str, Tuple[List[int], List[float]]],
    entry_time: int,
    context_windows_ms: List[int],
    max_context_lag_ms: int,
) -> dict:
    features: dict = {}
    if not context_index:
        return features
    for sym, (times, prices) in context_index.items():
        if not times:
            continue
        idx = bisect.bisect_right(times, entry_time) - 1
        if idx < 0:
            continue
        age_ms = entry_time - times[idx]
        features[f"ctx_{sym}_ageMs"] = age_ms
        if age_ms > max_context_lag_ms:
            continue
        end_price = prices[idx]
        if not is_finite_num(end_price) or end_price <= 0:
            continue
        features[f"ctx_{sym}_price"] = end_price

        for window_ms in context_windows_ms:
            label = f"{int(window_ms / 60000)}m"
            start_time = entry_time - window_ms
            start_idx = bisect.bisect_left(times, start_time)
            if start_idx > idx:
                continue
            start_price = prices[start_idx]
            if not is_finite_num(start_price) or start_price <= 0:
                continue
            trend = ((end_price - start_price) / start_price) * 100.0
            features[f"ctx_{sym}_trend_{label}"] = trend

            window_prices = prices[start_idx : idx + 1]
            if len(window_prices) < 4:
                continue
            returns: List[float] = []
            for i in range(1, len(window_prices)):
                prev = window_prices[i - 1]
                nxt = window_prices[i]
                if not (prev > 0 and nxt > 0):
                    continue
                returns.append(((nxt - prev) / prev) * 100.0)
            if len(returns) < 3:
                continue
            mean = sum(returns) / float(len(returns))
            variance = sum((r - mean) ** 2 for r in returns) / float(len(returns) - 1)
            features[f"ctx_{sym}_vol_{label}"] = math.sqrt(max(variance, 0.0))
    return features


def write_jsonl(rows: List[dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ML-ready datasets from snapshots + labels.")
    parser.add_argument("--snapshots-dir", default="data", help="Directory containing snapshots_*.jsonl")
    parser.add_argument("--labels", required=True, help="Path to returns.jsonl from labeler.py")
    parser.add_argument("--output", default="dataset.parquet", help="Output dataset path")
    parser.add_argument("--horizon-min", type=int, default=5, help="Label horizon in minutes")
    parser.add_argument("--mode", choices=["all", "regime", "micro"], default="all")
    parser.add_argument("--format", choices=["parquet", "csv", "jsonl"], default="parquet")
    parser.add_argument("--context-symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--context-windows-min", default="1,5,15")
    parser.add_argument("--max-context-lag-min", type=int, default=5)
    parser.add_argument("--min-micro-completeness", type=float, default=0.25)
    parser.add_argument(
        "--target-field",
        default="longReturnPct",
        help="Label field to use as the primary training target",
    )
    args = parser.parse_args()

    horizon_ms = int(args.horizon_min) * 60 * 1000
    labels = load_labels(args.labels, horizon_ms)
    if not labels:
        raise SystemExit("No labels found for the requested horizon.")

    context_symbols = parse_symbol_list(args.context_symbols)
    context_windows_min = parse_int_list(args.context_windows_min)
    context_windows_ms = [m * 60 * 1000 for m in context_windows_min]
    max_context_lag_ms = int(args.max_context_lag_min) * 60 * 1000

    context_index = build_context_index(args.snapshots_dir, context_symbols)
    if context_symbols and not context_index:
        print("Warning: no context symbol data found; context features will be missing.")

    rows: List[dict] = []
    missing_labels = 0
    skipped_micro = 0

    for obj in iter_snapshots(args.snapshots_dir):
        symbol = obj.get("symbol")
        time_value = obj.get("time")
        if not symbol or time_value is None:
            continue
        try:
            entry_time = int(time_value)
        except Exception:
            continue

        label_key = (symbol, entry_time, horizon_ms)
        label = labels.get(label_key)
        if not label:
            missing_labels += 1
            continue

        features = dict(obj.get("features") or {})
        context_features = compute_context_features(
            context_index, entry_time, context_windows_ms, max_context_lag_ms
        )
        for key, value in context_features.items():
            if key not in features or not is_finite_num(features.get(key)):
                features[key] = value

        micro_comp = compute_micro_completeness(features)
        if args.mode == "micro" and micro_comp < float(args.min_micro_completeness):
            skipped_micro += 1
            continue
        if args.mode == "regime":
            features = drop_micro_features(features)
        else:
            features["microCompleteness"] = micro_comp

        target_val = label.get(args.target_field)
        if not is_finite_num(target_val):
            target_val = label.get("returnPct")
        if not is_finite_num(target_val):
            continue

        row = {
            "symbol": symbol,
            "entryTime": entry_time,
            "horizonMs": horizon_ms,
            "target": float(target_val),
            "targetField": args.target_field,
            "returnPct": label.get("returnPct"),
            "midReturnPct": label.get("midReturnPct"),
            "longReturnPct": label.get("longReturnPct"),
            "shortReturnPct": label.get("shortReturnPct"),
            "lagMs": label.get("lagMs"),
        }
        row.update(features)
        rows.append(row)

    print(
        f"Built {len(rows)} rows | missing_labels={missing_labels} | skipped_micro={skipped_micro}"
    )

    if not rows:
        raise SystemExit("No dataset rows were created.")

    rows.sort(key=lambda row: int(row.get("entryTime") or 0))

    if pd is None:
        print("Warning: pandas/pyarrow not available; writing JSONL instead.")
        write_jsonl(rows, args.output)
        print(f"Wrote dataset to {args.output} (jsonl fallback).")
        return

    df = pd.DataFrame(rows)

    if args.format == "parquet":
        try:
            df.to_parquet(args.output, index=False)
            print(f"Wrote dataset to {args.output} (parquet).")
            return
        except Exception as exc:  # pragma: no cover - depends on optional deps
            print(f"Parquet write failed ({exc}); falling back to CSV.")

    if args.format == "csv" or args.format == "parquet":
        df.to_csv(args.output, index=False)
        print(f"Wrote dataset to {args.output} (csv).")
        return

    write_jsonl(rows, args.output)
    print(f"Wrote dataset to {args.output} (jsonl).")


if __name__ == "__main__":
    main()
