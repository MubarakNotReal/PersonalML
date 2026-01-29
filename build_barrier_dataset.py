import argparse
import bisect
import glob
import json
import os
from typing import Dict, Iterable, List, Tuple

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pd = None


LABEL_MAP = {"TP": 1, "SL": 0, "TIME": 2}


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


def load_labels(path: str) -> Dict[Tuple[str, int], dict]:
    out: Dict[Tuple[str, int], dict] = {}
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("type") != "barrier":
                continue
            symbol = obj.get("symbol")
            entry_time = obj.get("entryTime")
            if not symbol or entry_time is None:
                continue
            key = (symbol, int(entry_time))
            out[key] = obj
    return out


def is_finite_num(value) -> bool:
    try:
        return value is not None and float(value) == float(value) and abs(float(value)) != float("inf")
    except Exception:
        return False


def write_jsonl(rows: List[dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build dataset from snapshots + barrier labels.")
    parser.add_argument("--snapshots-dir", default="data", help="Directory with snapshots_*.jsonl")
    parser.add_argument("--labels", required=True, help="Path to barrier_labels.jsonl")
    parser.add_argument("--output", default="barrier_dataset.parquet", help="Output dataset path")
    parser.add_argument("--format", choices=["parquet", "csv", "jsonl"], default="parquet")
    parser.add_argument("--side", choices=["long", "short"], default="long")
    parser.add_argument("--target-column", default="target", help="Target column name")
    parser.add_argument("--min-micro-completeness", type=float, default=0.25)
    args = parser.parse_args()

    labels = load_labels(args.labels)
    if not labels:
        raise SystemExit("No barrier labels found.")

    rows: List[dict] = []
    missing_labels = 0

    for obj in iter_snapshots(args.snapshots_dir):
        symbol = obj.get("symbol")
        time_value = obj.get("time")
        if not symbol or time_value is None:
            continue
        try:
            entry_time = int(time_value)
        except Exception:
            continue

        label = labels.get((symbol, entry_time))
        if not label:
            missing_labels += 1
            continue

        label_key = "labelLong" if args.side == "long" else "labelShort"
        label_value = label.get(label_key)
        if label_value not in LABEL_MAP:
            continue

        features = dict(obj.get("features") or {})
        micro_comp = features.get("microCompleteness")
        if is_finite_num(micro_comp) and float(micro_comp) < float(args.min_micro_completeness):
            continue

        row = {
            "symbol": symbol,
            "entryTime": entry_time,
            "horizonMs": label.get("horizonMs"),
            args.target_column: LABEL_MAP[label_value],
            "label": label_value,
            "tpBps": label.get("tpBps"),
            "slBps": label.get("slBps"),
            "eventType": label.get("eventType"),
        }
        row.update(features)
        rows.append(row)

    print(f"Built {len(rows)} rows | missing_labels={missing_labels}")

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
        except Exception as exc:  # pragma: no cover - optional deps
            print(f"Parquet write failed ({exc}); falling back to CSV.")

    if args.format == "csv" or args.format == "parquet":
        df.to_csv(args.output, index=False)
        print(f"Wrote dataset to {args.output} (csv).")
        return

    write_jsonl(rows, args.output)
    print(f"Wrote dataset to {args.output} (jsonl).")


if __name__ == "__main__":
    main()
