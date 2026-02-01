import glob
import json
import os
import time
from collections import Counter
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple


CORE_FEATURES = [
    "markPrice",
    "indexPrice",
    "fundingRate",
    "openInterest",
    "bestBid",
    "bestAsk",
    "spreadPct",
    "depthBidQty",
    "depthAskQty",
    "aggBuyQty",
    "aggSellQty",
    "liqBuyQty",
    "liqSellQty",
    "kline1mClose",
    "kline1mVol",
]


def iter_files(data_dir: str, pattern: str) -> List[str]:
    return sorted(glob.glob(os.path.join(data_dir, "**", pattern), recursive=True))


def tail_lines(path: str, max_lines: int) -> List[str]:
    if max_lines <= 0:
        return []
    lines: List[str] = []
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            block = 4096
            data = b""
            while size > 0 and data.count(b"\n") <= max_lines:
                step = block if size >= block else size
                size -= step
                handle.seek(size)
                data = handle.read(step) + data
            lines = data.splitlines()[-max_lines:]
    except FileNotFoundError:
        return []
    except Exception:
        return []
    return [line.decode("utf-8", errors="ignore") for line in lines]


def parse_jsonl_lines(lines: Iterable[str]) -> List[dict]:
    out: List[dict] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def read_recent_snapshots(data_dir: str, max_files: int, lines_per_file: int) -> Tuple[List[dict], List[str]]:
    files = iter_files(data_dir, "snapshots_*.jsonl")
    if not files:
        return [], []
    files = files[-max_files:]
    rows: List[dict] = []
    for path in files:
        lines = tail_lines(path, lines_per_file)
        rows.extend(parse_jsonl_lines(lines))
    return [row for row in rows if row.get("type") == "snapshot"], files


def read_recent_events(
    data_dir: str, event_type: str, max_files: int, lines_per_file: int
) -> Tuple[List[dict], List[str]]:
    files = iter_files(data_dir, f"events_{event_type}_*.jsonl")
    if not files:
        return [], []
    files = files[-max_files:]
    rows: List[dict] = []
    for path in files:
        lines = tail_lines(path, lines_per_file)
        rows.extend(parse_jsonl_lines(lines))
    return rows, files


def format_ts(ms: Optional[int]) -> str:
    if not ms:
        return "n/a"
    return datetime.utcfromtimestamp(ms / 1000).isoformat() + "Z"


def summarize_snapshots(
    rows: List[dict], max_age_sec: int, symbols_expected: Optional[List[str]], now_ms: int
):
    if not rows:
        print("No snapshot rows found.")
        return

    symbols = [row.get("symbol") for row in rows if row.get("symbol")]
    symbol_counts = Counter(symbols)
    newest_time = max((row.get("time") or 0) for row in rows)
    oldest_time = min((row.get("time") or 0) for row in rows)
    age_sec = (now_ms - newest_time) / 1000.0 if newest_time else None

    print(f"Snapshots checked: {len(rows)}")
    print(f"Symbols seen: {len(symbol_counts)}")
    print(f"Time span: {format_ts(oldest_time)} -> {format_ts(newest_time)}")
    if age_sec is not None:
        print(f"Latest snapshot age: {age_sec:.1f}s")
        if age_sec > max_age_sec:
            print(f"WARNING: latest snapshot is older than {max_age_sec}s")

    if symbols_expected:
        expected_set = set(symbols_expected)
        seen_set = set(symbol_counts.keys())
        missing = sorted(expected_set - seen_set)
        extra = sorted(seen_set - expected_set)
        print(f"Expected symbols: {len(expected_set)} | Missing: {len(missing)} | Extra: {len(extra)}")
        if missing:
            print(f"Missing sample: {', '.join(missing[:10])}")

    feature_coverage = Counter()
    feature_total = 0
    completeness_vals = []
    micro_vals = []
    for row in rows:
        features = row.get("features") or {}
        for key in CORE_FEATURES:
            feature_total += 1
            if features.get(key) is not None:
                feature_coverage[key] += 1
        if isinstance(features.get("featureCompleteness"), (int, float)):
            completeness_vals.append(float(features["featureCompleteness"]))
        if isinstance(features.get("microCompleteness"), (int, float)):
            micro_vals.append(float(features["microCompleteness"]))

    print("Core feature coverage (sample):")
    for key in CORE_FEATURES:
        ratio = feature_coverage.get(key, 0) / max(1, len(rows))
        print(f"  {key}: {ratio:.2f}")

    if completeness_vals:
        avg_comp = sum(completeness_vals) / len(completeness_vals)
        print(f"featureCompleteness avg: {avg_comp:.2f}")
    if micro_vals:
        avg_micro = sum(micro_vals) / len(micro_vals)
        print(f"microCompleteness avg: {avg_micro:.2f}")


def summarize_events(
    data_dir: str, event_types: List[str], max_age_sec: int, max_files: int, lines_per_file: int, now_ms: int
):
    for event_type in event_types:
        rows, files = read_recent_events(data_dir, event_type, max_files, lines_per_file)
        if not files:
            print(f"Events {event_type}: NONE (no files found)")
            continue
        if not rows:
            print(f"Events {event_type}: files found but no rows (buffering or empty)")
            continue
        times = [row.get("time") for row in rows if isinstance(row.get("time"), (int, float))]
        newest = max(times) if times else None
        oldest = min(times) if times else None
        age_sec = (now_ms - newest) / 1000.0 if newest else None
        print(
            f"Events {event_type}: rows={len(rows)} time_span={format_ts(oldest)} -> {format_ts(newest)}"
        )
        if newest and isinstance(max_age_sec, int):
            age = (now_ms - newest) / 1000.0
            if age > max_age_sec:
                print(f"  WARNING: latest {event_type} is older than {max_age_sec}s")


def load_symbol_list(path: Optional[str]) -> Optional[List[str]]:
    if not path:
        return None
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = handle.read().strip().splitlines()
    except Exception:
        return None
    out = []
    for line in raw:
        sym = line.strip().split()[0].upper()
        if sym.endswith("USDT"):
            out.append(sym)
    return sorted(set(out))


def main() -> None:
    # Edit these defaults if you want different checks.
    data_dir = "data"
    symbols_file = "symbols_topn.txt"
    event_types = ["aggTrade", "bookTicker", "depthUpdate", "markPriceUpdate"]
    max_files = 6
    lines_per_file = 500
    max_age_sec = 120

    symbols_expected = load_symbol_list(symbols_file)

    print(f"Data dir: {data_dir}")
    now_ms = int(time.time() * 1000)
    snapshots, snapshot_files = read_recent_snapshots(data_dir, max_files, lines_per_file)
    summarize_snapshots(snapshots, max_age_sec, symbols_expected, now_ms)

    print("\nRaw event health:")
    summarize_events(data_dir, event_types, max_age_sec, max_files, lines_per_file, now_ms)


if __name__ == "__main__":
    main()
