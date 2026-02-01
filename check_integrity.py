import glob
import json
import os
from collections import Counter, defaultdict
from datetime import datetime
from typing import Iterable, List, Optional, Tuple

DATA_DIR = "data"
MAX_FILES = 8
LINES_PER_FILE = 800
MAX_FUTURE_SKEW_SEC = 5
MAX_STALE_SEC = 120

DEFAULT_EVENT_TYPES = ["aggTrade", "bookTicker", "depthUpdate", "markPriceUpdate"]
REQUIRED_SNAPSHOT_FIELDS = ["symbol", "time", "price", "features"]
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


def parse_env(path: str) -> dict:
    values = {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
    except Exception:
        return values
    return values


def env_flag(values: dict, key: str, default: bool = False) -> bool:
    if key not in values:
        return default
    return str(values.get(key, "")).lower() == "true"


def resolve_event_types() -> List[str]:
    env_path = ".env" if os.path.exists(".env") else ".env.example"
    env = parse_env(env_path)
    if not env_flag(env, "LOG_RAW_EVENTS", True):
        return []

    allowed = set(
        [item.strip() for item in env.get("LOG_RAW_EVENTS_TYPES", "").split(",") if item.strip()]
    )
    if not allowed:
        allowed = set(DEFAULT_EVENT_TYPES)

    enabled = set()
    if env_flag(env, "ENABLE_AGG_TRADES", True) and "aggTrade" in allowed:
        enabled.add("aggTrade")
    if env_flag(env, "ENABLE_BOOK_TICKER", True) and "bookTicker" in allowed:
        enabled.add("bookTicker")
    if env_flag(env, "ENABLE_DEPTH", True) and "depthUpdate" in allowed:
        enabled.add("depthUpdate")
    if env_flag(env, "ENABLE_MARK_PRICE", True) and "markPriceUpdate" in allowed:
        enabled.add("markPriceUpdate")
    return sorted(enabled)

def format_ts(ms: Optional[int]) -> str:
    if not ms:
        return "n/a"
    return datetime.utcfromtimestamp(ms / 1000).isoformat() + "Z"

def load_recent(pattern: str) -> Tuple[List[dict], List[str]]:
    files = iter_files(DATA_DIR, pattern)
    if not files:
        return [], []
    files = files[-MAX_FILES:]
    rows: List[dict] = []
    for path in files:
        rows.extend(parse_jsonl_lines(tail_lines(path, LINES_PER_FILE)))
    return rows, files

def check_snapshots(rows: List[dict], now_ms: int) -> None:
    snapshots = [row for row in rows if row.get("type") == "snapshot"]
    if not snapshots:
        print("Snapshots: NONE")
        return
    times = [row.get("time") for row in snapshots if isinstance(row.get("time"), (int, float))]
    newest = max(times) if times else None
    oldest = min(times) if times else None
    age = (now_ms - newest) / 1000.0 if newest else None
    print(f"Snapshots: {len(snapshots)} | span {format_ts(oldest)} -> {format_ts(newest)}")
    if age is not None and age > MAX_STALE_SEC:
        print(f"  WARNING: latest snapshot older than {MAX_STALE_SEC}s")
    if newest and newest - now_ms > MAX_FUTURE_SKEW_SEC * 1000:
        print("  WARNING: snapshot time is in the future")

    missing_fields = 0
    feature_missing = Counter()
    for row in snapshots:
        for key in REQUIRED_SNAPSHOT_FIELDS:
            if key not in row:
                missing_fields += 1
        features = row.get("features") or {}
        for key in CORE_FEATURES:
            if features.get(key) is None:
                feature_missing[key] += 1
    if missing_fields:
        print(f"  WARNING: missing required snapshot fields: {missing_fields}")
    print("  Core feature missing ratio (sample):")
    for key in CORE_FEATURES:
        ratio = feature_missing.get(key, 0) / max(1, len(snapshots))
        print(f"    {key}: {ratio:.2f}")

def check_events(event_type: str, rows: List[dict], now_ms: int) -> None:
    if not rows:
        print(f"Events {event_type}: NONE")
        return
    times = [row.get("time") for row in rows if isinstance(row.get("time"), (int, float))]
    newest = max(times) if times else None
    oldest = min(times) if times else None
    age = (now_ms - newest) / 1000.0 if newest else None
    print(f"Events {event_type}: {len(rows)} | span {format_ts(oldest)} -> {format_ts(newest)}")
    if age is not None and age > MAX_STALE_SEC:
        print(f"  WARNING: latest {event_type} older than {MAX_STALE_SEC}s")
    if newest and newest - now_ms > MAX_FUTURE_SKEW_SEC * 1000:
        print(f"  WARNING: {event_type} time is in the future")

def check_alignment(snapshots: List[dict], event_map: dict) -> None:
    if not snapshots:
        return
    snaps_by_symbol = defaultdict(list)
    for row in snapshots:
        symbol = row.get("symbol")
        if symbol:
            snaps_by_symbol[symbol].append(row)
    print("Alignment (sample symbols):")
    sample_symbols = list(snaps_by_symbol.keys())[:5]
    for symbol in sample_symbols:
        snap_times = [row.get("time") for row in snaps_by_symbol[symbol] if row.get("time")]
        latest_snap = max(snap_times) if snap_times else None
        latest_events = {}
        for ev_type, rows in event_map.items():
            times = [r.get("time") for r in rows if r.get("time") and r.get("data", {}).get("s") == symbol]
            latest_events[ev_type] = max(times) if times else None
        lag_parts = []
        for ev_type, ts in latest_events.items():
            if ts and latest_snap:
                lag_parts.append(f"{ev_type} lag={(latest_snap - ts)/1000.0:.1f}s")
        print(f"  {symbol}: {', '.join(lag_parts) if lag_parts else 'no event data'}")

def main() -> None:
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    snapshot_rows, _ = load_recent("snapshots_*.jsonl")
    check_snapshots(snapshot_rows, now_ms)

    event_map = {}
    event_types = resolve_event_types()
    for ev_type in event_types:
        rows, _ = load_recent(f"events_{ev_type}_*.jsonl")
        event_map[ev_type] = rows
        check_events(ev_type, rows, now_ms)

    check_alignment(snapshot_rows, event_map)

if __name__ == "__main__":
    main()
