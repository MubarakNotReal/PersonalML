import argparse
import bisect
import glob
import json
import os
from typing import Dict, List, Tuple


def iter_snapshot_files(data_dir: str) -> List[str]:
    pattern = os.path.join(data_dir, "**", "snapshots_*.jsonl")
    return sorted(glob.glob(pattern, recursive=True))


def iter_event_files(data_dir: str, event_type: str) -> List[str]:
    pattern = os.path.join(data_dir, "**", f"events_{event_type}_*.jsonl")
    return sorted(glob.glob(pattern, recursive=True))


def safe_float(value):
    try:
        num = float(value)
        if num != num or num == float("inf") or num == float("-inf"):
            return None
        return num
    except Exception:
        return None


def safe_int(value):
    try:
        return int(value)
    except Exception:
        return None


def load_events(data_dir: str, event_type: str) -> Dict[str, List[Tuple[int, float]]]:
    by_symbol: Dict[str, List[Tuple[int, float]]] = {}
    files = iter_event_files(data_dir, event_type)
    for path in files:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                data = obj.get("data") or {}
                symbol = data.get("s")
                if not symbol:
                    continue
                event_time = safe_int(obj.get("time"))
                if event_time is None:
                    event_time = safe_int(data.get("E") or data.get("T") or data.get("t"))
                if event_time is None:
                    continue
                price = None
                if event_type == "aggTrade":
                    price = safe_float(data.get("p"))
                elif event_type == "bookTicker":
                    bid = safe_float(data.get("b"))
                    ask = safe_float(data.get("a"))
                    if bid is not None and ask is not None:
                        price = (bid + ask) / 2.0
                    else:
                        price = bid if bid is not None else ask
                elif event_type == "markPriceUpdate":
                    price = safe_float(data.get("p"))
                if price is None:
                    continue
                by_symbol.setdefault(symbol, []).append((event_time, price))
    for symbol, rows in by_symbol.items():
        rows.sort(key=lambda row: row[0])
    return by_symbol


def load_snapshots(data_dir: str) -> List[dict]:
    out: List[dict] = []
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
                if not obj.get("symbol"):
                    continue
                out.append(obj)
    out.sort(key=lambda row: int(row.get("time") or 0))
    return out


def classify_path(
    rows: List[Tuple[int, float]],
    start_time: int,
    horizon_ms: int,
    entry_price: float,
    tp_bps: float,
    sl_bps: float,
) -> Tuple[str, int, float]:
    tp_price = entry_price * (1.0 + tp_bps / 10000.0)
    sl_price = entry_price * (1.0 - sl_bps / 10000.0)
    end_time = start_time + horizon_ms

    times = [row[0] for row in rows]
    idx = bisect.bisect_left(times, start_time)
    label = "TIME"
    exit_time = end_time
    exit_price = entry_price

    for i in range(idx, len(rows)):
        t, price = rows[i]
        if t > end_time:
            break
        if price >= tp_price:
            label = "TP"
            exit_time = t
            exit_price = price
            break
        if price <= sl_price:
            label = "SL"
            exit_time = t
            exit_price = price
            break
    return label, exit_time, exit_price


def classify_path_short(
    rows: List[Tuple[int, float]],
    start_time: int,
    horizon_ms: int,
    entry_price: float,
    tp_bps: float,
    sl_bps: float,
) -> Tuple[str, int, float]:
    tp_price = entry_price * (1.0 - tp_bps / 10000.0)
    sl_price = entry_price * (1.0 + sl_bps / 10000.0)
    end_time = start_time + horizon_ms

    times = [row[0] for row in rows]
    idx = bisect.bisect_left(times, start_time)
    label = "TIME"
    exit_time = end_time
    exit_price = entry_price

    for i in range(idx, len(rows)):
        t, price = rows[i]
        if t > end_time:
            break
        if price <= tp_price:
            label = "TP"
            exit_time = t
            exit_price = price
            break
        if price >= sl_price:
            label = "SL"
            exit_time = t
            exit_price = price
            break
    return label, exit_time, exit_price


def main() -> None:
    parser = argparse.ArgumentParser(description="Label snapshots with TP/SL path using raw events.")
    parser.add_argument("--snapshots-dir", default="data", help="Directory with snapshots_*.jsonl")
    parser.add_argument("--events-dir", default="data", help="Directory with events_*.jsonl")
    parser.add_argument("--event-type", default="aggTrade", choices=["aggTrade", "bookTicker", "markPriceUpdate"])
    parser.add_argument("--output", default="barrier_labels.jsonl", help="Output JSONL path")
    parser.add_argument("--horizon-sec", type=int, default=30)
    parser.add_argument("--tp-bps", type=float, default=12.0)
    parser.add_argument("--sl-bps", type=float, default=8.0)
    parser.add_argument("--side", choices=["long", "short", "both"], default="both")
    args = parser.parse_args()

    horizon_ms = int(args.horizon_sec) * 1000
    if horizon_ms <= 0:
        raise SystemExit("horizon-sec must be > 0")

    snapshots = load_snapshots(args.snapshots_dir)
    if not snapshots:
        raise SystemExit("No snapshots found.")

    events_by_symbol = load_events(args.events_dir, args.event_type)
    if not events_by_symbol:
        raise SystemExit("No raw events found for selected event type.")

    written = 0
    missing_events = 0

    with open(args.output, "w", encoding="utf-8") as out:
        for snap in snapshots:
            symbol = snap.get("symbol")
            entry_time = safe_int(snap.get("time"))
            entry_price = safe_float(snap.get("price"))
            if not symbol or entry_time is None or entry_price is None:
                continue
            rows = events_by_symbol.get(symbol)
            if not rows:
                missing_events += 1
                continue

            payload = {
                "type": "barrier",
                "symbol": symbol,
                "entryTime": entry_time,
                "entryPrice": entry_price,
                "horizonMs": horizon_ms,
                "tpBps": args.tp_bps,
                "slBps": args.sl_bps,
                "eventType": args.event_type,
                "snapshotId": f"snap-{symbol}-{entry_time}",
            }

            if args.side in ("long", "both"):
                label, exit_time, exit_price = classify_path(
                    rows, entry_time, horizon_ms, entry_price, args.tp_bps, args.sl_bps
                )
                payload.update(
                    {
                        "labelLong": label,
                        "exitTimeLong": exit_time,
                        "exitPriceLong": exit_price,
                    }
                )
            if args.side in ("short", "both"):
                label, exit_time, exit_price = classify_path_short(
                    rows, entry_time, horizon_ms, entry_price, args.tp_bps, args.sl_bps
                )
                payload.update(
                    {
                        "labelShort": label,
                        "exitTimeShort": exit_time,
                        "exitPriceShort": exit_price,
                    }
                )

            out.write(json.dumps(payload) + "\n")
            written += 1

    print(
        f"Wrote {written} labels | missing_events={missing_events} | event_type={args.event_type}"
    )


if __name__ == "__main__":
    main()
