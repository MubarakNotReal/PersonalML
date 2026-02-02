import argparse
import bisect
import glob
import tempfile
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


def stream_snapshots_to_tmp(data_dir: str, tmp_dir: str) -> List[str]:
    os.makedirs(tmp_dir, exist_ok=True)
    handles = {}
    try:
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
                    symbol = obj.get("symbol")
                    entry_time = safe_int(obj.get("time"))
                    entry_price = safe_float(obj.get("price"))
                    if not symbol or entry_time is None or entry_price is None:
                        continue
                    out_row = {"time": entry_time, "price": entry_price}
                    handle_out = handles.get(symbol)
                    if handle_out is None:
                        handle_out = open(os.path.join(tmp_dir, f"{symbol}_snap.jsonl"), "a", encoding="utf-8")
                        handles[symbol] = handle_out
                    handle_out.write(json.dumps(out_row) + "\n")
    finally:
        for handle_out in handles.values():
            try:
                handle_out.close()
            except Exception:
                pass
    return sorted(glob.glob(os.path.join(tmp_dir, "*_snap.jsonl")))


def stream_events_to_tmp(data_dir: str, event_type: str, tmp_dir: str) -> List[str]:
    os.makedirs(tmp_dir, exist_ok=True)
    handles = {}
    try:
        for path in iter_event_files(data_dir, event_type):
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
                    out_row = {"time": event_time, "price": price}
                    handle_out = handles.get(symbol)
                    if handle_out is None:
                        handle_out = open(os.path.join(tmp_dir, f"{symbol}_evt.jsonl"), "a", encoding="utf-8")
                        handles[symbol] = handle_out
                    handle_out.write(json.dumps(out_row) + "\n")
    finally:
        for handle_out in handles.values():
            try:
                handle_out.close()
            except Exception:
                pass
    return sorted(glob.glob(os.path.join(tmp_dir, "*_evt.jsonl")))


def load_symbol_rows(path: str) -> List[Tuple[int, float]]:
    rows: List[Tuple[int, float]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            t = safe_int(obj.get("time"))
            price = safe_float(obj.get("price"))
            if t is None or price is None:
                continue
            rows.append((t, price))
    rows.sort(key=lambda row: row[0])
    return rows


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
    parser.add_argument("--tmp-dir", default=None, help="Optional temp directory for streaming files")
    parser.add_argument("--keep-tmp", action="store_true", help="Keep temp files after labeling")
    args = parser.parse_args()

    horizon_ms = int(args.horizon_sec) * 1000
    if horizon_ms <= 0:
        raise SystemExit("horizon-sec must be > 0")

    tmp_dir = args.tmp_dir or tempfile.mkdtemp(prefix="labeler_barrier_tmp_")
    snap_files = stream_snapshots_to_tmp(args.snapshots_dir, tmp_dir)
    event_files = stream_events_to_tmp(args.events_dir, args.event_type, tmp_dir)
    if not snap_files:
        raise SystemExit("No snapshots found.")
    if not event_files:
        raise SystemExit("No raw events found for selected event type.")

    written = 0
    missing_events = 0

    with open(args.output, "w", encoding="utf-8") as out:
        event_index = {
            os.path.basename(path).replace("_evt.jsonl", ""): path for path in event_files
        }
        for snap_path in snap_files:
            symbol = os.path.basename(snap_path).replace("_snap.jsonl", "")
            rows = load_symbol_rows(event_index.get(symbol, ""))
            if not rows:
                missing_events += 1
                continue
            snapshots = load_symbol_rows(snap_path)
            for entry_time, entry_price in snapshots:
                if entry_time is None or entry_price is None:
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

    if not args.keep_tmp:
        for path in snap_files + event_files:
            try:
                os.remove(path)
            except Exception:
                pass
        if args.tmp_dir is None:
            try:
                os.rmdir(tmp_dir)
            except Exception:
                pass


if __name__ == "__main__":
    main()
