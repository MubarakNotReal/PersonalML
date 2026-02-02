import argparse
import json
import os
import glob
import tempfile
import bisect
import math


def safe_float(value):
    try:
        num = float(value)
    except Exception:
        return None
    return num if math.isfinite(num) else None


def iter_snapshot_files(data_dir):
    pattern = os.path.join(data_dir, '**', 'snapshots_*.jsonl')
    return sorted(glob.glob(pattern, recursive=True))


def stream_snapshots_to_tmp(data_dir, tmp_dir):
    os.makedirs(tmp_dir, exist_ok=True)
    file_handles = {}
    try:
        for path in iter_snapshot_files(data_dir):
            with open(path, 'r', encoding='utf-8') as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    if obj.get('type') != 'snapshot':
                        continue
                    symbol = obj.get('symbol')
                    time = obj.get('time')
                    price = safe_float(obj.get('price'))
                    if symbol is None or time is None or price is None:
                        continue
                    features = obj.get('features') or {}
                    bid = safe_float(features.get('bestBid'))
                    ask = safe_float(features.get('bestAsk'))
                    funding = safe_float(features.get('fundingRate'))
                    out_row = {
                        "time": int(time),
                        "price": price,
                        "bid": bid,
                        "ask": ask,
                        "funding": funding,
                    }
                    handle_out = file_handles.get(symbol)
                    if handle_out is None:
                        handle_out = open(os.path.join(tmp_dir, f"{symbol}.jsonl"), "a", encoding="utf-8")
                        file_handles[symbol] = handle_out
                    handle_out.write(json.dumps(out_row) + "\n")
    finally:
        for handle_out in file_handles.values():
            try:
                handle_out.close()
            except Exception:
                pass


def load_symbol_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            time = obj.get("time")
            price = safe_float(obj.get("price"))
            if time is None or price is None:
                continue
            rows.append(
                (
                    int(time),
                    price,
                    safe_float(obj.get("bid")),
                    safe_float(obj.get("ask")),
                    safe_float(obj.get("funding")),
                )
            )
    rows.sort(key=lambda x: x[0])
    return rows


def pick_price(value, fallback):
    return value if value is not None and value > 0 else fallback


def compute_execution_returns(entry_price, entry_bid, entry_ask, target_price, target_bid, target_ask, cost_frac):
    entry_bid = pick_price(entry_bid, entry_price)
    entry_ask = pick_price(entry_ask, entry_price)
    target_bid = pick_price(target_bid, target_price)
    target_ask = pick_price(target_ask, target_price)

    mid_return_pct = ((target_price - entry_price) / entry_price) * 100.0

    long_entry = entry_ask * (1.0 + cost_frac)
    long_exit = target_bid * (1.0 - cost_frac)
    long_return_pct = ((long_exit - long_entry) / long_entry) * 100.0

    short_entry = entry_bid * (1.0 - cost_frac)
    short_exit = target_ask * (1.0 + cost_frac)
    short_return_pct = ((short_entry - short_exit) / short_entry) * 100.0

    return (
        mid_return_pct,
        long_return_pct,
        short_return_pct,
        entry_bid,
        entry_ask,
        target_bid,
        target_ask,
    )


def compute_funding_adjust_pct(funding_rate, horizon_ms, eight_hours_ms):
    if funding_rate is None or eight_hours_ms <= 0:
        return 0.0
    funding_frac = funding_rate * (horizon_ms / float(eight_hours_ms))
    return funding_frac * 100.0


def main():
    parser = argparse.ArgumentParser(description='Generate future return labels from snapshots.')
    parser.add_argument('--data-dir', default='data', help='Directory containing snapshots_*.jsonl')
    parser.add_argument('--output', default='returns.jsonl', help='Output JSONL path')
    parser.add_argument('--horizons-min', default='1,5,15', help='Comma-separated horizons in minutes')
    parser.add_argument(
        '--max-lag-pct',
        type=float,
        default=0.10,
        help='Max allowed lag as a fraction of horizon (e.g., 0.10 = 10%%)',
    )
    parser.add_argument('--fee-bps', type=float, default=4.0, help='Fee in basis points per side')
    parser.add_argument(
        '--slippage-bps', type=float, default=2.0, help='Slippage in basis points per side'
    )
    parser.add_argument(
        '--require-bbo',
        action='store_true',
        help='Require best bid/ask on both entry and target snapshots',
    )
    parser.add_argument(
        '--disable-funding',
        action='store_true',
        help='Ignore funding rate adjustments even if fundingRate is present',
    )
    parser.add_argument(
        '--tmp-dir',
        default=None,
        help='Optional temp directory for streaming snapshots (reduces memory usage)',
    )
    parser.add_argument(
        '--keep-tmp',
        action='store_true',
        help='Keep temporary files after labeling (useful for debugging)',
    )
    args = parser.parse_args()

    horizons = [int(h.strip()) for h in args.horizons_min.split(',') if h.strip().isdigit()]
    horizons_ms = sorted({h * 60 * 1000 for h in horizons})
    max_lag_pct = max(0.0, float(args.max_lag_pct))
    cost_bps = max(0.0, float(args.fee_bps) + float(args.slippage_bps))
    cost_frac = cost_bps / 10000.0
    funding_enabled = not args.disable_funding
    eight_hours_ms = 8 * 60 * 60 * 1000

    tmp_dir = args.tmp_dir or tempfile.mkdtemp(prefix="labeler_tmp_")
    stream_snapshots_to_tmp(args.data_dir, tmp_dir)
    symbol_files = sorted(glob.glob(os.path.join(tmp_dir, "*.jsonl")))
    if not symbol_files:
        raise SystemExit('No snapshots found.')

    with open(args.output, 'w', encoding='utf-8') as out:
        for path in symbol_files:
            symbol = os.path.splitext(os.path.basename(path))[0]
            rows = load_symbol_rows(path)
            if not rows:
                continue
            times = [row[0] for row in rows]
            prices = [row[1] for row in rows]
            bids = [row[2] for row in rows]
            asks = [row[3] for row in rows]
            for idx, (t, price, bid, ask, funding_rate) in enumerate(rows):
                for horizon_ms in horizons_ms:
                    target_time = t + horizon_ms
                    j = bisect.bisect_left(times, target_time)
                    if j >= len(times):
                        continue
                    target_price = prices[j]
                    actual_target_time = times[j]
                    lag_ms = actual_target_time - target_time
                    max_lag_ms = int(horizon_ms * max_lag_pct)
                    if lag_ms > max_lag_ms:
                        continue
                    if price <= 0 or target_price <= 0:
                        continue
                    target_bid = bids[j]
                    target_ask = asks[j]
                    if args.require_bbo and (
                        bid is None or ask is None or target_bid is None or target_ask is None
                    ):
                        continue
                    (
                        mid_return_pct,
                        long_return_pct,
                        short_return_pct,
                        entry_bid_used,
                        entry_ask_used,
                        target_bid_used,
                        target_ask_used,
                    ) = compute_execution_returns(
                        price, bid, ask, target_price, target_bid, target_ask, cost_frac
                    )

                    funding_adj_pct = (
                        compute_funding_adjust_pct(funding_rate, horizon_ms, eight_hours_ms)
                        if funding_enabled
                        else 0.0
                    )
                    long_return_pct -= funding_adj_pct
                    short_return_pct += funding_adj_pct

                    payload = {
                        'type': 'return',
                        'symbol': symbol,
                        'entryTime': t,
                        'entryPrice': price,
                        'targetTime': target_time,
                        'actualTargetTime': actual_target_time,
                        'lagMs': lag_ms,
                        'maxLagMs': max_lag_ms,
                        'entryBid': entry_bid_used,
                        'entryAsk': entry_ask_used,
                        'targetBid': target_bid_used,
                        'targetAsk': target_ask_used,
                        'targetPrice': target_price,
                        'horizonMs': horizon_ms,
                        'horizonMin': horizon_ms / 60000,
                        'feeBps': float(args.fee_bps),
                        'slippageBps': float(args.slippage_bps),
                        'costBpsPerSide': cost_bps,
                        'fundingRate': funding_rate,
                        'fundingAdjPct': funding_adj_pct,
                        'midReturnPct': mid_return_pct,
                        'longReturnPct': long_return_pct,
                        'shortReturnPct': short_return_pct,
                        'returnPct': mid_return_pct,
                        'snapshotId': f'snap-{symbol}-{t}',
                    }
                    out.write(json.dumps(payload) + '\n')

    if not args.keep_tmp:
        for path in symbol_files:
            try:
                os.remove(path)
            except Exception:
                pass
        if args.tmp_dir is None:
            try:
                os.rmdir(tmp_dir)
            except Exception:
                pass

    print(f'Wrote labels to {args.output}')


if __name__ == '__main__':
    main()
