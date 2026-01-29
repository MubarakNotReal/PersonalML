# Ultimate ML Collector

This folder is a **fresh, ideal data collection stack** for Binance USDT‑M Futures.
It captures rich microstructure + futures‑specific signals and writes **snapshot‑ready ML data**.

## What it collects
**Streams (WebSocket)**
- Book ticker (best bid/ask)
- Depth updates (order book)
- Agg trades (trade flow)
- Liquidations (force orders)
- Mark price (mark/index/funding)
- 24h ticker (vol, change)
- Klines 1m / 5m

**Polling (REST)**
- Open interest
- Funding / premium index
- 24h stats (if stream disabled)

**Outputs**
- `data/snapshots_YYYYMMDD_HH.jsonl` – unified per‑symbol snapshots
- Optional raw stream dumps per stream (`events_*.jsonl`)

Each snapshot includes:
- Price, spread, imbalance
- Depth metrics
- Trade flow (buy/sell volume, counts, VWAP)
- Funding + OI + deltas
- Trend + volatility (5m/15m)
- Regime‑style features

## Setup
Copy `.env.example` to `.env` and adjust.

### Recommended baseline (live)
- TOP_N: 80
- SNAPSHOT_INTERVAL_MS: 1000 (1s)
- DEPTH_SPEED: 1000ms (lighter) or 100ms (heavy)

## Run
From repo root:
```
npm install
npm run collector
```

## Historical backfill (ideal ML)
Use the backfill script to capture long‑horizon regimes.
Defaults are already set in `.env.example`:
- BACKFILL_MONTHS: 12
- BACKFILL_TOP_N: 80
- BACKFILL_INTERVALS: 1m
- BACKFILL_OPEN_INTEREST_PERIOD: 5m

Run it once:
```
npm run backfill
```

Outputs:
- `data_backfill/YYYYMMDD/snapshots_HH.jsonl`

Note: backfill snapshots **won’t include live microstructure** (order book, trade flow),
so you should still run the live collector for days/weeks.

## Labels (future returns)
When you have enough snapshots, generate return labels:
```
python labeler.py --data-dir data --output returns.jsonl --horizons-min 1,5,15
```

You can also label backfill data:
```
python labeler.py --data-dir data_backfill --output returns_backfill.jsonl --horizons-min 1,5,15
```

## Labels (tick path / TP-SL barriers)
If you enabled raw events, you can label snapshots using the **true tick path**:
```
python labeler_barrier.py --snapshots-dir data --events-dir data --event-type aggTrade --horizon-sec 30 --tp-bps 12 --sl-bps 8 --side both --output barrier_labels.jsonl
```

## Build datasets
Return-forecast dataset:
```
python build_dataset.py --snapshots-dir data --labels returns.jsonl --output dataset.parquet --horizon-min 5 --mode all
```

Barrier dataset:
```
python build_barrier_dataset.py --snapshots-dir data --labels barrier_labels.jsonl --output barrier_dataset.parquet --side long
```

## Train
Return model:
```
python train.py --data dataset.parquet --target-column target --model-out model.joblib --meta-out model_meta.json
```

Barrier model:
```
python train_barrier.py --data barrier_dataset.parquet --target-column target --model-out barrier_model.joblib --meta-out barrier_meta.json
```

## Notes
- Logging **depth + trades for hundreds of symbols** is heavy. Increase gradually.
- If you need historical L2/liquidations, you *must* keep this running to record them.
- Consider running on a dedicated machine with fast disk.

## Key upgrades
- Depth order books are now stitched from REST snapshots + diff streams before computing depth features.
- Trade flow and liquidation features now use rolling windows (1s/5s/30s/5m) instead of lifetime totals.
- Snapshot writing is buffered and async to avoid blocking the event loop under load.
- Each snapshot now includes data freshness (`*AgeMs`) plus completeness metrics.
- Context market features (BTC/ETH trend + volatility) are included for every symbol.
- A minimal ML pipeline is now included: `build_dataset.py`, `train.py`, and `backtest.py`.

## ML pipeline
1. Generate execution-aware labels with lag tolerance:
```
python labeler.py --data-dir data --output returns.jsonl --horizons-min 1,5,15 --max-lag-pct 0.1 --fee-bps 4 --slippage-bps 2
```
2. Build datasets. Use separate regime and microstructure datasets:
```
python build_dataset.py --snapshots-dir data_backfill --labels returns_backfill.jsonl --output regime.parquet --horizon-min 5 --mode regime
python build_dataset.py --snapshots-dir data --labels returns.jsonl --output micro.parquet --horizon-min 5 --mode micro
```
3. Train and backtest:
```
python train.py --data micro.parquet --model-out micro_model.joblib --meta-out micro_model_meta.json
python backtest.py --data micro.parquet --model micro_model.joblib --meta micro_model_meta.json --long-threshold 0.02 --short-threshold 0.02
```
