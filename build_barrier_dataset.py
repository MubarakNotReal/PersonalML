import argparse
import bisect
import glob
import json
import os
import sqlite3
import tempfile
from typing import Dict, Iterable, List, Tuple

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pd = None

try:
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pa = None
    pq = None


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


def build_labels_db(labels_path: str, tmp_dir: str) -> sqlite3.Connection:
    os.makedirs(tmp_dir, exist_ok=True)
    db_path = os.path.join(tmp_dir, "barrier_labels.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS labels (symbol TEXT, entryTime INTEGER, payload TEXT, PRIMARY KEY(symbol, entryTime))"
    )
    conn.execute("DELETE FROM labels")
    with open(labels_path, "r", encoding="utf-8") as handle:
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
            conn.execute(
                "INSERT OR REPLACE INTO labels(symbol, entryTime, payload) VALUES(?,?,?)",
                (symbol, int(entry_time), json.dumps(obj)),
            )
    conn.commit()
    return conn


def fetch_label(conn: sqlite3.Connection, symbol: str, entry_time: int) -> dict:
    cur = conn.execute(
        "SELECT payload FROM labels WHERE symbol=? AND entryTime=?",
        (symbol, entry_time),
    )
    row = cur.fetchone()
    if not row:
        return {}
    try:
        return json.loads(row[0])
    except Exception:
        return {}


def is_finite_num(value) -> bool:
    try:
        return value is not None and float(value) == float(value) and abs(float(value)) != float("inf")
    except Exception:
        return False


def write_jsonl(rows: List[dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


class RowWriter:
    def __init__(self, path: str, fmt: str):
        self.path = path
        self.fmt = fmt
        self.handle = None
        self.writer = None
        self.parquet_writer = None
        self.columns: List[str] = []

    def _ensure_columns(self, rows: List[dict]) -> None:
        if self.columns:
            return
        cols = set()
        for row in rows:
            cols.update(row.keys())
        self.columns = sorted(cols)

    def write_rows(self, rows: List[dict]) -> None:
        if not rows:
            return
        self._ensure_columns(rows)
        if self.fmt == "jsonl":
            if self.handle is None:
                self.handle = open(self.path, "w", encoding="utf-8")
            for row in rows:
                self.handle.write(json.dumps(row) + "\n")
            return
        if self.fmt == "csv":
            import csv
            if self.handle is None:
                self.handle = open(self.path, "w", encoding="utf-8", newline="")
                self.writer = csv.DictWriter(self.handle, fieldnames=self.columns)
                self.writer.writeheader()
            for row in rows:
                self.writer.writerow({k: row.get(k) for k in self.columns})
            return
        if self.fmt == "parquet":
            if pa is None or pq is None:
                raise RuntimeError("pyarrow not available for parquet streaming")
            table = pa.Table.from_pylist(rows, schema=self.parquet_writer.schema if self.parquet_writer else None)
            if self.parquet_writer is None:
                self.parquet_writer = pq.ParquetWriter(self.path, table.schema)
            self.parquet_writer.write_table(table)
            return

    def close(self) -> None:
        if self.parquet_writer is not None:
            self.parquet_writer.close()
        if self.handle is not None:
            self.handle.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build dataset from snapshots + barrier labels.")
    parser.add_argument("--snapshots-dir", default="data", help="Directory with snapshots_*.jsonl")
    parser.add_argument("--labels", required=True, help="Path to barrier_labels.jsonl")
    parser.add_argument("--output", default="barrier_dataset.parquet", help="Output dataset path")
    parser.add_argument("--format", choices=["parquet", "csv", "jsonl"], default="parquet")
    parser.add_argument("--side", choices=["long", "short"], default="long")
    parser.add_argument("--target-column", default="target", help="Target column name")
    parser.add_argument("--min-micro-completeness", type=float, default=0.25)
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Stream rows to disk to reduce memory usage (recommended for large datasets)",
    )
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--tmp-dir", default=None)
    args = parser.parse_args()

    labels = None
    labels_db = None
    if args.stream:
        tmp_dir = args.tmp_dir or tempfile.mkdtemp(prefix="build_barrier_")
        labels_db = build_labels_db(args.labels, tmp_dir)
    else:
        labels = load_labels(args.labels)
        if not labels:
            raise SystemExit("No barrier labels found.")

    rows: List[dict] = []
    missing_labels = 0
    writer = None
    if args.stream:
        writer = RowWriter(args.output, args.format)

    for obj in iter_snapshots(args.snapshots_dir):
        symbol = obj.get("symbol")
        time_value = obj.get("time")
        if not symbol or time_value is None:
            continue
        try:
            entry_time = int(time_value)
        except Exception:
            continue

        if labels_db is not None:
            label = fetch_label(labels_db, symbol, entry_time)
        else:
            label = labels.get((symbol, entry_time)) if labels is not None else None
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
        if args.stream:
            rows.append(row)
            if len(rows) >= args.batch_size:
                try:
                    writer.write_rows(rows)
                except RuntimeError:
                    writer = RowWriter(args.output, "jsonl")
                    writer.write_rows(rows)
                    args.format = "jsonl"
                rows = []
        else:
            rows.append(row)

    if args.stream:
        if rows:
            try:
                writer.write_rows(rows)
            except RuntimeError:
                writer = RowWriter(args.output, "jsonl")
                writer.write_rows(rows)
                args.format = "jsonl"
        if writer is not None:
            writer.close()
        print(
            f"Built {missing_labels} missing_labels | wrote streaming dataset to {args.output} ({args.format})."
        )
        if labels_db is not None:
            labels_db.close()
        return

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
