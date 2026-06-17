#!/usr/bin/env python3
"""TSBS Influx line protocol runner for InfluxDB 3 Core.

TSBS does not ship an InfluxDB 3 loader/query runner. This adapter reuses the
official TSBS-generated Influx line protocol data and runs equivalent SQL
queries through the InfluxDB 3 v3 API.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from influxdb3_client import InfluxDB3Client


DEFAULT_DATA = "var/benchmarks/tsbs/influx-cpu-scale100-1h.data"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default=DEFAULT_DATA)
    parser.add_argument("--db", default="tsbs_influx3_bench_opt")
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--query-count", type=int, default=100)
    parser.add_argument("--query-workers", type=int, default=4)
    parser.add_argument("--output", default="var/benchmarks/tsbs/influxdb3-optimized.json")
    return parser


def count_metrics(path: Path) -> tuple[int, int]:
    lines = 0
    metrics = 0
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            text = line.strip()
            if not text:
                continue
            lines += 1
            fields = text.split(" ", 2)[1]
            metrics += fields.count(",") + 1
    return lines, metrics


def read_lines(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            text = line.strip()
            if text:
                yield text


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round((len(ordered) - 1) * pct)))
    return ordered[idx]


def run_lastpoint_queries(client: InfluxDB3Client, count: int, workers: int) -> dict[str, object]:
    query = """
        SELECT *
        FROM cpu
        WHERE time = (SELECT MAX(time) FROM cpu)
        ORDER BY hostname
    """

    def one_query() -> tuple[float, int]:
        start = time.time()
        rows = client.query_sql(query)
        return (time.time() - start) * 1000, len(rows)

    latencies: list[float] = []
    row_counts: list[int] = []
    wall_start = time.time()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(one_query) for _ in range(count)]
        for future in as_completed(futures):
            latency_ms, rows = future.result()
            latencies.append(latency_ms)
            row_counts.append(rows)
    wall_s = time.time() - wall_start

    return {
        "query_count": count,
        "query_workers": workers,
        "query_wall_s": wall_s,
        "query_rate_qps": count / wall_s if wall_s > 0 else 0,
        "latency_ms": {
            "min": min(latencies) if latencies else 0,
            "median": statistics.median(latencies) if latencies else 0,
            "mean": statistics.mean(latencies) if latencies else 0,
            "max": max(latencies) if latencies else 0,
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
        },
        "query_result_rows_min": min(row_counts) if row_counts else 0,
        "query_result_rows_max": max(row_counts) if row_counts else 0,
    }


def main() -> None:
    args = build_parser().parse_args()
    data_path = Path(args.file)
    line_count, metric_count = count_metrics(data_path)

    client = InfluxDB3Client(args.db)
    client.recreate_database()

    load_start = time.time()
    client.write_lines(
        read_lines(data_path),
        batch_size=args.batch_size,
        precision="nanosecond",
        workers=args.workers,
    )
    load_elapsed = time.time() - load_start

    count_query = client.query_sql("SELECT COUNT(*) AS rows FROM cpu")
    query_results = run_lastpoint_queries(client, args.query_count, args.query_workers)

    result = {
        "benchmark": "TSBS generated data with InfluxDB3 v3 API adapter",
        "database": "InfluxDB 3 Core",
        "db": args.db,
        "file": str(data_path),
        "batch_size": args.batch_size,
        "workers": args.workers,
        "line_count": line_count,
        "metric_count": metric_count,
        "load_elapsed_s": load_elapsed,
        "load_metric_rate": metric_count / load_elapsed if load_elapsed > 0 else 0,
        "load_row_rate": line_count / load_elapsed if load_elapsed > 0 else 0,
        "count_query": count_query,
        **query_results,
    }

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
