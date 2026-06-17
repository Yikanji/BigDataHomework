#!/usr/bin/env python3
"""IoTDB Bench-style write workload for InfluxDB 3 Core.

The official IoTDB Bench release has InfluxDB 1.x/2.x adapters but no
InfluxDB 3 adapter. This script keeps the same small local workload shape used
for the InfluxDB 2 IoTDB Bench run and writes it through the InfluxDB 3 v3 API.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from influxdb3_client import InfluxDB3Client, line_protocol


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="iotbench_influx3_equiv_opt")
    parser.add_argument("--devices", type=int, default=20)
    parser.add_argument("--sensors", type=int, default=5)
    parser.add_argument("--rows-per-loop", type=int, default=10)
    parser.add_argument("--loops", type=int, default=50)
    parser.add_argument("--batch-size", type=int, default=20000)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--output", default="var/benchmarks/iotbench-influxdb3-optimized.json")
    return parser


def generate_lines(devices: int, sensors: int, rows_per_loop: int, loops: int, base_ms: int):
    for loop in range(loops):
        for device in range(devices):
            for row in range(rows_per_loop):
                ts = base_ms + (loop * rows_per_loop + row) * 1000
                fields = {
                    f"s{sensor}": float(device * 0.1 + sensor + loop * 0.001 + row * 0.01)
                    for sensor in range(sensors)
                }
                yield line_protocol("iotbench", {"device": f"d{device}"}, fields, ts)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    idx = min(len(values) - 1, int(round((len(values) - 1) * pct)))
    return values[idx]


def timed_queries(client: InfluxDB3Client) -> dict[str, float | int]:
    queries = {
        "latest_ms": """
            SELECT time, s0
            FROM iotbench
            WHERE device = 'd0'
            ORDER BY time DESC
            LIMIT 1
        """,
        "range_ms": """
            SELECT time, s0
            FROM iotbench
            WHERE device = 'd0'
            ORDER BY time
        """,
        "aggregate_ms": """
            SELECT device, AVG(s0) AS mean_s0
            FROM iotbench
            GROUP BY device
        """,
    }
    results: dict[str, float | int] = {}
    for name, query in queries.items():
        start = time.time()
        rows = client.query_sql(query)
        results[name] = round((time.time() - start) * 1000, 3)
        results[f"{name}_rows"] = len(rows)
    return results


def main() -> None:
    args = build_parser().parse_args()
    client = InfluxDB3Client(args.db)
    client.recreate_database()

    line_count = args.devices * args.rows_per_loop * args.loops
    point_count = line_count * args.sensors
    base_ms = int(time.time() * 1000) - args.rows_per_loop * args.loops * 1000

    start = time.time()
    client.write_lines(
        generate_lines(args.devices, args.sensors, args.rows_per_loop, args.loops, base_ms),
        batch_size=args.batch_size,
        precision="millisecond",
        workers=args.workers,
    )
    elapsed = time.time() - start

    count_start = time.time()
    count_result = client.query_sql("SELECT COUNT(*) AS rows FROM iotbench")
    count_query_ms = (time.time() - count_start) * 1000
    query_results = timed_queries(client)

    result = {
        "benchmark": "IoTDB Bench equivalent for InfluxDB3",
        "database": "InfluxDB 3 Core",
        "db": args.db,
        "devices": args.devices,
        "sensors": args.sensors,
        "rows_per_loop": args.rows_per_loop,
        "loops": args.loops,
        "batch_size": args.batch_size,
        "workers": args.workers,
        "line_count": line_count,
        "point_count": point_count,
        "elapsed_s": elapsed,
        "throughput_points_s": point_count / elapsed if elapsed > 0 else 0,
        "throughput_lines_s": line_count / elapsed if elapsed > 0 else 0,
        "count_query_ms": count_query_ms,
        "count_result": count_result,
        "queries": query_results,
    }

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
