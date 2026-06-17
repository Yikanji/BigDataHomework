#!/usr/bin/env python3
"""InfluxDB 2/3 cardinality sweep using the same line protocol workload."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Iterable

import requests
from requests.adapters import HTTPAdapter

from influxdb3_client import InfluxDB3Client, line_protocol


INFLUXDB2_URL = "http://127.0.0.1:8086"
INFLUXDB2_ORG = "test-org"
INFLUXDB2_TOKEN = "dev-token-for-testing"

OUT_DIR = Path("var/benchmarks/influx-cardinality")


def parse_csv_list(raw: str) -> list[int]:
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round((len(ordered) - 1) * pct)))
    return ordered[idx]


def iter_batches(lines: Iterable[str], batch_size: int):
    batch: list[str] = []
    for line in lines:
        batch.append(line)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def generate_lines(table: str, series: int, points_per_series: int, base_ms: int):
    for point_idx in range(points_per_series):
        ts = base_ms + point_idx * 60_000
        for series_idx in range(series):
            yield line_protocol(
                table,
                {
                    "device": f"d{series_idx}",
                    "fleet": f"f{series_idx % 100}",
                    "site": f"s{series_idx % 10}",
                },
                {"value": float((series_idx % 1000) + point_idx * 0.01)},
                ts,
            )


class InfluxDB2Client:
    def __init__(
        self,
        url: str = INFLUXDB2_URL,
        org: str = INFLUXDB2_ORG,
        token: str = INFLUXDB2_TOKEN,
    ):
        self.url = url.rstrip("/")
        self.org = org
        self.token = token
        self.headers = {"Authorization": f"Token {token}"}
        self.session = requests.Session()
        self.session.trust_env = False
        adapter = HTTPAdapter(pool_connections=8, pool_maxsize=8)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.org_id = self._get_org_id()

    def _get_org_id(self) -> str:
        resp = self.session.get(
            f"{self.url}/api/v2/orgs",
            params={"org": self.org},
            headers=self.headers,
            timeout=30,
        )
        resp.raise_for_status()
        orgs = resp.json().get("orgs", [])
        if not orgs:
            raise RuntimeError(f"InfluxDB2 org not found: {self.org}")
        return orgs[0]["id"]

    def recreate_bucket(self, bucket: str) -> None:
        resp = self.session.get(
            f"{self.url}/api/v2/buckets",
            params={"orgID": self.org_id, "name": bucket},
            headers=self.headers,
            timeout=30,
        )
        if resp.status_code == 404:
            existing_buckets = []
        else:
            resp.raise_for_status()
            existing_buckets = resp.json().get("buckets", [])
        for existing in existing_buckets:
            delete_resp = self.session.delete(
                f"{self.url}/api/v2/buckets/{existing['id']}",
                headers=self.headers,
                timeout=30,
            )
            if delete_resp.status_code not in (204, 404):
                delete_resp.raise_for_status()

        create_resp = self.session.post(
            f"{self.url}/api/v2/buckets",
            json={"orgID": self.org_id, "name": bucket, "retentionRules": []},
            headers={**self.headers, "Content-Type": "application/json"},
            timeout=30,
        )
        if create_resp.status_code not in (201, 409):
            create_resp.raise_for_status()

    def write_lines(self, bucket: str, lines: Iterable[str], batch_size: int) -> None:
        for batch in iter_batches(lines, batch_size):
            resp = self.session.post(
                f"{self.url}/api/v2/write",
                params={"org": self.org, "bucket": bucket, "precision": "ms"},
                headers={**self.headers, "Content-Type": "text/plain"},
                data="\n".join(batch),
                timeout=60,
            )
            if resp.status_code != 204:
                raise RuntimeError(f"InfluxDB2 write failed: {resp.status_code} {resp.text[:500]}")

    def query_flux(self, query: str) -> int:
        resp = self.session.post(
            f"{self.url}/api/v2/query",
            params={"org": self.org},
            json={"query": query, "type": "flux"},
            headers={**self.headers, "Content-Type": "application/json", "Accept": "application/csv"},
            timeout=120,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"InfluxDB2 query failed: {resp.status_code} {resp.text[:500]}\nFlux: {query}")
        return count_flux_rows(resp.text)


def count_flux_rows(text: str) -> int:
    count = 0
    for line in text.splitlines():
        if not line or line.startswith("#") or line.startswith(",result,table,"):
            continue
        count += 1
    return count


def timed(repeats: int, fn):
    latencies: list[float] = []
    rows: list[int] = []
    for _ in range(repeats):
        start = time.time()
        row_count = fn()
        latencies.append((time.time() - start) * 1000)
        rows.append(row_count)
    return {
        "repeats": repeats,
        "rows_min": min(rows) if rows else 0,
        "rows_max": max(rows) if rows else 0,
        "latency_ms": {
            "median": statistics.median(latencies) if latencies else 0,
            "mean": statistics.mean(latencies) if latencies else 0,
            "p95": percentile(latencies, 0.95),
            "max": max(latencies) if latencies else 0,
        },
    }


def run_influxdb2(series: int, points_per_series: int, batch_size: int, repeats: int) -> dict[str, object]:
    bucket = f"cardinality_{series}"
    client = InfluxDB2Client()
    client.recreate_bucket(bucket)
    base_ms = int(time.time() * 1000) - points_per_series * 60_000
    total_points = series * points_per_series

    start = time.time()
    client.write_lines(bucket, generate_lines("cardinality", series, points_per_series, base_ms), batch_size)
    load_elapsed = time.time() - start

    last_device = f"d{series - 1}"
    single_query = f'''
from(bucket: "{bucket}")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "cardinality" and r._field == "value" and r.device == "{last_device}")
  |> last()
'''
    all_query = f'''
from(bucket: "{bucket}")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "cardinality" and r._field == "value")
  |> group(columns: ["device"])
  |> last()
'''

    return {
        "target": "influxdb2",
        "database": "InfluxDB 2.9.1",
        "series": series,
        "points_per_series": points_per_series,
        "total_points": total_points,
        "batch_size": batch_size,
        "load_elapsed_s": load_elapsed,
        "load_points_s": total_points / load_elapsed if load_elapsed > 0 else 0,
        "single_last": timed(repeats, lambda: client.query_flux(single_query)),
        "all_series_last": timed(repeats, lambda: client.query_flux(all_query)),
    }


def run_influxdb3(
    series: int,
    points_per_series: int,
    batch_size: int,
    repeats: int,
    db: str = "smoke",
) -> dict[str, object]:
    table = f"cardinality_s{series}_p{points_per_series}_{int(time.time())}"
    client = InfluxDB3Client(db)
    base_ms = int(time.time() * 1000) - points_per_series * 60_000
    total_points = series * points_per_series

    start = time.time()
    client.write_lines(generate_lines(table, series, points_per_series, base_ms), batch_size=batch_size)
    load_elapsed = time.time() - start

    last_device = f"d{series - 1}"
    single_query = f"""
        SELECT value
        FROM {table}
        WHERE device = '{last_device}'
        ORDER BY time DESC
        LIMIT 1
    """
    all_query = """
        SELECT device, MAX(time) AS last_time
        FROM {table}
        GROUP BY device
    """.format(table=table)

    return {
        "target": "influxdb3",
        "database": "InfluxDB 3.9.3 Core",
        "db": db,
        "table": table,
        "series": series,
        "points_per_series": points_per_series,
        "total_points": total_points,
        "batch_size": batch_size,
        "load_elapsed_s": load_elapsed,
        "load_points_s": total_points / load_elapsed if load_elapsed > 0 else 0,
        "single_last": timed(repeats, lambda: len(client.query_sql(single_query))),
        "all_series_last": timed(repeats, lambda: len(client.query_sql(all_query))),
    }


def write_markdown(results: list[dict[str, object]], output: Path) -> None:
    lines = [
        "# InfluxDB Cardinality Sweep",
        "",
        "Same line protocol workload for InfluxDB 2 and InfluxDB 3. Each series has one `device` tag plus low-cardinality `fleet` and `site` tags.",
        "",
        "| Target | Series | Points | Load points/s | Single last median | All-series last median | All-series rows |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in results:
        single = row["single_last"]["latency_ms"]["median"]  # type: ignore[index]
        all_last = row["all_series_last"]["latency_ms"]["median"]  # type: ignore[index]
        rows = row["all_series_last"]["rows_max"]  # type: ignore[index]
        lines.append(
            "| {database} | {series:,} | {points:,} | {load:,.0f} | {single:.2f} ms | {all_last:.2f} ms | {rows:,} |".format(
                database=row["database"],
                series=row["series"],
                points=row["total_points"],
                load=row["load_points_s"],
                single=single,
                all_last=all_last,
                rows=rows,
            )
        )
    lines.append("")
    lines.append("Use this as a high-cardinality mechanism check, not as a replacement for TSBS or DROID.")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", default="influxdb2,influxdb3")
    parser.add_argument("--series", default="1000,10000,50000")
    parser.add_argument("--points-per-series", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--query-repeats", type=int, default=3)
    parser.add_argument("--influxdb3-db", default="smoke")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    targets = [x.strip() for x in args.targets.split(",") if x.strip()]
    series_values = parse_csv_list(args.series)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    valid_targets = {"influxdb2", "influxdb3"}
    results: list[dict[str, object]] = []
    for target in targets:
        if target not in valid_targets:
            raise ValueError(f"unknown target: {target}")
        for series in series_values:
            print(f"[run] target={target} series={series}")
            if target == "influxdb3":
                result = run_influxdb3(
                    series,
                    args.points_per_series,
                    args.batch_size,
                    args.query_repeats,
                    args.influxdb3_db,
                )
            else:
                result = run_influxdb2(series, args.points_per_series, args.batch_size, args.query_repeats)
            results.append(result)
            print(
                "  load={load:,.0f} pts/s single={single:.2f}ms all={all_last:.2f}ms rows={rows}".format(
                    load=result["load_points_s"],
                    single=result["single_last"]["latency_ms"]["median"],  # type: ignore[index]
                    all_last=result["all_series_last"]["latency_ms"]["median"],  # type: ignore[index]
                    rows=result["all_series_last"]["rows_max"],  # type: ignore[index]
                )
            )

            (output_dir / "summary.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
            write_markdown(results, output_dir / "summary.md")

    print(f"[done] {output_dir / 'summary.md'}")


if __name__ == "__main__":
    main()
