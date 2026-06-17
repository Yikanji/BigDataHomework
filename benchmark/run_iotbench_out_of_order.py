#!/usr/bin/env python3
"""Run IoTDB Bench out-of-order write sweep for IoTDB and InfluxDB 2.x."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools" / "iot-benchmark"
OUTPUT_DIR = ROOT / "var" / "benchmarks" / "iotbench-ooo"

TARGETS = {
    "iotdb": {
        "label": "IoTDB 2.0",
        "runner": TOOLS / "iotdb-2.0" / "iot-benchmark-iotdb-2.0",
        "base_config": TOOLS / "conf-iotdb-smoke" / "config.properties",
        "overrides": {
            "DB_SWITCH": "IoTDB-200-SESSION_BY_TABLET",
            "IoTDB_DIALECT_MODE": "tree",
            "HOST": "127.0.0.1",
            "PORT": "6667",
            "USERNAME": "root",
            "PASSWORD": "root",
            "DB_NAME": "iotbench_ooo_iotdb",
            "ENABLE_IOTDB_RPC_COMPRESSION": "true",
            "GROUP_NUMBER": "1",
        },
    },
    "influxdb2": {
        "label": "InfluxDB 2.9.1",
        "runner": TOOLS / "influxdb-2.0" / "iot-benchmark-influxdb-2.0",
        "base_config": TOOLS / "conf-influxdb2-run" / "config.properties",
        "overrides": {
            "DB_SWITCH": "InfluxDB-2.x",
            "HOST": "127.0.0.1",
            "PORT": "8086",
            "USERNAME": "admin",
            "PASSWORD": "password123456",
            "TOKEN": "dev-token-for-testing",
            "INFLUXDB_ORG": "test-org",
        },
    },
}


COMMON_OVERRIDES = {
    "IS_DELETE_DATA": "true",
    "INIT_WAIT_TIME": "1000",
    "BENCHMARK_WORK_MODE": "testWithDefaultPath",
    "LOOP": "50",
    "TEST_MAX_TIME": "0",
    "USE_MEASUREMENT": "true",
    "DEVICE_NUMBER": "20",
    "SENSOR_NUMBER": "5",
    "SCHEMA_CLIENT_NUMBER": "1",
    "DATA_CLIENT_NUMBER": "1",
    "OPERATION_PROPORTION": "1:0:0:0:0:0:0:0:0:0:0:0:0",
    "BATCH_SIZE_PER_WRITE": "10",
    "DEVICE_NUM_PER_WRITE": "1",
    "CREATE_SCHEMA": "true",
    "POINT_STEP": "1000",
    "TIMESTAMP_PRECISION": "ms",
    "TEST_DATA_PERSISTENCE": "None",
    "CSV_OUTPUT": "true",
    "IS_QUIET_MODE": "false",
    "LOG_PRINT_INTERVAL": "5",
    "RESULT_PRINT_INTERVAL": "0",
}


def ratio_slug(ratio: float) -> str:
    return str(ratio).replace(".", "p")


def build_config(target: str, ratio: float) -> Path:
    target_cfg = TARGETS[target]
    config_dir = OUTPUT_DIR / "configs" / target / f"ratio_{ratio_slug(ratio)}"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_dir / "config.properties"
    function_file = target_cfg["runner"] / "conf" / "function.xml"

    text = target_cfg["base_config"].read_text(encoding="utf-8")
    overrides = dict(COMMON_OVERRIDES)
    overrides.update(target_cfg["overrides"])
    overrides["DB_NAME"] = f"iotbench_ooo_{target}_r{ratio_slug(ratio)}"
    if ratio == 0:
        overrides["IS_OUT_OF_ORDER"] = "false"
        overrides["OUT_OF_ORDER_RATIO"] = "0"
    else:
        overrides["IS_OUT_OF_ORDER"] = "true"
        overrides["OUT_OF_ORDER_MODE"] = "BATCH"
        overrides["OUT_OF_ORDER_RATIO"] = str(ratio)

    lines = [
        "",
        "########################################################",
        "######## Local out-of-order Part 2 overrides ###########",
        "########################################################",
    ]
    lines.extend(f"{key}={value}" for key, value in overrides.items())
    config_file.write_text(text.rstrip() + "\n" + "\n".join(lines) + "\n", encoding="utf-8")
    if function_file.exists():
        shutil.copy2(function_file, config_dir / "function.xml")
    return config_dir


def parse_log(text: str) -> dict[str, object]:
    result: dict[str, object] = {}
    ingest_line = None
    latency_line = None
    for line in text.splitlines():
        if re.match(r"^INGESTION\s+\d+", line):
            if ingest_line is None:
                ingest_line = line
            else:
                latency_line = line

    if ingest_line:
        parts = ingest_line.split()
        result.update(
            {
                "ok_operations": int(parts[1]),
                "ok_points": int(parts[2]),
                "fail_operations": int(parts[3]),
                "fail_points": int(parts[4]),
                "throughput_points_s": float(parts[5]),
            }
        )
    if latency_line:
        parts = latency_line.split()
        result["latency_ms"] = {
            "avg": float(parts[1]),
            "min": float(parts[2]),
            "p10": float(parts[3]),
            "p25": float(parts[4]),
            "median": float(parts[5]),
            "p75": float(parts[6]),
            "p90": float(parts[7]),
            "p95": float(parts[8]),
            "p99": float(parts[9]),
            "p999": float(parts[10]),
            "max": float(parts[11]),
            "slowest_thread": float(parts[12]),
        }

    elapsed_match = re.search(r"Test elapsed time \(not include schema creation\): ([0-9.]+) second", text)
    if elapsed_match:
        result["elapsed_s"] = float(elapsed_match.group(1))
    return result


def run_one(target: str, ratio: float) -> dict[str, object]:
    config_dir = build_config(target, ratio)
    target_cfg = TARGETS[target]
    log_dir = OUTPUT_DIR / "logs" / target
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"ratio_{ratio_slug(ratio)}.log"

    cmd = [
        str(target_cfg["runner"] / "benchmark.sh"),
        "-cf",
        str(config_dir),
        "-heapsize",
        "512m",
        "-maxheapsize",
        "1g",
    ]
    started_at = time.time()
    proc = subprocess.run(
        cmd,
        cwd=target_cfg["runner"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    log_file.write_text(proc.stdout, encoding="utf-8")
    parsed = parse_log(proc.stdout)
    result = {
        "target": target,
        "database": target_cfg["label"],
        "ratio": ratio,
        "out_of_order": ratio != 0,
        "mode": "BATCH" if ratio != 0 else "ordered",
        "config_dir": str(config_dir.relative_to(ROOT)),
        "log": str(log_file.relative_to(ROOT)),
        "returncode": proc.returncode,
        "wall_s": time.time() - started_at,
        **parsed,
    }
    return result


def write_outputs(results: list[dict[str, object]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_json = OUTPUT_DIR / "summary.json"
    summary_md = OUTPUT_DIR / "summary.md"
    summary_json.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "# IoTDB Bench Out-of-Order Sweep",
        "",
        "Workload: 20 devices, 5 sensors, 10 rows per write batch, 50 loops, 50,000 logical points.",
        "",
        "| Target | Out-of-order ratio | Throughput points/s | Avg latency ms | p95 ms | p99 ms | Fail points | Log |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for result in results:
        latency = result.get("latency_ms") or {}
        lines.append(
            "| {database} | {ratio} | {throughput:.2f} | {avg:.2f} | {p95:.2f} | {p99:.2f} | {fail_points} | `{log}` |".format(
                database=result["database"],
                ratio=result["ratio"],
                throughput=float(result.get("throughput_points_s", 0)),
                avg=float(latency.get("avg", 0)),
                p95=float(latency.get("p95", 0)),
                p99=float(latency.get("p99", 0)),
                fail_points=int(result.get("fail_points", 0)),
                log=result["log"],
            )
        )
    summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", default="iotdb,influxdb2")
    parser.add_argument("--ratios", default="0,0.3,0.5,0.8")
    args = parser.parse_args()

    targets = [item.strip() for item in args.targets.split(",") if item.strip()]
    ratios = [float(item.strip()) for item in args.ratios.split(",") if item.strip()]
    results: list[dict[str, object]] = []

    for target in targets:
        for ratio in ratios:
            print(f"[run] target={target} ratio={ratio}")
            result = run_one(target, ratio)
            print(
                f"  rc={result['returncode']} throughput={result.get('throughput_points_s', 0)} "
                f"fail_points={result.get('fail_points', 'n/a')} log={result['log']}"
            )
            results.append(result)
            write_outputs(results)

    write_outputs(results)
    print(f"[done] {OUTPUT_DIR / 'summary.md'}")


if __name__ == "__main__":
    main()
