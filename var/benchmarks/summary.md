# TSDB Benchmark Summary

Generated on 2026-06-17.

## Environment

- InfluxDB 2.9.1: `127.0.0.1:8086`
- InfluxDB 3.9.3 Core: `127.0.0.1:8181`
- IoTDB 2.0.8 standalone: `127.0.0.1:6667`
- PostgreSQL 18.4: `127.0.0.1:5432`
- MySQL 8.4: `127.0.0.1:3306`
- DolphinDB 3.00.5: `127.0.0.1:8848`

## InfluxDB 3 Adapter Status And Comparability

IoTDB Bench and TSBS do not have native InfluxDB 3 adapters in the versions used here.

- IoTDB Bench was run officially for InfluxDB 2. InfluxDB 3 was run with a same-shape v3 API adapter.
- TSBS was run officially for InfluxDB 2 through the v1-compatible API. InfluxDB 3 reused TSBS-generated line protocol and ran equivalent SQL queries through the v3 API.
- The optimized InfluxDB 3 runner uses HTTP session reuse, large bounded batches, and multi-field line protocol rows where the data model allows it.
- Do not cite the optimized InfluxDB 3 IoTDB-Bench-equivalent throughput as a direct speedup over official InfluxDB 2. It is dominated by batch size, request count, and field packing differences.
- A small-batch calibration was added to show this sensitivity.

## IoTDB Bench

Workload: 20 devices, 5 sensors, 10 rows per loop, 50 loops, 50,000 points.

| Target | Runner | Points | Write throughput | Notes |
|---|---:|---:|---:|---|
| InfluxDB 2.9.1 | Official IoTDB Bench | 50,000 | 16,756 points/s | avg 1.92 ms, p50 1.12 ms, p95 1.96 ms |
| InfluxDB 3 Core | v3 adapter, optimized | 50,000 | 263,755 points/s | 10,000 multi-field rows, one large bounded batch; not directly comparable |
| InfluxDB 3 Core | v3 adapter, small-batch calibration | 5,000 | 50 points/s | 1,000 multi-field rows, 100 small HTTP batches; sensitivity check only |

Files:

- `var/benchmarks/iotbench-influxdb2-summary.json`
- `var/benchmarks/iotbench-influxdb3-optimized.json`
- `var/benchmarks/iotbench-influxdb3-smallbatch-calibration.json`

## IoTDB Bench Out-of-Order Sweep

Purpose: delayed / out-of-order data mechanism test for Part 2. This uses IoTDB Bench's built-in out-of-order generator rather than the DROID trace.

Workload: 20 devices, 5 sensors, 10 rows per write batch, 50 loops, 50,000 logical points. Out-of-order mode is `BATCH`, which models delayed batch backfill.

| Target | Out-of-order ratio | Write throughput | Avg latency | p95 latency | p99 latency | Fail points |
|---|---:|---:|---:|---:|---:|---:|
| IoTDB 2.0 | 0.0 | 34,875 points/s | 0.39 ms | 0.37 ms | 4.02 ms | 0 |
| IoTDB 2.0 | 0.3 | 34,088 points/s | 0.42 ms | 0.43 ms | 3.53 ms | 0 |
| IoTDB 2.0 | 0.5 | 34,527 points/s | 0.40 ms | 0.36 ms | 3.43 ms | 0 |
| IoTDB 2.0 | 0.8 | 35,844 points/s | 0.35 ms | 0.39 ms | 3.31 ms | 0 |
| InfluxDB 2.9.1 | 0.0 | 20,118 points/s | 1.41 ms | 2.63 ms | 3.81 ms | 0 |
| InfluxDB 2.9.1 | 0.3 | 14,418 points/s | 2.38 ms | 3.42 ms | 7.66 ms | 0 |
| InfluxDB 2.9.1 | 0.5 | 14,265 points/s | 2.42 ms | 6.82 ms | 14.87 ms | 0 |
| InfluxDB 2.9.1 | 0.8 | 20,246 points/s | 1.40 ms | 2.22 ms | 3.51 ms | 0 |

Reading: IoTDB is stable across this small delayed-batch sweep. InfluxDB 2 shows degradation at 0.3 and 0.5, but the 0.8 point recovers, so do not present this as a monotonic curve without a larger repeat run. Use it as evidence that delayed-data handling is a separate mechanism experiment from plain sequential write throughput.

Files:

- `var/benchmarks/iotbench-ooo/summary.md`
- `var/benchmarks/iotbench-ooo/summary.json`
- `benchmark/run_iotbench_out_of_order.py`

## TSBS

Workload: TSBS `cpu-only`, scale 100, 1 hour, 36,000 rows / 360,000 metrics, 100 lastpoint queries with 4 workers.

| Target | Runner | Load throughput | Query throughput | Query latency |
|---|---:|---:|---:|---|
| InfluxDB 2.9.1 | Official TSBS | 1,555,221 metrics/s | 318.69 qps | p50 12.20 ms, p95 18.08 ms, p99 20.64 ms |
| InfluxDB 3 Core | v3 adapter, optimized | 160,124 metrics/s | 152.89 qps | p50 25.66 ms, p95 34.16 ms, p99 37.43 ms |

Files:

- `var/benchmarks/tsbs/load-influxdb2-scale100-1h.json`
- `var/benchmarks/tsbs/query-influxdb2-lastpoint-scale100.json`
- `var/benchmarks/tsbs/influxdb3-optimized.json`

## InfluxDB Cardinality Sweep

Purpose: high-cardinality mechanism test for Part 2. This uses the same generated line protocol shape for InfluxDB 2 and InfluxDB 3, so it is more symmetric than the official-vs-adapter IoTDB Bench comparison.

Workload: one `device` tag per series, low-cardinality `fleet` and `site` tags, 5 points per series. Queries are repeated 3 times and report median latency.

| Target | Series | Points | Load throughput | Single-series last | All-series last | All-series rows |
|---|---:|---:|---:|---:|---:|---:|
| InfluxDB 2.9.1 | 1,000 | 5,000 | 45,536 points/s | 3.30 ms | 19.68 ms | 1,000 |
| InfluxDB 2.9.1 | 10,000 | 50,000 | 174,816 points/s | 4.30 ms | 189.30 ms | 10,000 |
| InfluxDB 2.9.1 | 50,000 | 250,000 | 176,488 points/s | 3.44 ms | 879.83 ms | 50,000 |
| InfluxDB 3 Core | 1,000 | 5,000 | 16,853 points/s | 8.30 ms | 14.83 ms | 1,000 |
| InfluxDB 3 Core | 10,000 | 50,000 | 10,151 points/s | 3.18 ms | 21.43 ms | 10,000 |
| InfluxDB 3 Core | 50,000 | 250,000 | 10,033 points/s | 8.35 ms | 96.10 ms | 50,000 |

Reading: InfluxDB 2 writes faster in this small generated workload, but all-series last-point latency scales much more sharply with series count. InfluxDB 3 writes slower through the v3 API path here, but its SQL all-series last query stays lower at 50k series. Use this to discuss cardinality and query-engine behavior, not as a replacement for TSBS.

Files:

- `var/benchmarks/influx-cardinality/summary.md`
- `var/benchmarks/influx-cardinality/summary.json`
- `benchmark/influx_cardinality_sweep.py`

## DROID, 20 Episodes

Dataset source: `zxm@10.213.80.111:/mnt/huawei_nas/Datasets/DROID/1.0.0`.

Local shard: `var/droid/1.0.0/r2d2_faceblur-train.tfrecord-00000-of-02048`.

Workload: 20 trajectories, 5,298 steps, 111,258 time-series points.

| Metric | InfluxDB 3 | IoTDB | PostgreSQL | DolphinDB |
|---|---:|---:|---:|---:|
| Write throughput | 52,527 points/s | 46,658 points/s | 98,636 rows/s | 446,060 rows/s |
| Downsample | 6.6 ms | 5.2 ms | 2.3 ms | 1.1 ms |
| Interpolation | 16.3 ms | 3.2 ms | 4.2 ms | 1.3 ms |
| Sliding window stddev | 6.0 ms | 3.7 ms | 1.1 ms | 0.9 ms |
| Cross-episode aggregation | 3.8 ms | 3.5 ms | 5.6 ms | 1.9 ms |
| Disk usage | 24M | 17M | 119M | 432.1M |

File:

- `var/benchmarks/droid-scenario-real-20ep.log`

## DROID, First Shard Full Run

Command target was 60 trajectories, but the first local shard only yielded 40 trajectories.

Workload: 40 trajectories, 12,783 steps, 268,443 time-series points.

| Metric | InfluxDB 3 | IoTDB | PostgreSQL | DolphinDB |
|---|---:|---:|---:|---:|
| Write throughput | 45,326 points/s | 93,592 points/s | 95,827 rows/s | 385,506 rows/s |
| Downsample | 8.2 ms | 3.9 ms | 1.1 ms | 1.0 ms |
| Interpolation | 17.7 ms | 3.1 ms | 4.0 ms | 1.2 ms |
| Sliding window stddev | 5.6 ms | 3.6 ms | 0.5 ms | 0.9 ms |
| Cross-episode aggregation | 4.8 ms | 3.5 ms | 9.4 ms | 3.6 ms |
| Disk usage | 27M | 18M | 205M | 440.2M |

File:

- `var/benchmarks/droid-scenario-real-60ep.log`

## DROID, 60 Episodes, Two Shards, PostgreSQL And MySQL

Local shards:

- `var/droid/1.0.0/r2d2_faceblur-train.tfrecord-00000-of-02048`
- `var/droid/1.0.0/r2d2_faceblur-train.tfrecord-00001-of-02048`

Workload: 60 trajectories, 17,979 steps, 377,559 time-series points.

| Metric | InfluxDB 3 | IoTDB | PostgreSQL | MySQL | DolphinDB |
|---|---:|---:|---:|---:|---:|
| Write throughput | 47,457 points/s | 115,009 points/s | 99,934 rows/s | 62,664 rows/s | 376,019 rows/s |
| Downsample | 7.3 ms | 4.2 ms | 2.1 ms | 0.7 ms | 0.9 ms |
| Interpolation | 18.1 ms | 2.6 ms | 4.0 ms | 6.1 ms | 1.2 ms |
| Sliding window stddev | 5.8 ms | 4.1 ms | 0.6 ms | 0.9 ms | 0.8 ms |
| Cross-episode aggregation | 7.9 ms | 4.4 ms | 12.9 ms | 135.6 ms | 3.3 ms |
| Disk usage | 38M | 19M | 268M | 337M | 447.3M |

File:

- `var/benchmarks/droid-scenario-real-60ep-2shards-mysql-postgres.log`

Previous run without MySQL:

- `var/benchmarks/droid-scenario-real-60ep-2shards.log`
