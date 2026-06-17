# IoTDB Bench Out-of-Order Sweep

Workload: 20 devices, 5 sensors, 10 rows per write batch, 50 loops, 50,000 logical points.

| Target | Out-of-order ratio | Throughput points/s | Avg latency ms | p95 ms | p99 ms | Fail points | Log |
|---|---:|---:|---:|---:|---:|---:|---|
| IoTDB 2.0 | 0.0 | 34875.23 | 0.39 | 0.37 | 4.02 | 0 | `var/benchmarks/iotbench-ooo/logs/iotdb/ratio_0p0.log` |
| IoTDB 2.0 | 0.3 | 34088.05 | 0.42 | 0.43 | 3.53 | 0 | `var/benchmarks/iotbench-ooo/logs/iotdb/ratio_0p3.log` |
| IoTDB 2.0 | 0.5 | 34526.53 | 0.40 | 0.36 | 3.43 | 0 | `var/benchmarks/iotbench-ooo/logs/iotdb/ratio_0p5.log` |
| IoTDB 2.0 | 0.8 | 35843.96 | 0.35 | 0.39 | 3.31 | 0 | `var/benchmarks/iotbench-ooo/logs/iotdb/ratio_0p8.log` |
| InfluxDB 2.9.1 | 0.0 | 20118.08 | 1.41 | 2.63 | 3.81 | 0 | `var/benchmarks/iotbench-ooo/logs/influxdb2/ratio_0p0.log` |
| InfluxDB 2.9.1 | 0.3 | 14417.90 | 2.38 | 3.42 | 7.66 | 0 | `var/benchmarks/iotbench-ooo/logs/influxdb2/ratio_0p3.log` |
| InfluxDB 2.9.1 | 0.5 | 14264.58 | 2.42 | 6.82 | 14.87 | 0 | `var/benchmarks/iotbench-ooo/logs/influxdb2/ratio_0p5.log` |
| InfluxDB 2.9.1 | 0.8 | 20246.16 | 1.40 | 2.22 | 3.51 | 0 | `var/benchmarks/iotbench-ooo/logs/influxdb2/ratio_0p8.log` |
