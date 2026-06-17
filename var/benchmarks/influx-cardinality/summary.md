# InfluxDB Cardinality Sweep

Same line protocol workload for InfluxDB 2 and InfluxDB 3. Each series has one `device` tag plus low-cardinality `fleet` and `site` tags.

| Target | Series | Points | Load points/s | Single last median | All-series last median | All-series rows |
|---|---:|---:|---:|---:|---:|---:|
| InfluxDB 2.9.1 | 1,000 | 5,000 | 45,536 | 3.30 ms | 19.68 ms | 1,000 |
| InfluxDB 2.9.1 | 10,000 | 50,000 | 174,816 | 4.30 ms | 189.30 ms | 10,000 |
| InfluxDB 2.9.1 | 50,000 | 250,000 | 176,488 | 3.44 ms | 879.83 ms | 50,000 |
| InfluxDB 3.9.3 Core | 1,000 | 5,000 | 16,853 | 8.30 ms | 14.83 ms | 1,000 |
| InfluxDB 3.9.3 Core | 10,000 | 50,000 | 10,151 | 3.18 ms | 21.43 ms | 10,000 |
| InfluxDB 3.9.3 Core | 50,000 | 250,000 | 10,033 | 8.35 ms | 96.10 ms | 50,000 |

Use this as a high-cardinality mechanism check, not as a replacement for TSBS or DROID.
