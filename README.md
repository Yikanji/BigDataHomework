# 时序数据库系统调研：InfluxDB vs IoTDB

## 环境要求

- Docker >= 20.0 + Docker Compose >= 2.0
- Python >= 3.8
- pip 包: `influxdb-client`, `iotdb`, `pandas`, `numpy`

## 快速启动

### 1. 安装 Python 依赖

```bash
pip install influxdb-client apache-iotdb pandas numpy
```

### 2. 启动数据库

```bash
docker compose up -d
```

### 3. 验证服务状态

```bash
# InfluxDB
curl http://localhost:8086/health

# IoTDB
docker exec tsdb-iotdb /iotdb/sbin/start-cli.sh -h 127.0.0.1 -e 'show version'
```

### 4. 运行 Benchmark

```bash
cd benchmark
python3 run_benchmark.py
```

可修改脚本中的 `NUM_DEVICES`, `SENSORS_PER_DEVICE`, `POINTS_PER_SENSOR` 调整数据规模。

### 5. 停止服务

```bash
docker compose down
```

## 目录结构

```
BigData/
├── docker-compose.yml     # InfluxDB + IoTDB 容器编排
├── data/                  # 数据库持久化数据
│   ├── influxdb/
│   └── iotdb/
├── benchmark/
│   └── run_benchmark.py   # Python Benchmark 脚本
└── report/
    └── report.md          # 调研报告
```
