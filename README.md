# 时序数据库系统调研：InfluxDB vs IoTDB

课程大作业题目三。对比 InfluxDB 和 IoTDB 在时序数据（含 DROID 真实机器人操控数据集）上的性能表现。

## 环境要求

- Docker >= 20.0 + Docker Compose >= 2.0
- Python >= 3.12 + Conda

## 快速启动

### 1. Python 环境

```bash
conda create -n tfrecord_env python=3.12 -y
conda activate tfrecord_env
pip install influxdb-client apache-iotdb pandas numpy tfrecord mysql-connector-python pyarrow
```

### 2. 启动数据库

```bash
docker compose up -d   # InfluxDB:8086, IoTDB:6667, MySQL:3306
```

### 3. 验证服务

```bash
curl http://localhost:8086/health                                       # InfluxDB
docker exec tsdb-iotdb /iotdb/sbin/start-cli.sh -h 127.0.0.1 -e 'show version'   # IoTDB
docker exec tsdb-mysql mysql -uroot -proot123 -e 'SELECT 1'                       # MySQL
```

### 4. 运行 Benchmark

> **注意**：DROID 相关脚本需要 DROID 数据集（TFRecord 格式，约 1.7TB）。
> 设置环境变量指定路径，否则脚本报错退出：
> ```bash
> export DROID_PATH="/your/path/to/DROID/1.0.0"
> ```

| 脚本 | 说明 | 数据依赖 |
|---|---|---|
| `run_benchmark.py` | 合成数据写入/查询/磁盘占用对比 | 无，自动生成 |
| `droid_benchmark.py` | DROID 真实轨迹的读写和 4 类查询 | **需要 DROID** |
| `droid_scenario.py` | 场景化测试：降采样、插值填充、滑动窗口、跨轨迹聚合 | **需要 DROID** |
| `mysql_compare.py` | MySQL 对照测试（同等数据下的 SQL 实现） | **需要 DROID** |

```bash
cd benchmark

# 合成数据测试（无需额外数据）
python3 run_benchmark.py

# 场景化测试（需要 DROID）
python3 droid_scenario.py

# MySQL 对照（需要 DROID）
python3 mysql_compare.py
```

### 5. 停止服务

```bash
docker compose down
```

## 目录结构

```
BigData/
├── docker-compose.yml          # InfluxDB + IoTDB + MySQL 容器编排
├── data/                       # 数据库持久化数据 (gitignore)
├── benchmark/
│   ├── run_benchmark.py        # 合成数据 benchmark
│   ├── droid_benchmark.py      # DROID 读写 + 查询测试
│   ├── droid_scenario.py       # DROID 场景化测试
│   └── mysql_compare.py        # MySQL 对照实验
├── report/
│   └── report.md               # 调研报告
└── README.md
```

## 测试场景说明

### droid_scenario.py（核心对比）

| 场景 | 测试内容 | 体现 TSDB 什么 |
|---|---|---|
| 降采样 | 原始 15Hz → 1s 窗口均值 | TSDB 原生 GROUP BY 时间窗口 |
| 插值填充 | 删除部分数据后用 FILL(LINEAR) 补全 | IoTDB 服务端插值 vs InfluxDB/MySQL 需客户端实现 |
| 滑动窗口 | 1s 窗口标准差检测关节抖动 | 列式存储 + 内置 STDDEV 的性能优势 |
| 跨轨迹聚合 | 60 条轨迹全局 AVG | 路径通配 `root.*.sensor` 的跨设备查询能力 |

### mysql_compare.py（关系数据库对照）

同等数据规模下，用 MySQL 实现相同的 4 个场景操作，展示关系数据库在处理时序/分析负载时的局限。

## 授权

本仓库为大作业实验代码，仅供参考。
