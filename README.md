# 时序数据库系统调研：InfluxDB vs IoTDB vs DolphinDB

课程大作业题目三。对比 InfluxDB、IoTDB、DolphinDB 在时序数据（含 DROID 真实机器人操控数据集）上的性能表现，并使用 PostgreSQL 作为关系数据库对照。

当前实验版本：

| 系统 | 版本/镜像 | 端口 |
|---|---|---|
| InfluxDB 3 Core | `influxdb:3.9.3-core` | 8181 |
| Apache IoTDB | `apache/iotdb:2.0.8-standalone` | 6667 |
| PostgreSQL | `postgres:18.4` | 5432 |
| DolphinDB | `dolphindb/dolphindb:v3.00.5` | 8848 |

## 环境要求

- Docker >= 20.0 + Docker Compose >= 2.0
- Python >= 3.12 + Conda

## 快速启动

### 1. Python 环境

```bash
conda create -n tfrecord_env python=3.12 -y
conda activate tfrecord_env
pip install "pandas<3" numpy apache-iotdb tfrecord pyarrow requests "psycopg[binary]" dolphindb
```

> DolphinDB Python SDK 当前要求 `pandas<3`，建议使用独立 conda 环境，不要直接污染 base 环境。

### 2. 启动数据库

```bash
docker compose pull
docker compose up -d   # InfluxDB 3:8181, IoTDB:6667, PostgreSQL:5432, DolphinDB:8848
```

> IoTDB 2.0 使用 `data/iotdb2`，不会复用旧版 1.3 的 `data/iotdb` 目录。

### 3. 验证服务

```bash
docker exec tsdb-influxdb3 influxdb3 --version
docker exec tsdb-iotdb /iotdb/sbin/start-cli.sh -h 127.0.0.1 -e 'show cluster'
docker exec tsdb-postgres psql -U postgres -d droid -c 'SELECT 1'
python3 - <<'PY'
import dolphindb as ddb
s = ddb.Session()
s.connect("127.0.0.1", 8848, "admin", "123456")
print(s.run("version()"))
s.close()
PY
```

### 4. 运行 Benchmark

> **注意**：DROID 相关脚本需要 DROID 数据集（TFRecord 格式，约 1.7TB）。
> 设置环境变量指定路径，否则脚本报错退出：
> ```bash
> export DROID_PATH="/your/path/to/DROID/1.0.0"
> ```

| 脚本 | 说明 | 数据依赖 |
|---|---|---|
| `run_benchmark.py` | 四库合成数据写入/查询/磁盘占用对比 | 无，自动生成 |
| `droid_scenario.py` | 四库场景化测试：降采样、插值填充、滑动窗口、跨轨迹聚合 | **需要 DROID** |
| `droid_benchmark.py` | DROID 真实轨迹的基础读写和 4 类查询，当前只对比 InfluxDB/IoTDB | **需要 DROID** |
| `postgres_compare.py` | PostgreSQL 单库诊断脚本 | **需要 DROID** |
| `mysql_compare.py` | 旧版 MySQL 对照脚本，保留作历史参考 | **需要 DROID** |

```bash
cd benchmark

# 合成数据测试（无需额外数据）
python3 run_benchmark.py

# 场景化测试（需要 DROID）
python3 droid_scenario.py

# PostgreSQL 单库诊断（可选）
python3 postgres_compare.py
```

可以用 `BENCH_TARGETS` 只跑指定数据库，支持 `influx`、`iotdb`、`postgres`、`dolphindb`、`all`：

```bash
# 只跑 DolphinDB，正式合成数据规模
BENCH_TARGETS=dolphindb python3 run_benchmark.py

# 只跑 DolphinDB，小规模 smoke test
BENCH_TARGETS=dolphindb \
BENCH_NUM_DEVICES=2 \
BENCH_SENSORS_PER_DEVICE=2 \
BENCH_POINTS_PER_SENSOR=20 \
BENCH_BATCH_SIZE=20 \
python3 run_benchmark.py

# 只跑 DolphinDB 的 DROID 场景，使用真实 DROID 数据
BENCH_TARGETS=dolphindb DROID_PATH="/your/path/to/DROID/1.0.0" python3 droid_scenario.py

# 只用于验证 DolphinDB 场景链路的 synthetic DROID 小数据，不作为正式实验结果
BENCH_TARGETS=dolphindb \
DROID_SYNTHETIC=1 \
DROID_SYNTHETIC_EPISODES=2 \
DROID_SYNTHETIC_STEPS=80 \
BENCH_BATCH_SIZE=500 \
python3 droid_scenario.py
```

### 5. 停止服务

```bash
docker compose down
```

## 目录结构

```
BigData/
├── docker-compose.yml          # InfluxDB 3 + IoTDB 2.0 + PostgreSQL + DolphinDB 容器编排
├── config/                     # 本地开发 token 等容器配置
├── data/                       # 数据库持久化数据 (gitignore)
├── benchmark/
│   ├── influxdb3_client.py     # InfluxDB 3 HTTP API helper
│   ├── dolphindb_client.py     # DolphinDB Python API helper
│   ├── run_benchmark.py        # 四库合成数据 benchmark
│   ├── droid_benchmark.py      # DROID 读写 + 查询测试
│   ├── droid_scenario.py       # 四库 DROID 场景化测试
│   ├── postgres_compare.py     # PostgreSQL 对照实验
│   └── mysql_compare.py        # 旧版 MySQL 对照脚本
├── report/
│   └── report.md               # 调研报告
└── README.md
```

## 测试场景说明

### droid_scenario.py（核心对比）

| 场景 | 测试内容 | 体现 TSDB 什么 |
|---|---|---|
| 降采样 | 原始 15Hz → 1s 窗口均值 | TSDB 原生 GROUP BY 时间窗口 |
| 插值填充 | 删除部分数据后用 FILL(LINEAR) / gap-fill / SQL 插值补全 | IoTDB `FILL(LINEAR)`、InfluxDB 3 gapfill、PostgreSQL SQL、DolphinDB interpolation |
| 滑动窗口 | 1s 窗口标准差检测关节抖动 | 列式存储 + 内置 STDDEV 的性能优势 |
| 跨轨迹聚合 | 60 条轨迹全局 AVG | 路径通配 `root.*.sensor` 的跨设备查询能力 |

### postgres_compare.py（关系数据库单库诊断）

同等数据规模下，只运行 PostgreSQL 的 4 个场景操作。正式四库对照优先看 `droid_scenario.py`。

## 授权

本仓库为大作业实验代码，仅供参考。
