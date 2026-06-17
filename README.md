# 时序数据库系统调研：InfluxDB3 vs IoTDB2 vs PostgreSQL vs DolphinDB

课程大作业题目三。在 DROID 真实机器人操控数据集和标准 Benchmark 上，对比五款数据库系统的写入吞吐、查询延迟、磁盘占用及场景化时序操作能力。

## 系统版本

| 系统 | 镜像 | 端口 | 查询接口 |
|---|---|---|---|
| InfluxDB 3 Core | `influxdb:3.9.3-core` | 8181 | SQL (HTTP API) |
| Apache IoTDB | `apache/iotdb:2.0.8-standalone` | 6667 | SQL (RPC + JDBC) |
| PostgreSQL | `postgres:18.4` | 5432 | SQL |
| MySQL | `mysql:8.4` | 3306 | SQL |
| DolphinDB | `dolphindb/dolphindb:v3.00.5` | 8848 | 类 SQL + 函数式 (Python/RPC) |

---

## 1. 环境要求

- Docker >= 20.0 + Docker Compose >= 2.0
- Python >= 3.12 + Conda
- DROID 数据集（可选，仅 `droid_scenario.py` / `droid_benchmark.py` 需要）
  - TFRecord 格式，约 1.7TB（https://droid-dataset.github.io）
  - 设置环境变量 `DROID_PATH` 指向 `1.0.0/` 目录

---

## 2. 安装

### 2.1 创建 Conda 环境

```bash
conda create -n tfrecord_env python=3.12 -y
conda activate tfrecord_env
```

### 2.2 安装 Python 依赖

```bash
pip install \
  "pandas<3" \
  numpy \
  apache-iotdb \
  tfrecord \
  pyarrow \
  requests \
  "psycopg[binary]" \
  dolphindb \
  mysql-connector-python
```

> DolphinDB SDK 当前要求 `pandas<3`，因此显式指定版本。

### 2.3 拉取 Docker 镜像

```bash
docker compose pull
```

> 若 Docker Hub 不可达，参考 [Troubleshooting](#troubleshooting)。

---

## 3. 启动与验证

### 3.1 启动所有服务

```bash
docker compose up -d
```

### 3.2 验证各服务

```bash
# InfluxDB 3
docker exec tsdb-influxdb3 influxdb3 --version

# IoTDB
docker exec tsdb-iotdb /iotdb/sbin/start-cli.sh -h 127.0.0.1 -e 'show cluster'

# PostgreSQL
docker exec tsdb-postgres psql -U postgres -d droid -c 'SELECT 1'

# MySQL
docker exec tsdb-mysql mysql -uroot -proot123 -e 'SELECT 1'

# DolphinDB
python3 - <<'PY'
import dolphindb as ddb
s = ddb.Session()
s.connect("127.0.0.1", 8848, "admin", "123456")
print(s.run("version()"))
s.close()
PY
```

### 3.3 停止

```bash
docker compose down
```

---

## 4. Benchmark 脚本

### 4.1 脚本总览

| 脚本 | 对比系统 | 数据 | 用途 |
|---|---|---|---|
| `run_benchmark.py` | 全部 | 合成 | 基础读写吞吐、查询延迟、磁盘占用 |
| `droid_scenario.py` | 全部 | **DROID** | 场景化测试：降采样/插值/滑窗/跨轨迹聚合 |
| `droid_benchmark.py` | InfluxDB3 / IoTDB2 | **DROID** | 单轨迹读写 + 4 类基础查询 |
| `run_iotbench_out_of_order.py` | InfluxDB2 / IoTDB2 | 合成 | 乱序写入比例扫掠（0% / 30% / 50% / 80%） |
| `influx_cardinality_sweep.py` | InfluxDB2 / InfluxDB3 | 合成 | Series 基数膨胀对查询延迟的影响 |
| `influxdb3_iotbench_equiv.py` | InfluxDB3 | 合成 | InfluxDB 3 等价 IoTDB Bench 测试 |
| `postgres_compare.py` | PostgreSQL | **DROID** | 单库诊断脚本 |
| `mysql_compare.py` | MySQL | **DROID** | 旧版对照脚本，保留作历史参考 |
| `tsbs_influxdb3_adapter.py` | InfluxDB3 | TSBS | TSBS benchmark 适配 |
| `dolphindb_client.py` | DolphinDB | — | 工具模块 |
| `influxdb3_client.py` | InfluxDB3 | — | 工具模块 |

### 4.2 run_benchmark.py —— 合成数据基础测试

四库统一测试，自动生成模拟温度传感器数据。

```bash
cd benchmark

# 全部库，默认 100 万点
python3 run_benchmark.py

# 只跑特定库
BENCH_TARGETS=influx,iotdb python3 run_benchmark.py

# 小规模 smoke test
BENCH_NUM_DEVICES=10 BENCH_SENSORS_PER_DEVICE=3 BENCH_POINTS_PER_SENSOR=100 BENCH_BATCH_SIZE=100 python3 run_benchmark.py
```

环境变量：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `BENCH_TARGETS` | `all` | 逗号分隔：`influx` / `iotdb` / `postgres` / `dolphindb` / `all` |
| `BENCH_NUM_DEVICES` | `100` | 模拟设备数 |
| `BENCH_SENSORS_PER_DEVICE` | `10` | 每设备传感器数 |
| `BENCH_POINTS_PER_SENSOR` | `1000` | 每传感器时间点数 |
| `BENCH_BATCH_SIZE` | `5000` | 批量写入大小 |

### 4.3 droid_scenario.py —— DROID 场景化测试

**核心脚本**。使用 DROID 真实机器人操控数据，测试四个具身智能场景下的数据库表现。

```bash
# 前置条件
export DROID_PATH="/your/path/to/DROID/1.0.0"

# 全部库，默认 60 条轨迹、2 个分片
python3 droid_scenario.py

# 只跑指定库
BENCH_TARGETS=dolphindb python3 droid_scenario.py

# Synthetic DROID 小数据（无需真实 DROID，仅用于验证链路）
DROID_SYNTHETIC=1 DROID_SYNTHETIC_EPISODES=2 DROID_SYNTHETIC_STEPS=80 python3 droid_scenario.py
```

**四个测试场景**：

| 场景 | 操作 | 具身智能现实意义 |
|---|---|---|
| 降采样 | 原始 15Hz → 1s 窗口均值 | 监控面板"关节过去 1 小时走势" |
| 插值填充 | 删除 50 点后用 FILL / gap-fill / SQL 插值补全 | 传感器丢帧后保持运动规划连续 |
| 滑动窗口抖动检测 | 1s 窗口 STDDEV 检测关节速度突变 | 伺服电机异常振动预警 |
| 跨轨迹全局聚合 | 数十条轨迹的关节位置 AVG + COUNT | 训练数据质量巡检、故障模式统计 |

### 4.4 run_iotbench_out_of_order.py —— 乱序写入压力测试

```bash
cd benchmark
python3 run_iotbench_out_of_order.py
```

在 InfluxDB 2 和 IoTDB 2 上注入 0% / 30% / 50% / 80% 的乱序数据比例，测量写入吞吐和尾延迟变化。

### 4.5 influx_cardinality_sweep.py —— Cardinality 扫掠

```bash
cd benchmark
python3 influx_cardinality_sweep.py
```

在 InfluxDB 2 和 InfluxDB 3 上分别创建 1k / 10k / 50k 条 series，测量全量 last-point 查询延迟随基数增长的变化趋势。

---

## 5. 测试结果

所有结果保存在 `var/benchmarks/` 目录下。

| 文件 | 内容 |
|---|---|
| `summary.md` | 全量测试结果汇总 |
| `droid-scenario-real-60ep-2shards-mysql-postgres.log` | DROID 60 条轨迹五库对照 |
| `iotbench-ooo/summary.md` | 乱序写入扫掠结果 |
| `influx-cardinality/summary.md` | Cardinality 扫掠结果 |
| `tsbs/` | TSBS benchmark 加载与查询日志 |

---

## 6. 目录结构

```
BigData/
├── docker-compose.yml                    # 容器编排
├── config/
│   └── influxdb3-admin-token.json        # InfluxDB 3 管理 token
├── data/                                  # 数据库持久化数据 (gitignore)
│   ├── influxdb3/                        # InfluxDB 3
│   ├── iotdb2/                           # IoTDB 2.0
│   ├── postgres/                         # PostgreSQL
│   ├── mysql8/                           # MySQL
│   └── dolphindb/                        # DolphinDB
├── benchmark/
│   ├── run_benchmark.py                  # 合成数据基准测试
│   ├── droid_scenario.py                 # DROID 场景化测试（核心）
│   ├── droid_benchmark.py                # DROID 基础读写测试
│   ├── run_iotbench_out_of_order.py      # 乱序写入测试
│   ├── influx_cardinality_sweep.py       # Cardinality 扫掠
│   ├── influxdb3_iotbench_equiv.py       # InfluxDB 3 IoTBench 等价
│   ├── tsbs_influxdb3_adapter.py         # TSBS 适配
│   ├── postgres_compare.py               # PostgreSQL 单库诊断
│   ├── mysql_compare.py                  # MySQL 对照（历史参考）
│   ├── dolphindb_client.py               # DolphinDB 工具模块
│   └── influxdb3_client.py               # InfluxDB 3 工具模块
├── var/benchmarks/                        # 测试结果 (git tracked)
├── report/
│   ├── report.md                         # 调研报告
│   └── proposal.md                       # 课程 Proposal
└── README.md
```

---

## 7. Troubleshooting

### Docker 镜像无法拉取

本机 Docker Hub 直接访问受限时：

1. **通过跳板机拉取**：在有外网的机器上 `docker save` 导出为 tar，`scp` 回本机 `docker load`。
2. **DaoCloud 镜像**：`docker pull docker.m.daocloud.io/<镜像名> && docker tag`
3. **SOCKS5 代理**：`ssh -D 1080 user@proxy && HTTP_PROXY=socks5://127.0.0.1:1080 docker pull`

### IoTDB 2.0 启动失败

1. 确保 data 目录干净：`sudo rm -rf data/iotdb2/*` 后重试
2. 首次启动需要 2-3 分钟（ConfigNode + DataNode 初始化），检查 `docker logs tsdb-iotdb`
3. 确保 Docker 宿主机内核参数 `vm.max_map_count >= 262144`

### DROID 数据中不存在

```bash
git clone https://huggingface.co/dataset...  # 参考 DROID 官方文档
export DROID_PATH="/absolute/path/to/DROID/1.0.0"
```

或使用 Synthetic DROID 模式跳过真实数据：
```bash
DROID_SYNTHETIC=1 DROID_SYNTHETIC_EPISODES=5 python3 droid_scenario.py
```

---

## 授权

本仓库为大作业实验代码，仅供参考。
