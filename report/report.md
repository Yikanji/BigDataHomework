# 时序数据库系统调研：InfluxDB vs IoTDB

---

## 1. 背景与目标

时序数据库（Time Series Database, TSDB）是专门针对时间序列数据优化的数据库系统，广泛应用于物联网监控、金融分析、运维监控等领域。本次调研选取两个代表性的开源时序数据库——**InfluxDB** 和 **Apache IoTDB**，从系统架构、数据模型、存储引擎、查询语言、性能表现等方面进行深入对比分析。

### 调研对象

| 系统 | 开发者 | 版本 | 定位 |
|------|--------|------|------|
| InfluxDB | InfluxData | v2.7.12 | 通用时序数据库，监控/APM/实时分析 |
| Apache IoTDB | Apache 软件基金会 | v1.3.2 | 面向物联网的时序数据库，工业/智能制造 |

---

## 2. 系统架构分析

### 2.1 InfluxDB 架构

InfluxDB v2 采用单体架构，内部由以下核心组件组成：

- **TSM 存储引擎 (Time-Structured Merge Tree)**：一种针对时序数据优化的 LSM-tree 变体。写入先进入内存 WAL（Write-Ahead Log），再写入缓存的 Cache，达到阈值后合并压缩写入磁盘。与通用 LSM 不同，TSM 按时间范围 + Series Key 组织数据，使得时间范围扫描非常高效。

- **倒排索引 (Inverted Index / TSI)**：从 tag 值到 series ID 的映射。InfluxDB 的数据模型是 `measurement + tag set → field values`，每个唯一的 measurement+tags 组合构成一个 series。倒排索引使得按 tag 过滤的查询能快速定位目标 series。

- **分片 (Shard) 与过期策略**：数据按时间范围自动分片，每个 shard 覆盖一个固定时间窗口。过期数据可整片删除，IO 效率高。

- **查询引擎**：使用 Flux 函数式查询语言，支持管道式数据流处理。编译器将 Flux 语句转换为执行计划，push-down 下推到存储层过滤，减少数据传输。

**数据模型**：

```
measurement: temperature
tags:        location=room_a, sensor_id=1
field:       value=23.5
timestamp:   2026-05-11T12:00:00Z
```

### 2.2 IoTDB 架构

IoTDB v1.3 采用**分离式架构**，由 ConfigNode 和 DataNode 组成：

- **ConfigNode（元数据节点）**：管理集群元数据，包括存储组注册、TimeSeries Schema、权限控制、负载均衡决策等。采用 Raft 协议保证一致性。

- **DataNode（数据节点）**：负责实际数据存储和查询执行。每个 DataNode 独立管理本地的 TsFile 存储和 WAL。

- **TsFile 列式存储格式**：IoTDB 自研的文件格式，具有以下特性：
  - 按设备（Device）+ 时间组织数据 Chunk
  - 每个传感器（Measurement）单独存储为一个 Chunk，支持列式扫描
  - 支持多种编码方式（Gorilla、RLE、TS_2DIFF 等）和压缩算法
  - 文件尾部包含索引信息（BloomFilter + 时间范围），加速过滤

- **查询引擎**：支持类 SQL 语法，原生支持降采样（GROUP BY time）、插值（Fill）、路径通配（`root.**.s0`）等时序特有操作。

**数据模型（树形路径）**：

```
root.region.factory.device.sensor
└─ root (存储组)
   └─ test_bench.d0.s0 (时间序列)
```

---

## 3. 关键设计对比

| 维度 | InfluxDB v2 | Apache IoTDB v1.3 |
|------|-------------|-------------------|
| **存储引擎** | TSM Tree (LSM变体) | TsFile 列式文件 + WAL |
| **索引结构** | 倒排索引 (TSI) | BloomFilter + 时间范围索引 |
| **数据模型** | Measurement + Tags (标签集) | 树形路径 (root.a.b.c) |
| **查询语言** | Flux (函数式) | SQL-like |
| **架构** | 单体（v2）/ 集群（v3） | 分离式（ConfigNode + DataNode） |
| **压缩策略** | Snappy/GZIP/LZ4 | Gorilla/RLE/TS_2DIFF + Snappy/LZ4/Zstd |
| **过期删除** | Shard 级别整片删除 | TTL 按时间清理 |
| **写入链路** | WAL → Cache → TSM 压缩 | WAL → MemTable → TsFile Flush |
| **查询下推** | Filter push-down | Predicate push-down + 索引过滤 |
| **生态集成** | Telegraf, Grafana, Kapacitor | 与 Hadoop/Spark/Flink 集成 |

**核心差异**：

1. **数据模型哲学**：InfluxDB 的 tag/field 模型适合"多维度标签 + 少量数值"的监控场景；IoTDB 的树状路径模型天然映射工业设备层级（工厂→产线→设备→传感器）。

2. **存储格式**：InfluxDB 的 TSM 偏向写优化及时间范围扫描；IoTDB 的 TsFile 列式格式在分析查询（跨设备聚合单传感器）上更有优势，因为相同传感器的数据在物理上相邻存储。

3. **查询适用性**：InfluxDB 的 Flux 表达能力更强，适合流式数据处理；IoTDB 的 SQL 兼容性更好，易于与现有 BI 工具集成。

---

## 4. 部署与测试环境

### 4.1 环境配置

| 项目 | 规格 |
|------|------|
| CPU | 多核 x86_64 |
| 内存 | 62 GB |
| 磁盘 | 853 GB SSD |
| OS | Arch Linux |
| Docker | v29.4.3 + Compose v5.1.3 |

### 4.2 Docker Compose 部署

```yaml
services:
  influxdb:
    image: influxdb:2.7
    ports: ["8086:8086"]
    volumes: [./data/influxdb:/var/lib/influxdb2]
    environment:
      DOCKER_INFLUXDB_INIT_MODE: setup
      DOCKER_INFLUXDB_INIT_ORG: test-org
      DOCKER_INFLUXDB_INIT_BUCKET: test-bucket
      DOCKER_INFLUXDB_INIT_ADMIN_TOKEN: dev-token-for-testing

  iotdb:
    image: apache/iotdb:1.3.2-standalone
    ports: ["6667:6667", "18080:18080"]
    volumes: [./data/iotdb:/iotdb/data]
```

启动命令：`docker compose up -d`

### 4.3 测试工具

使用自研 Python Benchmark 脚本，基于官方客户端库：
- InfluxDB：`influxdb-client-python` (HTTP API)
- IoTDB：`apache-iotdb` (RPC Session API)

---

## 5. 性能测试

### 5.1 测试方案

**数据规模**：100 设备 × 10 传感器 × 1000 时间点 = **1,000,000 数据点**

**数据模型**：模拟温度传感器，值服从正态分布 N(25, 5)

**测试维度**：

| 测试项 | InfluxDB 查询 | IoTDB 查询 |
|--------|--------------|-----------|
| 写入吞吐 | 批量 write API | insert_records 批量 API |
| 点查询 | `last()` 过滤器 | `last_value(*)` |
| 范围查询 | `range(start: -1h)` | 时间范围 SELECT |
| 聚合查询 | `group() + mean()` | `COUNT(*)` 跨设备 |
| 多传感器查询 | `aggregateWindow(every: 1m)` | `AVG(s0~s4)` |
| 磁盘占用 | `du -sh data/influxdb` | `du -sh data/iotdb` |

### 5.2 测试结果

#### 写入性能

| 指标 | InfluxDB | IoTDB |
|------|----------|-------|
| 吞吐量 (pts/s) | **104,601** | 8,679 |
| 总耗时 (s) | 9.56 | 115.22 |

*注：IoTDB Python Session API 非批量写入最优路径，使用 Tablet API 或 CLI import 可获得更高吞吐。*

#### 查询延迟

| 查询类型 | InfluxDB (ms) | IoTDB (ms) | IoTDB 优势 |
|----------|--------------|-----------|-----------|
| 点查询 | 11.8 | 9.8 | 1.2x |
| 范围查询 (1h) | 24.4 | **4.5** | 5.4x |
| 全量聚合 | 39.8 | **7.1** | 5.6x |
| 多传感器 AVG | 26.3 | **5.9** | 4.5x |

#### 磁盘空间占用

| 指标 | InfluxDB | IoTDB |
|------|----------|-------|
| 100 万点占用 | **14 MB** | 73 MB |
| 每点平均 | ~14.7 bytes | ~76.5 bytes |

### 5.3 结果分析

1. **写入性能**：InfluxDB 的 TSM 引擎在写入路径上具有显著优势。其内存缓冲 + batch 压缩机制实现了 10 万+ pts/s 的吞吐。IoTDB 写入受限于 Python Session API 的单条写入方式，在生产环境中通过 Tablet API（列式批量写入）可将吞吐提升到相近水平。

2. **查询性能**：IoTDB 在分析型查询上全面领先，主要得益于：
   - **TsFile 列式存储**：查询单传感器（如 `AVG(s0)`）时，仅需读取对应列的数据块，而非整行
   - **BloomFilter + 时间范围索引**：加速数据定位，跳过不相关文件
   - **Chunk 级别统计信息**：在聚合查询时可直接使用预计算的最大/最小/计数等，避免完整扫描

3. **存储效率**：InfluxDB 的压缩更激进（数据 + 索引合并存储），空间占用更低。IoTDB 由于列式存储和索引分离，原始存储开销更大，但 TsFile 支持多种编码（Gorilla 对浮点数据压缩率极高），实际生产数据压缩后差距会缩小。

---

## 6. 总结与选型建议

### 6.1 系统特点总结

**InfluxDB**：
- 优势：写入吞吐高、部署简单、监控生态完善（Telegraf + Grafana + Kapacitor）、文档丰富
- 劣势：查询性能在大跨度聚合场景下不及列式存储、Flux 学习曲线较陡、v2→v3 迁移不确定性

**IoTDB**：
- 优势：列式存储使分析查询极快、树状数据模型天然适合工业物联网层级、SQL 兼容降低使用门槛、与大数据生态（Hadoop/Spark）集成好
- 劣势：部署复杂度较高（ConfigNode + DataNode 分离）、写入路径优化不如 InfluxDB 成熟、社区规模较小

### 6.2 选型建议

| 场景 | 推荐 |
|------|------|
| IT 运维监控（服务器/容器/网络指标） | **InfluxDB** |
| 工业物联网（设备传感器、智能制造） | **IoTDB** |
| 高频金融数据分析 | **InfluxDB** |
| 跨设备/跨区域的聚合分析 | **IoTDB** |
| 对 SQL 生态依赖强的场景 | **IoTDB** |
| 快速原型和简单部署 | **InfluxDB** |

---

## 7. 参考文献

1. InfluxDB Documentation. https://docs.influxdata.com/influxdb/v2/
2. Apache IoTDB Documentation. https://iotdb.apache.org/UserGuide/latest/
3. InfluxDB TSM Engine Design. https://docs.influxdata.com/influxdb/v2/reference/internals/storage-engine/
4. IoTDB TsFile Format. https://iotdb.apache.org/UserGuide/latest/StorageEngine/TsFile.html
5. Wang, C. et al. "Apache IoTDB: Time-series database for IoT applications." *Proceedings of the ACM SIGMOD*, 2020.
6. Time Series Benchmark Suite (TSBS). https://github.com/timescale/tsbs
