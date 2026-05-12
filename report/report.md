# 时序数据库系统调研：InfluxDB vs IoTDB
## ——面向具身智能场景的胜任力分析与 MySQL 对照

---

## 1. 背景与目标

时序数据库是专门针对时间序列数据优化的数据库系统，广泛应用于 IT 运维监控、工业物联网等领域。然而，具身智能（Embodied AI）——机器人操控、VLA 模型训练、多模态感知与决策——产生的时序数据与经典监控场景有本质差异。

本报告选取两个代表性开源时序数据库 **InfluxDB** 和 **Apache IoTDB**，从系统架构、数据模型、存储引擎等维度进行深入分析，并设计两阶段实验：

1. **通用 benchmark**：合成数据下的读写吞吐和查询延迟
2. **具身智能场景 benchmark**：使用 **DROID 数据集**（真实机器人操控轨迹）测试降采样、插值填充、滑动窗口异常检测、跨轨迹聚合四个场景，并引入 **MySQL** 作为关系数据库对照

调研目标不仅是比较性能，更是**精准定位 TSDB 的设计假设与具身智能负载之间的契合点和失配点**，为"具身智能需要什么样的新一代数据系统"提供实证基础。

### 调研对象

| 系统 | 开发者 | 版本 | 定位 |
|------|--------|------|------|
| InfluxDB | InfluxData | v2.7.12 | 通用时序数据库，IT 运维监控/实时分析 |
| Apache IoTDB | Apache 基金会 | v1.3.2 | 工业物联网时序数据库，设备/传感器层级 |
| MySQL | Oracle | 8.0 | 关系数据库，通用 OLTP 基准 |

---

## 2. 系统架构分析

### 2.1 InfluxDB

**起源**：InfluxData 公司 2013 年始建，目标用户为 DevOps——存服务器指标、应用性能、网络监控。核心假设：数千台服务器持续上报数十种标量指标，tag 基数远小于数据行数，写多读少。

**TSM 存储引擎 (Time-Structured Merge Tree)**：LSM-tree 的时序优化变体。写入先入 WAL → 内存 Cache → 达到阈值后合并压缩落盘。按时间范围 + Series Key 组织数据，时间范围扫描高效。

**倒排索引 (TSI)**：tag 值到 series ID 的映射，低基数 tag 过滤是核心查询模式。

**查询语言**：Flux 函数式语言，支持管道式数据流处理和 push-down 下推。

### 2.2 Apache IoTDB

**起源**：清华软件学院研发，2018 年捐给 Apache。目标场景为电力电网、工厂设备、车联网——"非常多的设备，每个设备有一组固定传感器"。数据模型为树状路径：`root.工厂.产线.设备.传感器`。

**分离式架构**：ConfigNode（元数据管理，Raft 一致性）+ DataNode（数据存储）。边-云协同是一等设计目标。

**TsFile 列式存储**：自研文件格式，按设备+时间组织数据 Chunk。每个传感器单独存储为一个 Chunk，支持列式扫描。文件尾部含 BloomFilter + 时间范围索引。编码支持 Gorilla、RLE、TS_2DIFF，压缩支持 LZ4/Zstd。

**查询引擎**：类 SQL，原生支持降采样（`GROUP BY` 时间）、插值（`FILL(LINEAR)`）、路径通配（`root.**.sensor`）。

### 2.3 MySQL（对照）

InnoDB 存储引擎，B+ 树索引，行式存储。无时序专用优化。用于展示"通用数据库实现 TSDB 操作"的额外开销。

---

## 3. 关键设计对比

| 维度 | InfluxDB v2 | IoTDB v1.3 | MySQL 8.0 |
|------|-------------|------------|-----------|
| **存储引擎** | TSM Tree (LSM 变体) | TsFile 列式文件 | InnoDB B+ 树行式 |
| **索引结构** | 倒排索引 (TSI) | BloomFilter + 时间范围 | B+ 树主键+辅助索引 |
| **数据模型** | Measurement + Tags | 树状路径 | 关系表 |
| **时序原生能力** | 降采样、过期策略 | 降采样、FILL、TTL | 无（需手写 SQL） |
| **写入优化** | WAL + Cache + 批量压缩 | WAL + MemTable + Flush | 索引维护 + redo log |
| **分析查询** | 需遍历所有 series point | 列式跳过无关维度 | 全表扫描或索引覆盖 |

**核心差异**：

1. **数据模型**：InfluxDB tag/field 模型适合多维度标签监控；IoTDB 树状路径天然映射设备层级。两种模型都不支持"episode"（有起止、有任务标签的离散操作片段）作为一等公民。
2. **存储格式**：IoTDB 列式格式在聚合查询（跨设备求单一传感器均值）上占优；InfluxDB 行/列混合格式在时间范围扫描上高效。
3. **查询语义**：两系统的算子是 SUM/AVG/PERCENTILE，缺乏 DTW 距离、轨迹形状匹配、滑动窗口标准差等具身场景需要的复杂时序操作（MySQL 同样缺乏）。

---

## 4. 部署与测试环境

| 项目 | 规格 |
|------|------|
| CPU | 多核 x86_64 |
| 内存 | 62 GB |
| 磁盘 | 853 GB SSD |
| OS | Arch Linux |
| Docker | v29.4.3 + Compose v5.1.3 |

### Docker Compose 部署

```yaml
services:
  influxdb:
    image: influxdb:2.7
    ports: ["8086:8086"]
    volumes: [./data/influxdb:/var/lib/influxdb2]
    environment:
      DOCKER_INFLUXDB_INIT_ORG: test-org
      DOCKER_INFLUXDB_INIT_BUCKET: test-bucket
      DOCKER_INFLUXDB_INIT_ADMIN_TOKEN: dev-token-for-testing

  iotdb:
    image: apache/iotdb:1.3.2-standalone
    ports: ["6667:6667"]
    volumes: [./data/iotdb:/iotdb/data]

  mysql:
    image: mysql:8.0
    ports: ["3306:3306"]
    environment:
      MYSQL_ROOT_PASSWORD: root123
      MYSQL_DATABASE: droid
```

### 测试工具

自研 Python Benchmark 脚本，基于官方客户端库：
- InfluxDB：`influxdb-client-python` (HTTP API)
- IoTDB：`apache-iotdb` (RPC Session API)
- MySQL：`mysql-connector-python`
- 数据解析：`tfrecord`（DROID TFRecord 格式）

---

## 5. 实验一：通用合成数据 Benchmark

### 5.1 方案

**数据规模**：100 设备 × 10 传感器 × 1000 点 = **1,000,000 数据点**

**数据模型**：模拟温度传感器，正态分布 N(25, 5)

### 5.2 结果

| 指标 | InfluxDB | IoTDB |
|------|----------|-------|
| 写入吞吐 (pts/s) | **104,601** | 8,679 |
| 点查询 (ms) | 11.8 | 9.8 |
| 范围查询 1h (ms) | 24.4 | **4.5** |
| 全量聚合 (ms) | 39.8 | **7.1** |
| 磁盘占用 (100万点) | **14 MB** | 73 MB |

### 5.3 分析

- **InfluxDB 写快 12×**：TSM 的 WAL → Cache → 批量压缩链路成熟，内存缓冲吸收写入峰值
- **IoTDB 查快 5-6×**：TsFile 列式存储使聚合查询只需扫描目标列的 Chunk，Chunk 级预聚合统计信息可直接返回
- **InfluxDB 省空间 5×**：数据与索引合并存储，IoTDB 列式+索引分离导致存储开销更大

---

## 6. 实验二：DROID 真实机器人数据 Benchmark

### 6.1 数据与测试方案

**数据**：从 DROID 数据集（76k 条机器人操控轨迹）中加载 **60 条真实轨迹**，包含关节位置（7DOF）、关节速度（7DOF）、末端位姿（6DOF）、夹爪位置（1DOF）等 6 类时序字段，共 **377,559** 个数据点。DROID 控制频率 ~15Hz。

**四个场景测试**：

DROID 数据集模拟的是真实机器人操控过程——机械臂抓取物体、拧螺丝、开门等操作任务，每个片段 10-30 秒。以下四个场景分别映射到工业生产、研发调试、故障诊断中最常出现的数据库查询需求。

---

**场景 1：降采样（原始 15Hz → 1s 窗口均值）**

- **测试操作**：对单关节位置数据，将每 15 个原始点聚合成 1 个秒级均值
- **具身智能场景**：机器人操作台部署后，运维人员需要持续监控各关节的运动趋势——"过去 24 小时 shoulder_pitch 关节的平均角度曲线是否在漂移？"原始 15Hz 数据量太大，必须按秒/分钟降采样后存入监控面板
- **为什么测它**：降采样是 TSDB 最核心的原生能力（`GROUP BY` 时间窗口）。关系库要实现同等功能需手写 `FLOOR(timestamp/1000)` 等分桶逻辑，且没有时间窗口的索引优化

**场景 2：插值填充（FILL LINEAR）**

- **测试操作**：人为删除关节位置数据的中间 50 个连续点（模拟 3 秒丢帧），用线性插值补全缺失区段
- **具身智能场景**：真实工厂环境中 WiFi 干扰、视觉遮挡、编码器瞬时故障都会导致短暂丢帧。控制系统需要接收连续轨迹——断掉的 3 秒关节曲线不是"跳过就行"，而是必须用合理值填充以保证运动规划的连续性
- **为什么测它**：TSDB 的 `FILL` 语法在服务端直接返回填充后的数据，无需将原始数据拉回客户端再插值。这对数据量大、实时性要求高的机器人系统是刚性需求。MySQL 实现同等功能需手写 `LAG() OVER` + `LEAD() OVER` 双层窗口函数，6 行 SQL 才能等价 IoTDB 一句 `FILL(LINEAR)`

**场景 3：滑动窗口抖动检测（STDDEV）**

- **测试操作**：在单关节速度数据上，每 1 秒窗口计算标准差，找出标准差突高的片段
- **具身智能场景**：机械臂伺服电机的抖动是疲劳损坏/螺丝松动的早期信号——关节角速度的瞬时标准差突然升高→电机控制环振荡。一条真实的操作轨迹（如拧螺丝）通常 15-30 秒、200-500 步，需要在整条轨迹的每个 1 秒窗口上连续计算 STDDEV
- **为什么测它**：这是一个典型的时间序列模式查询——不是"查某个时间点的值"，而是"找出符合模式特征的时间片段"。TSDB 内置 `STDDEV()` 聚合 + 时间窗口让这个查询直接在存储层完成。关系库同样能算，但行式存储导致每次窗口计算都要扫描完整行的所有传感器列，IO 量数倍于列式存储

**场景 4：跨轨迹全局统计**

- **测试操作**：对数据库中全部 60 条轨迹的所有关节位置，计算全局 AVG 和 COUNT
- **具身智能场景**：这对应两种真实需求。**(1) 数据集质量管理**：76k 条 DROID 轨迹入库后，质量工程师要巡检——"所有轨迹中，哪个关节的读数方差最大？是否有轨迹的关节角分布异常（比如某条轨迹的 shoulder 始终卡在机械限位）？"**(2) VLA 模型训练前的数据筛选**：训练机器人基础模型（如 OpenVLA、π0）前，需要从百万条轨迹中筛选"关节活动范围正常的"、"无传感器故障的"子集，这需要全量聚合查询
- **为什么测它**：这是列式存储 vs 行式存储差距最大的操作。跨轨迹查询意味着 IO 覆盖所有 episode 的数据文件——列式存储只需读关节位置一列，行式存储必须全表扫描每一行

---

### 6.2 结果：三库对照

| 场景 | InfluxDB | IoTDB | MySQL |
|---|---|---|---|
| 写入 (pts/s) | 89,605 | **121,191** | 9,560 |
| 降采样 (ms) | 13.9 | **1.1** | 10.5 |
| 插值填充 (ms) | 4.4* | **1.5** | 11.6† |
| 滑动窗口 STDDEV (ms) | 59.9 | **2.1** | 6.4 |
| 跨轨迹聚合 (ms) | 17.9 | **2.4** | **628.9** |
| 磁盘占用 (38万点) | **12M** | 24M | — |

> \* InfluxDB 无原生 FILL，需客户端插值；† MySQL 需 6 行 LAG/LEAD 窗口函数 SQL

### 6.3 分析

**场景 1 (降采样)**：IoTDB 的 TsFile Chunk 自带 min/max/sum/count 预计算，`GROUP BY(1s)` 直接取元数据，1.1ms 返回。InfluxDB 和 MySQL 分别需 13.9ms 和 10.5ms 展开原始点计算。

**场景 2 (插值填充)**：IoTDB 一句 `FILL(LINEAR)` 服务端返回。InfluxDB 无对等功能，需在客户端拉数据→插值→理解。MySQL 需手写 `LAG() OVER + LEAD() OVER + CASE WHEN` 6 行 SQL 实现等价逻辑，数据量大时窗口函数性能指数级下降。

**场景 3 (滑动窗口)**：IoTDB 列式存储只读 jvel 一列，`STDDEV() + GROUP BY(1s)` 2.1ms。MySQL 行存需扫描每行的全部列（关节×7 + 位姿×6 + 夹爪），IO 量多倍，6.4ms。InfluxDB 的 aggregateWindow 无预聚合标准差，需实时计算，59.9ms。

**场景 4 (跨轨迹聚合)**：IoTDB 路径通配 `root.droid.*.jpos_d0` 在物理存储中就是连续的一段列，只读关节位置，2.4ms。MySQL 需**全表扫描 37 万行**做 `GROUP BY`，耗时 628.9ms——**IoTDB 快了 262 倍**。这是列式存储 vs 行式存储最极致的展示。

---

## 7. TSDB 在具身智能场景下的胜任力评估

### 7.1 胜任区（TSDB 做得好的）

1. **单传感器时间窗口聚合**（降采样、连续聚合）：TSDB 的核心设计假设，两个系统都表现优秀
2. **平滑信号的压缩**：Gorilla/delta-of-delta 等压缩算法对缓变信号（关节匀速运动）压缩比可达 10-20×
3. **按时间范围的点查询和范围查询**：时间索引成熟，延迟低

### 7.2 失配区（TSDB 做不了或做不好的）

1. **Episode 不是一等公民**
   - 机器人数据以 episode（操作片段）为自然单位，有起止时间、任务标签、成功标志
   - 两个 TSDB 都只有连续时间流概念，查"episode_5 的全部数据"需应用层维护额外元数据表

2. **高维张量字段无原生支持**
   - xHand 触觉是 16×16 阵列 @1kHz，G1 全身 23 关节 @500Hz——这些不是标量
   - 存入 TSDB 只有三种烂方案：每维一列（宽表爆炸）、JSON（压缩失效）、BLOB（退化为文件存储）

3. **多模态盲区**
   - 图像、点云、语言指令与关节数据共存于同一 episode，TSDB 只能管标量
   - 跨模态查询（"视觉相似且力曲线匹配"）无法在 TSDB 内完成

4. **设计假设与具身数据不兼容**
   - Tag 低基数假设：episode_id 百万级别，tag 索引膨胀
   - 有序写入假设：多传感器异步采集、网络延迟导致乱序
   - 压缩假设：接触瞬间力矩跳变、动作切换速度突变，Gorilla 压缩比骤降

### 7.3 MySQL 对照的启示

MySQL 在 4 个场景均未胜出——即使是它的主场（小范围精确查询、事务一致性），在时序分析负载上也无法和专用系统竞争。但 TSDB 同样不是正确答案。**具身智能需要的是介于专用 TSDB 和通用关系库之间的第三种方案。**

---

## 8. 讨论：具身智能需要什么样的数据系统

### 8.1 设计原则

基于实验发现的失配点，新一代具身智能数据系统应具备：

1. **Episode 作为一等公民**：schema 层面支持 episode 起止时间、任务标签、成功标志、本体类型
2. **多模态联合存储**：RGB/深度/点云走对象存储+引用指针，标量/低维信号走列式压缩，元数据走关系表，三者在同一 episode 标识下对齐
3. **张量时序原生支持**：为高维同构传感器（触觉阵列、关节组）设计块的列式存储，利用空间相关性压缩
4. **跨存储引擎的查询优化器**：一个查询同时涉及时序聚合 + 向量相似检索 + 元数据过滤时，查询计划能自动拆解、调度、合并结果

### 8.2 架构方向：具身数据 Lakehouse

推荐基于现代数据湖仓一体（Lakehouse）架构构建——底层统一存储 Parquet/Lance 列式文件在对象存储上，上层挂载多个专用引擎（DuckDB 列式分析、LanceDB 向量检索、自研 Pons 层做实时 context 编码），不同引擎操作同一份文件。这避免了"InfluxDB + MySQL + Milvus 三系统胶水黏合"带来的跨引擎 JOIN 延迟、数据一致性和运维复杂度问题。

---

## 9. 总结

本报告通过对 InfluxDB 和 IoTDB 的系统架构分析和两阶段性能测试，得出以下结论：

1. **TSDB 的核心能力（降采样、连续聚合、平滑压缩）在具身场景下依然有效**——单传感器时序查询是它们的主场。
2. **TSDB 的设计假设与具身数据之间存在着不可调和的 gap**：episode 概念缺失、张量字段无解、多模态无法处理、cardinality 与排序假设不成立。
3. **MySQL 对照实验表明**，通用关系库在时序分析负载上的性能差距为数倍至数百倍，但 TSDB 同样不是终极答案——需要第三种方案。
4. **基于 Lakehouse 架构的中间件/数据工具集**，而非全新数据库系统，是目前最务实的落地路径。这也为后续的研究工作（Cross-Embodiment Memory、Retrieval-Augmented VLA）奠定了数据基础设施。

---

## 参考文献

1. InfluxDB Documentation. https://docs.influxdata.com/influxdb/v2/
2. Apache IoTDB Documentation. https://iotdb.apache.org/UserGuide/latest/
3. Wang, C. et al. "Apache IoTDB: Time-series database for IoT applications." *SIGMOD*, 2020.
4. Khazatsky, A. et al. "DROID: A Large-Scale In-The-Wild Robot Manipulation Dataset." *RSS*, 2024.
5. Armbrust, M. et al. "Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics." *CIDR*, 2021.
6. Time Series Benchmark Suite (TSBS). https://github.com/timescale/tsbs
