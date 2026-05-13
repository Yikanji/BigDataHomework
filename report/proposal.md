# 大作业 Proposal：时序数据库系统调研

---

## 选题

**题目三：时序数据库系统调研**

对 InfluxDB 和 Apache IoTDB 进行调研，比较两个系统的设计思想和系统架构，在容器平台部署并进行性能测试与对比分析。

---

## 成员

| 角色 | 姓名 | 学号 |
|------|------|------|
| 组长 | [姓名] | [学号] |
| 组员 | [姓名] | [学号] |
| 组员 | [姓名] | [学号] |

---

## 组名

[组名]

---

## 分工

| 成员 | 负责内容 |
|------|---------|
| 组长 | 实验设计与统筹、性能测试执行、报告撰写 |
| 组员 1 | 文献调研（InfluxDB/IoTDB 架构分析与对比）、系统部署 |
| 组员 2 | Benchmark 工具搭建、数据可视化、答辩 PPT |

---

## 计划方案

### 系统部署

在容器平台上分别部署 InfluxDB v2 和 Apache IoTDB v1.3，两系统共享同一容器集群环境（4 容器 × 16C / 32GB / 100GB）。

### 文献调研

通过阅读论文和设计文档，深入理解两种系统的特性：
- **InfluxDB**：TSM 存储引擎、倒排索引、Flux 查询语言
- **IoTDB**：TsFile 列式存储格式、树状路径数据模型、分离式架构（ConfigNode + DataNode）

### 性能测试

采用时序数据库常见 Benchmark 工具（IoTDB-Benchmark 或 TSBS），对以下维度进行对比：

1. **数据加载性能**：大规模时序数据的写入吞吐量（points/sec）及耗时
2. **查询处理性能**：点查询、范围查询、聚合查询等典型时序查询的延迟
3. **空间占用**：同等数据量下的磁盘存储开销对比

尝试调整各系统的配置参数（缓存大小、压缩算法、分片策略等）以优化各自性能。

### 预期产出

- 调研报告（≥6 页 A4 PDF）：系统架构分析、实验设计与结果、对比总结
- 程序代码（含 README 安装运行说明）
- 答辩 PPT（20-30 分钟）

---

## 组会计划

| 周次 | 内容 |
|------|------|
| 第 1 周（5/13-5/20） | 文献调研 + 系统部署 + Proposal 提交 |
| 第 2 周（5/20-5/27） | Benchmark 工具搭建 + 初步性能测试 |
| 第 3 周（5/27-6/3） | 完整性能测试 + 配置调优 |
| 第 4 周（6/3-6/10） | 数据分析 + 报告撰写 |
| 第 5 周（6/10-6/17） | 报告定稿 + PPT 制作 + 答辩准备 |

---

## 参考文献

1. InfluxDB Documentation. https://docs.influxdata.com/influxdb/v2/
2. Apache IoTDB User Guide. https://iotdb.apache.org/UserGuide/
3. Wang, C. et al. "Apache IoTDB: Time-series database for IoT applications." *SIGMOD* 2020.
4. Time Series Benchmark Suite (TSBS). https://github.com/timescale/tsbs
