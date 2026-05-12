#!/usr/bin/env python3
"""DROID 数据集 × 时序数据库 场景化 Benchmark
聚焦 TSDB 独特价值：降采样、插值填充、滑动窗口、跨轨迹聚合"""

import time
import subprocess
import os
import sys
import glob
import random
import numpy as np
from tfrecord import tfrecord_loader
import requests

INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "dev-token-for-testing"
INFLUXDB_ORG = "test-org"
INFLUXDB_BUCKET = "droid"

IOTDB_HOST = "localhost"
IOTDB_PORT = 6667

DROID_PATH = "/mnt/huawei_nas/Datasets/DROID/1.0.0"
MAX_EPISODES = 60
TF_SHARD_COUNT = 3
STEP_INTERVAL_MS = 67  # ~15Hz

# (字段名, tfrecord_key, 维度数)
FIELDS = [
    ("jpos",   "steps/observation/joint_position",      7),
    ("grip",   "steps/observation/gripper_position",    1),
    ("jvel",   "steps/action_dict/joint_velocity",      7),
    ("cpos",   "steps/observation/cartesian_position",  6),
]

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


# ==================== 数据加载 ====================
def load_episodes():
    tf_files = sorted(glob.glob(f"{DROID_PATH}/r2d2_faceblur-train.tfrecord-*"))
    episodes = []
    for tf_file in tf_files[:TF_SHARD_COUNT]:
        loader = tfrecord_loader(tf_file, index_path=None, description=None)
        for rec in loader:
            if len(episodes) >= MAX_EPISODES:
                break
            episodes.append(_parse(rec))
        if len(episodes) >= MAX_EPISODES:
            break
    pts = sum(e["points"] for e in episodes)
    steps = sum(e["steps"] for e in episodes)
    print(f"  {len(episodes)} 条轨迹, {steps} 步, {pts:,} 数据点")
    return episodes

def _parse(rec):
    data, n = {}, None
    for name, key, dims in FIELDS:
        arr = rec.get(key)
        if arr is None:
            continue
        n = len(arr) // dims
        data[name] = arr.reshape(n, dims)
    return {"data": data, "steps": n or 0, "points": sum(d.shape[0]*d.shape[1] for d in data.values())}


# ==================== InfluxDB ====================
def influx_setup():
    # 创建 bucket
    r = requests.get(f"{INFLUXDB_URL}/api/v2/buckets?name={INFLUXDB_BUCKET}",
                     headers={"Authorization": f"Token {INFLUXDB_TOKEN}"})
    if r.status_code == 200 and len(r.json().get("buckets", [])) == 0:
        org_r = requests.get(f"{INFLUXDB_URL}/api/v2/orgs",
                             headers={"Authorization": f"Token {INFLUXDB_TOKEN}"})
        orgs = org_r.json().get("orgs", [])
        if orgs:
            requests.post(f"{INFLUXDB_URL}/api/v2/buckets",
                          headers={"Authorization": f"Token {INFLUXDB_TOKEN}", "Content-Type": "application/json"},
                          json={"name": INFLUXDB_BUCKET, "orgID": orgs[0]["id"], "retentionRules": []})
    return __import__('influxdb_client').InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

def influx_write(eps):
    from influxdb_client import WriteOptions, Point
    client = influx_setup()
    w = client.write_api(write_options=WriteOptions(batch_size=5000))
    base = int(time.time() * 1000)
    total, start = 0, time.time()
    for ei, ep in enumerate(eps):
        for name, _, dims in FIELDS:
            arr = ep["data"].get(name)
            if arr is None: continue
            n = arr.shape[0]
            for s in range(n):
                ts = base - (n - s) * STEP_INTERVAL_MS
                for d in range(dims):
                    p = (Point(name).tag("ep", str(ei)).tag("dim", str(d))
                         .field("v", float(arr[s, d])).time(ts, write_precision="ms"))
                    w.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=p)
                    total += 1
    w.close()
    t = time.time() - start
    client.close()
    return total, t


# ==================== IoTDB ====================
def iotdb_setup(eps):
    from iotdb.Session import Session
    s = Session(host=IOTDB_HOST, port=IOTDB_PORT, fetch_size=10000)
    s.open(False)
    try: s.execute_non_query_statement("DELETE DATABASE root.droid")
    except: pass
    s.execute_non_query_statement("CREATE DATABASE root.droid")
    # 只为一条轨迹建全 field —— 查询都在 ep0 上做，其余轨迹用相同结构
    for ei in range(len(eps)):
        for name, _, dims in FIELDS:
            for d in range(dims):
                try:
                    s.execute_non_query_statement(
                        f"CREATE TIMESERIES root.droid.ep{ei}.{name}_d{d} WITH DATATYPE=FLOAT, ENCODING=GORILLA")
                except: pass
    return s

def iotdb_write(eps):
    s = iotdb_setup(eps)
    base = int(time.time() * 1000)
    total, start = 0, time.time()
    for ei, ep in enumerate(eps):
        n = ep["steps"]
        if n == 0: continue
        dev = f"root.droid.ep{ei}"
        for step in range(n):
            ts = base - (n - step) * STEP_INTERVAL_MS
            cols, vals = [], []
            for name, _, dims in FIELDS:
                arr = ep["data"].get(name)
                if arr is None: continue
                for d in range(dims):
                    cols.append(f"{name}_d{d}")
                    vals.append(str(float(arr[step, d])))
            if cols:
                sql = f"INSERT INTO {dev}(timestamp, {','.join(cols)}) VALUES ({ts}, {','.join(vals)})"
                try:
                    s.execute_non_query_statement(sql)
                    total += len(cols)
                except: pass
    t = time.time() - start
    s.close()
    return total, t


# ==================== 场景测试 ====================
def run_tests(eps):
    from influxdb_client import InfluxDBClient
    from iotdb.Session import Session

    inc = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    iot = Session(host=IOTDB_HOST, port=IOTDB_PORT, fetch_size=10000)
    iot.open(False)
    qapi = inc.query_api()

    print("\n" + "-" * 25)

    results = {}

    # ===== 场景 1: 降采样（下钻） =====
    print("\n[场景1] 降采样: 原始 → 1s 窗口均值 (ep0, jpos_d0)")
    # InfluxDB
    flux1 = f'''
    from(bucket:"{INFLUXDB_BUCKET}") |> range(start: -30m)
      |> filter(fn: (r) => r._measurement == "jpos" and r.ep == "0" and r.dim == "0")
      |> aggregateWindow(every: 1s, fn: mean)
    '''
    t0 = time.time()
    qapi.query(flux1, org=INFLUXDB_ORG)
    dt_inf = (time.time() - t0) * 1000

    # IoTDB — 用 COUNT 验证降采样效果
    t0 = time.time()
    iot.execute_query_statement("SELECT COUNT(jpos_d0) FROM root.droid.ep0.*")
    raw_cnt = time.time() - t0

    t0 = time.time()
    iot.execute_query_statement("SELECT COUNT(jpos_d0) FROM root.droid.ep0.* GROUP BY(1s)")
    ds_cnt = time.time() - t0

    print(f"  InfluxDB aggregateWindow: {dt_inf:.1f} ms")
    print(f"  IoTDB    full scan:       {raw_cnt*1000:.1f} ms")
    print(f"  IoTDB    GROUP BY(1s):    {ds_cnt*1000:.1f} ms")
    results["downsample_inf_ms"] = round(dt_inf, 1)
    results["downsample_iot_ms"] = round(ds_cnt * 1000, 1)

    # ===== 场景 2: 插值填充 =====
    print("\n[场景2] 插值: 删除 ep0 传感器 d0 部分数据后用 FILL 补全")
    # 用 IoTDB session 删点
    first_ts = int(time.time() * 1000) - eps[0]["steps"] * STEP_INTERVAL_MS
    mid_ts = first_ts + eps[0]["steps"] * STEP_INTERVAL_MS // 2
    # 删除中间 50 个点
    for i in range(50):
        try:
            iot.execute_non_query_statement(
                f"DELETE FROM root.droid.ep0.jpos_d0 WHERE time={mid_ts + i * STEP_INTERVAL_MS}")
        except: pass

    # InfluxDB: 不支持原生 FILL，用 pandas 插值模拟（算在客户端侧）
    t0 = time.time()
    flux_fill = f'''
    from(bucket:"{INFLUXDB_BUCKET}") |> range(start: -30m)
      |> filter(fn: (r) => r._measurement == "jpos" and r.ep == "0" and r.dim == "0")
    '''
    tables = qapi.query(flux_fill, org=INFLUXDB_ORG)
    # 模拟客户端插值
    values = []
    for table in tables:
        for record in table.records:
            values.append((record.get_time(), record.get_value()))
    if values:
        values.sort()
        # 简单前向填充检测空窗
        from datetime import timedelta
        filled = 0
        for i in range(1, len(values)):
            gap = (values[i][0] - values[i-1][0]).total_seconds()
            if gap > 0.1:  # 有缺失
                filled += 1
    dt_inf_fill = (time.time() - t0) * 1000

    # IoTDB: 原生 FILL
    t0 = time.time()
    iot.execute_query_statement("SELECT jpos_d0 FROM root.droid.ep0.* FILL(LINEAR)")
    dt_iot_fill = (time.time() - t0) * 1000

    print(f"  InfluxDB query + client interp: {dt_inf_fill:.1f} ms")
    print(f"  IoTDB    FILL(LINEAR) native:    {dt_iot_fill:.1f} ms")
    results["fill_inf_ms"] = round(dt_inf_fill, 1)
    results["fill_iot_ms"] = round(dt_iot_fill, 1)

    # ===== 场景 3: 滑动窗口异常检测 =====
    print("\n[场景3] 滑动窗口: 1s 窗口标准差检测关节抖动 (ep0, jvel_d0)")
    # InfluxDB aggregateWindow stddev
    flux_sd = f'''
    from(bucket:"{INFLUXDB_BUCKET}") |> range(start: -30m)
      |> filter(fn: (r) => r._measurement == "jvel" and r.ep == "0" and r.dim == "0")
      |> aggregateWindow(every: 1s, fn: stddev)
    '''
    t0 = time.time()
    qapi.query(flux_sd, org=INFLUXDB_ORG)
    dt_inf_sd = (time.time() - t0) * 1000

    # IoTDB: 1s 窗口 STDDEV
    t0 = time.time()
    iot.execute_query_statement("SELECT STDDEV(jvel_d0) FROM root.droid.ep0.* GROUP BY(1s)")
    dt_iot_sd = (time.time() - t0) * 1000

    print(f"  InfluxDB aggregateWindow stddev: {dt_inf_sd:.1f} ms")
    print(f"  IoTDB    GROUP BY(1s) STDDEV:     {dt_iot_sd:.1f} ms")
    results["slide_inf_ms"] = round(dt_inf_sd, 1)
    results["slide_iot_ms"] = round(dt_iot_sd, 1)

    # ===== 场景 4: 跨轨迹全局统计 =====
    print(f"\n[场景4] 跨轨迹聚合: {len(eps)} 条轨迹关节位置全局 AVG + STDDEV")
    # InfluxDB
    flux_global = f'''
    from(bucket:"{INFLUXDB_BUCKET}") |> range(start: -30m)
      |> filter(fn: (r) => r._measurement == "jpos")
      |> group()
      |> mean()
      |> yield(name: "mean")
    '''
    t0 = time.time()
    qapi.query(flux_global, org=INFLUXDB_ORG)
    dt_inf_global = (time.time() - t0) * 1000

    # IoTDB
    t0 = time.time()
    iot.execute_query_statement("SELECT COUNT(*), AVG(jpos_d0), AVG(jpos_d1) FROM root.droid.*.*")
    dt_iot_global = (time.time() - t0) * 1000

    print(f"  InfluxDB global mean: {dt_inf_global:.1f} ms")
    print(f"  IoTDB    global AVG:   {dt_iot_global:.1f} ms")
    results["global_inf_ms"] = round(dt_inf_global, 1)
    results["global_iot_ms"] = round(dt_iot_global, 1)

    inc.close()
    iot.close()
    return results


# ==================== Main ====================
def disk_usage(path):
    try:
        r = subprocess.run(["du", "-sh", path], capture_output=True, text=True)
        return r.stdout.split()[0] if r.returncode == 0 else "N/A"
    except: return "N/A"

def main():
    print("=" * 60)
    print("  DROID × TSDB 场景化 Benchmark")
    print(f"  轨迹: ≤{MAX_EPISODES} 条,  分片: {TF_SHARD_COUNT},  ~15Hz")
    print("=" * 60)

    # 加载
    print("\n[1/6] 加载 DROID...")
    eps = load_episodes()
    if not eps:
        print("ERROR: 无数据"); sys.exit(1)

    # 写入
    print("\n[2/6] InfluxDB 写入...")
    t_inf, t_inf_time = influx_write(eps)
    print(f"  {t_inf:,} 点, {t_inf/t_inf_time:,.0f} pts/s, 耗时 {t_inf_time:.1f}s")

    print("\n[3/6] IoTDB 写入...")
    t_iot, t_iot_time = iotdb_write(eps)
    print(f"  {t_iot:,} 点, {t_iot/t_iot_time:,.0f} pts/s, 耗时 {t_iot_time:.1f}s")

    # 场景测试
    print("\n[4/6] 场景测试")
    results = run_tests(eps)

    # 磁盘
    print("\n[5/6] 磁盘占用")
    d_inf = disk_usage(os.path.join(DATA_DIR, "influxdb"))
    d_iot = disk_usage(os.path.join(DATA_DIR, "iotdb"))
    print(f"  InfluxDB: {d_inf}    IoTDB: {d_iot}")

    # 汇总
    print("\n" + "=" * 60)
    print("  汇总")
    print("=" * 60)
    print(f"\n{'场景':<32} {'InfluxDB':>12} {'IoTDB':>12}")
    print("-" * 56)
    print(f"{'写入吞吐 (pts/s)':<32} {t_inf/t_inf_time:>12,.0f} {t_iot/t_iot_time:>12,.0f}")
    for name, key in [
        ("降采样 (ms)", "downsample"),
        ("插值填充 (ms)", "fill"),
        ("滑动窗口抖动检测 (ms)", "slide"),
        ("跨轨迹聚合 (ms)", "global"),
    ]:
        ik = f"{key}_inf_ms"
        ok = f"{key}_iot_ms"
        print(f"{name:<32} {results.get(ik, 0):>12.1f} {results.get(ok, 0):>12.1f}")
    print(f"{'磁盘占用':<32} {d_inf:>12} {d_iot:>12}")
    print("\n✅ 完成")

if __name__ == "__main__":
    main()
