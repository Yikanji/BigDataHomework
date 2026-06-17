#!/usr/bin/env python3
"""DROID 数据集 → InfluxDB / IoTDB 时序数据库性能对比"""

import time
import subprocess
import os
import sys
import glob
import numpy as np
from tfrecord import tfrecord_loader
from iotdb.Session import Session as IoTDBSession
from influxdb3_client import InfluxDB3Client, line_protocol

# ============ Config ============
INFLUXDB_DB = "droid"

IOTDB_HOST = "localhost"
IOTDB_PORT = 6667

DROID_PATH = os.environ.get("DROID_PATH", "/mnt/huawei_nas/Datasets/DROID/1.0.0")
MAX_EPISODES = 20   # 轨迹数
TF_SHARD_COUNT = 2   # 读取前 2 个 TFRecord 分片
STEP_INTERVAL_MS = 50

# 时序字段名 → (tfrecord_key, dims)
TIME_SERIES_FIELDS = [
    ("obs_joint_position",      "steps/observation/joint_position",        7),
    ("obs_cartesian_position",  "steps/observation/cartesian_position",    6),
    ("obs_gripper_position",    "steps/observation/gripper_position",      1),
    ("act_joint_position",      "steps/action_dict/joint_position",        7),
    ("act_joint_velocity",      "steps/action_dict/joint_velocity",        7),
    ("act_cartesian_position",  "steps/action_dict/cartesian_position",    6),
]

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


# ============ DROID 数据加载 ============
def load_droid_episodes():
    tf_files = sorted(glob.glob(f"{DROID_PATH}/r2d2_faceblur-train.tfrecord-*"))
    episodes = []
    for tf_file in tf_files[:TF_SHARD_COUNT]:
        loader = tfrecord_loader(tf_file, index_path=None, description=None)
        for record in loader:
            if len(episodes) >= MAX_EPISODES:
                break
            episodes.append(_parse_episode(record))
        if len(episodes) >= MAX_EPISODES:
            break

    total_points = sum(ep['point_count'] for ep in episodes)
    total_steps = sum(ep['num_steps'] for ep in episodes)
    print(f"  加载 {len(episodes)} 条轨迹, {total_steps} 步, {total_points:,} 个数据点")
    return episodes

def _parse_episode(record: dict) -> dict:
    data = {}
    num_steps = None
    for field_name, tf_key, dims in TIME_SERIES_FIELDS:
        arr = record.get(tf_key)
        if arr is None:
            continue
        num_steps = len(arr) // dims
        data[field_name] = arr.reshape(num_steps, dims)

    lang_arr = record.get("steps/language_instruction")
    lang = bytes(lang_arr[:80]).decode("utf-8", errors="ignore").rstrip("\x00") if lang_arr is not None and len(lang_arr) > 0 else ""

    point_count = sum(d.shape[0] * d.shape[1] for d in data.values())
    return {"data": data, "num_steps": num_steps or 0, "point_count": point_count, "language": lang}


# ============ InfluxDB ============
def influxdb_setup():
    client = InfluxDB3Client(INFLUXDB_DB)
    client.recreate_database()
    return client

def influxdb_write_test(episodes: list) -> dict:
    client = influxdb_setup()

    base_time = int(time.time() * 1000)

    def rows():
        for ep_idx, ep in enumerate(episodes):
            for field_name, _, dims in TIME_SERIES_FIELDS:
                arr = ep["data"].get(field_name)
                if arr is None:
                    continue
                num_steps = arr.shape[0]
                for step in range(num_steps):
                    ts = base_time - (num_steps - step) * STEP_INTERVAL_MS
                    for dim in range(dims):
                        yield line_protocol(
                            field_name,
                            {"episode_id": str(ep_idx), "dim": str(dim)},
                            {"value": float(arr[step, dim])},
                            ts,
                        )

    total_points = sum(ep["point_count"] for ep in episodes)
    start = time.time()
    client.write_lines(rows())
    elapsed = time.time() - start
    throughput = total_points / elapsed if elapsed > 0 else 0
    return {"db": "InfluxDB 3", "total_points": total_points, "elapsed_s": round(elapsed, 2), "throughput_ps": round(throughput)}

def influxdb_query_tests(episodes: list) -> dict:
    client = InfluxDB3Client(INFLUXDB_DB)
    results = {}

    # Q1: 点查询 - 单关节最新位置
    q1 = """
    SELECT time, value
    FROM obs_joint_position
    WHERE episode_id = '0' AND dim = '0'
    ORDER BY time DESC
    LIMIT 1
    """
    t0 = time.time(); client.query_sql(q1); results["point_query_ms"] = round((time.time()-t0)*1000, 1)

    # Q2: 范围查询 - 单关节全轨迹
    q2 = """
    SELECT time, value
    FROM obs_joint_position
    WHERE episode_id = '0'
      AND dim = '0'
      AND time >= now() - INTERVAL '30 minutes'
    ORDER BY time
    """
    t0 = time.time(); client.query_sql(q2); results["range_query_ms"] = round((time.time()-t0)*1000, 1)

    # Q3: 单轨迹所有关节均值
    q3 = """
    SELECT dim, AVG(value) AS mean_value
    FROM obs_joint_position
    WHERE episode_id = '0'
      AND time >= now() - INTERVAL '30 minutes'
    GROUP BY dim
    ORDER BY dim
    """
    t0 = time.time(); client.query_sql(q3); results["aggregate_ms"] = round((time.time()-t0)*1000, 1)

    # Q4: 跨轨迹关节速度全局统计
    q4 = """
    SELECT AVG(value) AS mean_value, COUNT(value) AS cnt
    FROM act_joint_velocity
    WHERE time >= now() - INTERVAL '30 minutes'
    """
    t0 = time.time(); client.query_sql(q4); results["cross_episode_ms"] = round((time.time()-t0)*1000, 1)

    return results


# ============ IoTDB ============
def iotdb_setup(episodes: list):
    session = IoTDBSession(host=IOTDB_HOST, port=IOTDB_PORT, fetch_size=10000)
    session.open(False)
    try:
        session.execute_non_query_statement("DELETE DATABASE root.droid")
    except Exception:
        pass
    session.execute_non_query_statement("CREATE DATABASE root.droid")

    for ep_idx in range(len(episodes)):
        for field_name, _, dims in TIME_SERIES_FIELDS:
            for dim in range(dims):
                ts_name = f"root.droid.ep{ep_idx}.{field_name}_d{dim}"
                try:
                    session.execute_non_query_statement(
                        f"CREATE TIMESERIES {ts_name} WITH DATATYPE=FLOAT, ENCODING=GORILLA"
                    )
                except Exception:
                    pass
    return session

def iotdb_write_test(episodes: list) -> dict:
    session = iotdb_setup(episodes)
    total_points = 0
    base_time = int(time.time() * 1000)
    start = time.time()

    for ep_idx, ep in enumerate(episodes):
        device = f"root.droid.ep{ep_idx}"
        num_steps = ep["num_steps"]
        if num_steps == 0:
            continue

        # 按 step 组装所有传感器值，批量写入
        for step in range(num_steps):
            ts = base_time - (num_steps - step) * STEP_INTERVAL_MS
            cols = []
            vals = []
            for field_name, _, dims in TIME_SERIES_FIELDS:
                arr = ep["data"].get(field_name)
                if arr is None:
                    continue
                for dim in range(dims):
                    cols.append(f"{field_name}_d{dim}")
                    vals.append(str(float(arr[step, dim])))

            if cols:
                cols_str = ", ".join(cols)
                vals_str = ", ".join(vals)
                sql = f"INSERT INTO {device}(timestamp, {cols_str}) VALUES ({ts}, {vals_str})"
                try:
                    session.execute_non_query_statement(sql)
                    total_points += len(cols)
                except Exception:
                    pass

    elapsed = time.time() - start
    throughput = total_points / elapsed if elapsed > 0 else 0
    session.close()
    return {"db": "IoTDB", "total_points": total_points, "elapsed_s": round(elapsed, 2), "throughput_ps": round(throughput)}

def iotdb_query_tests() -> dict:
    session = IoTDBSession(host=IOTDB_HOST, port=IOTDB_PORT, fetch_size=10000)
    session.open(False)
    results = {}

    # Q1: 点查询
    sql1 = "SELECT last_value(*) FROM root.droid.ep0.obs_joint_position_d0"
    t0 = time.time(); session.execute_query_statement(sql1); results["point_query_ms"] = round((time.time()-t0)*1000, 1)

    # Q2: 范围查询
    sql2 = "SELECT * FROM root.droid.ep0.obs_joint_position_d0"
    t0 = time.time(); session.execute_query_statement(sql2); results["range_query_ms"] = round((time.time()-t0)*1000, 1)

    # Q3: 单轨迹所有关节均值
    sql3 = "SELECT AVG(obs_joint_position_d0),AVG(obs_joint_position_d1),AVG(obs_joint_position_d2),AVG(obs_joint_position_d3),AVG(obs_joint_position_d4),AVG(obs_joint_position_d5),AVG(obs_joint_position_d6) FROM root.droid.ep0.*"
    t0 = time.time(); session.execute_query_statement(sql3); results["aggregate_ms"] = round((time.time()-t0)*1000, 1)

    # Q4: 跨轨迹关节速度全局统计
    sql4 = "SELECT COUNT(*) FROM root.droid.*.act_joint_velocity_d0"
    t0 = time.time(); session.execute_query_statement(sql4); results["cross_episode_ms"] = round((time.time()-t0)*1000, 1)

    session.close()
    return results


# ============ Disk Usage ============
def get_disk_usage(path: str) -> str:
    try:
        result = subprocess.run(["du", "-sh", path], capture_output=True, text=True)
        return result.stdout.split()[0] if result.returncode == 0 else "N/A"
    except Exception:
        return "N/A"


# ============ Main ============
def main():
    print("=" * 65)
    print("  DROID 数据集 × 时序数据库性能对比")
    print(f"  最多 {MAX_EPISODES} 条轨迹, 前 {TF_SHARD_COUNT} 个分片")
    print("=" * 65)

    print("\n[1/5] 加载 DROID 数据...")
    episodes = load_droid_episodes()
    if not episodes:
        print("ERROR: 未加载到数据")
        sys.exit(1)

    print("\n[2/5] InfluxDB 3 写入测试...")
    influx_result = influxdb_write_test(episodes)
    print(f"  InfluxDB 3: {influx_result['throughput_ps']:,} points/s, "
          f"共 {influx_result['total_points']:,} 点, 耗时 {influx_result['elapsed_s']}s")

    print("\n[3/5] IoTDB 写入测试...")
    iotdb_result = iotdb_write_test(episodes)
    print(f"  IoTDB:    {iotdb_result['throughput_ps']:,} points/s, "
          f"共 {iotdb_result['total_points']:,} 点, 耗时 {iotdb_result['elapsed_s']}s")

    print("\n[4/5] 查询性能测试...")
    influx_queries = influxdb_query_tests(episodes)
    iotdb_queries = iotdb_query_tests()

    print("\n[5/5] 磁盘空间占用...")
    influxdb_disk = get_disk_usage(os.path.join(DATA_DIR, "influxdb3"))
    iotdb_disk = get_disk_usage(os.path.join(DATA_DIR, "iotdb2"))
    print(f"  InfluxDB 3: {influxdb_disk}")
    print(f"  IoTDB:    {iotdb_disk}")

    # 轨迹样本
    print("\n--- 轨迹样本 ---")
    for i, ep in enumerate(episodes[:5]):
        if ep["language"]:
            print(f"  Ep.{i}: {ep['language'][:80]}")

    # 汇总
    print("\n" + "=" * 65)
    print("  测试结果汇总")
    print("=" * 65)
    print(f"\n{'指标':<30} {'InfluxDB 3':>15} {'IoTDB':>15}")
    print("-" * 60)
    print(f"{'写入吞吐 (points/s)':<30} {influx_result['throughput_ps']:>15,} {iotdb_result['throughput_ps']:>15,}")
    print(f"{'写入总耗时 (s)':<30} {influx_result['elapsed_s']:>15.1f} {iotdb_result['elapsed_s']:>15.1f}")
    print(f"{'总数据点数':<30} {influx_result['total_points']:>15,} {iotdb_result['total_points']:>15,}")

    for name, key in [("点查询 (ms)", "point_query_ms"), ("范围查询全轨迹 (ms)", "range_query_ms"),
                       ("分组聚合 (ms)", "aggregate_ms"), ("跨轨迹统计 (ms)", "cross_episode_ms")]:
        print(f"{name:<30} {influx_queries[key]:>15.1f} {iotdb_queries[key]:>15.1f}")
    print(f"{'磁盘占用':<30} {influxdb_disk:>15} {iotdb_disk:>15}")
    print("\n✅ DROID Benchmark 完成！")

if __name__ == "__main__":
    main()
