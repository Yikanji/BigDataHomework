#!/usr/bin/env python3
"""时序数据库性能对比 Benchmark: InfluxDB v2 vs IoTDB v1.3"""

import time
import random
import subprocess
import os
from influxdb_client import InfluxDBClient, WriteOptions, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from iotdb.Session import Session as IoTDBSession

# ============ Config ============
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "dev-token-for-testing"
INFLUXDB_ORG = "test-org"
INFLUXDB_BUCKET = "test-bucket"

IOTDB_HOST = "localhost"
IOTDB_PORT = 6667

NUM_DEVICES = 100       # 设备数
SENSORS_PER_DEVICE = 10 # 每设备传感器数
POINTS_PER_SENSOR = 1000  # 每传感器写入点数（小时级，1秒间隔）
BATCH_SIZE = 5000       # 每批写入点数

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

# ============ Data Generation ============
def generate_data():
    """生成模拟时序数据: 所有设备 × 传感器 × 时间点"""
    base_time = int(time.time()) * 1000  # 毫秒时间戳
    all_points = []
    for device_id in range(NUM_DEVICES):
        for sensor_id in range(SENSORS_PER_DEVICE):
            for t in range(POINTS_PER_SENSOR):
                ts = base_time - (POINTS_PER_SENSOR - t) * 1000
                value = round(random.gauss(25.0, 5.0), 2)  # 模拟温度正态分布
                all_points.append((device_id, sensor_id, ts, value))
    return all_points

# ============ InfluxDB Tests ============
def influxdb_setup():
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    # Clean up existing data for fresh test
    delete_api = client.delete_api()
    try:
        delete_api.delete(
            start="1970-01-01T00:00:00Z",
            stop="2099-12-31T23:59:59Z",
            predicate='_measurement="sensor_data"',
            bucket=INFLUXDB_BUCKET,
            org=INFLUXDB_ORG,
        )
    except Exception:
        pass
    return client

def influxdb_write_test(points):
    client = influxdb_setup()
    write_api = client.write_api(write_options=WriteOptions(batch_size=BATCH_SIZE))
    total = len(points)
    
    start = time.time()
    for i, (device_id, sensor_id, ts, value) in enumerate(points):
        p = Point("sensor_data") \
            .tag("device_id", str(device_id)) \
            .tag("sensor_id", str(sensor_id)) \
            .field("value", value) \
            .time(ts, write_precision="ms")
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=p)
    write_api.close()
    elapsed = time.time() - start
    throughput = total / elapsed if elapsed > 0 else 0
    client.close()
    return {"db": "InfluxDB", "total_points": total, "elapsed_s": round(elapsed, 2), "throughput_ps": round(throughput)}

def influxdb_query_tests():
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    query_api = client.query_api()
    results = {}
    
    # Query 1: 单个传感器最新值（点查询）
    flux1 = f'''
    from(bucket:"{INFLUXDB_BUCKET}")
      |> range(start: -10m)
      |> filter(fn: (r) => r._measurement == "sensor_data")
      |> filter(fn: (r) => r.device_id == "0" and r.sensor_id == "0")
      |> last()
    '''
    start = time.time()
    res = query_api.query(flux1, org=INFLUXDB_ORG)
    elapsed = time.time() - start
    results["point_query_ms"] = round(elapsed * 1000, 1)
    
    # Query 2: 单个传感器 1 小时范围查询
    flux2 = f'''
    from(bucket:"{INFLUXDB_BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "sensor_data")
      |> filter(fn: (r) => r.device_id == "0" and r.sensor_id == "0")
    '''
    start = time.time()
    res = query_api.query(flux2, org=INFLUXDB_ORG)
    elapsed = time.time() - start
    results["range_query_1h_ms"] = round(elapsed * 1000, 1)
    
    # Query 3: 所有设备某传感器 1 小时均值聚合
    flux3 = f'''
    from(bucket:"{INFLUXDB_BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "sensor_data")
      |> filter(fn: (r) => r.sensor_id == "0")
      |> group(columns: ["device_id"])
      |> mean()
    '''
    start = time.time()
    res = query_api.query(flux3, org=INFLUXDB_ORG)
    elapsed = time.time() - start
    results["aggregate_groupby_ms"] = round(elapsed * 1000, 1)
    
    # Query 4: 单设备所有传感器 1 小时降采样（1 分钟间隔）
    flux4 = f'''
    from(bucket:"{INFLUXDB_BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "sensor_data")
      |> filter(fn: (r) => r.device_id == "0")
      |> aggregateWindow(every: 1m, fn: mean)
    '''
    start = time.time()
    res = query_api.query(flux4, org=INFLUXDB_ORG)
    elapsed = time.time() - start
    results["downsample_1m_ms"] = round(elapsed * 1000, 1)
    
    client.close()
    return results

# ============ IoTDB Tests ============
def iotdb_setup():
    session = IoTDBSession(host=IOTDB_HOST, port=IOTDB_PORT, fetch_size=10000)
    session.open(False)  # False = enable redirection
    try:
        session.execute_non_query_statement("DELETE STORAGE GROUP root.*")
    except Exception:
        pass
    session.execute_non_query_statement("CREATE DATABASE root.test_bench")
    return session

def iotdb_write_test(points):
    session = iotdb_setup()
    # Create timeseries
    device_ids = set(item[0] for item in points)
    sensor_ids = set(item[1] for item in points)
    for d in sorted(device_ids):
        for s in sorted(sensor_ids):
            session.execute_non_query_statement(
                f"CREATE TIMESERIES root.test_bench.d{d}.s{s} WITH DATATYPE=FLOAT, ENCODING=GORILLA"
            )
    
    # Insert data using insert_record batch
    total = len(points)
    start = time.time()
    batch = []
    for device_id, sensor_id, ts, value in points:
        batch.append((device_id, sensor_id, ts, value))
        if len(batch) >= BATCH_SIZE:
            _iotdb_batch_insert(session, batch)
            batch = []
    if batch:
        _iotdb_batch_insert(session, batch)
    
    elapsed = time.time() - start
    throughput = total / elapsed if elapsed > 0 else 0
    session.close()
    return {"db": "IoTDB", "total_points": total, "elapsed_s": round(elapsed, 2), "throughput_ps": round(throughput)}

def _iotdb_batch_insert(session, batch):
    """Batch insert via insert_records"""
    # Group by device_id
    from collections import defaultdict
    groups = defaultdict(list)
    for device_id, sensor_id, ts, value in batch:
        groups[device_id].append((sensor_id, ts, value))
    
    for device_id, records in groups.items():
        timestamps = [r[1] // 1000 for r in records]
        measurements_list = [[str(r[0])] for r in records]
        types_list = [["FLOAT"] for _ in records]
        values_list = [[str(r[2])] for r in records]
        try:
            session.insert_records(
                [f"root.test_bench.d{device_id}"] * len(records),
                timestamps,
                measurements_list,
                types_list,
                values_list
            )
        except Exception as e:
            # Fallback to single insert if batch fails
            for sensor_id, ts, value in records:
                session.execute_non_query_statement(
                    f"INSERT INTO root.test_bench.d{device_id}(timestamp, s{sensor_id}) VALUES ({ts // 1000}, {value})"
                )

def iotdb_query_tests():
    session = IoTDBSession(host=IOTDB_HOST, port=IOTDB_PORT, fetch_size=10000)
    session.open(False)
    results = {}
    
    # Query 1: 单个传感器最新值
    sql1 = "SELECT last_value(*) FROM root.test_bench.d0.s0"
    start = time.time()
    session.execute_query_statement(sql1)
    elapsed = time.time() - start
    results["point_query_ms"] = round(elapsed * 1000, 1)
    
    # Query 2: 单个传感器 1 小时范围查询
    sql2 = "SELECT * FROM root.test_bench.d0.s0 ORDER BY TIME DESC LIMIT 3600"
    start = time.time()
    session.execute_query_statement(sql2)
    elapsed = time.time() - start
    results["range_query_1h_ms"] = round(elapsed * 1000, 1)
    
    # Query 3: 所有设备所有传感器 COUNT 聚合
    sql3 = "SELECT COUNT(*) FROM root.test_bench.*.*"
    start = time.time()
    session.execute_query_statement(sql3)
    elapsed = time.time() - start
    results["aggregate_groupby_ms"] = round(elapsed * 1000, 1)
    
    # Query 4: 单设备所有传感器 1 小时范围 AVG 聚合
    sql4 = "SELECT AVG(s0), AVG(s1), AVG(s2), AVG(s3), AVG(s4) FROM root.test_bench.d0.*"
    start = time.time()
    session.execute_query_statement(sql4)
    elapsed = time.time() - start
    results["downsample_1m_ms"] = round(elapsed * 1000, 1)
    
    session.close()
    return results

# ============ Disk Usage ============
def get_disk_usage(path):
    try:
        result = subprocess.run(
            ["du", "-sh", path],
            capture_output=True, text=True
        )
        return result.stdout.split()[0] if result.returncode == 0 else "N/A"
    except Exception:
        return "N/A"

# ============ Main ============
def main():
    print("=" * 60)
    print("  时序数据库性能对比 Benchmark")
    print(f"  配置: {NUM_DEVICES} 设备 × {SENSORS_PER_DEVICE} 传感器 × {POINTS_PER_SENSOR} 点")
    print(f"  总数据点数: {NUM_DEVICES * SENSORS_PER_DEVICE * POINTS_PER_SENSOR:,}")
    print("=" * 60)
    
    # Generate data
    print("\n[1/5] 生成测试数据...")
    points = generate_data()
    random.shuffle(points)  # 乱序写入模拟真实场景
    print(f"  生成 {len(points):,} 个数据点")
    
    # InfluxDB Write
    print("\n[2/5] InfluxDB 写入测试...")
    influx_result = influxdb_write_test(points)
    print(f"  InfluxDB: {influx_result['throughput_ps']:,} points/s, 耗时 {influx_result['elapsed_s']}s")
    
    # IoTDB Write
    print("\n[3/5] IoTDB 写入测试...")
    iotdb_result = iotdb_write_test(points)
    print(f"  IoTDB: {iotdb_result['throughput_ps']:,} points/s, 耗时 {iotdb_result['elapsed_s']}s")
    
    # Query tests
    print("\n[4/5] 查询性能测试...")
    influx_queries = influxdb_query_tests()
    iotdb_queries = iotdb_query_tests()
    
    # Disk usage
    print("\n[5/5] 磁盘空间占用...")
    influxdb_disk = get_disk_usage(os.path.join(DATA_DIR, "influxdb"))
    iotdb_disk = get_disk_usage(os.path.join(DATA_DIR, "iotdb"))
    print(f"  InfluxDB: {influxdb_disk}")
    print(f"  IoTDB:    {iotdb_disk}")
    
    # Summary
    print("\n" + "=" * 60)
    print("  测试结果汇总")
    print("=" * 60)
    
    print(f"\n{'指标':<25} {'InfluxDB':>15} {'IoTDB':>15}")
    print("-" * 55)
    print(f"{'写入吞吐 (points/s)':<25} {influx_result['throughput_ps']:>15,} {iotdb_result['throughput_ps']:>15,}")
    print(f"{'写入总耗时 (s)':<25} {influx_result['elapsed_s']:>15} {iotdb_result['elapsed_s']:>15}")
    
    query_tests = [
        ("点查询 (ms)", "point_query_ms"),
        ("范围查询 1h (ms)", "range_query_1h_ms"),
        ("全量聚合 (ms)", "aggregate_groupby_ms"),
        ("多传感器AVG (ms)", "downsample_1m_ms"),
    ]
    for name, key in query_tests:
        print(f"{name:<25} {influx_queries[key]:>15} {iotdb_queries[key]:>15}")
    
    print(f"{'磁盘占用':<25} {influxdb_disk:>15} {iotdb_disk:>15}")
    
    print("\n✅ Benchmark 完成！")

if __name__ == "__main__":
    main()
