#!/usr/bin/env python3
"""通用合成数据 Benchmark: InfluxDB 3 vs IoTDB 2.0 vs PostgreSQL vs DolphinDB."""

import os
import random
import subprocess
import time
from collections import defaultdict

try:
    import psycopg
except ImportError:
    psycopg = None

try:
    from iotdb.Session import Session as IoTDBSession
except ImportError:
    IoTDBSession = None

from dolphindb_client import DolphinDBClient, SYNTHETIC_DB, SYNTHETIC_TABLE
from influxdb3_client import InfluxDB3Client, line_protocol


INFLUXDB_DB = "test_bench"

IOTDB_HOST = os.environ.get("IOTDB_HOST", "localhost")
IOTDB_PORT = int(os.environ.get("IOTDB_PORT", "6667"))

POSTGRES_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "root123"),
    "dbname": os.environ.get("POSTGRES_DB", "droid"),
}

NUM_DEVICES = int(os.environ.get("BENCH_NUM_DEVICES", "100"))
SENSORS_PER_DEVICE = int(os.environ.get("BENCH_SENSORS_PER_DEVICE", "10"))
POINTS_PER_SENSOR = int(os.environ.get("BENCH_POINTS_PER_SENSOR", "1000"))
BATCH_SIZE = int(os.environ.get("BENCH_BATCH_SIZE", "5000"))

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

ALL_TARGETS = ("influx", "iotdb", "postgres", "dolphindb")
TARGET_LABELS = {
    "influx": "InfluxDB 3",
    "iotdb": "IoTDB",
    "postgres": "PostgreSQL",
    "dolphindb": "DolphinDB",
}
TARGET_ALIASES = {
    "influx": "influx",
    "influxdb": "influx",
    "influxdb3": "influx",
    "iotdb": "iotdb",
    "pg": "postgres",
    "postgres": "postgres",
    "postgresql": "postgres",
    "ddb": "dolphindb",
    "dolphindb": "dolphindb",
}


def selected_targets():
    raw = os.environ.get("BENCH_TARGETS", "all")
    targets = []
    for token in raw.replace(";", ",").split(","):
        name = token.strip().lower().replace("-", "")
        if not name:
            continue
        if name == "all":
            return list(ALL_TARGETS)
        target = TARGET_ALIASES.get(name)
        if target is None:
            valid = ", ".join(["all", *TARGET_ALIASES])
            raise ValueError(f"未知 BENCH_TARGETS={token!r}，可用值: {valid}")
        if target not in targets:
            targets.append(target)
    return targets or list(ALL_TARGETS)


def generate_data():
    base_time = int(time.time()) * 1000
    all_points = []
    for device_id in range(NUM_DEVICES):
        for sensor_id in range(SENSORS_PER_DEVICE):
            for t in range(POINTS_PER_SENSOR):
                ts = base_time - (POINTS_PER_SENSOR - t) * 1000
                value = round(random.gauss(25.0, 5.0), 2)
                all_points.append((device_id, sensor_id, ts, value))
    return all_points


# ============ InfluxDB ============
def influxdb_setup():
    client = InfluxDB3Client(INFLUXDB_DB)
    client.recreate_database()
    return client


def influxdb_write_test(points):
    client = influxdb_setup()

    def rows():
        for device_id, sensor_id, ts, value in points:
            yield line_protocol(
                "sensor_data",
                {"device_id": str(device_id), "sensor_id": str(sensor_id)},
                {"value": value},
                ts,
            )

    start = time.time()
    client.write_lines(rows(), batch_size=BATCH_SIZE)
    elapsed = time.time() - start
    return _write_result(len(points), elapsed)


def influxdb_query_tests():
    client = InfluxDB3Client(INFLUXDB_DB)
    return {
        "point_query_ms": _timed_ms(
            lambda: client.query_sql(
                """
                SELECT time, value
                FROM sensor_data
                WHERE device_id = '0' AND sensor_id = '0'
                ORDER BY time DESC
                LIMIT 1
                """
            )
        ),
        "range_query_1h_ms": _timed_ms(
            lambda: client.query_sql(
                """
                SELECT time, value
                FROM sensor_data
                WHERE device_id = '0'
                  AND sensor_id = '0'
                  AND time >= now() - INTERVAL '1 hour'
                """
            )
        ),
        "aggregate_groupby_ms": _timed_ms(
            lambda: client.query_sql(
                """
                SELECT device_id, AVG(value) AS mean_value
                FROM sensor_data
                WHERE sensor_id = '0'
                  AND time >= now() - INTERVAL '1 hour'
                GROUP BY device_id
                """
            )
        ),
        "downsample_1m_ms": _timed_ms(
            lambda: client.query_sql(
                """
                SELECT DATE_BIN(INTERVAL '1 minute', time) AS bucket, sensor_id, AVG(value) AS mean_value
                FROM sensor_data
                WHERE device_id = '0'
                  AND time >= now() - INTERVAL '1 hour'
                GROUP BY 1, sensor_id
                ORDER BY 1, sensor_id
                """
            )
        ),
    }


# ============ IoTDB ============
def iotdb_setup():
    if IoTDBSession is None:
        raise RuntimeError("缺少 apache-iotdb，请先安装：pip install apache-iotdb")
    session = IoTDBSession(host=IOTDB_HOST, port=IOTDB_PORT, fetch_size=10000)
    session.open(False)
    try:
        session.execute_non_query_statement("DELETE DATABASE root.test_bench")
    except Exception:
        pass
    session.execute_non_query_statement("CREATE DATABASE root.test_bench")
    return session


def iotdb_write_test(points):
    session = iotdb_setup()
    for d in range(NUM_DEVICES):
        for s in range(SENSORS_PER_DEVICE):
            session.execute_non_query_statement(
                f"CREATE TIMESERIES root.test_bench.d{d}.s{s} WITH DATATYPE=FLOAT, ENCODING=GORILLA"
            )

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
    session.close()
    return _write_result(len(points), elapsed)


def _iotdb_batch_insert(session, batch):
    groups = defaultdict(list)
    for device_id, sensor_id, ts, value in batch:
        groups[device_id].append((sensor_id, ts, value))

    for device_id, records in groups.items():
        timestamps = [r[1] for r in records]
        measurements_list = [[f"s{r[0]}"] for r in records]
        types_list = [["FLOAT"] for _ in records]
        values_list = [[str(r[2])] for r in records]
        try:
            session.insert_records(
                [f"root.test_bench.d{device_id}"] * len(records),
                timestamps,
                measurements_list,
                types_list,
                values_list,
            )
        except Exception:
            for sensor_id, ts, value in records:
                session.execute_non_query_statement(
                    f"INSERT INTO root.test_bench.d{device_id}(timestamp, s{sensor_id}) VALUES ({ts}, {value})"
                )


def iotdb_query_tests():
    if IoTDBSession is None:
        raise RuntimeError("缺少 apache-iotdb，请先安装：pip install apache-iotdb")
    session = IoTDBSession(host=IOTDB_HOST, port=IOTDB_PORT, fetch_size=10000)
    session.open(False)
    try:
        return {
            "point_query_ms": _timed_ms(
                lambda: session.execute_query_statement("SELECT last_value(*) FROM root.test_bench.d0.s0")
            ),
            "range_query_1h_ms": _timed_ms(
                lambda: session.execute_query_statement(
                    "SELECT * FROM root.test_bench.d0.s0 ORDER BY TIME DESC LIMIT 3600"
                )
            ),
            "aggregate_groupby_ms": _timed_ms(
                lambda: session.execute_query_statement("SELECT COUNT(*) FROM root.test_bench.*.*")
            ),
            "downsample_1m_ms": _timed_ms(
                lambda: session.execute_query_statement(
                    "SELECT AVG(s0), AVG(s1), AVG(s2), AVG(s3), AVG(s4) FROM root.test_bench.d0.*"
                )
            ),
        }
    finally:
        session.close()


# ============ PostgreSQL ============
def postgres_setup():
    if psycopg is None:
        raise RuntimeError('缺少 psycopg，请先安装：pip install "psycopg[binary]"')
    conn = psycopg.connect(**POSTGRES_CONFIG)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS synthetic_sensor_data")
        cur.execute(
            """
            CREATE TABLE synthetic_sensor_data (
                device_id INTEGER NOT NULL,
                sensor_id INTEGER NOT NULL,
                ts BIGINT NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (device_id, sensor_id, ts)
            )
            """
        )
        cur.execute("CREATE INDEX idx_synthetic_sensor ON synthetic_sensor_data (sensor_id, device_id)")
        cur.execute("CREATE INDEX idx_synthetic_ts ON synthetic_sensor_data (ts)")
    conn.commit()
    return conn


def postgres_write_test(points):
    conn = postgres_setup()
    start = time.time()
    with conn.cursor() as cur:
        for i in range(0, len(points), BATCH_SIZE):
            cur.executemany("INSERT INTO synthetic_sensor_data VALUES (%s,%s,%s,%s)", points[i : i + BATCH_SIZE])
            conn.commit()
    elapsed = time.time() - start
    conn.close()
    return _write_result(len(points), elapsed)


def postgres_query_tests():
    conn = psycopg.connect(**POSTGRES_CONFIG)
    with conn.cursor() as cur:
        results = {
            "point_query_ms": _timed_ms(
                lambda: cur.execute(
                    """
                    SELECT ts, value
                    FROM synthetic_sensor_data
                    WHERE device_id = 0 AND sensor_id = 0
                    ORDER BY ts DESC
                    LIMIT 1
                    """
                ).fetchall()
            ),
            "range_query_1h_ms": _timed_ms(
                lambda: cur.execute(
                    """
                    SELECT ts, value
                    FROM synthetic_sensor_data
                    WHERE device_id = 0 AND sensor_id = 0
                    ORDER BY ts DESC
                    LIMIT 3600
                    """
                ).fetchall()
            ),
            "aggregate_groupby_ms": _timed_ms(
                lambda: cur.execute(
                    """
                    SELECT device_id, AVG(value)
                    FROM synthetic_sensor_data
                    WHERE sensor_id = 0
                    GROUP BY device_id
                    """
                ).fetchall()
            ),
            "downsample_1m_ms": _timed_ms(
                lambda: cur.execute(
                    """
                    SELECT FLOOR(ts / 60000.0), sensor_id, AVG(value)
                    FROM synthetic_sensor_data
                    WHERE device_id = 0
                    GROUP BY 1, sensor_id
                    ORDER BY 1, sensor_id
                    """
                ).fetchall()
            ),
        }
    conn.close()
    return results


# ============ DolphinDB ============
def dolphindb_write_test(points):
    client = DolphinDBClient()
    try:
        client.recreate_synthetic_table()
        start = time.time()
        total = client.append_rows(
            SYNTHETIC_DB,
            SYNTHETIC_TABLE,
            ["device_id", "sensor_id", "ts", "value"],
            points,
            batch_size=BATCH_SIZE,
        )
        elapsed = time.time() - start
        return _write_result(total, elapsed)
    finally:
        client.close()


def dolphindb_query_tests():
    client = DolphinDBClient()
    try:
        table = f't = loadTable("{SYNTHETIC_DB}", "{SYNTHETIC_TABLE}")\n'
        return {
            "point_query_ms": _timed_ms(
                lambda: client.run(
                    table
                    + """
                    select ts, value from t
                    where device_id = 0 and sensor_id = 0
                    order by ts desc
                    limit 1
                    """
                )
            ),
            "range_query_1h_ms": _timed_ms(
                lambda: client.run(
                    table
                    + """
                    select ts, value from t
                    where device_id = 0 and sensor_id = 0
                    order by ts desc
                    limit 3600
                    """
                )
            ),
            "aggregate_groupby_ms": _timed_ms(
                lambda: client.run(
                    table
                    + """
                    select avg(value) as mean_value from t
                    where sensor_id = 0
                    group by device_id
                    """
                )
            ),
            "downsample_1m_ms": _timed_ms(
                lambda: client.run(
                    table
                    + """
                    select avg(value) as mean_value from t
                    where device_id = 0
                    group by floor(ts / 60000), sensor_id
                    """
                )
            ),
        }
    finally:
        client.close()


# ============ Helpers ============
def _timed_ms(fn):
    start = time.time()
    fn()
    return round((time.time() - start) * 1000, 1)


def _write_result(total, elapsed):
    return {
        "total_points": total,
        "elapsed_s": round(elapsed, 2),
        "throughput_ps": round(total / elapsed) if elapsed > 0 else 0,
    }


def get_disk_usage(path):
    try:
        result = subprocess.run(["du", "-sh", path], capture_output=True, text=True, check=False)
        return result.stdout.split()[0] if result.returncode == 0 and result.stdout else "N/A"
    except Exception:
        return "N/A"


def get_container_disk_usage(container, path):
    try:
        result = subprocess.run(
            ["docker", "exec", container, "du", "-sh", path],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.stdout.split()[0] if result.returncode == 0 and result.stdout else "N/A"
    except Exception:
        return "N/A"


def _fmt(value):
    if isinstance(value, float):
        return f"{value:.1f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _row(name, values):
    print(f"{name:<25}" + "".join(f"{_fmt(v):>15}" for v in values))


def main():
    targets = selected_targets()
    print("=" * 80)
    print("  通用合成数据 Benchmark")
    print(f"  配置: {NUM_DEVICES} 设备 × {SENSORS_PER_DEVICE} 传感器 × {POINTS_PER_SENSOR} 点")
    print(f"  总数据点数: {NUM_DEVICES * SENSORS_PER_DEVICE * POINTS_PER_SENSOR:,}")
    print(f"  目标库: {', '.join(TARGET_LABELS[t] for t in targets)}")
    print("=" * 80)

    print("\n[准备] 生成测试数据...")
    points = generate_data()
    random.shuffle(points)
    print(f"  生成 {len(points):,} 个数据点")

    write_tests = {
        "influx": influxdb_write_test,
        "iotdb": iotdb_write_test,
        "postgres": postgres_write_test,
        "dolphindb": dolphindb_write_test,
    }
    query_tests = {
        "influx": influxdb_query_tests,
        "iotdb": iotdb_query_tests,
        "postgres": postgres_query_tests,
        "dolphindb": dolphindb_query_tests,
    }
    disk_tests = {
        "influx": lambda: get_disk_usage(os.path.join(DATA_DIR, "influxdb3")),
        "iotdb": lambda: get_disk_usage(os.path.join(DATA_DIR, "iotdb2")),
        "postgres": lambda: get_disk_usage(os.path.join(DATA_DIR, "postgres")),
        "dolphindb": lambda: get_container_disk_usage("tsdb-dolphindb", "/data/ddb"),
    }

    write_results = {}
    for target in targets:
        label = TARGET_LABELS[target]
        print(f"\n[写入] {label}...")
        write_results[target] = write_tests[target](points)
        print(
            f"  {label}: {write_results[target]['throughput_ps']:,} rows/s, "
            f"耗时 {write_results[target]['elapsed_s']}s"
        )

    print("\n[查询] 查询性能测试...")
    query_results = {}
    for target in targets:
        label = TARGET_LABELS[target]
        print(f"  {label}...")
        query_results[target] = query_tests[target]()

    print("\n[磁盘] 空间占用...")
    disks = {}
    for target in targets:
        disks[target] = disk_tests[target]()
        print(f"  {TARGET_LABELS[target]}: {disks[target]}")

    print("\n" + "=" * 80)
    print("  测试结果汇总")
    print("=" * 80)
    print(f"\n{'指标':<25}" + "".join(f"{TARGET_LABELS[t]:>15}" for t in targets))
    print("-" * (25 + 15 * len(targets)))
    _row(
        "写入吞吐 (points/s)",
        [write_results[target]["throughput_ps"] for target in targets],
    )
    _row(
        "写入总耗时 (s)",
        [write_results[target]["elapsed_s"] for target in targets],
    )

    for name, key in [
        ("点查询 (ms)", "point_query_ms"),
        ("范围查询 1h (ms)", "range_query_1h_ms"),
        ("全量聚合 (ms)", "aggregate_groupby_ms"),
        ("多传感器AVG (ms)", "downsample_1m_ms"),
    ]:
        _row(name, [query_results[target][key] for target in targets])

    _row("磁盘占用", [disks[target] for target in targets])
    print("\n✅ Benchmark 完成")


if __name__ == "__main__":
    main()
