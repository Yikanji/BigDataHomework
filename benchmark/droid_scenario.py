#!/usr/bin/env python3
"""DROID 场景化 Benchmark: InfluxDB 3 vs IoTDB 2.0 vs PostgreSQL vs DolphinDB."""

import glob
import os
import subprocess
import sys
import time

try:
    import psycopg
except ImportError:
    psycopg = None

try:
    import mysql.connector
except ImportError:
    mysql = None

from tfrecord import tfrecord_loader

from dolphindb_client import DolphinDBClient, DROID_DB, DROID_TABLE
from influxdb3_client import InfluxDB3Client, line_protocol


INFLUXDB_DB = "droid"

IOTDB_HOST = os.environ.get("IOTDB_HOST", "localhost")
IOTDB_PORT = int(os.environ.get("IOTDB_PORT", "6667"))

POSTGRES_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "root123"),
    "dbname": os.environ.get("POSTGRES_DB", "droid"),
}

MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("MYSQL_PORT", "3306")),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", "root123"),
    "database": os.environ.get("MYSQL_DB", "droid"),
}

DROID_PATH = os.environ.get("DROID_PATH", "/mnt/huawei_nas/Datasets/DROID/1.0.0")
MAX_EPISODES = int(os.environ.get("DROID_MAX_EPISODES", "60"))
TF_SHARD_COUNT = int(os.environ.get("DROID_TF_SHARD_COUNT", "3"))
STEP_INTERVAL_MS = 67
BATCH_SIZE = int(os.environ.get("BENCH_BATCH_SIZE", "5000"))

FIELDS = [
    ("jpos", "steps/observation/joint_position", 7),
    ("grip", "steps/observation/gripper_position", 1),
    ("jvel", "steps/action_dict/joint_velocity", 7),
    ("cpos", "steps/observation/cartesian_position", 6),
]

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

ALL_TARGETS = ("influx", "iotdb", "postgres", "mysql", "dolphindb")
TARGET_LABELS = {
    "influx": "InfluxDB 3",
    "iotdb": "IoTDB",
    "postgres": "PostgreSQL",
    "mysql": "MySQL",
    "dolphindb": "DolphinDB",
}
TARGET_RESULT_KEYS = {
    "influx": "inf",
    "iotdb": "iot",
    "postgres": "pg",
    "mysql": "mysql",
    "dolphindb": "ddb",
}
TARGET_ALIASES = {
    "influx": "influx",
    "influxdb": "influx",
    "influxdb3": "influx",
    "iotdb": "iotdb",
    "pg": "postgres",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
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


# ==================== 数据加载 ====================
def load_episodes():
    if os.environ.get("DROID_SYNTHETIC", "0") == "1":
        return load_synthetic_episodes()

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


def load_synthetic_episodes():
    import numpy as np

    count = int(os.environ.get("DROID_SYNTHETIC_EPISODES", "4"))
    steps = int(os.environ.get("DROID_SYNTHETIC_STEPS", "120"))
    episodes = []
    for episode_id in range(count):
        data = {}
        x = np.arange(steps, dtype=float)
        for name, _, dims in FIELDS:
            cols = []
            for dim in range(dims):
                cols.append(np.sin(x / 12 + dim) + episode_id * 0.1 + dim * 0.01)
            data[name] = np.stack(cols, axis=1)
        episodes.append(
            {
                "data": data,
                "steps": steps,
                "points": sum(values.shape[0] * values.shape[1] for values in data.values()),
            }
        )
    pts = sum(e["points"] for e in episodes)
    print(f"  synthetic {len(episodes)} 条轨迹, {count * steps} 步, {pts:,} 数据点")
    return episodes


def _parse(rec):
    data, n = {}, None
    for name, key, dims in FIELDS:
        arr = rec.get(key)
        if arr is None:
            continue
        n = len(arr) // dims
        data[name] = arr.reshape(n, dims)
    return {"data": data, "steps": n or 0, "points": sum(d.shape[0] * d.shape[1] for d in data.values())}


# ==================== InfluxDB ====================
def influx_write(eps):
    client = InfluxDB3Client(INFLUXDB_DB)
    client.recreate_database()
    base = int(time.time() * 1000)

    def rows():
        for ei, ep in enumerate(eps):
            for name, _, dims in FIELDS:
                arr = ep["data"].get(name)
                if arr is None:
                    continue
                n = arr.shape[0]
                for step in range(n):
                    ts = base - (n - step) * STEP_INTERVAL_MS
                    yield line_protocol(
                        name,
                        {"ep": str(ei)},
                        {f"d{dim}": float(arr[step, dim]) for dim in range(dims)},
                        ts,
                    )

    total = sum(e["points"] for e in eps)
    start = time.time()
    client.write_lines(rows(), batch_size=BATCH_SIZE)
    return total, time.time() - start


# ==================== IoTDB ====================
def iotdb_setup(eps):
    from iotdb.Session import Session

    session = Session(host=IOTDB_HOST, port=IOTDB_PORT, fetch_size=10000)
    session.open(False)
    try:
        session.execute_non_query_statement("DELETE DATABASE root.droid")
    except Exception:
        pass
    session.execute_non_query_statement("CREATE DATABASE root.droid")
    for ei in range(len(eps)):
        for name, _, dims in FIELDS:
            for dim in range(dims):
                try:
                    session.execute_non_query_statement(
                        f"CREATE TIMESERIES root.droid.ep{ei}.{name}_d{dim} "
                        "WITH DATATYPE=FLOAT, ENCODING=GORILLA"
                    )
                except Exception:
                    pass
    return session


def iotdb_write(eps):
    session = iotdb_setup(eps)
    base = int(time.time() * 1000)
    total, start = 0, time.time()
    for ei, ep in enumerate(eps):
        n = ep["steps"]
        if n == 0:
            continue
        dev = f"root.droid.ep{ei}"
        for step in range(n):
            ts = base - (n - step) * STEP_INTERVAL_MS
            cols, vals = [], []
            for name, _, dims in FIELDS:
                arr = ep["data"].get(name)
                if arr is None:
                    continue
                for dim in range(dims):
                    cols.append(f"{name}_d{dim}")
                    vals.append(str(float(arr[step, dim])))
            if cols:
                sql = f"INSERT INTO {dev}(timestamp, {','.join(cols)}) VALUES ({ts}, {','.join(vals)})"
                try:
                    session.execute_non_query_statement(sql)
                    total += len(cols)
                except Exception:
                    pass
    elapsed = time.time() - start
    session.close()
    return total, elapsed


# ==================== PostgreSQL ====================
def postgres_setup():
    if psycopg is None:
        raise RuntimeError('缺少 psycopg，请先安装：pip install "psycopg[binary]"')
    conn = psycopg.connect(**POSTGRES_CONFIG)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS droid_data")
        cur.execute(
            """
            CREATE TABLE droid_data (
                episode_id INTEGER NOT NULL,
                step INTEGER NOT NULL,
                ts BIGINT NOT NULL,
                field VARCHAR(10) NOT NULL,
                dim INTEGER NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (episode_id, step, field, dim)
            )
            """
        )
        cur.execute("CREATE INDEX idx_droid_ep_field ON droid_data (episode_id, field)")
        cur.execute("CREATE INDEX idx_droid_field_dim ON droid_data (field, dim)")
        cur.execute("CREATE INDEX idx_droid_ts ON droid_data (ts)")
    conn.commit()
    return conn


def postgres_write(eps):
    conn = postgres_setup()
    base = int(time.time() * 1000)
    total, start = 0, time.time()
    batch = []
    with conn.cursor() as cur:
        for ei, ep in enumerate(eps):
            n = ep["steps"]
            if n == 0:
                continue
            for name, _, dims in FIELDS:
                arr = ep["data"].get(name)
                if arr is None:
                    continue
                for step in range(n):
                    ts = base - (n - step) * STEP_INTERVAL_MS
                    for dim in range(dims):
                        batch.append((ei, step, ts, name, dim, float(arr[step, dim])))
                        if len(batch) >= BATCH_SIZE:
                            cur.executemany("INSERT INTO droid_data VALUES (%s,%s,%s,%s,%s,%s)", batch)
                            conn.commit()
                            total += len(batch)
                            batch = []
        if batch:
            cur.executemany("INSERT INTO droid_data VALUES (%s,%s,%s,%s,%s,%s)", batch)
            conn.commit()
            total += len(batch)
    return conn, total, time.time() - start


# ==================== MySQL ====================
def mysql_setup():
    if mysql is None:
        raise RuntimeError("缺少 mysql-connector-python，请先安装：pip install mysql-connector-python")

    db_name = MYSQL_CONFIG["database"]
    server_config = {key: value for key, value in MYSQL_CONFIG.items() if key != "database"}
    conn = mysql.connector.connect(**server_config)
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
    cur.execute(f"USE `{db_name}`")
    cur.execute("DROP TABLE IF EXISTS droid_data")
    cur.execute(
        """
        CREATE TABLE droid_data (
            episode_id INT NOT NULL,
            step INT NOT NULL,
            ts BIGINT NOT NULL,
            field VARCHAR(10) NOT NULL,
            dim INT NOT NULL,
            value DOUBLE NOT NULL,
            PRIMARY KEY (episode_id, step, field, dim),
            INDEX idx_droid_ep_field (episode_id, field),
            INDEX idx_droid_field_dim (field, dim),
            INDEX idx_droid_ts (ts)
        ) ENGINE=InnoDB
        """
    )
    conn.commit()
    cur.close()
    return conn


def mysql_write(eps):
    conn = mysql_setup()
    cur = conn.cursor()
    base = int(time.time() * 1000)
    total, start = 0, time.time()
    batch = []
    for ei, ep in enumerate(eps):
        n = ep["steps"]
        if n == 0:
            continue
        for name, _, dims in FIELDS:
            arr = ep["data"].get(name)
            if arr is None:
                continue
            for step in range(n):
                ts = base - (n - step) * STEP_INTERVAL_MS
                for dim in range(dims):
                    batch.append((ei, step, ts, name, dim, float(arr[step, dim])))
                    if len(batch) >= BATCH_SIZE:
                        cur.executemany("INSERT INTO droid_data VALUES (%s,%s,%s,%s,%s,%s)", batch)
                        conn.commit()
                        total += len(batch)
                        batch = []
    if batch:
        cur.executemany("INSERT INTO droid_data VALUES (%s,%s,%s,%s,%s,%s)", batch)
        conn.commit()
        total += len(batch)
    cur.close()
    return conn, total, time.time() - start


# ==================== DolphinDB ====================
def dolphindb_write(eps):
    client = DolphinDBClient()
    client.recreate_droid_table()
    base = int(time.time() * 1000)

    def rows():
        for ei, ep in enumerate(eps):
            n = ep["steps"]
            if n == 0:
                continue
            for name, _, dims in FIELDS:
                arr = ep["data"].get(name)
                if arr is None:
                    continue
                for step in range(n):
                    ts = base - (n - step) * STEP_INTERVAL_MS
                    for dim in range(dims):
                        yield (ei, step, ts, name, dim, float(arr[step, dim]))

    start = time.time()
    try:
        total = client.append_rows(
            DROID_DB,
            DROID_TABLE,
            ["episode_id", "step", "ts", "field", "dim", "value"],
            rows(),
            batch_size=BATCH_SIZE,
        )
        return total, time.time() - start
    finally:
        client.close()


# ==================== 场景测试 ====================
def run_tests(eps, postgres_conn):
    from iotdb.Session import Session

    inc = InfluxDB3Client(INFLUXDB_DB)
    iot = Session(host=IOTDB_HOST, port=IOTDB_PORT, fetch_size=10000)
    iot.open(False)
    ddb = DolphinDBClient()

    results = {}
    ddb_table = f't = loadTable("{DROID_DB}", "{DROID_TABLE}")\n'

    try:
        print("\n" + "-" * 25)

        print("\n[场景1] 降采样: 原始 → 1s 窗口均值 (ep0, jpos_d0)")
        sql_inf_downsample = """
        SELECT DATE_BIN(INTERVAL '1 second', time) AS bucket, AVG(d0) AS mean_v
        FROM jpos
        WHERE ep = '0' AND time >= now() - INTERVAL '30 minutes'
        GROUP BY 1
        ORDER BY 1
        """
        results["downsample_inf_ms"] = _timed_ms(lambda: inc.query_sql(sql_inf_downsample))

        results["downsample_iot_ms"] = _timed_ms(
            lambda: iot.execute_query_statement("SELECT COUNT(jpos_d0) FROM root.droid.ep0.* GROUP BY(1s)")
        )

        with postgres_conn.cursor() as cur:
            results["downsample_pg_ms"] = _timed_ms(
                lambda: cur.execute(
                    """
                    SELECT FLOOR(step * %s) AS bucket, AVG(value), COUNT(*)
                    FROM droid_data
                    WHERE episode_id = 0 AND field = 'jpos' AND dim = 0
                    GROUP BY 1 ORDER BY 1
                    """,
                    (STEP_INTERVAL_MS / 1000.0,),
                ).fetchall()
            )

        results["downsample_ddb_ms"] = _timed_ms(
            lambda: ddb.run(
                ddb_table
                + """
                select avg(value) as mean_value, count(value) as cnt from t
                where episode_id = 0 and field = `jpos and dim = 0
                group by floor(step * 67 / 1000)
                """
            )
        )

        print(
            f"  InfluxDB 3 {results['downsample_inf_ms']:.1f} ms | "
            f"IoTDB {results['downsample_iot_ms']:.1f} ms | "
            f"PostgreSQL {results['downsample_pg_ms']:.1f} ms | "
            f"DolphinDB {results['downsample_ddb_ms']:.1f} ms"
        )

        print("\n[场景2] 插值: 删除 ep0/jpos_d0 中间 50 点后线性补全")
        first_ts = int(time.time() * 1000) - eps[0]["steps"] * STEP_INTERVAL_MS
        mid_ts = first_ts + eps[0]["steps"] * STEP_INTERVAL_MS // 2
        for i in range(50):
            try:
                iot.execute_non_query_statement(
                    f"DELETE FROM root.droid.ep0.jpos_d0 WHERE time={mid_ts + i * STEP_INTERVAL_MS}"
                )
            except Exception:
                pass

        sql_inf_fill = """
        SELECT date_bin_gapfill(INTERVAL '67 milliseconds', time) AS bucket,
               interpolate(AVG(d0)) AS filled_v
        FROM jpos
        WHERE ep = '0'
          AND time >= now() - INTERVAL '30 minutes'
          AND time <= now()
        GROUP BY 1
        ORDER BY 1
        """
        results["fill_inf_ms"] = _timed_ms(lambda: inc.query_sql(sql_inf_fill))
        results["fill_iot_ms"] = _timed_ms(
            lambda: iot.execute_query_statement("SELECT jpos_d0 FROM root.droid.ep0.* FILL(LINEAR)")
        )

        with postgres_conn.cursor() as cur:
            cur.execute("SELECT MIN(step), MAX(step) FROM droid_data WHERE episode_id=0 AND field='jpos' AND dim=0")
            min_s, max_s = cur.fetchone()
            mid_s = (min_s + max_s) // 2
            cur.execute(
                """
                DELETE FROM droid_data
                WHERE episode_id=0 AND field='jpos' AND dim=0 AND step BETWEEN %s AND %s
                """,
                (mid_s, mid_s + 49),
            )
            postgres_conn.commit()
            results["fill_pg_ms"] = _timed_ms(
                lambda: cur.execute(
                    """
                    WITH raw AS (
                        SELECT step, value
                        FROM droid_data
                        WHERE episode_id = 0 AND field = 'jpos' AND dim = 0
                    ),
                    grid AS (
                        SELECT generate_series(%s::int, %s::int) AS step
                    )
                    SELECT
                        g.step,
                        COALESCE(
                            r.value,
                            p.value + (g.step - p.step) * (n.value - p.value) / NULLIF(n.step - p.step, 0)
                        ) AS filled_value
                    FROM grid g
                    LEFT JOIN raw r ON r.step = g.step
                    LEFT JOIN LATERAL (
                        SELECT step, value FROM raw WHERE step < g.step ORDER BY step DESC LIMIT 1
                    ) p ON r.value IS NULL
                    LEFT JOIN LATERAL (
                        SELECT step, value FROM raw WHERE step > g.step ORDER BY step ASC LIMIT 1
                    ) n ON r.value IS NULL
                    ORDER BY g.step
                    """,
                    (min_s, max_s),
                ).fetchall()
            )

        ddb.run(
            ddb_table
            + f"""
            delete from t
            where episode_id = 0 and field = `jpos and dim = 0 and step >= {mid_s} and step <= {mid_s + 49}
            """
        )
        results["fill_ddb_ms"] = _timed_ms(
            lambda: ddb.run(
                ddb_table
                + """
                raw = select step, value from t
                      where episode_id = 0 and field = `jpos and dim = 0
                      order by step
                model = linearInterpolateFit(raw.step$DOUBLE, raw.value, "extrapolate", true)
                grid = min(raw.step)..max(raw.step)
                predict(model, grid$DOUBLE)
                """
            )
        )

        print(
            f"  InfluxDB 3 {results['fill_inf_ms']:.1f} ms | "
            f"IoTDB {results['fill_iot_ms']:.1f} ms | "
            f"PostgreSQL {results['fill_pg_ms']:.1f} ms | "
            f"DolphinDB {results['fill_ddb_ms']:.1f} ms"
        )

        print("\n[场景3] 滑动窗口: 1s 窗口 STDDEV 检测关节抖动 (ep0, jvel_d0)")
        sql_inf_sd = """
        SELECT DATE_BIN(INTERVAL '1 second', time) AS bucket, STDDEV(d0) AS stddev_v
        FROM jvel
        WHERE ep = '0' AND time >= now() - INTERVAL '30 minutes'
        GROUP BY 1
        ORDER BY 1
        """
        results["slide_inf_ms"] = _timed_ms(lambda: inc.query_sql(sql_inf_sd))
        results["slide_iot_ms"] = _timed_ms(
            lambda: iot.execute_query_statement("SELECT STDDEV(jvel_d0) FROM root.droid.ep0.* GROUP BY(1s)")
        )
        with postgres_conn.cursor() as cur:
            results["slide_pg_ms"] = _timed_ms(
                lambda: cur.execute(
                    """
                    SELECT FLOOR(step * %s) AS bucket, STDDEV_POP(value)
                    FROM droid_data
                    WHERE episode_id = 0 AND field = 'jvel' AND dim = 0
                    GROUP BY 1 ORDER BY 1
                    """,
                    (STEP_INTERVAL_MS / 1000.0,),
                ).fetchall()
            )
        results["slide_ddb_ms"] = _timed_ms(
            lambda: ddb.run(
                ddb_table
                + """
                select std(value) as std_value from t
                where episode_id = 0 and field = `jvel and dim = 0
                group by floor(step * 67 / 1000)
                """
            )
        )
        print(
            f"  InfluxDB 3 {results['slide_inf_ms']:.1f} ms | "
            f"IoTDB {results['slide_iot_ms']:.1f} ms | "
            f"PostgreSQL {results['slide_pg_ms']:.1f} ms | "
            f"DolphinDB {results['slide_ddb_ms']:.1f} ms"
        )

        print(f"\n[场景4] 跨轨迹聚合: {len(eps)} 条轨迹关节位置全局 AVG + STDDEV")
        sql_inf_global = """
        SELECT COUNT(d0) AS cnt, AVG(d0) AS mean_v, STDDEV(d0) AS stddev_v
        FROM jpos
        WHERE time >= now() - INTERVAL '30 minutes'
        """
        results["global_inf_ms"] = _timed_ms(lambda: inc.query_sql(sql_inf_global))
        results["global_iot_ms"] = _timed_ms(
            lambda: iot.execute_query_statement("SELECT COUNT(*), AVG(jpos_d0), AVG(jpos_d1) FROM root.droid.*.*")
        )
        with postgres_conn.cursor() as cur:
            results["global_pg_ms"] = _timed_ms(
                lambda: cur.execute(
                    """
                    SELECT field, dim, AVG(value), COUNT(*), STDDEV_POP(value)
                    FROM droid_data
                    WHERE field = 'jpos'
                    GROUP BY field, dim
                    ORDER BY field, dim
                    """
                ).fetchall()
            )
        results["global_ddb_ms"] = _timed_ms(
            lambda: ddb.run(
                ddb_table
                + """
                select avg(value) as mean_value, count(value) as cnt, std(value) as std_value from t
                where field = `jpos
                group by field, dim
                """
            )
        )
        print(
            f"  InfluxDB 3 {results['global_inf_ms']:.1f} ms | "
            f"IoTDB {results['global_iot_ms']:.1f} ms | "
            f"PostgreSQL {results['global_pg_ms']:.1f} ms | "
            f"DolphinDB {results['global_ddb_ms']:.1f} ms"
        )

        return results
    finally:
        iot.close()
        ddb.close()


def print_scenario_result(results, targets, scenario):
    parts = []
    for target in targets:
        key = f"{scenario}_{TARGET_RESULT_KEYS[target]}_ms"
        parts.append(f"{TARGET_LABELS[target]} {results[key]:.1f} ms")
    print("  " + " | ".join(parts))


def run_selected_tests(eps, postgres_conn, mysql_conn, targets):
    inc = InfluxDB3Client(INFLUXDB_DB) if "influx" in targets else None
    iot = None
    ddb = DolphinDBClient() if "dolphindb" in targets else None
    if "iotdb" in targets:
        from iotdb.Session import Session

        iot = Session(host=IOTDB_HOST, port=IOTDB_PORT, fetch_size=10000)
        iot.open(False)

    results = {}
    ddb_table = f't = loadTable("{DROID_DB}", "{DROID_TABLE}")\n'
    min_s, max_s = 0, max(eps[0]["steps"] - 1, 0)
    delete_start = max(min_s, (max_s - 49) // 2)
    delete_end = min(max_s, delete_start + 49)

    try:
        print("\n" + "-" * 25)

        print("\n[场景1] 降采样: 原始 -> 1s 窗口均值 (ep0, jpos_d0)")
        if "influx" in targets:
            sql_inf_downsample = """
            SELECT DATE_BIN(INTERVAL '1 second', time) AS bucket, AVG(d0) AS mean_v
            FROM jpos
            WHERE ep = '0' AND time >= now() - INTERVAL '30 minutes'
            GROUP BY 1
            ORDER BY 1
            """
            results["downsample_inf_ms"] = _timed_ms(lambda: inc.query_sql(sql_inf_downsample))

        if "iotdb" in targets:
            results["downsample_iot_ms"] = _timed_ms(
                lambda: iot.execute_query_statement("SELECT COUNT(jpos_d0) FROM root.droid.ep0.* GROUP BY(1s)")
            )

        if "postgres" in targets:
            with postgres_conn.cursor() as cur:
                results["downsample_pg_ms"] = _timed_ms(
                    lambda: cur.execute(
                        """
                        SELECT FLOOR(step * %s) AS bucket, AVG(value), COUNT(*)
                        FROM droid_data
                        WHERE episode_id = 0 AND field = 'jpos' AND dim = 0
                        GROUP BY 1 ORDER BY 1
                        """,
                        (STEP_INTERVAL_MS / 1000.0,),
                    ).fetchall()
                )

        if "mysql" in targets:
            cur = mysql_conn.cursor()
            try:
                results["downsample_mysql_ms"] = _timed_ms(
                    lambda: (
                        cur.execute(
                            """
                            SELECT FLOOR(step * %s) AS bucket, AVG(value), COUNT(*)
                            FROM droid_data
                            WHERE episode_id = 0 AND field = 'jpos' AND dim = 0
                            GROUP BY bucket ORDER BY bucket
                            """,
                            (STEP_INTERVAL_MS / 1000.0,),
                        ),
                        cur.fetchall(),
                    )
                )
            finally:
                cur.close()

        if "dolphindb" in targets:
            results["downsample_ddb_ms"] = _timed_ms(
                lambda: ddb.run(
                    ddb_table
                    + """
                    select avg(value) as mean_value, count(value) as cnt from t
                    where episode_id = 0 and field = `jpos and dim = 0
                    group by floor(step * 67 / 1000)
                    """
                )
            )
        print_scenario_result(results, targets, "downsample")

        print("\n[场景2] 插值: 删除 ep0/jpos_d0 中间 50 点后线性补全")
        if "iotdb" in targets:
            first_ts = int(time.time() * 1000) - eps[0]["steps"] * STEP_INTERVAL_MS
            mid_ts = first_ts + delete_start * STEP_INTERVAL_MS
            for i in range(delete_end - delete_start + 1):
                try:
                    iot.execute_non_query_statement(
                        f"DELETE FROM root.droid.ep0.jpos_d0 WHERE time={mid_ts + i * STEP_INTERVAL_MS}"
                    )
                except Exception:
                    pass
            results["fill_iot_ms"] = _timed_ms(
                lambda: iot.execute_query_statement("SELECT jpos_d0 FROM root.droid.ep0.* FILL(LINEAR)")
            )

        if "influx" in targets:
            sql_inf_fill = """
            SELECT date_bin_gapfill(INTERVAL '67 milliseconds', time) AS bucket,
                   interpolate(AVG(d0)) AS filled_v
            FROM jpos
            WHERE ep = '0'
              AND time >= now() - INTERVAL '30 minutes'
              AND time <= now()
            GROUP BY 1
            ORDER BY 1
            """
            results["fill_inf_ms"] = _timed_ms(lambda: inc.query_sql(sql_inf_fill))

        if "postgres" in targets:
            with postgres_conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM droid_data
                    WHERE episode_id=0 AND field='jpos' AND dim=0 AND step BETWEEN %s AND %s
                    """,
                    (delete_start, delete_end),
                )
                postgres_conn.commit()
                results["fill_pg_ms"] = _timed_ms(
                    lambda: cur.execute(
                        """
                        WITH raw AS (
                            SELECT step, value
                            FROM droid_data
                            WHERE episode_id = 0 AND field = 'jpos' AND dim = 0
                        ),
                        grid AS (
                            SELECT generate_series(%s::int, %s::int) AS step
                        )
                        SELECT
                            g.step,
                            COALESCE(
                                r.value,
                                p.value + (g.step - p.step) * (n.value - p.value) / NULLIF(n.step - p.step, 0)
                            ) AS filled_value
                        FROM grid g
                        LEFT JOIN raw r ON r.step = g.step
                        LEFT JOIN LATERAL (
                            SELECT step, value FROM raw WHERE step < g.step ORDER BY step DESC LIMIT 1
                        ) p ON r.value IS NULL
                        LEFT JOIN LATERAL (
                            SELECT step, value FROM raw WHERE step > g.step ORDER BY step ASC LIMIT 1
                        ) n ON r.value IS NULL
                        ORDER BY g.step
                        """,
                        (min_s, max_s),
                    ).fetchall()
                )

        if "mysql" in targets:
            cur = mysql_conn.cursor()
            try:
                cur.execute(
                    """
                    DELETE FROM droid_data
                    WHERE episode_id=0 AND field='jpos' AND dim=0 AND step BETWEEN %s AND %s
                    """,
                    (delete_start, delete_end),
                )
                mysql_conn.commit()
                results["fill_mysql_ms"] = _timed_ms(
                    lambda: (
                        cur.execute(
                            """
                            WITH RECURSIVE grid(step) AS (
                                SELECT %s
                                UNION ALL
                                SELECT step + 1 FROM grid WHERE step < %s
                            ),
                            raw AS (
                                SELECT step, value
                                FROM droid_data
                                WHERE episode_id = 0 AND field = 'jpos' AND dim = 0
                            ),
                            expanded AS (
                                SELECT g.step, r.value
                                FROM grid g
                                LEFT JOIN raw r ON r.step = g.step
                            ),
                            bounds AS (
                                SELECT
                                    step,
                                    value,
                                    MAX(CASE WHEN value IS NOT NULL THEN step END)
                                        OVER (ORDER BY step ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_step,
                                    MIN(CASE WHEN value IS NOT NULL THEN step END)
                                        OVER (ORDER BY step ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS next_step
                                FROM expanded
                            )
                            SELECT
                                b.step,
                                COALESCE(
                                    b.value,
                                    p.value + (b.step - b.prev_step) * (n.value - p.value)
                                        / NULLIF(b.next_step - b.prev_step, 0)
                                ) AS filled_value
                            FROM bounds b
                            LEFT JOIN raw p ON p.step = b.prev_step
                            LEFT JOIN raw n ON n.step = b.next_step
                            ORDER BY b.step
                            """,
                            (min_s, max_s),
                        ),
                        cur.fetchall(),
                    )
                )
            finally:
                cur.close()

        if "dolphindb" in targets:
            ddb.run(
                ddb_table
                + f"""
                delete from t
                where episode_id = 0 and field = `jpos and dim = 0 and step >= {delete_start} and step <= {delete_end}
                """
            )
            results["fill_ddb_ms"] = _timed_ms(
                lambda: ddb.run(
                    ddb_table
                    + """
                    raw = select step, value from t
                          where episode_id = 0 and field = `jpos and dim = 0
                          order by step
                    model = linearInterpolateFit(raw.step$DOUBLE, raw.value, "extrapolate", true)
                    grid = min(raw.step)..max(raw.step)
                    predict(model, grid$DOUBLE)
                    """
                )
            )
        print_scenario_result(results, targets, "fill")

        print("\n[场景3] 滑动窗口: 1s 窗口 STDDEV 检测关节抖动 (ep0, jvel_d0)")
        if "influx" in targets:
            sql_inf_sd = """
            SELECT DATE_BIN(INTERVAL '1 second', time) AS bucket, STDDEV(d0) AS stddev_v
            FROM jvel
            WHERE ep = '0' AND time >= now() - INTERVAL '30 minutes'
            GROUP BY 1
            ORDER BY 1
            """
            results["slide_inf_ms"] = _timed_ms(lambda: inc.query_sql(sql_inf_sd))

        if "iotdb" in targets:
            results["slide_iot_ms"] = _timed_ms(
                lambda: iot.execute_query_statement("SELECT STDDEV(jvel_d0) FROM root.droid.ep0.* GROUP BY(1s)")
            )

        if "postgres" in targets:
            with postgres_conn.cursor() as cur:
                results["slide_pg_ms"] = _timed_ms(
                    lambda: cur.execute(
                        """
                        SELECT FLOOR(step * %s) AS bucket, STDDEV_POP(value)
                        FROM droid_data
                        WHERE episode_id = 0 AND field = 'jvel' AND dim = 0
                        GROUP BY 1 ORDER BY 1
                        """,
                        (STEP_INTERVAL_MS / 1000.0,),
                    ).fetchall()
                )

        if "mysql" in targets:
            cur = mysql_conn.cursor()
            try:
                results["slide_mysql_ms"] = _timed_ms(
                    lambda: (
                        cur.execute(
                            """
                            SELECT FLOOR(step * %s) AS bucket, STDDEV_POP(value)
                            FROM droid_data
                            WHERE episode_id = 0 AND field = 'jvel' AND dim = 0
                            GROUP BY bucket ORDER BY bucket
                            """,
                            (STEP_INTERVAL_MS / 1000.0,),
                        ),
                        cur.fetchall(),
                    )
                )
            finally:
                cur.close()

        if "dolphindb" in targets:
            results["slide_ddb_ms"] = _timed_ms(
                lambda: ddb.run(
                    ddb_table
                    + """
                    select std(value) as std_value from t
                    where episode_id = 0 and field = `jvel and dim = 0
                    group by floor(step * 67 / 1000)
                    """
                )
            )
        print_scenario_result(results, targets, "slide")

        print(f"\n[场景4] 跨轨迹聚合: {len(eps)} 条轨迹关节位置全局 AVG + STDDEV")
        if "influx" in targets:
            sql_inf_global = """
            SELECT COUNT(d0) AS cnt, AVG(d0) AS mean_v, STDDEV(d0) AS stddev_v
            FROM jpos
            WHERE time >= now() - INTERVAL '30 minutes'
            """
            results["global_inf_ms"] = _timed_ms(lambda: inc.query_sql(sql_inf_global))

        if "iotdb" in targets:
            results["global_iot_ms"] = _timed_ms(
                lambda: iot.execute_query_statement("SELECT COUNT(*), AVG(jpos_d0), AVG(jpos_d1) FROM root.droid.*.*")
            )

        if "postgres" in targets:
            with postgres_conn.cursor() as cur:
                results["global_pg_ms"] = _timed_ms(
                    lambda: cur.execute(
                        """
                        SELECT field, dim, AVG(value), COUNT(*), STDDEV_POP(value)
                        FROM droid_data
                        WHERE field = 'jpos'
                        GROUP BY field, dim
                        ORDER BY field, dim
                        """
                    ).fetchall()
                )

        if "mysql" in targets:
            cur = mysql_conn.cursor()
            try:
                results["global_mysql_ms"] = _timed_ms(
                    lambda: (
                        cur.execute(
                            """
                            SELECT field, dim, AVG(value), COUNT(*), STDDEV_POP(value)
                            FROM droid_data
                            WHERE field = 'jpos'
                            GROUP BY field, dim
                            ORDER BY field, dim
                            """
                        ),
                        cur.fetchall(),
                    )
                )
            finally:
                cur.close()

        if "dolphindb" in targets:
            results["global_ddb_ms"] = _timed_ms(
                lambda: ddb.run(
                    ddb_table
                    + """
                    select avg(value) as mean_value, count(value) as cnt, std(value) as std_value from t
                    where field = `jpos
                    group by field, dim
                    """
                )
            )
        print_scenario_result(results, targets, "global")

        return results
    finally:
        if iot is not None:
            iot.close()
        if ddb is not None:
            ddb.close()


# ==================== Helpers ====================
def _timed_ms(fn):
    start = time.time()
    fn()
    return round((time.time() - start) * 1000, 1)


def disk_usage(path):
    try:
        result = subprocess.run(["du", "-sh", path], capture_output=True, text=True, check=False)
        return result.stdout.split()[0] if result.returncode == 0 and result.stdout else "N/A"
    except Exception:
        return "N/A"


def container_disk_usage(container, path):
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
    print(f"{name:<32}" + "".join(f"{_fmt(v):>14}" for v in values))


def main():
    targets = selected_targets()
    print("=" * 88)
    print("  DROID 场景化 Benchmark")
    print(f"  轨迹: ≤{MAX_EPISODES} 条, 分片: {TF_SHARD_COUNT}, ~15Hz")
    print(f"  目标库: {', '.join(TARGET_LABELS[t] for t in targets)}")
    print("=" * 88)

    print("\n[准备] 加载 DROID...")
    eps = load_episodes()
    if not eps:
        print("ERROR: 无数据，请检查 DROID_PATH")
        sys.exit(1)

    write_results = {}
    postgres_conn = None
    mysql_conn = None
    if "influx" in targets:
        print("\n[写入] InfluxDB 3...")
        total, elapsed = influx_write(eps)
        write_results["influx"] = (total, elapsed)
        print(f"  {total:,} 点, {total/elapsed:,.0f} pts/s, 耗时 {elapsed:.1f}s")

    if "iotdb" in targets:
        print("\n[写入] IoTDB...")
        total, elapsed = iotdb_write(eps)
        write_results["iotdb"] = (total, elapsed)
        print(f"  {total:,} 点, {total/elapsed:,.0f} pts/s, 耗时 {elapsed:.1f}s")

    if "postgres" in targets:
        print("\n[写入] PostgreSQL...")
        postgres_conn, total, elapsed = postgres_write(eps)
        write_results["postgres"] = (total, elapsed)
        print(f"  {total:,} 行, {total/elapsed:,.0f} rows/s, 耗时 {elapsed:.1f}s")

    if "mysql" in targets:
        print("\n[写入] MySQL...")
        mysql_conn, total, elapsed = mysql_write(eps)
        write_results["mysql"] = (total, elapsed)
        print(f"  {total:,} 行, {total/elapsed:,.0f} rows/s, 耗时 {elapsed:.1f}s")

    if "dolphindb" in targets:
        print("\n[写入] DolphinDB...")
        total, elapsed = dolphindb_write(eps)
        write_results["dolphindb"] = (total, elapsed)
        print(f"  {total:,} 行, {total/elapsed:,.0f} rows/s, 耗时 {elapsed:.1f}s")

    print("\n[场景] 场景测试")
    results = run_selected_tests(eps, postgres_conn, mysql_conn, targets)
    if postgres_conn is not None:
        postgres_conn.close()
    if mysql_conn is not None:
        mysql_conn.close()

    print("\n[磁盘] 磁盘占用")
    disk_tests = {
        "influx": lambda: disk_usage(os.path.join(DATA_DIR, "influxdb3")),
        "iotdb": lambda: disk_usage(os.path.join(DATA_DIR, "iotdb2")),
        "postgres": lambda: disk_usage(os.path.join(DATA_DIR, "postgres")),
        "mysql": lambda: disk_usage(os.path.join(DATA_DIR, "mysql8")),
        "dolphindb": lambda: container_disk_usage("tsdb-dolphindb", "/data/ddb"),
    }
    disks = {}
    for target in targets:
        disks[target] = disk_tests[target]()
        print(f"  {TARGET_LABELS[target]}: {disks[target]}")

    print("\n[汇总]")
    print("\n" + "=" * 88)
    print("  多库对照结果")
    print("=" * 88)
    print(f"\n{'场景':<32}" + "".join(f"{TARGET_LABELS[t]:>14}" for t in targets))
    print("-" * (32 + 14 * len(targets)))
    _row("写入吞吐 (pts/s)", [write_results[target][0] / write_results[target][1] for target in targets])
    for name, key in [
        ("降采样 (ms)", "downsample"),
        ("插值填充 (ms)", "fill"),
        ("滑动窗口抖动检测 (ms)", "slide"),
        ("跨轨迹聚合 (ms)", "global"),
    ]:
        _row(
            name,
            [results[f"{key}_{TARGET_RESULT_KEYS[target]}_ms"] for target in targets],
        )
    _row("磁盘占用", [disks[target] for target in targets])
    print("\n✅ 完成")


if __name__ == "__main__":
    main()
