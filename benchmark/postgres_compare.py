#!/usr/bin/env python3
"""PostgreSQL 对照测试：同等 DROID 数据下，关系数据库做 TSDB 场景操作。"""

import glob
import os
import time

from tfrecord import tfrecord_loader
import psycopg


DROID_PATH = os.environ.get("DROID_PATH", "/mnt/huawei_nas/Datasets/DROID/1.0.0")
MAX_EPISODES = 60
TF_SHARD_COUNT = 3
STEP_INTERVAL_MS = 67

FIELDS = [
    ("jpos", "steps/observation/joint_position", 7),
    ("grip", "steps/observation/gripper_position", 1),
    ("jvel", "steps/action_dict/joint_velocity", 7),
    ("cpos", "steps/observation/cartesian_position", 6),
]

POSTGRES_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "root123"),
    "dbname": os.environ.get("POSTGRES_DB", "droid"),
}


def load_episodes():
    tf_files = sorted(glob.glob(f"{DROID_PATH}/r2d2_faceblur-train.tfrecord-*"))
    eps = []
    for tf_file in tf_files[:TF_SHARD_COUNT]:
        loader = tfrecord_loader(tf_file, index_path=None, description=None)
        for rec in loader:
            if len(eps) >= MAX_EPISODES:
                break
            eps.append(_parse(rec))
        if len(eps) >= MAX_EPISODES:
            break
    return eps


def _parse(rec):
    data, n = {}, None
    for name, key, dims in FIELDS:
        arr = rec.get(key)
        if arr is None:
            continue
        n = len(arr) // dims
        data[name] = arr.reshape(n, dims)
    return {"data": data, "steps": n or 0}


def postgres_setup():
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
        cur.execute("CREATE INDEX idx_ep_field ON droid_data (episode_id, field)")
        cur.execute("CREATE INDEX idx_field_dim ON droid_data (field, dim)")
        cur.execute("CREATE INDEX idx_ts ON droid_data (ts)")
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
                for s in range(n):
                    ts = base - (n - s) * STEP_INTERVAL_MS
                    for d in range(dims):
                        batch.append((ei, s, ts, name, d, float(arr[s, d])))
                        if len(batch) >= 5000:
                            cur.executemany("INSERT INTO droid_data VALUES (%s,%s,%s,%s,%s,%s)", batch)
                            conn.commit()
                            total += len(batch)
                            batch = []

        if batch:
            cur.executemany("INSERT INTO droid_data VALUES (%s,%s,%s,%s,%s,%s)", batch)
            conn.commit()
            total += len(batch)

    elapsed = time.time() - start
    return conn, total, elapsed


def postgres_tests(conn, eps):
    results = {}
    step_interval_sec = STEP_INTERVAL_MS / 1000.0

    with conn.cursor() as cur:
        print("\n[场景1] PostgreSQL: 降采样 1s GROUP BY")
        t0 = time.time()
        cur.execute(
            """
            SELECT FLOOR(step * %s) AS bucket, AVG(value), COUNT(*)
            FROM droid_data
            WHERE episode_id = 0 AND field = 'jpos' AND dim = 0
            GROUP BY 1 ORDER BY 1
            """,
            (step_interval_sec,),
        )
        rows = cur.fetchall()
        dt = (time.time() - t0) * 1000
        print(f"  GROUP BY 时间桶, {len(rows)} 桶, {dt:.1f} ms")
        results["downsample_ms"] = round(dt, 1)

        print("\n[场景2] PostgreSQL: 删除中间 50 个点后，用 generate_series + LATERAL 插值")
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
        deleted = cur.rowcount
        conn.commit()
        print(f"  已删除 {deleted} 个点")

        t0 = time.time()
        cur.execute(
            """
            WITH raw AS (
                SELECT step, value
                FROM droid_data
                WHERE episode_id = 0 AND field = 'jpos' AND dim = 0
            ),
            grid AS (
                SELECT generate_series(%s, %s) AS step
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
        )
        rows = cur.fetchall()
        dt = (time.time() - t0) * 1000
        print(f"  generate_series/LATERAL 插值, {len(rows)} 行, {dt:.1f} ms")
        results["fill_ms"] = round(dt, 1)

        print("\n[场景3] PostgreSQL: 1s 窗口 STDDEV 抖动检测")
        t0 = time.time()
        cur.execute(
            """
            SELECT FLOOR(step * %s) AS bucket, STDDEV_POP(value)
            FROM droid_data
            WHERE episode_id = 0 AND field = 'jvel' AND dim = 0
            GROUP BY 1 ORDER BY 1
            """,
            (step_interval_sec,),
        )
        rows = cur.fetchall()
        dt = (time.time() - t0) * 1000
        print(f"  GROUP BY STDDEV_POP, {len(rows)} 窗口, {dt:.1f} ms")
        results["slide_ms"] = round(dt, 1)

        print(f"\n[场景4] PostgreSQL: {len(eps)} 条轨迹跨 episode 全局 AVG")
        t0 = time.time()
        cur.execute(
            """
            SELECT field, dim, AVG(value), COUNT(*), STDDEV_POP(value)
            FROM droid_data
            WHERE field = 'jpos'
            GROUP BY field, dim
            ORDER BY field, dim
            """
        )
        rows = cur.fetchall()
        dt = (time.time() - t0) * 1000
        print(f"  GROUP BY field, dim, {len(rows)} 组, {dt:.1f} ms")
        results["global_ms"] = round(dt, 1)

    return results


def main():
    print("=" * 60)
    print("  PostgreSQL × DROID 场景对照测试")
    print(f"  轨迹: ≤{MAX_EPISODES} 条")
    print("=" * 60)

    print("\n[1/3] 加载 DROID...")
    eps = load_episodes()
    if not eps:
        raise SystemExit("ERROR: 未加载到 DROID 数据，请检查 DROID_PATH")
    npts = sum(ep["steps"] * sum(d for _, _, d in FIELDS) for ep in eps)
    print(f"  {len(eps)} 条轨迹, {npts:,} 数据点")

    print("\n[2/3] PostgreSQL 写入...")
    conn, total, t_write = postgres_write(eps)
    print(f"  {total:,} 行, {total/t_write:,.0f} rows/s, 耗时 {t_write:.1f}s")

    print("\n[3/3] PostgreSQL 场景测试")
    results = postgres_tests(conn, eps)
    conn.close()

    print("\n" + "=" * 70)
    print("  PostgreSQL 对照结果")
    print("=" * 70)
    print(f"\n{'场景':<32} {'PostgreSQL':>12}")
    print("-" * 46)
    print(f"{'写入 (rows/s)':<32} {total/t_write:>12,.0f}")
    for name, key in [
        ("降采样 GROUP BY(1s) (ms)", "downsample_ms"),
        ("插值填充 (ms)", "fill_ms"),
        ("滑动窗口 STDDEV (ms)", "slide_ms"),
        ("跨轨迹聚合 (ms)", "global_ms"),
    ]:
        print(f"{name:<32} {results[key]:>12.1f}")

    print("\n✅ 完成")


if __name__ == "__main__":
    main()
