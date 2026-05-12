#!/usr/bin/env python3
"""MySQL 对照测试：同等 DROID 数据下，关系数据库做 TSDB 场景操作"""

import time
import glob
import numpy as np
from tfrecord import tfrecord_loader
import mysql.connector

DROID_PATH = "/mnt/huawei_nas/Datasets/DROID/1.0.0"
MAX_EPISODES = 60
TF_SHARD_COUNT = 3
STEP_INTERVAL_MS = 67

FIELDS = [
    ("jpos", "steps/observation/joint_position", 7),
    ("grip", "steps/observation/gripper_position", 1),
    ("jvel", "steps/action_dict/joint_velocity", 7),
    ("cpos", "steps/observation/cartesian_position", 6),
]

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "root123",
    "database": "droid",
    "allow_local_infile": True,
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


def mysql_setup():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS droid_data")
    cur.execute("""
        CREATE TABLE droid_data (
            episode_id INT NOT NULL,
            step INT NOT NULL,
            ts BIGINT NOT NULL,
            field VARCHAR(10) NOT NULL,
            dim INT NOT NULL,
            value DOUBLE NOT NULL,
            PRIMARY KEY (episode_id, step, field, dim),
            INDEX idx_ep_field (episode_id, field),
            INDEX idx_field_dim (field, dim),
            INDEX idx_ts (ts)
        ) ENGINE=InnoDB
    """)
    conn.commit()
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
            for s in range(n):
                ts = base - (n - s) * STEP_INTERVAL_MS
                for d in range(dims):
                    batch.append((ei, s, ts, name, d, float(arr[s, d])))
                    if len(batch) >= 5000:
                        cur.executemany(
                            "INSERT INTO droid_data VALUES (%s,%s,%s,%s,%s,%s)", batch
                        )
                        conn.commit()
                        total += len(batch)
                        batch = []
    if batch:
        cur.executemany("INSERT INTO droid_data VALUES (%s,%s,%s,%s,%s,%s)", batch)
        conn.commit()
        total += len(batch)
    t = time.time() - start
    cur.close()
    return conn, total, t


def mysql_tests(conn, eps):
    cur = conn.cursor()
    results = {}
    step_interval_sec = STEP_INTERVAL_MS / 1000.0

    # ===== 场景1: 降采样（1s 均值） =====
    print("\n[场景1] MySQL: 降采样 1s GROUP BY")
    t0 = time.time()
    cur.execute("""
        SELECT FLOOR(step * %s / 15) as bucket, AVG(value), COUNT(*)
        FROM droid_data
        WHERE episode_id = 0 AND field = 'jpos' AND dim = 0
        GROUP BY bucket ORDER BY bucket
    """, (step_interval_sec,))
    rows = cur.fetchall()
    dt = (time.time() - t0) * 1000
    print(f"  GROUP BY 子查询, {len(rows)} 桶, {dt:.1f} ms")
    results["downsample_ms"] = round(dt, 1)

    # ===== 场景2: 插值填充 =====
    print("\n[场景2] MySQL: 删除中间 50 个点后，用 LAG/LEAD 窗口函数做间距填充")
    # 先删点
    cur.execute("SELECT MIN(step), MAX(step) FROM droid_data WHERE episode_id=0 AND field='jpos' AND dim=0")
    min_s, max_s = cur.fetchone()
    mid_s = (min_s + max_s) // 2
    cur.execute("DELETE FROM droid_data WHERE episode_id=0 AND field='jpos' AND dim=0 AND step BETWEEN %s AND %s",
                (mid_s, mid_s + 49))
    conn.commit()
    deleted = cur.rowcount
    print(f"  已删除 {deleted} 个点")

    t0 = time.time()
    # LAG 拿前一个值, LEAD 拿后一个值, 线性插值
    cur.execute("""
        WITH raw AS (
            SELECT step, value,
                   LAG(step) OVER w AS prev_step,
                   LAG(value) OVER w AS prev_val,
                   LEAD(step) OVER w AS next_step,
                   LEAD(value) OVER w AS next_val
            FROM droid_data
            WHERE episode_id = 0 AND field = 'jpos' AND dim = 0
            WINDOW w AS (ORDER BY step)
        )
        SELECT step, value,
               CASE WHEN prev_val IS NOT NULL AND next_val IS NOT NULL
                    THEN prev_val + (step - prev_step) * (next_val - prev_val) / NULLIF(next_step - prev_step, 0)
                    ELSE value END AS filled_value
        FROM raw ORDER BY step
    """)
    rows = cur.fetchall()
    dt = (time.time() - t0) * 1000
    print(f"  LAG/LEAD 窗口插值, {len(rows)} 行, {dt:.1f} ms")
    results["fill_ms"] = round(dt, 1)

    # ===== 场景3: 滑动窗口抖动检测 =====
    print("\n[场景3] MySQL: 1s 窗口 STDDEV 抖动检测")
    t0 = time.time()
    cur.execute("""
        SELECT FLOOR(step * %s / 15) as bucket, STDDEV(value)
        FROM droid_data
        WHERE episode_id = 0 AND field = 'jvel' AND dim = 0
        GROUP BY bucket ORDER BY bucket
    """, (step_interval_sec,))
    rows = cur.fetchall()
    dt = (time.time() - t0) * 1000
    print(f"  GROUP BY STDDEV, {len(rows)} 窗口, {dt:.1f} ms")
    results["slide_ms"] = round(dt, 1)

    # ===== 场景4: 跨轨迹全局统计 =====
    print(f"\n[场景4] MySQL: {len(eps)} 条轨迹跨 episode 全局 AVG")
    t0 = time.time()
    cur.execute("""
        SELECT field, dim, AVG(value), COUNT(*), STDDEV(value)
        FROM droid_data
        WHERE field = 'jpos'
        GROUP BY field, dim
        ORDER BY field, dim
    """)
    rows = cur.fetchall()
    dt = (time.time() - t0) * 1000
    print(f"  GROUP BY field, dim, {len(rows)} 组, {dt:.1f} ms")
    results["global_ms"] = round(dt, 1)

    cur.close()
    return results


def main():
    print("=" * 60)
    print("  MySQL × DROID 场景对照测试")
    print(f"  轨迹: ≤{MAX_EPISODES} 条")
    print("=" * 60)

    print("\n[1/3] 加载 DROID...")
    eps = load_episodes()
    npts = sum(ep["steps"] * sum(d for _, _, d in FIELDS) for ep in eps)
    print(f"  {len(eps)} 条轨迹, {npts:,} 数据点")

    print("\n[2/3] MySQL 写入...")
    conn, total, t_write = mysql_write(eps)
    print(f"  {total:,} 行, {total/t_write:,.0f} rows/s, 耗时 {t_write:.1f}s")

    print("\n[3/3] MySQL 场景测试")
    results = mysql_tests(conn, eps)
    conn.close()

    # === 对照之前的 TSDB 结果 ===
    tsdb = {
        # 格式: (InfluxDB, IoTDB)
        "downsample": (13.9, 1.1),
        "fill":        (4.4,  1.5),
        "slide":       (59.9, 2.1),
        "global":      (17.9, 2.4),
    }

    print("\n" + "=" * 70)
    print("  四库对照")
    print("=" * 70)
    print(f"\n{'场景':<28} {'InfluxDB':>10} {'IoTDB':>10} {'MySQL':>10}")
    print("-" * 58)
    print(f"{'写入 (rows/s)':<28} {'89,605':>10} {'121,191':>10} {t_write and f'{total/t_write:,.0f}':>10}")
    for name, key in [("降采样 GROUP BY(1s) (ms)", "downsample"),
                       ("插值 FILL (ms)", "fill"),
                       ("滑动窗口 STDDEV (ms)", "slide"),
                       ("跨轨迹聚合 (ms)", "global")]:
        inf, iot = tsdb[key]
        mysql_val = results.get(f"{key}_ms", 0)
        print(f"{name:<28} {inf:>10.1f} {iot:>10.1f} {mysql_val:>10.1f}")

    print("\n✅ 完成")

if __name__ == "__main__":
    main()
