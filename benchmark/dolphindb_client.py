"""Small DolphinDB helper for the benchmark scripts."""

import os


DOLPHINDB_HOST = os.environ.get("DOLPHINDB_HOST", "127.0.0.1")
DOLPHINDB_PORT = int(os.environ.get("DOLPHINDB_PORT", "8848"))
DOLPHINDB_USER = os.environ.get("DOLPHINDB_USER", "admin")
DOLPHINDB_PASSWORD = os.environ.get("DOLPHINDB_PASSWORD", "123456")
DEFAULT_BATCH_SIZE = int(os.environ.get("DOLPHINDB_BATCH_SIZE", "5000"))

SYNTHETIC_DB = "dfs://tsdb_synthetic"
SYNTHETIC_TABLE = "sensor_data"
DROID_DB = "dfs://droid_bench"
DROID_TABLE = "droid_data"


class DolphinDBClient:
    def __init__(
        self,
        host=DOLPHINDB_HOST,
        port=DOLPHINDB_PORT,
        user=DOLPHINDB_USER,
        password=DOLPHINDB_PASSWORD,
    ):
        try:
            import dolphindb as ddb
        except ImportError as exc:
            raise RuntimeError(
                "缺少 DolphinDB Python SDK，请先安装：pip install dolphindb"
            ) from exc

        self.session = ddb.Session() if hasattr(ddb, "Session") else ddb.session()
        self.session.connect(host, port, user, password)

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass

    def run(self, script):
        return self.session.run(script)

    def recreate_synthetic_table(self):
        self.run(
            f"""
            if(existsDatabase("{SYNTHETIC_DB}")){{
                dropDatabase("{SYNTHETIC_DB}")
            }}
            schema = table(1:0, `device_id`sensor_id`ts`value, [INT, INT, LONG, DOUBLE])
            db = database("{SYNTHETIC_DB}", HASH, [INT, 16])
            db.createPartitionedTable(schema, `{SYNTHETIC_TABLE}, `device_id)
            """
        )

    def recreate_droid_table(self):
        self.run(
            f"""
            if(existsDatabase("{DROID_DB}")){{
                dropDatabase("{DROID_DB}")
            }}
            schema = table(1:0, `episode_id`step`ts`field`dim`value, [INT, INT, LONG, SYMBOL, INT, DOUBLE])
            db = database("{DROID_DB}", HASH, [INT, 16])
            db.createPartitionedTable(schema, `{DROID_TABLE}, `episode_id)
            """
        )

    def append_rows(self, db_path, table_name, columns, rows, batch_size=DEFAULT_BATCH_SIZE):
        import pandas as pd

        total = 0
        batch = []
        for row in rows:
            batch.append(row)
            if len(batch) >= batch_size:
                total += self._append_dataframe(db_path, table_name, pd.DataFrame(batch, columns=columns))
                batch = []
        if batch:
            total += self._append_dataframe(db_path, table_name, pd.DataFrame(batch, columns=columns))
        return total

    def _append_dataframe(self, db_path, table_name, df):
        self.session.upload({"batch_df": df})
        self.run(f'tableInsert(loadTable("{db_path}", "{table_name}"), batch_df)')
        return len(df)
