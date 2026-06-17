"""Small InfluxDB 3 Core HTTP helper for the benchmark scripts."""

from __future__ import annotations

import math
import os
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from typing import Iterable

import requests
from requests.adapters import HTTPAdapter


INFLUXDB3_URL = os.environ.get("INFLUXDB3_URL", "http://localhost:8181")
INFLUXDB3_TOKEN = os.environ.get(
    "INFLUXDB3_TOKEN", "apiv3_local_dev_token_for_tsdb_benchmark_20260617"
)
DEFAULT_WRITE_BATCH_SIZE = int(os.environ.get("INFLUXDB3_WRITE_BATCH_SIZE", "5000"))
DEFAULT_WRITE_WORKERS = int(os.environ.get("INFLUXDB3_WRITE_WORKERS", "1"))


def _escape_key(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def _format_field_value(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value}i"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("line protocol does not support NaN or infinity")
        return repr(value)
    text = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def line_protocol(table: str, tags: dict[str, str], fields: dict[str, object], timestamp_ms: int) -> str:
    tag_part = "".join(f",{_escape_key(k)}={_escape_key(v)}" for k, v in tags.items())
    field_part = ",".join(f"{_escape_key(k)}={_format_field_value(v)}" for k, v in fields.items())
    return f"{_escape_key(table)}{tag_part} {field_part} {timestamp_ms}"


class InfluxDB3Client:
    def __init__(self, db: str, url: str = INFLUXDB3_URL, token: str = INFLUXDB3_TOKEN):
        self.db = db
        self.url = url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}
        self._thread_local = threading.local()

    def _session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            session.trust_env = False
            pool_size = max(DEFAULT_WRITE_WORKERS * 2, 8)
            adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._thread_local.session = session
        return session

    def recreate_database(self) -> None:
        session = self._session()
        delete_resp = session.delete(
            f"{self.url}/api/v3/configure/database",
            params={"db": self.db},
            headers=self.headers,
            timeout=30,
        )
        if delete_resp.status_code not in (200, 404):
            delete_resp.raise_for_status()

        create_resp = session.post(
            f"{self.url}/api/v3/configure/database",
            json={"db": self.db},
            headers={**self.headers, "Content-Type": "application/json"},
            timeout=30,
        )
        if create_resp.status_code not in (200, 409):
            create_resp.raise_for_status()

    def write_lines(
        self,
        lines: Iterable[str],
        batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
        precision: str = "millisecond",
        workers: int = DEFAULT_WRITE_WORKERS,
    ) -> None:
        if workers <= 1:
            for batch in self._iter_batches(lines, batch_size):
                self._write_batch(batch, precision)
            return

        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending = set()
            for batch in self._iter_batches(lines, batch_size):
                pending.add(executor.submit(self._write_batch, batch, precision))
                if len(pending) >= workers * 2:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        future.result()
            for future in as_completed(pending):
                future.result()

    def _iter_batches(self, lines: Iterable[str], batch_size: int):
        batch: list[str] = []
        for line in lines:
            batch.append(line)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _write_batch(self, batch: list[str], precision: str) -> None:
        resp = self._session().post(
            f"{self.url}/api/v3/write_lp",
            params={"db": self.db, "precision": precision},
            headers={**self.headers, "Content-Type": "text/plain"},
            data="\n".join(batch),
            timeout=60,
        )
        if resp.status_code != 204:
            raise RuntimeError(f"InfluxDB 3 write failed: {resp.status_code} {resp.text[:500]}")

    def query_sql(self, query: str):
        resp = self._session().post(
            f"{self.url}/api/v3/query_sql",
            json={"db": self.db, "format": "json", "params": {}, "q": query},
            headers={**self.headers, "Content-Type": "application/json"},
            timeout=60,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"InfluxDB 3 query failed: {resp.status_code} {resp.text[:500]}\nSQL: {query}")
        return resp.json()
