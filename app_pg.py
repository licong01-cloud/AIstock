import os
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras as pg_extras


load_dotenv(override=True)


def _get_db_cfg() -> Dict[str, Any]:
    host = os.getenv("TDX_DB_HOST", "localhost")
    port = int(os.getenv("TDX_DB_PORT", "5432"))
    name = os.getenv("TDX_DB_NAME", "aistock")
    user = os.getenv("TDX_DB_USER", "postgres")
    password = os.getenv("TDX_DB_PASSWORD", "lc78080808")
    return {"host": host, "port": port, "dbname": name, "user": user, "password": password}


@contextmanager
def get_conn(cfg: Optional[Dict[str, Any]] = None):
    cfg = cfg or _get_db_cfg()
    conn = psycopg2.connect(**cfg)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetch_all(sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Tuple[Any, ...]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def execute(sql: str, params: Optional[Tuple[Any, ...]] = None) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount


def execute_values(sql: str, values: Iterable[Tuple[Any, ...]], page_size: int = 1000) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            pg_extras.execute_values(cur, sql, values, page_size=page_size)
