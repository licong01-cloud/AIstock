from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras


def _load_dotenv_if_present(repo_root: Path) -> None:
    """Load .env into process environment (best-effort, no dependency on python-dotenv)."""

    env_path = repo_root / ".env"
    if not env_path.exists():
        return

    try:
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if not k:
                continue
            # Do not override existing env (match backend.main load_dotenv override=True behavior is not required here)
            os.environ.setdefault(k, v)
    except Exception:
        # Best effort.
        return


def _db_cfg() -> Dict[str, Any]:
    return {
        "host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("TDX_DB_PORT", "5432")),
        "user": os.getenv("TDX_DB_USER", "postgres"),
        "password": os.getenv("TDX_DB_PASSWORD", ""),
        "dbname": os.getenv("TDX_DB_NAME", "aistock"),
        "application_name": os.getenv("DB_DIAG_APP_NAME", "AIstock-db-diag"),
        "connect_timeout": int(os.getenv("DB_DIAG_CONNECT_TIMEOUT", "5")),
    }


def _fetchall(conn, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []
        return [dict(r) for r in rows]


SQL_ACTIVITY = """
select
  now() as ts,
  pid,
  usename,
  application_name,
  client_addr,
  state,
  wait_event_type,
  wait_event,
  now() - query_start as query_age,
  left(query, 200) as query
from pg_stat_activity
where datname = current_database()
order by query_start asc;
"""

SQL_LOCKS = """
select
  blocked.pid as blocked_pid,
  blocking.pid as blocking_pid,
  now() - blocked_activity.query_start as blocked_for,
  left(blocked_activity.query, 160) as blocked_query,
  left(blocking_activity.query, 160) as blocking_query,
  blocked_activity.wait_event_type,
  blocked_activity.wait_event
from pg_locks blocked
join pg_stat_activity blocked_activity on blocked_activity.pid = blocked.pid
join pg_locks blocking
  on blocking.locktype = blocked.locktype
 and blocking.database is not distinct from blocked.database
 and blocking.relation is not distinct from blocked.relation
 and blocking.page is not distinct from blocked.page
 and blocking.tuple is not distinct from blocked.tuple
 and blocking.virtualxid is not distinct from blocked.virtualxid
 and blocking.transactionid is not distinct from blocked.transactionid
 and blocking.classid is not distinct from blocked.classid
 and blocking.objid is not distinct from blocked.objid
 and blocking.objsubid is not distinct from blocked.objsubid
 and blocking.pid <> blocked.pid
join pg_stat_activity blocking_activity on blocking_activity.pid = blocking.pid
where not blocked.granted;
"""


def main() -> int:
    # Repo root = this file's parent (tools/) -> repo root
    repo_root = Path(__file__).resolve().parents[1]
    _load_dotenv_if_present(repo_root)

    cfg = _db_cfg()
    dsn_safe = {k: ("***" if k == "password" else v) for k, v in cfg.items()}
    print("[db-diag] connecting with:", dsn_safe)

    try:
        conn = psycopg2.connect(**cfg)
    except Exception as e:
        print("[db-diag] connect failed:", repr(e))
        return 2

    try:
        conn.autocommit = True
        print("\n[db-diag] pg_stat_activity")
        rows = _fetchall(conn, SQL_ACTIVITY)
        for r in rows:
            print(
                "pid={pid} state={state} wait={wtype}/{wevt} age={age} app={app} query={query}".format(
                    pid=r.get("pid"),
                    state=r.get("state"),
                    wtype=r.get("wait_event_type") or "-",
                    wevt=r.get("wait_event") or "-",
                    age=str(r.get("query_age")),
                    app=r.get("application_name") or "-",
                    query=(r.get("query") or "").replace("\n", " "),
                )
            )

        print("\n[db-diag] lock wait chains")
        lrows = _fetchall(conn, SQL_LOCKS)
        if not lrows:
            print("(no blocked locks)")
        else:
            for r in lrows:
                print(
                    "blocked_pid={bpid} blocking_pid={bking} blocked_for={bf} wait={wtype}/{wevt}\n  blocked={bq}\n  blocking={bkq}".format(
                        bpid=r.get("blocked_pid"),
                        bking=r.get("blocking_pid"),
                        bf=str(r.get("blocked_for")),
                        wtype=r.get("wait_event_type") or "-",
                        wevt=r.get("wait_event") or "-",
                        bq=(r.get("blocked_query") or "").replace("\n", " "),
                        bkq=(r.get("blocking_query") or "").replace("\n", " "),
                    )
                )

        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
