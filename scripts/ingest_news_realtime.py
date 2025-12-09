from __future__ import annotations

"""CLI entrypoint for real-time news ingestion.

This script is designed to be called by the existing TDXScheduler via the
``market.ingestion_schedules`` table with dataset="news_realtime".

It simply loads environment variables, then delegates to
``backend.ingestion.news_ingestion.run_once_for_all_sources``.
"""

import argparse
import os
import sys
import datetime as dt
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


# Ensure the AIstock project root (which contains the ``backend`` package) is
# on sys.path when this script is executed directly via an absolute path.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.db.pg_pool import get_conn
from backend.ingestion.news_ingestion import run_once_for_all_sources


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run real-time news ingestion once.")
    parser.add_argument(
        "--timeout-cls",
        type=int,
        default=30,
        help="Timeout in seconds for CLS telegraph requests (default: 30)",
    )
    # The generic ingestion runner may pass extra arguments such as
    # ``--datasets news_realtime --bulk-session-tune``. We accept and ignore
    # them so this script is compatible with that interface.
    parser.add_argument(
        "--datasets",
        nargs="*",
        help="Ignored placeholder to be compatible with generic ingestion CLI.",
    )
    parser.add_argument(
        "--bulk-session-tune",
        action="store_true",
        help="Ignored flag to be compatible with generic ingestion CLI.",
    )
    # We don't actually need the extra values, just avoid parse errors.
    args, _unknown = parser.parse_known_args()
    return args


def main(argv: list[str] | None = None) -> int:
    # Load environment so NEWS_INGEST_VERBOSE_LOG and DB config are available.
    load_dotenv(override=True)
    args = parse_args()

    inserted = run_once_for_all_sources(timeout_cls=args.timeout_cls)

    # 控制是否在 stdout 上打印每次插入条数，避免高频调度产生大量噪音日志。
    # 仅针对本脚本生效，不影响其他任务：
    # - NEWS_INGEST_VERBOSE_LOG=true  保持原有行为，打印 Inserted N
    # - NEWS_INGEST_VERBOSE_LOG=false 默认安静运行，只保留调度器自己的元信息
    verbose_flag = os.getenv("NEWS_INGEST_VERBOSE_LOG", "false").lower() == "true"
    if verbose_flag:
        print(f"[ingest_news_realtime] Inserted {inserted} news articles")

    # 每小时给出一次按来源的入库汇总信息，直接打印到 stdout，供运行日志查看。
    # 逻辑：当当前分钟数 == 0 时，对过去 60 分钟的 ingest_time 做聚合统计。
    try:
        now = dt.datetime.now(dt.timezone.utc)
        if now.minute == 0:
            window_minutes = 60
            sql = """
                SELECT source,
                       COUNT(*) AS cnt,
                       MIN(publish_time) AS min_ts,
                       MAX(publish_time) AS max_ts
                  FROM app.news_articles_ts
                 WHERE ingest_time >= NOW() - (%s || ' minutes')::interval
                 GROUP BY source
                 ORDER BY source
            """
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (window_minutes,))
                    rows = cur.fetchall()

            print(f"[news_realtime_summary] window(last_minutes)={window_minutes}")
            if not rows:
                print("[news_realtime_summary] no rows ingested in this window.")
            else:
                for source, cnt, min_ts, max_ts in rows:
                    min_str = min_ts.strftime("%Y-%m-%d %H:%M:%S") if min_ts else ""
                    max_str = max_ts.strftime("%Y-%m-%d %H:%M:%S") if max_ts else ""
                    print(
                        f"[news_realtime_summary] source={source} count={int(cnt or 0)} "
                        f"publish_time_range=[{min_str} .. {max_str}]",
                        flush=True,
                    )
    except Exception:
        # 汇总仅用于观测，不影响主流程，任何异常都静默忽略。
        pass

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
