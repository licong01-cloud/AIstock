"""Batch download daily K-line history via xtquant for all A-share stocks.

Usage (from project root):

    python -m backend.scripts.download_xtquant_daily_history

This script:
- Reads all stock codes from local data management DB (stock_basic table);
- For each code, calls xtdata.download_history_data(code, "1d");
- Aims to ensure past ~3 years of daily K-lines are cached locally on F: drive.

It is idempotent and safe to re-run; xtquant will skip already-downloaded data.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from backend.db.pg_pool import get_conn

logger = logging.getLogger(__name__)


def load_all_stock_codes() -> list[str]:
    """Load all A-share stock codes from stock_basic dataset.

    Expects a table like `basic.stock_basic` or `public.stock_basic` with a
    `stock_code` (or `ts_code`) column containing codes such as 600000.SH.
    Adjust column / schema names here if your actual schema differs.
    """
    codes: list[str] = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Try a few common layouts defensively.
            tried = False

            for schema, code_col in [
                ("basic", "stock_code"),
                ("basic", "ts_code"),
                ("public", "stock_code"),
                ("public", "ts_code"),
            ]:
                try:
                    tried = True
                    cur.execute(
                        f"SELECT {code_col} FROM {schema}.stock_basic WHERE {code_col} IS NOT NULL"
                    )
                    rows = cur.fetchall()
                    if rows:
                        codes = sorted({row[0] for row in rows if row and row[0]})
                        logger.info(
                            "Loaded %d stock codes from %s.stock_basic.%s",
                            len(codes),
                            schema,
                            code_col,
                        )
                        return codes
                except Exception:
                    continue

            if not codes:
                raise RuntimeError(
                    "Unable to load stock codes from stock_basic; please adjust schema/column in script."
                )

    return codes


def download_daily_history(codes: list[str]) -> None:
    from xtquant import xtdata  # type: ignore[import]

    total = len(codes)
    logger.info("Start downloading daily history for %d symbols via xtquant", total)

    for idx, code in enumerate(codes, 1):
        try:
            xtdata.download_history_data(code, "1d")
            if idx % 50 == 0 or idx == total:
                logger.info("progress: %d / %d (last: %s)", idx, total, code)
        except Exception as exc:
            logger.warning("download_history_data failed for %s: %s", code, exc)

    logger.info("Daily history download finished for %d symbols", total)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
    )

    logger.info("Loading stock codes from stock_basic ...")
    codes = load_all_stock_codes()
    logger.info("Loaded %d stock codes", len(codes))

    download_daily_history(codes)


if __name__ == "__main__":
    main()
