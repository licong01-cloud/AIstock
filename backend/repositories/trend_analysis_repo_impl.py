from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2.extras as pg_extras

from ..db.pg_pool import get_conn


class TrendAnalysisRepoPG:
    """PostgreSQL-backed repository for stock trend analysis results.

    This lives alongside StockAnalysisRepoPG but writes to dedicated tables:
    - app.trend_analysis_records
    - app.trend_analyst_results

    Prediction-related fields are stored as JSONB and analyst reports as TEXT,
    following the user's requirements.
    """

    # Reuse the same JSON sanitisation pattern as analysis_repo_impl
    def _sanitize_json(self, obj: Any) -> Any:
        """Best-effort conversion of numpy / special floats into JSON-safe types."""

        try:
            import numpy as np  # type: ignore

            np_available = True
        except Exception:  # noqa: BLE001
            np_available = False

        if isinstance(obj, dict):
            return {k: self._sanitize_json(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            t = [self._sanitize_json(v) for v in obj]
            return type(obj)(t) if isinstance(obj, tuple) else t

        if np_available:
            import numpy as np  # type: ignore

            if isinstance(obj, np.floating):
                if np.isnan(obj) or np.isinf(obj):  # type: ignore[attr-defined]
                    return None
                return float(obj)
            if isinstance(obj, np.integer):  # type: ignore[attr-defined]
                return int(obj)
            if isinstance(obj, np.bool_):  # type: ignore[attr-defined]
                return bool(obj)

        if isinstance(obj, float):
            # Normalise NaN/Infinity to null
            if obj != obj or obj in (float("inf"), float("-inf")):  # noqa: PLR0124
                return None
            return obj

        return obj

    def _safe_dumps(self, payload: Any) -> str:
        cleaned = self._sanitize_json(payload)
        import json

        return json.dumps(cleaned, ensure_ascii=False, allow_nan=False, default=str)

    def save_trend_analysis(
        self,
        symbol: str,
        analysis_date: datetime,
        mode: str,
        stock_info: Dict[str, Any],
        final_predictions: Any,
        prediction_evolution: Any,
        analyst_rows: List[Dict[str, Any]],
    ) -> int:
        """Persist a trend-analysis run and its per-analyst results.

        Parameters
        ----------
        symbol:
            TS code of the analysed stock.
        analysis_date:
            Logical analysis time (point-in-time of the data snapshot).
        mode:
            "realtime" or "backtest".
        stock_info:
            Basic stock info snapshot used for this run.
        final_predictions:
            JSON-serialisable structure of TrendPredictionHorizon list.
        prediction_evolution:
            JSON-serialisable structure of PredictionStep list.
        analyst_rows:
            List of dicts, each containing:
              - analyst_key: str
              - analyst_name: str
              - role: str
              - raw_text: str
              - conclusion_json: Any
              - created_at: datetime
        """

        created_at = datetime.now(timezone.utc)

        insert_record_sql = (
            "INSERT INTO app.trend_analysis_records (ts_code, analysis_date, mode, "
            "stock_info, final_predictions, prediction_evolution, created_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id"
        )

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    insert_record_sql,
                    (
                        symbol,
                        analysis_date,
                        mode,
                        pg_extras.Json(stock_info, dumps=self._safe_dumps),
                        pg_extras.Json(final_predictions, dumps=self._safe_dumps),
                        pg_extras.Json(prediction_evolution, dumps=self._safe_dumps),
                        created_at,
                    ),
                )
                record_id = int(cur.fetchone()[0])

                if analyst_rows:
                    insert_analyst_sql = (
                        "INSERT INTO app.trend_analyst_results ("  # noqa: S608
                        "record_id, analyst_key, analyst_name, role, raw_text, "
                        "conclusion_json, created_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    )
                    for row in analyst_rows:
                        cur.execute(
                            insert_analyst_sql,
                            (
                                record_id,
                                row.get("analyst_key"),
                                row.get("analyst_name"),
                                row.get("role"),
                                row.get("raw_text"),
                                pg_extras.Json(
                                    row.get("conclusion_json"),
                                    dumps=self._safe_dumps,
                                ),
                                row.get("created_at") or created_at,
                            ),
                        )

            conn.commit()

        return record_id

    def get_trend_analysis(self, record_id: int) -> Optional[Dict[str, Any]]:
        """Load a trend-analysis run and its analyst results by id.

        Returns a dict with keys:
          - record: main record row
          - analysts: list of analyst result rows
        or None if not found.
        """

        record_sql = (
            "SELECT id, ts_code, analysis_date, mode, stock_info, "
            "final_predictions, prediction_evolution, created_at "
            "FROM app.trend_analysis_records WHERE id = %s"
        )
        analysts_sql = (
            "SELECT id, record_id, analyst_key, analyst_name, role, raw_text, "
            "conclusion_json, created_at "
            "FROM app.trend_analyst_results WHERE record_id = %s "
            "ORDER BY created_at ASC, id ASC"
        )

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(record_sql, (record_id,))
                r = cur.fetchone()
                if not r:
                    return None

                record = {
                    "id": r[0],
                    "ts_code": r[1],
                    "analysis_date": r[2],
                    "mode": r[3],
                    "stock_info": r[4],
                    "final_predictions": r[5],
                    "prediction_evolution": r[6],
                    "created_at": r[7],
                }

                cur.execute(analysts_sql, (record_id,))
                analysts: List[Dict[str, Any]] = []
                for row in cur.fetchall():
                    analysts.append(
                        {
                            "id": row[0],
                            "record_id": row[1],
                            "analyst_key": row[2],
                            "analyst_name": row[3],
                            "role": row[4],
                            "raw_text": row[5],
                            "conclusion_json": row[6],
                            "created_at": row[7],
                        }
                    )

        return {"record": record, "analysts": analysts}

    def list_records(
        self,
        symbol_or_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List trend-analysis records for history views.

        Filtering is kept intentionally simple here; rating-based filtering is
        implemented in the service layer based on the JSON final_predictions.
        """

        where_clauses: list[str] = []
        params: list[Any] = []

        if symbol_or_name:
            where_clauses.append(
                "(ts_code ILIKE %s OR stock_info->>'name' ILIKE %s)"
            )
            like = f"%{symbol_or_name}%"
            params.extend([like, like])

        if start_date:
            where_clauses.append("(analysis_date::date >= %s)")
            params.append(start_date)

        if end_date:
            where_clauses.append("(analysis_date::date <= %s)")
            params.append(end_date)

        where = ""
        if where_clauses:
            where = "WHERE " + " AND ".join(where_clauses)

        sql = (
            "SELECT id, ts_code, stock_info, analysis_date, mode, "
            "final_predictions, created_at "
            "FROM app.trend_analysis_records "
            f"{where} "
            "ORDER BY created_at DESC"
        )

        items: List[Dict[str, Any]] = []

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for r in cur.fetchall():
                    stock_info = r[2] if isinstance(r[2], dict) else {}
                    items.append(
                        {
                            "id": int(r[0]),
                            "symbol": r[1] or "",
                            "stock_name": stock_info.get("name")
                            or stock_info.get("stock_name")
                            or "",
                            "analysis_date": r[3],
                            "mode": r[4] or "",
                            "final_predictions": r[5],
                            "created_at": r[6],
                        }
                    )

        return items


trend_analysis_repo = TrendAnalysisRepoPG()
