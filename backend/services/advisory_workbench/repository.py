"""Bounded SQL projections over existing Advisory facts. All reads are read-only."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta

from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import capture_read_snapshot, get_conn
from backend.services.advisory_program import MARKET_PRICE_UNIT_DIVISOR
from .projection import WorkbenchError


EPISODES = """
WITH latest AS (
 SELECT DISTINCT ON (episode_id) * FROM app.advisory_episode_return
 WHERE program_id=%s ORDER BY episode_id,updated_at DESC,created_at DESC
), scoped AS (
 SELECT e.*, origin.binding_version_id AS origin_binding_id,
 CASE WHEN origin.run_type='REPLAY' THEN 'REPLAY'
      WHEN origin.forward_run_id IS NOT NULL THEN 'FORWARD' ELSE 'UNKNOWN' END AS origin_source
 FROM latest e LEFT JOIN LATERAL (
   SELECT d.binding_version_id,r.run_type,f.forward_run_id
   FROM app.advisory_daily_review d
   LEFT JOIN app.advisory_review_run r ON r.review_run_id=d.review_run_id AND r.program_id=d.program_id
   LEFT JOIN app.advisory_forward_run f ON f.review_run_id=d.review_run_id AND f.program_id=d.program_id
   WHERE d.program_id=e.program_id AND d.episode_id=e.episode_id AND d.action='ENTER'
   ORDER BY d.trade_date,d.review_run_id LIMIT 1
 ) origin ON TRUE
) """

VERSION_COLUMNS = """list_version_id,program_id,binding_version_id,review_run_id,trade_date,
previous_list_version_id,version_status,active_count,entered_count,held_count,exited_count,
waiting_count,created_at,summary_json->'advisory_date_context' AS date_context"""


class ReadRepository:
    def __init__(self, connection=None):
        self.connection = connection

    @contextmanager
    def transaction(self):
        with capture_read_snapshot() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout = '15s'")
            yield ReadRepository(connection)

    def rows(self, sql, params=()):
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, params)
                return [dict(row) for row in cursor.fetchall()]

    def programs(self, status="ENABLED"):
        where = "" if status == "ALL" else "WHERE status=%s"
        return self.rows(f"SELECT * FROM app.advisory_program {where} ORDER BY program_name,program_id",
                         () if status == "ALL" else (status,))

    def program(self, program_id):
        rows = self.rows("SELECT * FROM app.advisory_program WHERE program_id=%s", (program_id,))
        if not rows:
            raise WorkbenchError("PROGRAM_NOT_FOUND", "荐股任务不存在。", 404)
        return rows[0]

    def bindings(self, program_id):
        return self.rows("""SELECT binding_version_id,program_version,activation_status,
            effective_from_trade_date,effective_to_trade_date,package_ids,created_at
            FROM app.advisory_strategy_binding_version WHERE program_id=%s
            ORDER BY created_at DESC,binding_version_id""", (program_id,))

    def package_models(self, package_ids):
        return self.rows("""SELECT package_id,alpha_mode, ARRAY(
            SELECT DISTINCT model->>'model_type' FROM jsonb_array_elements(
              CASE jsonb_typeof(manifest_json->'model_asset')
                WHEN 'array' THEN manifest_json->'model_asset'
                WHEN 'object' THEN jsonb_build_array(manifest_json->'model_asset')
                ELSE '[]'::jsonb END) model
            WHERE model->>'model_type' IS NOT NULL ORDER BY model->>'model_type') AS model_types
            FROM strategy_pkg.package WHERE package_id=ANY(%s) ORDER BY package_id""", (package_ids,)) if package_ids else []

    def versions(self, program_id, limit=None, offset=0):
        suffix = " LIMIT %s OFFSET %s" if limit is not None else ""
        return self.rows(f"SELECT {VERSION_COLUMNS} FROM app.advisory_recommendation_list_version "
                         "WHERE program_id=%s ORDER BY trade_date DESC,created_at DESC,list_version_id" + suffix,
                         (program_id, limit, offset) if limit is not None else (program_id,))

    def items(self, program_id, version_ids):
        if not version_ids:
            return []
        return self.rows("""SELECT list_item_id,list_version_id,program_id,binding_version_id,episode_id,
            symbol,item_payload_json->>'stock_name' AS stock_name,item_state,action,previous_action,
            rank,score,previous_rank,reason_code,entry_price,exit_price,effective_trade_date,price_basis
            FROM app.advisory_recommendation_list_item WHERE program_id=%s AND list_version_id=ANY(%s)
            ORDER BY rank NULLS LAST,symbol,list_item_id""", (program_id, version_ids))

    def forward(self, program_id, target=None):
        where = " AND target_trade_date=%s" if target else ""
        return self.rows("""SELECT forward_run_id,program_id,binding_version_id,target_trade_date,
            decision_as_of_trade_date,publication_status,settlement_status,last_stage,last_reason_code,
            jsonb_build_object('context', jsonb_build_object('reason_code',
                last_error_json->'context'->>'reason_code')) AS last_error_json,
            attempt_count,updated_at,published_at FROM app.advisory_forward_run
            WHERE program_id=%s""" + where + " ORDER BY target_trade_date DESC,updated_at DESC LIMIT 1",
            (program_id, target) if target else (program_id,))

    def calendar(self, start: date, end: date):
        return self.rows("""SELECT cal_date,is_trading FROM market.trading_calendar
            WHERE cal_date BETWEEN %s AND %s ORDER BY cal_date""", (start, end + timedelta(days=30)))

    @staticmethod
    def episode_filter(filters):
        where, values = [], []
        if filters.get("source", "ALL") != "ALL":
            where.append("origin_source=%s")
            values.append(filters["source"])
        if filters.get("binding"):
            if filters["binding"] == "UNKNOWN":
                where.append("origin_binding_id IS NULL")
            else:
                where.append("origin_binding_id=%s")
                values.append(filters["binding"])
        if filters.get("status") in {"ACTIVE", "EXITED"}:
            where.append("episode_status=%s")
            values.append(filters["status"])
        if "episode_ids" in filters:
            where.append("episode_id=ANY(%s)")
            values.append(filters["episode_ids"])
        for field, op in (("start", ">="), ("end", "<=")):
            if filters.get(field):
                where.append(f"signal_date {op} %s")
                values.append(filters[field])
        if filters.get("q"):
            where.append("(symbol ILIKE %s OR episode_payload_json->>'stock_name' ILIKE %s)")
            values.extend(["%" + filters["q"] + "%"] * 2)
        for param, column in (("exit_reason", "exit_reason"), ("episode_id", "episode_id")):
            if filters.get(param):
                where.append(f"{column}=%s")
                values.append(filters[param])
        return (" WHERE " + " AND ".join(where) if where else ""), values

    def episodes(self, program_id, filters, *, limit=None, offset=0):
        where, values = self.episode_filter(filters)
        params = [program_id, *values]
        if limit is None:
            rows = self.rows(EPISODES + "SELECT * FROM scoped" + where + " ORDER BY signal_date DESC,episode_id", params)
            return rows, len(rows)
        total = self.rows(EPISODES + "SELECT count(*) AS n FROM scoped" + where, params)[0]["n"]
        rows = self.rows(EPISODES + "SELECT * FROM scoped" + where +
                         " ORDER BY signal_date DESC,episode_id LIMIT %s OFFSET %s", [*params, limit, offset])
        return rows, total

    def decisions(self, program_id, filters, *, limit=20, offset=0):
        where, params = ["program_id=%s"], [program_id]
        for field, column, op in (("start", "trade_date", ">="), ("end", "trade_date", "<="),
                                  ("episode_id", "episode_id", "=")):
            if filters.get(field):
                where.append(f"{column}{op}%s")
                params.append(filters[field])
        if filters.get("q"):
            where.append("(code ILIKE %s OR decision_input_json->>'stock_name' ILIKE %s)")
            params.extend(["%" + filters["q"] + "%"] * 2)
        condition = " AND ".join(where)
        total = self.rows(f"SELECT count(*) AS n FROM app.advisory_daily_review WHERE {condition}", params)[0]["n"]
        rows = self.rows("""SELECT episode_id,code AS symbol,trade_date,action,reason_code,review_status,
            binding_version_id,review_run_id,rank,current_price,feature_availability_ts AS observed_at,
            decision_input_json->>'stock_name' AS stock_name FROM app.advisory_daily_review WHERE """ + condition +
            " ORDER BY trade_date DESC,code,review_run_id LIMIT %s OFFSET %s", [*params, limit, offset])
        return rows, total

    def marks(self, symbols, data_date):
        if not symbols or not data_date:
            return {}
        rows = self.rows("""SELECT ts_code,trade_date,close_li/%s AS price
            FROM market.kline_daily_raw WHERE ts_code=ANY(%s) AND trade_date=%s AND close_li>0""",
            (MARKET_PRICE_UNIT_DIVISOR, symbols, data_date))
        return {r["ts_code"]: r for r in rows}

    def watermark(self, program_id, cutoff):
        """Only source facts relevant to this program; no global txid churn."""
        state = self.rows("""
            SELECT (SELECT count(*) FROM app.advisory_episode_return WHERE program_id=%s) AS episode_count,
            (SELECT max(updated_at) FROM app.advisory_episode_return WHERE program_id=%s) AS episode_at,
            (SELECT count(*) FROM app.advisory_daily_review WHERE program_id=%s) AS decision_count,
            (SELECT max(feature_availability_ts) FROM app.advisory_daily_review WHERE program_id=%s) AS decision_at,
            (SELECT max(updated_at) FROM app.advisory_forward_run WHERE program_id=%s) AS forward_at
            """, [program_id] * 5)[0]
        bounds = self.rows("""SELECT min(day) AS first_day FROM (
            SELECT min(effective_entry_date) AS day FROM app.advisory_episode_return WHERE program_id=%s
            UNION ALL SELECT min(trade_date) FROM app.advisory_recommendation_list_version WHERE program_id=%s
            UNION ALL SELECT min(coalesce(summary_json->'advisory_date_context'->>'selection_as_of_trade_date',
                summary_json->'advisory_date_context'->>'decision_as_of_trade_date')::date)
                FROM app.advisory_recommendation_list_version WHERE program_id=%s
            ) b""", (program_id, program_id, program_id))[0]
        symbols = [r["symbol"] for r in self.rows("""SELECT DISTINCT symbol FROM app.advisory_episode_return WHERE program_id=%s
            UNION SELECT DISTINCT symbol FROM app.advisory_recommendation_list_item WHERE program_id=%s""", (program_id, program_id))]
        if not symbols or not bounds["first_day"]:
            return state
        first = bounds["first_day"] - timedelta(days=1)
        state["prices"] = self.rows("""SELECT md5(string_agg(concat_ws(':',k.ts_code,k.trade_date,k.open_li,k.close_li,k.high_li,k.low_li),
            '|' ORDER BY k.ts_code,k.trade_date)) AS digest FROM market.kline_daily_raw k
            WHERE k.ts_code=ANY(%s) AND k.trade_date BETWEEN %s AND %s""", (symbols, first, cutoff))[0]["digest"]
        state["suspensions"] = self.rows("""SELECT md5(string_agg(row_to_json(s)::text,'|' ORDER BY s.ts_code,s.trade_date,s.suspend_type)) AS digest
            FROM market.suspend_d s WHERE s.ts_code=ANY(%s) AND s.trade_date BETWEEN %s AND %s""",
            (symbols, first, cutoff))[0]["digest"]
        return state
