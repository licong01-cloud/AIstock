from __future__ import annotations

import base64
import hashlib
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import date
from typing import Any

import psycopg2.extras

from backend.services.advisory_historical_range.canonical import canonical_json_sha256

from .dataset_spool import RerankerDatasetSpool
from .errors import (
    AdvisoryModelingError,
    REASON_FEATURE_CLOSURE_INCOMPLETE,
    REASON_FEATURE_QUERY_REGISTRY_MISMATCH,
)
from .feature_schema import (
    FeatureQueryTemplateV1,
    FrozenFeatureQueryRegistryV1,
)
from .feature_snapshot import FeatureSourceRevisionV1
from .identity import validated_hash


ConnFactory = Callable[[], Any]


_QUERY_DEFINITIONS: tuple[
    tuple[str, str, Mapping[str, str], tuple[tuple[str, str], ...]], ...
] = (
    (
        "historical_pit_universe_existing_readonly",
        """
        SELECT cal.cal_date AS trade_date, span.ts_code
        FROM market.trading_calendar AS cal
        JOIN market.stock_universe_pit_spans AS span
          ON span.universe_key = %(universe_key)s
         AND span.eligible_start <= cal.cal_date
         AND span.eligible_end >= cal.cal_date
        WHERE cal.is_trading = TRUE
          AND cal.cal_date BETWEEN %(start_date)s AND %(end_date)s
        ORDER BY cal.cal_date, span.ts_code
        """,
        {"universe_key": "string", "start_date": "date", "end_date": "date"},
        (("trade_date", "date"), ("ts_code", "string")),
    ),
    (
        "historical_trading_calendar_window",
        """
        SELECT cal_date, is_trading
        FROM market.trading_calendar
        WHERE cal_date BETWEEN %(start_date)s AND %(end_date)s
        ORDER BY cal_date
        """,
        {"start_date": "date", "end_date": "date"},
        (("cal_date", "date"), ("is_trading", "bool")),
    ),
    (
        "historical_market_history_window",
        """
        SELECT price.trade_date, price.ts_code, price.open_li, price.high_li,
               price.low_li, price.close_li, price.volume_hand, price.amount_li,
               adj.adj_factor
        FROM market.kline_daily_raw AS price
        JOIN market.stock_universe_pit_spans AS span
          ON span.ts_code = price.ts_code
         AND span.universe_key = %(universe_key)s
         AND span.eligible_start <= price.trade_date
         AND span.eligible_end >= price.trade_date
        LEFT JOIN market.adj_factor AS adj
          ON adj.ts_code = price.ts_code AND adj.trade_date = price.trade_date
        WHERE price.trade_date BETWEEN %(start_date)s AND %(end_date)s
        ORDER BY price.trade_date, price.ts_code
        """,
        {"universe_key": "string", "start_date": "date", "end_date": "date"},
        (
            ("trade_date", "date"), ("ts_code", "string"), ("open_li", "decimal"),
            ("high_li", "decimal"), ("low_li", "decimal"), ("close_li", "decimal"),
            ("volume_hand", "decimal"), ("amount_li", "decimal"),
            ("adj_factor", "decimal"),
        ),
    ),
    (
        "historical_decision_mark_daily_market",
        """
        SELECT price.trade_date, price.ts_code, price.close_li, adj.adj_factor,
               limits.pre_close, limits.up_limit, limits.down_limit
        FROM market.kline_daily_raw AS price
        JOIN market.stock_universe_pit_spans AS span
          ON span.ts_code = price.ts_code
         AND span.universe_key = %(universe_key)s
         AND span.eligible_start <= price.trade_date
         AND span.eligible_end >= price.trade_date
        LEFT JOIN market.adj_factor AS adj
          ON adj.ts_code = price.ts_code AND adj.trade_date = price.trade_date
        LEFT JOIN market.stk_limit AS limits
          ON limits.ts_code = price.ts_code AND limits.trade_date = price.trade_date
        WHERE price.trade_date BETWEEN %(start_date)s AND %(end_date)s
        ORDER BY price.trade_date, price.ts_code
        """,
        {"universe_key": "string", "start_date": "date", "end_date": "date"},
        (
            ("trade_date", "date"), ("ts_code", "string"), ("close_li", "decimal"),
            ("adj_factor", "decimal"), ("pre_close", "decimal"),
            ("up_limit", "decimal"), ("down_limit", "decimal"),
        ),
    ),
    (
        "historical_decision_mark_market_state",
        """
        SELECT cal.cal_date AS trade_date, span.ts_code,
               (suspended.ts_code IS NOT NULL) AS suspended,
               basic.list_date, basic.delist_date,
               CASE
                 WHEN basic.list_date IS NULL OR basic.list_date > cal.cal_date THEN 'NOT_LISTED'
                 WHEN basic.delist_date IS NOT NULL AND basic.delist_date <= cal.cal_date THEN 'DELISTED'
                 ELSE 'LISTED'
               END AS list_status
        FROM market.trading_calendar AS cal
        JOIN market.stock_universe_pit_spans AS span
          ON span.universe_key = %(universe_key)s
         AND span.eligible_start <= cal.cal_date
         AND span.eligible_end >= cal.cal_date
        LEFT JOIN market.stock_basic AS basic ON basic.ts_code = span.ts_code
        LEFT JOIN (
            SELECT DISTINCT trade_date, ts_code
            FROM market.suspend_d
            WHERE suspend_type = 'S'
              AND trade_date BETWEEN %(start_date)s AND %(end_date)s
        ) AS suspended
          ON suspended.trade_date = cal.cal_date AND suspended.ts_code = span.ts_code
        WHERE cal.is_trading = TRUE
          AND cal.cal_date BETWEEN %(start_date)s AND %(end_date)s
        ORDER BY cal.cal_date, span.ts_code
        """,
        {"universe_key": "string", "start_date": "date", "end_date": "date"},
        (
            ("trade_date", "date"), ("ts_code", "string"), ("suspended", "bool"),
            ("list_date", "date"), ("delist_date", "date"), ("list_status", "string"),
        ),
    ),
    (
        "historical_fundamental_moneyflow_window",
        """
        SELECT basic.trade_date, basic.ts_code, basic.turnover_rate,
               basic.turnover_rate_f, flow.net_mf_amount
        FROM market.daily_basic AS basic
        JOIN market.stock_universe_pit_spans AS span
          ON span.ts_code = basic.ts_code
         AND span.universe_key = %(universe_key)s
         AND span.eligible_start <= basic.trade_date
         AND span.eligible_end >= basic.trade_date
        LEFT JOIN market.moneyflow_ts AS flow
          ON flow.ts_code = basic.ts_code AND flow.trade_date = basic.trade_date
        WHERE basic.trade_date BETWEEN %(start_date)s AND %(end_date)s
        ORDER BY basic.trade_date, basic.ts_code
        """,
        {"universe_key": "string", "start_date": "date", "end_date": "date"},
        (
            ("trade_date", "date"), ("ts_code", "string"),
            ("turnover_rate", "decimal"), ("turnover_rate_f", "decimal"),
            ("net_mf_amount", "decimal"),
        ),
    ),
    (
        "historical_suspend_lookup",
        """
        SELECT DISTINCT trade_date, ts_code, suspend_type
        FROM market.suspend_d
        WHERE suspend_type = 'S'
          AND trade_date BETWEEN %(start_date)s AND %(end_date)s
        ORDER BY trade_date, ts_code
        """,
        {"start_date": "date", "end_date": "date"},
        (("trade_date", "date"), ("ts_code", "string"), ("suspend_type", "string")),
    ),
    (
        "historical_industry_membership",
        """
        SELECT ts_code, COALESCE(l1_code, '') AS l1_code,
               COALESCE(l2_code, '') AS l2_code, COALESCE(l3_code, '') AS l3_code,
               in_date, out_date
        FROM market.sw_index_member
        WHERE in_date <= %(end_date)s
          AND (out_date IS NULL OR out_date >= %(start_date)s)
        ORDER BY ts_code, in_date, l2_code, l3_code
        """,
        {"start_date": "date", "end_date": "date"},
        (
            ("ts_code", "string"), ("l1_code", "string"), ("l2_code", "string"),
            ("l3_code", "string"), ("in_date", "date"), ("out_date", "date"),
        ),
    ),
)


def frozen_feature_query_registry_v1(*, repository_commit: str) -> FrozenFeatureQueryRegistryV1:
    templates: list[FeatureQueryTemplateV1] = []
    for query_id, sql, parameters, result_schema in _QUERY_DEFINITIONS:
        sql_bytes = sql.strip().encode("utf-8")
        templates.append(
            FeatureQueryTemplateV1(
                query_template_id=query_id,
                template_version="batch-b-range-v1",
                sql_bytes_base64=base64.b64encode(sql_bytes).decode("ascii"),
                sql_bytes_sha256=hashlib.sha256(sql_bytes).hexdigest(),
                parameter_schema=dict(parameters),
                result_schema=result_schema,
                repository_commit=repository_commit,
            )
        )
    return FrozenFeatureQueryRegistryV1(
        templates=tuple(templates),
        source_repository_commit=repository_commit,
    )


_IDENTITIES: dict[str, tuple[str, ...]] = {
    "historical_pit_universe_existing_readonly": ("trade_date", "ts_code"),
    "historical_trading_calendar_window": ("cal_date",),
    "historical_market_history_window": ("trade_date", "ts_code"),
    "historical_decision_mark_daily_market": ("trade_date", "ts_code"),
    "historical_decision_mark_market_state": ("trade_date", "ts_code"),
    "historical_fundamental_moneyflow_window": ("trade_date", "ts_code"),
    "historical_suspend_lookup": ("trade_date", "ts_code"),
    "historical_industry_membership": ("ts_code", "in_date", "l2_code", "l3_code"),
}


def _year_partitions(start: date, end: date) -> tuple[tuple[date, date], ...]:
    values: list[tuple[date, date]] = []
    year = start.year
    while year <= end.year:
        values.append((max(start, date(year, 1, 1)), min(end, date(year, 12, 31))))
        year += 1
    return tuple(values)


class PostgresFeatureSourceReader:
    """Stream frozen range-query rows into the Batch B spool using read-only transactions."""

    def __init__(
        self,
        *,
        conn_factory: ConnFactory,
        configured_host_hash: str,
        configured_port: int,
        configured_database_hash: str,
        configured_user_hash: str,
        statement_timeout_ms: int = 300_000,
    ) -> None:
        if conn_factory is None:
            raise ValueError("conn_factory is required")
        if statement_timeout_ms <= 0:
            raise ValueError("statement_timeout_ms must be positive")
        if configured_port <= 0 or configured_port > 65535:
            raise ValueError("configured_port must be a valid TCP port")
        self._conn_factory = conn_factory
        self._configured_host_hash = str(
            validated_hash(configured_host_hash, field_name="configured_host_hash")
        )
        self._configured_port = configured_port
        self._configured_database_hash = str(
            validated_hash(configured_database_hash, field_name="configured_database_hash")
        )
        self._configured_user_hash = str(
            validated_hash(configured_user_hash, field_name="configured_user_hash")
        )
        self._statement_timeout_ms = statement_timeout_ms

    def capture(
        self,
        *,
        registry: FrozenFeatureQueryRegistryV1,
        request_semantic_hash: str,
        start_date: date,
        end_date: date,
        spool: RerankerDatasetSpool,
        universe_key: str = "shsz_st_pit_active_v1",
    ) -> tuple[FeatureSourceRevisionV1, ...]:
        if start_date > end_date:
            raise ValueError("feature source range is reversed")
        revisions: list[FeatureSourceRevisionV1] = []
        for template in registry.templates:
            partitions = (
                ((start_date, end_date),)
                if template.query_template_id in {
                    "historical_trading_calendar_window",
                    "historical_industry_membership",
                }
                else _year_partitions(start_date, end_date)
            )
            for partition_start, partition_end in partitions:
                parameters = {
                    "start_date": partition_start,
                    "end_date": partition_end,
                    **({"universe_key": universe_key} if "universe_key" in template.parameter_schema else {}),
                }
                partition_key = f"{partition_start.isoformat()}..{partition_end.isoformat()}"
                with self._execute(template=template, parameters=parameters) as (
                    database_target_hash,
                    rows,
                ):
                    count, partition_hash = spool.append_partition(
                        source_kind="FEATURE_SOURCE",
                        source_identity=request_semantic_hash,
                        logical_role=template.query_template_id,
                        partition_key=partition_key,
                        rows=rows,
                        identity_fields=_IDENTITIES[template.query_template_id],
                        trade_date_field=(
                            "cal_date"
                            if template.query_template_id == "historical_trading_calendar_window"
                            else None
                            if template.query_template_id == "historical_industry_membership"
                            else "trade_date"
                        ),
                        symbol_field=(
                            None
                            if template.query_template_id == "historical_trading_calendar_window"
                            else "ts_code"
                        ),
                    )
                revisions.append(
                    FeatureSourceRevisionV1(
                        query_template_id=template.query_template_id,
                        query_template_hash=str(template.template_hash),
                        bound_parameter_hash=canonical_json_sha256(parameters),
                        partition_key=partition_key,
                        partition_hash=partition_hash,
                        business_min_date=partition_start,
                        business_max_date=partition_end,
                        result_schema_hash=canonical_json_sha256(template.result_schema),
                        cutoff_predicate_hash=canonical_json_sha256(
                            {
                                "query_template_hash": template.template_hash,
                                "business_max_date": partition_end,
                                "predicate": "row_business_date <= bound_end_date",
                            }
                        ),
                        database_target_hash=database_target_hash,
                        row_count=count,
                    )
                )
        if len({item.database_target_hash for item in revisions}) != 1:
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "feature source database target changed across frozen partitions",
            )
        return tuple(revisions)

    @contextmanager
    def _execute(
        self,
        *,
        template: FeatureQueryTemplateV1,
        parameters: Mapping[str, Any],
    ) -> Iterator[tuple[str, Iterator[Mapping[str, Any]]]]:
        sql = base64.b64decode(template.sql_bytes_base64).decode("utf-8")
        conn = self._conn_factory()
        cursor: Any | None = None
        try:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("SET LOCAL statement_timeout = %s", (self._statement_timeout_ms,))
            cursor.execute(
                """
                SELECT current_database() AS current_database,
                       current_user AS current_user,
                       inet_server_port() AS server_port,
                       current_setting('server_version_num')::integer AS server_version_num,
                       current_setting('transaction_read_only') AS transaction_read_only
                """
            )
            identity = cursor.fetchone()
            actual_database_hash = hashlib.sha256(
                str(identity["current_database"]).encode("utf-8")
            ).hexdigest()
            actual_user_hash = hashlib.sha256(
                str(identity["current_user"]).encode("utf-8")
            ).hexdigest()
            if (
                actual_database_hash != self._configured_database_hash
                or actual_user_hash != self._configured_user_hash
                or str(identity["transaction_read_only"]).lower() not in {"on", "true"}
            ):
                raise AdvisoryModelingError(
                    REASON_FEATURE_CLOSURE_INCOMPLETE,
                    "feature source database identity differs from explicit configuration",
                )
            database_target_hash = canonical_json_sha256(
                {
                    "configured_host_hash": self._configured_host_hash,
                    "configured_port": self._configured_port,
                    "database_hash": actual_database_hash,
                    "user_hash": actual_user_hash,
                    "server_port": int(identity["server_port"]),
                    "server_version_num": int(identity["server_version_num"]),
                    "transaction_read_only": True,
                }
            )
            cursor.execute(sql, dict(parameters))
            actual_columns = tuple(str(column.name) for column in cursor.description)
            expected_columns = tuple(name for name, _dtype in template.result_schema)
            if actual_columns != expected_columns:
                raise AdvisoryModelingError(
                    REASON_FEATURE_QUERY_REGISTRY_MISMATCH,
                    "feature query result columns differ from frozen schema",
                    context={"query_template_id": template.query_template_id},
                )

            def rows() -> Iterator[Mapping[str, Any]]:
                while True:
                    chunk = cursor.fetchmany(2000)
                    if not chunk:
                        break
                    for row in chunk:
                        yield dict(row)

            yield database_target_hash, rows()
            conn.rollback()
        except Exception as exc:
            conn.rollback()
            if isinstance(exc, AdvisoryModelingError):
                raise
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "configured feature source query failed",
                context={
                    "query_template_id": template.query_template_id,
                    "error_type": type(exc).__name__,
                },
            ) from exc
        finally:
            if cursor is not None:
                cursor.close()
            conn.close()
