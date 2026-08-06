"""Bounded file-backed source cache for historical catalog construction."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

import orjson
import psycopg2.extras

from .canonical import canonical_json_sha256
from .models import (
    HistoricalRangeCatalogPhase,
    HistoricalRangeSourceRequirementPlanV1,
)


CACHEABLE_QUERY_IDS = frozenset(
    {
        "historical_pit_universe_existing_readonly",
        "historical_st_risk_existing_readonly",
        "historical_trading_calendar_window",
        "historical_market_history_window",
        "historical_decision_mark_daily_market",
        "historical_decision_mark_market_state",
        "historical_fundamental_moneyflow_window",
        "historical_suspend_lookup",
        "historical_industry_membership",
    }
)

_CACHE_SCHEMA_VERSION = "advisory_historical_range_catalog_source_cache_v1"
_ORJSON_EXPONENT_NUMBER = re.compile(rb"e-?\d")
_JSONB_SCHEMA_HASH = canonical_json_sha256([{"name": "payload", "type_code": 3802}])
_FUNDAMENTAL_DATASETS = (
    "daily_basic",
    "moneyflow_ts",
    "bak_basic",
    "cyq_perf",
    "sector_data",
)
_RESULT_CACHE_LIMIT = 16_384
_HASH_FRAME_BATCH_BYTES = 1024 * 1024
logger = logging.getLogger(__name__)


class CatalogSourceCacheError(RuntimeError):
    pass


class CatalogSourceContentError(CatalogSourceCacheError):
    def __init__(self, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.context = dict(context or {})


def canonical_payload_bytes(value: Any) -> bytes:
    encoded = orjson.dumps(
        value,
        option=orjson.OPT_SORT_KEYS | orjson.OPT_PASSTHROUGH_DATETIME,
        default=str,
    )
    if _ORJSON_EXPONENT_NUMBER.search(encoded) is None:
        return encoded
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        ensure_ascii=False,
    ).encode("utf-8")


def frame_payloads(payloads: Iterable[bytes], *, required_non_null: tuple[str, ...] = ()) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    framed = bytearray()
    for encoded in payloads:
        if required_non_null:
            payload = orjson.loads(encoded)
            if any(payload.get(field) is None for field in required_non_null):
                raise CatalogSourceCacheError(
                    "catalog source cache row lacks required fields: " + ", ".join(required_non_null)
                )
        framed.extend(len(encoded).to_bytes(8, "big"))
        framed.extend(encoded)
        if len(framed) >= _HASH_FRAME_BATCH_BYTES:
            digest.update(framed)
            framed.clear()
        count += 1
    if framed:
        digest.update(framed)
    return count, digest.hexdigest()


class CatalogSourceFileCache:
    """Phase-scoped immutable bulk extract used by every catalog requirement.

    PostgreSQL is read only while the cache is built. Once the atomic SQLite file
    exists, requirement resolution never reconnects to PostgreSQL.
    """

    def __init__(
        self,
        *,
        root: Path,
        plan: HistoricalRangeSourceRequirementPlanV1,
        catalog_generation: int,
        phase: HistoricalRangeCatalogPhase,
    ) -> None:
        resolved_root = root.resolve(strict=True)
        if not resolved_root.is_dir():
            raise ValueError("catalog source cache root must be an existing directory")
        self._root = resolved_root
        self._plan = plan
        self._generation = catalog_generation
        self._phase = phase
        self._result_cache: OrderedDict[
            tuple[str, tuple[tuple[str, str], ...]], tuple[int, str, str]
        ] = OrderedDict()
        self._inflight_results: dict[
            tuple[str, tuple[tuple[str, str], ...]], threading.Event
        ] = {}
        self._result_lock = threading.Lock()
        self._ensure_lock = threading.Lock()
        self._verified_observed_at: datetime | None = None
        identity = canonical_json_sha256(
            {
                "schema_version": _CACHE_SCHEMA_VERSION,
                "planning_identity_hash": plan.planning_identity_hash,
                "requirement_plan_hash": plan.requirement_plan_hash,
                "catalog_generation": catalog_generation,
                "phase": phase.value,
            }
        )
        self.path = self._root / f"{identity}.sqlite3"

    def ready_observed_at(self) -> datetime | None:
        if self._verified_observed_at is not None:
            return self._verified_observed_at
        if not self.path.exists():
            return None
        payload = self._verify()
        try:
            observed_at = datetime.fromisoformat(str(payload["observed_at"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise CatalogSourceCacheError(
                "catalog source cache observed_at is invalid"
            ) from exc
        self._verified_observed_at = observed_at
        return observed_at

    def ensure(self, *, conn: Any, observed_at: datetime) -> None:
        with self._ensure_lock:
            if self.path.exists():
                self.ready_observed_at()
                return
            temporary = self._root / f".{self.path.stem}-{uuid.uuid4().hex}.tmp"
            try:
                self._build(path=temporary, conn=conn, observed_at=observed_at)
                try:
                    os.link(temporary, self.path)
                except FileExistsError:
                    self.ready_observed_at()
                else:
                    self.ready_observed_at()
            finally:
                temporary.unlink(missing_ok=True)

    def _connect(self, path: Path | None = None, *, readonly: bool = False) -> sqlite3.Connection:
        target = path or self.path
        if readonly:
            conn = sqlite3.connect(f"file:{target.as_posix()}?mode=ro", uri=True, timeout=30)
        else:
            conn = sqlite3.connect(target, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA cache_size=-131072")
        conn.execute("PRAGMA temp_store=FILE")
        return conn

    def _verify(self) -> dict[str, Any]:
        try:
            with self._connect(readonly=True) as conn:
                row = conn.execute("SELECT payload_json FROM cache_manifest WHERE singleton=1").fetchone()
                if row is None:
                    raise CatalogSourceCacheError("catalog source cache manifest is missing")
                payload = json.loads(str(row["payload_json"]))
                expected = {
                    "schema_version": _CACHE_SCHEMA_VERSION,
                    "planning_identity_hash": self._plan.planning_identity_hash,
                    "requirement_plan_hash": self._plan.requirement_plan_hash,
                    "catalog_generation": self._generation,
                    "phase": self._phase.value,
                    "status": "COMPLETE",
                }
                if any(payload.get(key) != value for key, value in expected.items()):
                    raise CatalogSourceCacheError("catalog source cache identity differs from request")
                integrity = conn.execute("PRAGMA quick_check").fetchone()
                if integrity is None or str(integrity[0]).lower() != "ok":
                    raise CatalogSourceCacheError("catalog source cache integrity check failed")
                return payload
        except (OSError, sqlite3.DatabaseError, ValueError, TypeError) as exc:
            if isinstance(exc, CatalogSourceCacheError):
                raise
            raise CatalogSourceCacheError(f"catalog source cache readback failed: {type(exc).__name__}") from exc

    def _build(self, *, path: Path, conn: Any, observed_at: datetime) -> None:
        bounds = self._bounds()
        psycopg2.extras.register_default_jsonb(conn, loads=orjson.loads)
        sqlite = self._connect(path)
        try:
            self._create_schema(sqlite)
            extractors = self._required_extractors()
            for label, extractor in extractors:
                started_at = time.monotonic()
                logger.info(
                    "Historical catalog bulk extract started: phase=%s source=%s",
                    self._phase.value,
                    label,
                )
                extractor(conn, sqlite, bounds)
                logger.info(
                    "Historical catalog bulk extract completed: phase=%s source=%s elapsed_seconds=%.1f",
                    self._phase.value,
                    label,
                    time.monotonic() - started_at,
                )
            payload = {
                "schema_version": _CACHE_SCHEMA_VERSION,
                "planning_identity_hash": self._plan.planning_identity_hash,
                "requirement_plan_hash": self._plan.requirement_plan_hash,
                "catalog_generation": self._generation,
                "phase": self._phase.value,
                "observed_at": observed_at.isoformat(),
                "status": "COMPLETE",
            }
            sqlite.execute(
                "INSERT INTO cache_manifest(singleton,payload_json) VALUES(1,?)",
                (json.dumps(payload, sort_keys=True, separators=(",", ":")),),
            )
            sqlite.commit()
            logger.info(
                "Historical catalog source cache ready: phase=%s path=%s",
                self._phase.value,
                self.path,
            )
        except BaseException:
            sqlite.rollback()
            raise
        finally:
            sqlite.close()

    def _required_extractors(self) -> tuple[tuple[str, Any], ...]:
        query_ids = {
            requirement.query_template_id
            for requirement in self._plan.requirements
            if requirement.query_template_id in CACHEABLE_QUERY_IDS
        }
        extractors: list[tuple[str, Any]] = []
        if query_ids & {
            "historical_pit_universe_existing_readonly",
            "historical_st_risk_existing_readonly",
            "historical_market_history_window",
            "historical_decision_mark_market_state",
            "historical_fundamental_moneyflow_window",
        }:
            extractors.append(("pit", self._extract_pit))
        if "historical_trading_calendar_window" in query_ids:
            extractors.append(("calendar", self._extract_calendar))
        if query_ids & {
            "historical_market_history_window",
            "historical_decision_mark_daily_market",
        }:
            extractors.append(("market", self._extract_market))
        if "historical_decision_mark_market_state" in query_ids:
            extractors.append(("stock-basic", self._extract_stock_basic))
        if query_ids & {
            "historical_decision_mark_market_state",
            "historical_suspend_lookup",
        }:
            extractors.append(("suspend", self._extract_suspend))
        if "historical_industry_membership" in query_ids:
            extractors.append(("industry", self._extract_industry))
        if "historical_fundamental_moneyflow_window" in query_ids:
            extractors.append(("fundamentals", self._extract_fundamentals))
        return tuple(extractors)

    def _bounds(self) -> dict[str, Any]:
        cacheable = [
            item for item in self._plan.requirements if item.query_template_id in CACHEABLE_QUERY_IDS
        ]
        if not cacheable:
            raise CatalogSourceCacheError("catalog plan has no cacheable source requirements")
        parameters = [item.parameter_template for item in cacheable]
        trade_dates = [date.fromisoformat(str(item["trade_date"])) for item in parameters if item.get("trade_date")]
        starts = [
            date.fromisoformat(str(item.get("start_date") or item.get("range_start")))
            for item in parameters
            if item.get("start_date") or item.get("range_start")
        ]
        universe_keys = sorted(
            {str(item.get("universe_key") or "shsz_st_pit_active_v1") for item in parameters}
        )
        if not trade_dates:
            raise CatalogSourceCacheError("catalog plan has no trade-date source bounds")
        return {
            "source_start": min(starts or trade_dates),
            "decision_start": min(trade_dates),
            "decision_end": max(trade_dates),
            "universe_keys": universe_keys,
        }

    @staticmethod
    def _create_schema(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF;
            CREATE TABLE cache_manifest(singleton INTEGER PRIMARY KEY CHECK(singleton=1), payload_json TEXT NOT NULL);
            CREATE TABLE pit(universe_key TEXT NOT NULL, ts_code TEXT NOT NULL, eligible_start TEXT NOT NULL, eligible_end TEXT NOT NULL);
            CREATE INDEX pit_lookup ON pit(universe_key, eligible_start, eligible_end, ts_code);
            CREATE TABLE calendar(cal_date TEXT PRIMARY KEY, payload BLOB NOT NULL);
            CREATE TABLE market(trade_date TEXT NOT NULL, ts_code TEXT NOT NULL, has_price INTEGER NOT NULL, history_payload BLOB, daily_payload BLOB NOT NULL, adj_missing INTEGER NOT NULL);
            CREATE INDEX market_date_symbol ON market(trade_date,ts_code);
            CREATE INDEX market_symbol_date ON market(ts_code,trade_date);
            CREATE TABLE stock_basic(ts_code TEXT PRIMARY KEY, list_date TEXT, delist_date TEXT, list_status TEXT);
            CREATE TABLE suspend(trade_date TEXT NOT NULL, ts_code TEXT NOT NULL, suspend_type TEXT, suspend_timing TEXT, payload BLOB NOT NULL);
            CREATE INDEX suspend_date_symbol ON suspend(trade_date,ts_code);
            CREATE TABLE industry(ts_code TEXT NOT NULL, in_date TEXT NOT NULL, out_date TEXT, l3_code TEXT, payload BLOB NOT NULL);
            CREATE INDEX industry_dates ON industry(in_date,out_date,ts_code);
            CREATE TABLE fundamental(dataset_name TEXT NOT NULL, trade_date TEXT NOT NULL, ts_code TEXT NOT NULL, payload BLOB NOT NULL);
            CREATE INDEX fundamental_lookup ON fundamental(dataset_name,ts_code,trade_date);
            """
        )

    @staticmethod
    def _stream(conn: Any, sql: str, params: tuple[Any, ...]) -> Iterable[Mapping[str, Any]]:
        cursor = conn.cursor(
            name=f"ahr_bulk_cache_{uuid.uuid4().hex}",
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        try:
            cursor.itersize = 2000
            cursor.execute(sql, params)
            while True:
                rows = cursor.fetchmany(2000)
                if not rows:
                    return
                yield from rows
        finally:
            cursor.close()

    def _extract_pit(self, source: Any, target: sqlite3.Connection, bounds: Mapping[str, Any]) -> None:
        rows = self._stream(
            source,
            """SELECT universe_key,ts_code,eligible_start,eligible_end FROM market.stock_universe_pit_spans
               WHERE universe_key=ANY(%s) AND eligible_end >= %s AND eligible_start <= %s
               ORDER BY universe_key,ts_code,eligible_start""",
            (bounds["universe_keys"], bounds["decision_start"], bounds["decision_end"]),
        )
        self._insert_many(
            target,
            "INSERT INTO pit VALUES(?,?,?,?)",
            ((row["universe_key"], row["ts_code"], str(row["eligible_start"]), str(row["eligible_end"])) for row in rows),
        )

    def _extract_calendar(self, source: Any, target: sqlite3.Connection, bounds: Mapping[str, Any]) -> None:
        rows = self._stream(
            source,
            """SELECT cal_date,
                      jsonb_build_object('cal_date',cal_date,'is_trading',is_trading) AS payload
                 FROM market.trading_calendar
                WHERE cal_date BETWEEN %s AND %s ORDER BY cal_date""",
            (bounds["source_start"], bounds["decision_end"]),
        )
        self._insert_many(
            target,
            "INSERT INTO calendar VALUES(?,?)",
            ((str(row["cal_date"]), canonical_payload_bytes(row["payload"])) for row in rows),
        )

    def _extract_market(self, source: Any, target: sqlite3.Connection, bounds: Mapping[str, Any]) -> None:
        rows = self._stream(
            source,
            """SELECT COALESCE(p.trade_date,a.trade_date) trade_date,
                      COALESCE(p.ts_code,a.ts_code) ts_code,
                      p.trade_date IS NOT NULL has_price,
                      CASE WHEN p.trade_date IS NOT NULL THEN jsonb_build_object(
                          'trade_date',p.trade_date,'ts_code',p.ts_code,
                          'open_li',p.open_li,'high_li',p.high_li,'low_li',p.low_li,
                          'close_li',p.close_li,'volume_hand',p.volume_hand,
                          'amount_li',p.amount_li,'adj_factor',a.adj_factor
                      ) END AS history_payload,
                      jsonb_build_object(
                          'trade_date',COALESCE(p.trade_date,a.trade_date),
                          'ts_code',COALESCE(p.ts_code,a.ts_code),
                          'close_li',p.close_li,'adj_factor',a.adj_factor
                      ) AS daily_payload,
                      p.trade_date IS NOT NULL AND a.adj_factor IS NULL AS adj_missing
               FROM market.kline_daily_raw p FULL OUTER JOIN market.adj_factor a USING(trade_date,ts_code)
               WHERE COALESCE(p.trade_date,a.trade_date) BETWEEN %s AND %s
               ORDER BY COALESCE(p.trade_date,a.trade_date),COALESCE(p.ts_code,a.ts_code)""",
            (bounds["source_start"], bounds["decision_end"]),
        )
        def values() -> Iterable[tuple[Any, ...]]:
            for row in rows:
                history = (
                    canonical_payload_bytes(row["history_payload"])
                    if row["history_payload"] is not None
                    else None
                )
                yield (
                    str(row["trade_date"]),
                    row["ts_code"],
                    int(row["has_price"]),
                    history,
                    canonical_payload_bytes(row["daily_payload"]),
                    int(row["adj_missing"]),
                )
        self._insert_many(target, "INSERT INTO market VALUES(?,?,?,?,?,?)", values())

    def _extract_stock_basic(self, source: Any, target: sqlite3.Connection, _bounds: Mapping[str, Any]) -> None:
        rows = self._stream(source, "SELECT ts_code,list_date,delist_date,list_status FROM market.stock_basic ORDER BY ts_code", ())
        self._insert_many(target, "INSERT INTO stock_basic VALUES(?,?,?,?)", ((r["ts_code"], self._optional_date(r["list_date"]), self._optional_date(r["delist_date"]), r["list_status"]) for r in rows))

    def _extract_suspend(self, source: Any, target: sqlite3.Connection, bounds: Mapping[str, Any]) -> None:
        rows = self._stream(
            source,
            """SELECT trade_date,ts_code,suspend_type,suspend_timing,
                      jsonb_build_object(
                          'trade_date',trade_date,'ts_code',ts_code,
                          'suspend_type',suspend_type,'suspend_timing',suspend_timing
                      ) AS payload
                 FROM market.suspend_d
                WHERE trade_date BETWEEN %s AND %s
                ORDER BY trade_date,ts_code,suspend_timing NULLS FIRST""",
            (bounds["decision_start"], bounds["decision_end"]),
        )
        self._insert_many(
            target,
            "INSERT INTO suspend VALUES(?,?,?,?,?)",
            (
                (
                    str(r["trade_date"]),
                    r["ts_code"],
                    r["suspend_type"],
                    r["suspend_timing"],
                    canonical_payload_bytes(r["payload"]),
                )
                for r in rows
            ),
        )

    def _extract_industry(self, source: Any, target: sqlite3.Connection, bounds: Mapping[str, Any]) -> None:
        rows = self._stream(
            source,
            """SELECT row_data.ts_code,row_data.in_date,row_data.out_date,row_data.l3_code,
                      to_jsonb(row_data) AS payload
                 FROM (
                     SELECT ts_code,l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,in_date,out_date
                       FROM market.sw_index_member
                      WHERE in_date <= %s AND (out_date IS NULL OR out_date >= %s)
                 ) AS row_data
                ORDER BY row_data.ts_code,row_data.in_date,row_data.l3_code NULLS LAST""",
            (bounds["decision_end"], bounds["decision_start"]),
        )
        self._insert_many(
            target,
            "INSERT INTO industry VALUES(?,?,?,?,?)",
            (
                (
                    r["ts_code"],
                    str(r["in_date"]),
                    self._optional_date(r["out_date"]),
                    r["l3_code"],
                    canonical_payload_bytes(r["payload"]),
                )
                for r in rows
            ),
        )

    def _extract_fundamentals(self, source: Any, target: sqlite3.Connection, bounds: Mapping[str, Any]) -> None:
        columns = {
            "daily_basic": "close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv",
            "moneyflow_ts": "buy_sm_vol,buy_sm_amount,sell_sm_vol,sell_sm_amount,buy_md_vol,buy_md_amount,sell_md_vol,sell_md_amount,buy_lg_vol,buy_lg_amount,sell_lg_vol,sell_lg_amount,buy_elg_vol,buy_elg_amount,sell_elg_vol,sell_elg_amount,net_mf_vol,net_mf_amount",
            "bak_basic": "pe_dyn,total_assets,liquid_assets,fixed_assets,reserved,reserved_pershare,eps,bvps,undp,per_undp,rev_yoy,profit_yoy,gpr,npr,holder_num",
            "cyq_perf": "his_low,his_high,cost_5pct,cost_15pct,cost_50pct,cost_85pct,cost_95pct,weight_avg,winner_rate",
            "sector_data": "sw2_open,sw2_high,sw2_low,sw2_close,sw2_pct_change,sw2_vol,sw2_amount,sw2_pe,sw2_pb,sw2_total_mv,sw2_mf_net_amt,sw2_mf_net_vol,sw2_mf_buy_elg_amt,sw2_mf_buy_elg_vol,sw2_mf_sell_elg_amt,sw2_mf_sell_elg_vol,sw2_mf_buy_lg_amt,sw2_mf_sell_lg_amt,sw2_mf_buy_md_amt,sw2_mf_sell_md_amt,sw2_mf_buy_sm_amt,sw2_mf_sell_sm_amt",
        }
        for dataset_name, field_list in columns.items():
            sql = (
                "SELECT row_data.trade_date,row_data.ts_code,to_jsonb(row_data) AS payload "
                f"FROM (SELECT trade_date,ts_code,{field_list} FROM market.{dataset_name} "
                "WHERE trade_date BETWEEN %s AND %s) AS row_data "
                "ORDER BY row_data.trade_date,row_data.ts_code"
            )
            rows = self._stream(source, sql, (bounds["source_start"], bounds["decision_end"]))
            self._insert_many(
                target,
                "INSERT INTO fundamental VALUES(?,?,?,?)",
                (
                    (
                        dataset_name,
                        str(r["trade_date"]),
                        r["ts_code"],
                        canonical_payload_bytes(r["payload"]),
                    )
                    for r in rows
                ),
            )

    @staticmethod
    def _insert_many(conn: sqlite3.Connection, sql: str, rows: Iterable[tuple[Any, ...]]) -> None:
        batch: list[tuple[Any, ...]] = []
        for row in rows:
            batch.append(row)
            if len(batch) == 2000:
                conn.executemany(sql, batch)
                batch.clear()
        if batch:
            conn.executemany(sql, batch)

    @staticmethod
    def _optional_date(value: Any) -> str | None:
        return None if value is None else str(value)

    def retrospective_content(self, query_id: str, parameters: Mapping[str, Any]) -> tuple[int, str, str]:
        if query_id not in CACHEABLE_QUERY_IDS:
            raise CatalogSourceCacheError(f"query template is not available in bulk cache: {query_id}")
        result_key = self._result_key(query_id, parameters)
        with self._result_lock:
            cached = self._result_cache.get(result_key)
            if cached is not None:
                self._result_cache.move_to_end(result_key)
                return cached
            inflight = self._inflight_results.get(result_key)
            if inflight is None:
                inflight = threading.Event()
                self._inflight_results[result_key] = inflight
                leader = True
            else:
                leader = False
        if not leader:
            inflight.wait()
            return self.retrospective_content(query_id, parameters)
        try:
            with self._connect(readonly=True) as conn:
                resolved = self._retrospective_content_on_connection(
                    conn=conn,
                    query_id=query_id,
                    parameters=parameters,
                )
            with self._result_lock:
                existing = self._result_cache.setdefault(result_key, resolved)
                self._result_cache.move_to_end(result_key)
                while len(self._result_cache) > _RESULT_CACHE_LIMIT:
                    self._result_cache.popitem(last=False)
                return existing
        finally:
            with self._result_lock:
                completed = self._inflight_results.pop(result_key, None)
                if completed is not None:
                    completed.set()

    @staticmethod
    def _result_key(
        query_id: str,
        parameters: Mapping[str, Any],
    ) -> tuple[str, tuple[tuple[str, str], ...]]:
        fields_by_query = {
            "historical_pit_universe_existing_readonly": ("universe_key", "trade_date"),
            "historical_st_risk_existing_readonly": ("universe_key", "trade_date"),
            "historical_trading_calendar_window": ("range_start", "trade_date"),
            "historical_market_history_window": ("universe_key", "start_date", "trade_date"),
            "historical_decision_mark_daily_market": ("trade_date",),
            "historical_decision_mark_market_state": ("trade_date",),
            "historical_fundamental_moneyflow_window": (
                "universe_key",
                "start_date",
                "trade_date",
            ),
            "historical_suspend_lookup": ("trade_date",),
            "historical_industry_membership": ("trade_date",),
        }
        return (
            query_id,
            tuple(
                (
                    field,
                    str(
                        parameters.get(field)
                        or (
                            "shsz_st_pit_active_v1"
                            if field == "universe_key"
                            else ""
                        )
                    ),
                )
                for field in fields_by_query[query_id]
            ),
        )

    @staticmethod
    def _retrospective_content_on_connection(
        *,
        conn: sqlite3.Connection,
        query_id: str,
        parameters: Mapping[str, Any],
    ) -> tuple[int, str, str]:
            trade_date = str(parameters["trade_date"])
            universe_key = str(parameters.get("universe_key") or "shsz_st_pit_active_v1")
            if query_id in {"historical_pit_universe_existing_readonly", "historical_st_risk_existing_readonly"}:
                symbols = [row[0] for row in conn.execute("SELECT ts_code FROM pit WHERE universe_key=? AND eligible_start<=? AND eligible_end>=? ORDER BY ts_code", (universe_key, trade_date, trade_date))]
                if any(not str(symbol).strip() for symbol in symbols):
                    raise CatalogSourceContentError("historical PIT universe row lacks ts_code")
                if len(symbols) != len(set(symbols)):
                    raise CatalogSourceContentError(
                        "historical PIT universe contains duplicate symbols"
                    )
                return len(symbols), canonical_json_sha256(symbols), _JSONB_SCHEMA_HASH
            if query_id == "historical_trading_calendar_window":
                return (*frame_payloads(row[0] for row in conn.execute("SELECT payload FROM calendar WHERE cal_date BETWEEN ? AND ? ORDER BY cal_date", (str(parameters["range_start"]), trade_date))), _JSONB_SCHEMA_HASH)
            if query_id == "historical_market_history_window":
                rows = conn.execute("""SELECT m.history_payload,m.adj_missing FROM market m JOIN pit p ON p.ts_code=m.ts_code AND p.universe_key=? AND p.eligible_start<=? AND p.eligible_end>=? WHERE m.has_price=1 AND m.trade_date BETWEEN ? AND ? ORDER BY m.trade_date,m.ts_code""", (universe_key, trade_date, trade_date, str(parameters["start_date"]), trade_date))
                def payloads() -> Iterable[bytes]:
                    for row in rows:
                        if int(row["adj_missing"]):
                            raise CatalogSourceContentError(
                                "historical source row lacks a required database value",
                                context={"missing_fields": ["adj_factor"]},
                            )
                        yield row["history_payload"]

                return (*frame_payloads(payloads()), _JSONB_SCHEMA_HASH)
            if query_id == "historical_decision_mark_daily_market":
                return (*frame_payloads(row[0] for row in conn.execute("SELECT daily_payload FROM market WHERE trade_date=? ORDER BY ts_code", (trade_date,))), _JSONB_SCHEMA_HASH)
            if query_id == "historical_decision_mark_market_state":
                suspended = {row[0] for row in conn.execute("SELECT DISTINCT ts_code FROM suspend WHERE trade_date=? AND suspend_type='S'", (trade_date,))}
                pit = {row[0] for row in conn.execute("SELECT ts_code FROM pit WHERE universe_key='shsz_st_pit_active_v1' AND eligible_start<=? AND eligible_end>=?", (trade_date, trade_date))}
                payloads = (canonical_payload_bytes({"ts_code": row["ts_code"], "list_date": row["list_date"], "delist_date": row["delist_date"], "list_status": row["list_status"], "suspended": row["ts_code"] in suspended, "pit_eligible": row["ts_code"] in pit}) for row in conn.execute("SELECT * FROM stock_basic WHERE list_date IS NULL OR list_date<=? ORDER BY ts_code", (trade_date,)))
                return (*frame_payloads(payloads), _JSONB_SCHEMA_HASH)
            if query_id == "historical_fundamental_moneyflow_window":
                start = str(parameters["start_date"])
                total = 0
                digest = hashlib.sha256()
                schemas = []
                for dataset_name in _FUNDAMENTAL_DATASETS:
                    rows = conn.execute("""SELECT f.payload FROM fundamental f JOIN pit p ON p.ts_code=f.ts_code AND p.universe_key=? AND p.eligible_start<=? AND p.eligible_end>=? WHERE f.dataset_name=? AND f.trade_date BETWEEN ? AND ? ORDER BY f.trade_date,f.ts_code""", (universe_key, trade_date, trade_date, dataset_name, start, trade_date))
                    count, content_hash = frame_payloads(row[0] for row in rows)
                    total += count
                    marker = canonical_payload_bytes({"dataset_name": dataset_name, "content_hash": content_hash, "row_count": count})
                    digest.update(len(marker).to_bytes(8, "big"))
                    digest.update(marker)
                    schemas.append({"dataset_name": dataset_name, "schema_hash": _JSONB_SCHEMA_HASH})
                return total, digest.hexdigest(), canonical_json_sha256(schemas)
            if query_id == "historical_suspend_lookup":
                payloads = (
                    row[0]
                    for row in conn.execute(
                        """SELECT payload FROM suspend
                             WHERE trade_date=? AND suspend_type='S'
                             ORDER BY ts_code,(suspend_timing IS NOT NULL),suspend_timing""",
                        (trade_date,),
                    )
                )
                return (*frame_payloads(payloads), _JSONB_SCHEMA_HASH)
            payloads = (
                row[0]
                for row in conn.execute(
                    """SELECT payload FROM industry
                         WHERE in_date<=? AND (out_date IS NULL OR out_date>=?)
                         ORDER BY ts_code,in_date,(l3_code IS NULL),l3_code""",
                    (trade_date, trade_date),
                )
            )
            return (*frame_payloads(payloads), _JSONB_SCHEMA_HASH)


__all__ = [
    "CACHEABLE_QUERY_IDS",
    "CatalogSourceCacheError",
    "CatalogSourceContentError",
    "CatalogSourceFileCache",
    "canonical_payload_bytes",
    "frame_payloads",
]
