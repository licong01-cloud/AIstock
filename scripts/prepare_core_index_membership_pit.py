"""Prepare, validate, and render core-index PIT stock universes.

The initial authority file is a repo-external JSON array whose rows carry
official effective dates.  Tushare ``index_weight`` is optional cross-check
input only; its monthly snapshot date never becomes a membership boundary.
"""

from __future__ import annotations

import argparse
import calendar as month_calendar
import contextlib
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras as pgx
from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.core_index_membership import (  # noqa: E402
    P0_POOL_IDS,
    POOL_DEFINITIONS,
    CoreIndexMembershipRepository,
    UniverseMode,
    UniverseSelection,
    resolve_universe,
)
from backend.services.dataset_release.index_pool_sidecar import write_sidecar  # noqa: E402


SCHEMA_VERSION = "core_index_membership_operator_v1"
MIGRATION_NAME = "core_index_membership_pit_20260904.sql"
_TARGETS = {"dev", "production"}
_MODES = {
    "preflight",
    "migrate",
    "migration-readback",
    "plan",
    "apply",
    "readback",
    "full-validate",
    "render",
}
_AUTHORITY_FIELDS = {
    "pool_id",
    "index_code",
    "ts_code",
    "effective_from",
    "effective_to_exclusive",
    "source_provider",
    "source_reference",
}


class CoreIndexMembershipOperatorError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    target: str
    host: str
    port: int
    user: str
    password: str
    dbname: str
    credential_location: str


@dataclass(frozen=True, slots=True)
class AuthorityRow:
    pool_id: str
    index_code: str
    ts_code: str
    effective_from: date
    effective_to_exclusive: date | None
    source_provider: str
    source_reference: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AuthorityRow":
        unknown = set(value) - _AUTHORITY_FIELDS
        missing = _AUTHORITY_FIELDS - set(value)
        if unknown or missing:
            raise CoreIndexMembershipOperatorError(
                f"authority row fields differ: missing={sorted(missing)} unknown={sorted(unknown)}"
            )
        pool_id = str(value["pool_id"]).strip().lower()
        definition = POOL_DEFINITIONS.get(pool_id)
        if definition is None:
            raise CoreIndexMembershipOperatorError(f"unknown pool_id: {pool_id!r}")
        index_code = str(value["index_code"]).strip().upper()
        provider = str(value["source_provider"]).strip().upper()
        if index_code != definition.index_code or provider != definition.source_provider:
            raise CoreIndexMembershipOperatorError(f"pool catalog mismatch for {pool_id}: {index_code}/{provider}")
        ts_code = str(value["ts_code"]).strip().upper()
        if len(ts_code) != 9 or ts_code[6:] not in {".SH", ".SZ"} or not ts_code[:6].isdigit():
            raise CoreIndexMembershipOperatorError(f"invalid A-share ts_code: {ts_code!r}")
        start = _parse_date(value["effective_from"], "effective_from")
        raw_end = value["effective_to_exclusive"]
        end = None if raw_end in {None, ""} else _parse_date(raw_end, "effective_to_exclusive")
        if end is not None and end <= start:
            raise CoreIndexMembershipOperatorError(
                f"effective_to_exclusive must follow effective_from for {pool_id}/{ts_code}"
            )
        reference = str(value["source_reference"]).strip()
        if not reference:
            raise CoreIndexMembershipOperatorError(f"source_reference is empty for {pool_id}/{ts_code}")
        return cls(pool_id, index_code, ts_code, start, end, provider, reference)

    def database_tuple(self) -> tuple[Any, ...]:
        return (
            self.pool_id,
            self.index_code,
            self.ts_code,
            self.effective_from,
            self.effective_to_exclusive,
            self.source_provider,
            self.source_reference,
        )


def _parse_date(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise CoreIndexMembershipOperatorError(f"{field} is not an ISO date") from exc


def load_authority_rows(path: Path) -> tuple[AuthorityRow, ...]:
    resolved = path.resolve(strict=True)
    try:
        resolved.relative_to(REPO_ROOT.resolve())
    except ValueError:
        pass
    else:
        raise CoreIndexMembershipOperatorError("authority file must be repo-external")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CoreIndexMembershipOperatorError("authority file is unreadable or invalid JSON") from exc
    if not isinstance(payload, list) or not payload:
        raise CoreIndexMembershipOperatorError("authority file must contain a non-empty JSON array")
    rows = tuple(AuthorityRow.from_mapping(row) for row in payload if isinstance(row, Mapping))
    if len(rows) != len(payload):
        raise CoreIndexMembershipOperatorError("every authority entry must be a JSON object")
    _validate_authority_rows(rows)
    return rows


def _validate_authority_rows(rows: Sequence[AuthorityRow]) -> None:
    identities: set[tuple[str, str, date]] = set()
    by_symbol: dict[tuple[str, str], list[AuthorityRow]] = {}
    for row in rows:
        identity = (row.pool_id, row.ts_code, row.effective_from)
        if identity in identities:
            raise CoreIndexMembershipOperatorError(f"duplicate authority identity: {identity}")
        identities.add(identity)
        by_symbol.setdefault((row.pool_id, row.ts_code), []).append(row)
    for identity, values in by_symbol.items():
        prior_end: date | None = None
        for index, row in enumerate(sorted(values, key=lambda item: item.effective_from)):
            if index > 0 and prior_end is None:
                raise CoreIndexMembershipOperatorError(f"open interval is not terminal for {identity}")
            if prior_end is not None and row.effective_from < prior_end:
                raise CoreIndexMembershipOperatorError(f"overlapping authority intervals for {identity}")
            prior_end = row.effective_to_exclusive


def _load_database_config(target: str, env_file: Path) -> DatabaseConfig:
    if target not in _TARGETS:
        raise CoreIndexMembershipOperatorError("database target must be dev or production")
    resolved = env_file.resolve(strict=True)
    values = dotenv_values(resolved)
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    keys = {
        name: f"{prefix}{suffix}"
        for name, suffix in {
            "host": "HOST",
            "port": "PORT",
            "user": "USER",
            "password": "PASSWORD",
            "dbname": "NAME",
        }.items()
    }
    missing = [name for name, key in keys.items() if not str(values.get(key) or "").strip()]
    if missing:
        raise CoreIndexMembershipOperatorError(f"{target} database config is missing: {missing}")
    config = DatabaseConfig(
        target=target,
        host=str(values[keys["host"]]).strip(),
        port=int(str(values[keys["port"]])),
        user=str(values[keys["user"]]).strip(),
        password=str(values[keys["password"]]),
        dbname=str(values[keys["dbname"]]).strip(),
        credential_location=str(resolved),
    )
    if target == "dev" and "dev" not in config.dbname.lower():
        raise CoreIndexMembershipOperatorError("TDX_DB_DEV_NAME must identify the existing DEV database")
    return config


@contextlib.contextmanager
def database_connection(config: DatabaseConfig, *, autocommit: bool = False) -> Iterator[Any]:
    conn = psycopg2.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=config.dbname,
    )
    conn.autocommit = autocommit
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def _connection_factory(config: DatabaseConfig) -> Callable[[], Any]:
    return lambda: database_connection(config)


def readback_summary(conn: Any, pool_ids: Sequence[str]) -> dict[str, Any]:
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute("SELECT current_database() AS database_name, current_user AS database_user")
        identity = dict(cur.fetchone())
        cur.execute(
            """
            SELECT pool_id, index_code, source_provider,
                   MIN(effective_from) AS first_effective_from,
                   MAX(effective_to_exclusive) AS latest_closed_boundary,
                   COUNT(*)::BIGINT AS interval_count,
                   COUNT(DISTINCT ts_code)::BIGINT AS symbol_count,
                   COUNT(*) FILTER (WHERE effective_to_exclusive IS NULL)::BIGINT AS current_count,
                   MAX(updated_at) AS revision
              FROM market.core_index_membership_pit
             WHERE pool_id = ANY(%s)
             GROUP BY pool_id, index_code, source_provider
             ORDER BY pool_id
            """,
            (list(pool_ids),),
        )
        rows = [dict(row) for row in cur.fetchall()]
    return {
        "database_name": str(identity["database_name"]),
        "database_user": str(identity["database_user"]),
        "pools": [_jsonable(row) for row in rows],
        "pool_count": len(rows),
        "interval_count": sum(int(row["interval_count"]) for row in rows),
    }


def database_preflight(conn: Any) -> dict[str, Any]:
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT current_database() AS database_name,
                   current_user AS database_user,
                   inet_server_addr()::TEXT AS server_address,
                   inet_server_port() AS server_port,
                   to_regclass('market.stock_universe_pit_spans')::TEXT AS canonical_pit_table,
                   to_regclass('market.kline_daily_raw')::TEXT AS daily_table,
                   to_regclass('market.core_index_membership_pit')::TEXT AS membership_table
            """
        )
        value = dict(cur.fetchone())
    value["dependencies_ready"] = bool(value["canonical_pit_table"] and value["daily_table"])
    return _jsonable(value)


def apply_migration(conn: Any) -> None:
    migration_root = REPO_ROOT / "backend" / "migrations"
    preflight = (migration_root / "core_index_membership_pit_20260904.preflight.sql").read_text(encoding="utf-8")
    migration = (migration_root / MIGRATION_NAME).read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(preflight)
        cur.execute(migration)


def migration_readback(conn: Any) -> dict[str, Any]:
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
              FROM information_schema.columns
             WHERE table_schema = 'market'
               AND table_name = 'core_index_membership_pit'
             ORDER BY ordinal_position
            """
        )
        columns = [dict(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT COUNT(*)::INTEGER AS constraint_count
              FROM pg_constraint
             WHERE conrelid = 'market.core_index_membership_pit'::regclass
            """
        )
        constraint_count = int(cur.fetchone()["constraint_count"])
        cur.execute(
            """
            SELECT COUNT(*)::INTEGER AS trigger_count
              FROM pg_trigger
             WHERE tgrelid = 'market.core_index_membership_pit'::regclass
               AND tgname = 'trg_validate_core_index_membership_pit'
               AND NOT tgisinternal
            """
        )
        trigger_count = int(cur.fetchone()["trigger_count"])
        cur.execute(
            """
            SELECT obj_description('market.core_index_membership_pit'::regclass) AS table_comment
            """
        )
        table_comment = str(cur.fetchone()["table_comment"] or "")
    expected_columns = [
        "pool_id",
        "index_code",
        "ts_code",
        "effective_from",
        "effective_to_exclusive",
        "source_provider",
        "source_reference",
        "updated_at",
    ]
    return {
        "status": (
            "PASS"
            if [row["column_name"] for row in columns] == expected_columns
            and constraint_count >= 5
            and trigger_count == 1
            and bool(table_comment)
            else "BLOCKED"
        ),
        "columns": columns,
        "constraint_count": constraint_count,
        "trigger_count": trigger_count,
        "table_comment": table_comment,
    }


def migration_constraint_smoke(conn: Any) -> dict[str, Any]:
    """Exercise the DEV constraints inside one transaction that is rolled back."""

    overlap_rejected = False
    catalog_mismatch_rejected = False
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM market.core_index_membership_pit")
        before_count = int(cur.fetchone()[0])
        cur.execute("SAVEPOINT core_index_smoke_all")
        cur.execute(
            """
            INSERT INTO market.core_index_membership_pit (
                pool_id, index_code, ts_code, effective_from,
                effective_to_exclusive, source_provider, source_reference
            ) VALUES ('csi300', '000300.SH', '999999.SZ', '2024-01-02', '2024-02-01', 'CSI', 'dev-smoke')
            """
        )
        cur.execute("SAVEPOINT core_index_smoke_overlap")
        try:
            cur.execute(
                """
                INSERT INTO market.core_index_membership_pit (
                    pool_id, index_code, ts_code, effective_from,
                    effective_to_exclusive, source_provider, source_reference
                ) VALUES ('csi300', '000300.SH', '999999.SZ', '2024-01-15', NULL, 'CSI', 'dev-smoke')
                """
            )
        except psycopg2.Error:
            overlap_rejected = True
            cur.execute("ROLLBACK TO SAVEPOINT core_index_smoke_overlap")
        cur.execute("SAVEPOINT core_index_smoke_catalog")
        try:
            cur.execute(
                """
                INSERT INTO market.core_index_membership_pit (
                    pool_id, index_code, ts_code, effective_from,
                    effective_to_exclusive, source_provider, source_reference
                ) VALUES ('csi300', '000905.SH', '999998.SZ', '2024-01-02', NULL, 'CSI', 'dev-smoke')
                """
            )
        except psycopg2.Error:
            catalog_mismatch_rejected = True
            cur.execute("ROLLBACK TO SAVEPOINT core_index_smoke_catalog")
        cur.execute("ROLLBACK TO SAVEPOINT core_index_smoke_all")
        cur.execute("SELECT COUNT(*) FROM market.core_index_membership_pit")
        after_count = int(cur.fetchone()[0])
    passed = overlap_rejected and catalog_mismatch_rejected and before_count == after_count
    return {
        "status": "PASS" if passed else "BLOCKED",
        "overlap_rejected": overlap_rejected,
        "catalog_mismatch_rejected": catalog_mismatch_rejected,
        "row_count_before": before_count,
        "row_count_after": after_count,
    }


def upsert_authority_rows(conn: Any, rows: Sequence[AuthorityRow]) -> int:
    ordered_rows = sorted(
        rows,
        key=lambda row: (row.pool_id, row.ts_code, row.effective_from),
    )
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('market.core_index_membership_pit'))")
        result = pgx.execute_values(
            cur,
            """
            INSERT INTO market.core_index_membership_pit (
                pool_id, index_code, ts_code, effective_from,
                effective_to_exclusive, source_provider, source_reference
            ) VALUES %s
            ON CONFLICT (pool_id, ts_code, effective_from) DO UPDATE SET
                index_code = EXCLUDED.index_code,
                effective_to_exclusive = EXCLUDED.effective_to_exclusive,
                source_provider = EXCLUDED.source_provider,
                source_reference = EXCLUDED.source_reference
            WHERE (
                market.core_index_membership_pit.index_code,
                market.core_index_membership_pit.effective_to_exclusive,
                market.core_index_membership_pit.source_provider,
                market.core_index_membership_pit.source_reference
            ) IS DISTINCT FROM (
                EXCLUDED.index_code,
                EXCLUDED.effective_to_exclusive,
                EXCLUDED.source_provider,
                EXCLUDED.source_reference
            )
            RETURNING pool_id
            """,
            [row.database_tuple() for row in ordered_rows],
            page_size=1000,
            fetch=True,
        )
    return len(result or ())


def _validate_dev_result(path: Path, *, pool_ids: Sequence[str], expected_mode: str) -> Mapping[str, Any]:
    try:
        result = json.loads(path.resolve(strict=True).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CoreIndexMembershipOperatorError("DEV result is unreadable or invalid") from exc
    if (
        result.get("schema_version") != SCHEMA_VERSION
        or result.get("database_target") != "dev"
        or result.get("mode") != expected_mode
        or result.get("status") != "PASS"
        or result.get("migration") != MIGRATION_NAME
        or set(result.get("pool_ids") or ()) != set(pool_ids)
        or (expected_mode == "apply" and int(result.get("readback", {}).get("pool_count") or 0) != len(pool_ids))
        or (
            expected_mode == "migrate"
            and (
                result.get("migration_readback", {}).get("status") != "PASS"
                or result.get("migration_constraint_smoke", {}).get("status") != "PASS"
            )
        )
    ):
        raise CoreIndexMembershipOperatorError("DEV result does not match this production load")
    return result


def validate_full_database(
    config: DatabaseConfig,
    *,
    pool_ids: Sequence[str],
    start_date: date,
    end_date: date,
    candidate_root: Path | None,
    tushare_fetcher: Callable[[str, date, date], Any] | None,
) -> dict[str, Any]:
    repository = CoreIndexMembershipRepository(_connection_factory(config))
    coverage = repository.fetch_pool_coverage(pool_ids)
    missing_pools = sorted(set(pool_ids) - set(coverage))
    coverage_errors = []
    for pool_id in pool_ids:
        item = coverage.get(pool_id)
        if item is not None and item.first_effective_from > POOL_DEFINITIONS[pool_id].history_start:
            coverage_errors.append(pool_id)
    resolved_by_pool = {}
    resolved_symbols: set[str] = set()
    for pool_id in pool_ids:
        if pool_id in missing_pools or pool_id in coverage_errors:
            continue
        resolved = resolve_universe(
            UniverseSelection(mode=UniverseMode.SINGLE_INDEX, pool_ids=(pool_id,)),
            start_date,
            end_date,
            repository=repository,
        )
        symbols = {row.ts_code for row in resolved.intervals}
        resolved_symbols.update(symbols)
        resolved_by_pool[pool_id] = {
            "interval_count": len(resolved.intervals),
            "symbol_count": len(symbols),
            "membership_revision": resolved.membership_revision,
        }
    union_summary = None
    if not missing_pools and not coverage_errors:
        union = resolve_universe(
            UniverseSelection(mode=UniverseMode.INDEX_UNION, pool_ids=tuple(reversed(pool_ids))),
            start_date,
            end_date,
            repository=repository,
        )
        union_reordered = resolve_universe(
            UniverseSelection(mode=UniverseMode.INDEX_UNION, pool_ids=tuple(pool_ids)),
            start_date,
            end_date,
            repository=repository,
        )
        order_invariant = union.intervals == union_reordered.intervals
        union_summary = {
            "interval_count": len(union.intervals),
            "symbol_count": len({row.ts_code for row in union.intervals}),
            "order_invariant": order_invariant,
        }
    physical = _physical_coverage(candidate_root, resolved_symbols) if candidate_root else None
    tushare = (
        _crosscheck_tushare(
            config=config,
            pool_ids=pool_ids,
            start_date=start_date,
            end_date=end_date,
            fetcher=tushare_fetcher,
        )
        if tushare_fetcher
        else None
    )
    errors = len(missing_pools) + len(coverage_errors)
    if union_summary and not union_summary["order_invariant"]:
        errors += 1
    if physical:
        errors += len(physical["daily_missing"]) + len(physical["minute_missing"])
    if tushare:
        errors += int(tushare["blocking_error_count"])
    return {
        "status": "PASS" if errors == 0 else "DATA_GAPS",
        "error_count": errors,
        "missing_pools": missing_pools,
        "coverage_start_errors": coverage_errors,
        "resolved_by_pool": resolved_by_pool,
        "union": union_summary,
        "physical_coverage": physical,
        "tushare_crosscheck": tushare,
    }


def _physical_coverage(candidate_root: Path, symbols: Iterable[str]) -> dict[str, Any]:
    root = candidate_root.resolve(strict=True)
    daily = root / "components" / "daily_bin_candidate" / "features"
    minute = root / "components" / "minute_bin_candidate" / "features"
    if not daily.is_dir() or not minute.is_dir():
        raise CoreIndexMembershipOperatorError("candidate daily/minute feature roots are missing")
    expected = sorted({symbol.lower() for symbol in symbols})
    daily_missing = [symbol.upper() for symbol in expected if not (daily / symbol).is_dir()]
    minute_missing = [symbol.upper() for symbol in expected if not (minute / symbol).is_dir()]
    return {
        "symbol_count": len(expected),
        "daily_missing": daily_missing,
        "minute_missing": minute_missing,
    }


def _crosscheck_tushare(
    *,
    config: DatabaseConfig,
    pool_ids: Sequence[str],
    start_date: date,
    end_date: date,
    fetcher: Callable[[str, date, date], Any],
) -> dict[str, Any]:
    repository = CoreIndexMembershipRepository(_connection_factory(config))
    rows = repository.fetch_membership_intervals(pool_ids, start_date, end_date)
    by_pool = {pool_id: [] for pool_id in pool_ids}
    for row in rows:
        by_pool[row.pool_id].append(row)
    checked = 0
    unavailable = 0
    mismatches: list[dict[str, Any]] = []
    for pool_id in pool_ids:
        definition = POOL_DEFINITIONS[pool_id]
        month_start = max(start_date, definition.history_start).replace(day=1)
        for first, last in _month_ranges(month_start, end_date):
            frame = fetcher(definition.index_code, first, last)
            if frame is None or frame.empty:
                unavailable += 1
                continue
            required = {"con_code", "trade_date"}
            if not required.issubset(frame.columns):
                raise CoreIndexMembershipOperatorError(
                    f"Tushare index_weight fields missing for {pool_id}: {sorted(required - set(frame.columns))}"
                )
            snapshot_date = max(_parse_tushare_date(value) for value in frame["trade_date"].tolist())
            upstream = {
                str(value).strip().upper()
                for value in frame.loc[
                    frame["trade_date"].map(_parse_tushare_date) == snapshot_date, "con_code"
                ].tolist()
            }
            database = {
                row.ts_code
                for row in by_pool[pool_id]
                if row.effective_from <= snapshot_date
                and (row.effective_to_exclusive is None or snapshot_date < row.effective_to_exclusive)
            }
            checked += 1
            if upstream != database:
                mismatches.append(
                    {
                        "pool_id": pool_id,
                        "snapshot_date": snapshot_date.isoformat(),
                        "upstream_count": len(upstream),
                        "database_count": len(database),
                        "missing_in_database": sorted(upstream - database)[:20],
                        "extra_in_database": sorted(database - upstream)[:20],
                    }
                )
            time.sleep(0.05)
    return {
        "checked_month_count": checked,
        "upstream_unavailable_month_count": unavailable,
        "mismatch_month_count": len(mismatches),
        "blocking_error_count": 0,
        "authority_effect": "advisory_only_l1_official_wins",
        "mismatch_examples": mismatches[:20],
    }


def _month_ranges(start_date: date, end_date: date) -> Iterator[tuple[date, date]]:
    current = start_date.replace(day=1)
    while current <= end_date:
        last_day = month_calendar.monthrange(current.year, current.month)[1]
        month_end = min(date(current.year, current.month, last_day), end_date)
        yield max(current, start_date), month_end
        current = date(current.year + (current.month == 12), 1 if current.month == 12 else current.month + 1, 1)


def _parse_tushare_date(value: Any) -> date:
    raw = str(value).strip().replace("-", "")
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError as exc:
        raise CoreIndexMembershipOperatorError(f"invalid Tushare trade_date: {value!r}") from exc


def _build_tushare_fetcher(env_file: Path) -> Callable[[str, date, date], Any]:
    values = dotenv_values(env_file.resolve(strict=True))
    token = str(values.get("TUSHARE_TOKEN") or os.environ.get("TUSHARE_TOKEN") or "").strip()
    if not token:
        raise CoreIndexMembershipOperatorError("TUSHARE_TOKEN is missing from the configured credential location")
    try:
        import tushare as ts
    except ImportError as exc:
        raise CoreIndexMembershipOperatorError("tushare package is not installed") from exc
    pro = ts.pro_api(token)

    def fetch(index_code: str, start: date, end: date) -> Any:
        return pro.index_weight(
            index_code=index_code,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            fields="index_code,con_code,trade_date,weight",
        )

    return fetch


def _write_result(path: Path, payload: Mapping[str, Any]) -> None:
    resolved = path.resolve(strict=False)
    try:
        resolved.relative_to(REPO_ROOT.resolve())
    except ValueError:
        pass
    else:
        raise CoreIndexMembershipOperatorError("result path must be repo-external")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    if resolved.exists():
        raise CoreIndexMembershipOperatorError("result path already exists; use a new task result path")
    resolved.write_text(json.dumps(_jsonable(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _jsonable(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", choices=sorted(_TARGETS), required=True)
    parser.add_argument("--mode", choices=sorted(_MODES), required=True)
    parser.add_argument("--env-file", default=str(REPO_ROOT / ".env"))
    parser.add_argument("--authority-file")
    parser.add_argument("--dev-result")
    parser.add_argument("--authorization-ref")
    parser.add_argument("--result-path", required=True)
    parser.add_argument("--start-date", default="2018-08-01")
    parser.add_argument("--end-date")
    parser.add_argument("--pool-id", action="append", dest="pool_ids")
    parser.add_argument("--candidate-root")
    parser.add_argument("--tushare-crosscheck", action="store_true")
    parser.add_argument("--output-dir")
    parser.add_argument("--replace-sidecars", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    config = _load_database_config(args.database, Path(args.env_file))
    pool_ids = tuple(sorted(set(args.pool_ids or P0_POOL_IDS)))
    unknown = sorted(set(pool_ids) - set(POOL_DEFINITIONS))
    if unknown:
        raise CoreIndexMembershipOperatorError(f"unknown pool ids: {unknown}")
    start_date = _parse_date(args.start_date, "start_date")
    end_date = _parse_date(args.end_date, "end_date") if args.end_date else None
    rows = load_authority_rows(Path(args.authority_file)) if args.authority_file else ()
    if args.mode in {"plan", "apply"} and not rows:
        raise CoreIndexMembershipOperatorError("plan/apply requires --authority-file")
    if rows and set(row.pool_id for row in rows) != set(pool_ids):
        raise CoreIndexMembershipOperatorError("authority file pool set must equal requested pool ids")
    if args.database == "production" and args.mode in {"migrate", "apply"}:
        if not str(args.authorization_ref or "").strip():
            raise CoreIndexMembershipOperatorError("production apply requires --authorization-ref")
        if not args.dev_result:
            raise CoreIndexMembershipOperatorError("production apply requires --dev-result")
        _validate_dev_result(Path(args.dev_result), pool_ids=pool_ids, expected_mode=args.mode)

    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": "PASS",
        "mode": args.mode,
        "database_target": args.database,
        "credential_location": config.credential_location,
        "migration": MIGRATION_NAME,
        "pool_ids": list(pool_ids),
        "created_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
    }
    if args.mode == "preflight":
        with database_connection(config) as conn:
            preflight = database_preflight(conn)
        result.update(preflight=preflight, database_ddl_executed=False, database_writes=0)
        if not preflight["dependencies_ready"]:
            result["status"] = "BLOCKED"
    elif args.mode == "migrate":
        with database_connection(config, autocommit=True) as conn:
            apply_migration(conn)
            readback = migration_readback(conn)
        smoke = None
        if args.database == "dev":
            with database_connection(config) as conn:
                smoke = migration_constraint_smoke(conn)
            if smoke["status"] != "PASS":
                readback["status"] = "BLOCKED"
        result.update(
            migration_readback=readback,
            migration_constraint_smoke=smoke,
            database_ddl_executed=True,
            database_writes=0,
            status=readback["status"],
            authorization_ref=args.authorization_ref,
        )
    elif args.mode == "migration-readback":
        with database_connection(config) as conn:
            readback = migration_readback(conn)
        result.update(
            migration_readback=readback,
            database_ddl_executed=False,
            database_writes=0,
            status=readback["status"],
        )
    elif args.mode == "plan":
        with database_connection(config) as conn:
            result["readback"] = readback_summary(conn, pool_ids)
        result["authority_row_count"] = len(rows)
        result["database_writes"] = 0
    elif args.mode == "apply":
        with database_connection(config) as conn:
            changed = upsert_authority_rows(conn, rows)
            readback = readback_summary(conn, pool_ids)
        result.update(
            authority_row_count=len(rows),
            changed_row_count=changed,
            readback=readback,
            database_writes=changed,
            authorization_ref=args.authorization_ref,
        )
        if readback["pool_count"] != len(pool_ids):
            result["status"] = "BLOCKED"
    elif args.mode == "readback":
        with database_connection(config) as conn:
            result["readback"] = readback_summary(conn, pool_ids)
        result["database_writes"] = 0
    elif args.mode == "full-validate":
        if end_date is None:
            raise CoreIndexMembershipOperatorError("full-validate requires --end-date")
        fetcher = _build_tushare_fetcher(Path(args.env_file)) if args.tushare_crosscheck else None
        validation = validate_full_database(
            config,
            pool_ids=pool_ids,
            start_date=start_date,
            end_date=end_date,
            candidate_root=Path(args.candidate_root) if args.candidate_root else None,
            tushare_fetcher=fetcher,
        )
        result.update(validation=validation, database_writes=0, status=validation["status"])
    else:
        if end_date is None or not args.output_dir:
            raise CoreIndexMembershipOperatorError("render requires --end-date and --output-dir")
        repository = CoreIndexMembershipRepository(_connection_factory(config))
        sidecars = []
        for pool_id in pool_ids:
            resolved = resolve_universe(
                UniverseSelection(mode=UniverseMode.SINGLE_INDEX, pool_ids=(pool_id,)),
                start_date,
                end_date,
                repository=repository,
            )
            sidecars.append(asdict(write_sidecar(Path(args.output_dir), resolved, replace=args.replace_sidecars)))
        if len(pool_ids) > 1:
            resolved = resolve_universe(
                UniverseSelection(mode=UniverseMode.INDEX_UNION, pool_ids=pool_ids),
                start_date,
                end_date,
                repository=repository,
            )
            sidecars.append(asdict(write_sidecar(Path(args.output_dir), resolved, replace=args.replace_sidecars)))
        result.update(sidecars=sidecars, database_writes=0)
    _write_result(Path(args.result_path), result)
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
