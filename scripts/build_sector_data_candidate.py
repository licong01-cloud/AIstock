#!/usr/bin/env python3
"""Build a repo-external C-013 P3A sector-data candidate without DB writes."""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.pg_pool import get_conn  # noqa: E402
from backend.services.dataset_release.canonical import canonical_json_bytes  # noqa: E402
from backend.services.industry_pit.artifact_store import read_candidate_bundle  # noqa: E402
from backend.services.industry_pit.candidate_builder import (  # noqa: E402
    FrozenDenominator,
    UniverseSpan,
)
from backend.services.industry_pit.contracts import (  # noqa: E402
    IndustryPitContractError,
    require_symbol,
)
from backend.services.sector_data_builder import (  # noqa: E402
    MONEYFLOW_FIELDS,
    SECTOR_DATA_OPPORTUNITY_SCHEMA,
    SW_DAILY_FIELDS,
    SectorDataBuildContractError,
    SectorDataCandidateBuilder,
    SectorDataSourceDay,
    write_sector_data_candidate,
)


def _git_identity() -> Mapping[str, Any]:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    tree = subprocess.run(
        ["git", "rev-parse", "HEAD^{tree}"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    return {"commit": commit, "tree": tree, "dirty": dirty}


def _date(value: Any, *, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise SectorDataBuildContractError(f"{field} is not an ISO date") from exc


def _denominator_contract(authority_bundle) -> Mapping[str, Any]:
    report = authority_bundle.preflight_report
    diagnostics = report.get("source_diagnostics")
    frozen = diagnostics.get("frozen_universe") if isinstance(diagnostics, Mapping) else None
    if not isinstance(frozen, Mapping):
        raise SectorDataBuildContractError("authority bundle lacks frozen universe receipt")
    required = {
        "universe_key",
        "rule_version",
        "scope",
        "state_start",
        "state_end",
        "status",
        "dirty",
        "source_fingerprint_sha256",
    }
    if not required.issubset(frozen):
        raise SectorDataBuildContractError("frozen universe receipt is incomplete")
    if frozen.get("status") != "ready" or frozen.get("dirty") is not False:
        raise SectorDataBuildContractError("frozen universe receipt is not ready/clean")
    return {
        **{key: frozen[key] for key in required},
        "window_start": report.get("window_start"),
        "window_end": report.get("window_end"),
    }


def _read_frozen_denominator(conn, *, contract: Mapping[str, Any]) -> FrozenDenominator:
    window_start = _date(contract["window_start"], field="window_start")
    window_end = _date(contract["window_end"], field="window_end")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT universe_key,rule_version,scope,start_date,end_date,status,dirty,
                   source_fingerprint_sha256
            FROM market.stock_universe_pit_state
            WHERE universe_key=%s
            """,
            (contract["universe_key"],),
        )
        rows = cur.fetchall()
        if len(rows) != 1:
            raise SectorDataBuildContractError("frozen universe state must resolve exactly one row")
        state = rows[0]
        observed = {
            "universe_key": state[0],
            "rule_version": state[1],
            "scope": state[2],
            "state_start": state[3].isoformat(),
            "state_end": state[4].isoformat(),
            "status": state[5],
            "dirty": bool(state[6]),
            "source_fingerprint_sha256": state[7],
        }
        expected = {key: contract[key] for key in observed}
        if observed != expected:
            raise SectorDataBuildContractError("frozen universe state differs from authority receipt")
        if window_start < state[3] or window_end > state[4]:
            raise SectorDataBuildContractError("authority window escapes frozen universe coverage")
        cur.execute(
            """
            SELECT cal_date::date
            FROM market.trading_calendar
            WHERE is_trading=true AND cal_date BETWEEN %s AND %s
            ORDER BY cal_date
            """,
            (window_start, window_end),
        )
        trading_dates = [row[0] for row in cur.fetchall()]
        cur.execute(
            """
            SELECT ts_code,eligible_start,eligible_end
            FROM market.stock_universe_pit_spans
            WHERE universe_key=%s AND eligible_start<=%s
              AND (eligible_end IS NULL OR eligible_end>=%s)
            ORDER BY ts_code,eligible_start
            """,
            (contract["universe_key"], window_end, window_start),
        )
        spans = [UniverseSpan(row[0], row[1], row[2]) for row in cur.fetchall()]
    return FrozenDenominator.build(
        window_start=window_start,
        window_end=window_end,
        trading_dates=trading_dates,
        universe_spans=spans,
    )


def _selected_dates(denominator: FrozenDenominator, args: argparse.Namespace) -> tuple[date, ...]:
    start = _date(args.start_date, field="start_date") if args.start_date else denominator.window_start
    end = _date(args.end_date, field="end_date") if args.end_date else denominator.window_end
    if start < denominator.window_start or end > denominator.window_end or end < start:
        raise SectorDataBuildContractError("selected dates escape or invert the authority window")
    left = bisect.bisect_left(denominator.trading_dates, start)
    right = bisect.bisect_right(denominator.trading_dates, end)
    dates = denominator.trading_dates[left:right]
    if args.max_trading_days is not None:
        if args.max_trading_days <= 0:
            raise SectorDataBuildContractError("max_trading_days must be positive")
        dates = dates[: args.max_trading_days]
    if not dates:
        raise SectorDataBuildContractError("selected candidate contains no trading dates")
    return dates


def _symbols_by_date(
    denominator: FrozenDenominator,
    dates: Sequence[date],
    *,
    selected_symbols: frozenset[str],
) -> Mapping[date, tuple[str, ...]]:
    output: dict[date, list[str]] = {value: [] for value in dates}
    for span in denominator.universe_spans:
        if selected_symbols and span.canonical_symbol not in selected_symbols:
            continue
        start = max(span.eligible_start, dates[0])
        end = min(span.eligible_end or dates[-1], dates[-1])
        left = bisect.bisect_left(dates, start)
        right = bisect.bisect_right(dates, end)
        for trade_date in dates[left:right]:
            output[trade_date].append(span.canonical_symbol)
    normalized = {
        trade_date: tuple(sorted(symbols))
        for trade_date, symbols in output.items()
        if symbols
    }
    if len(normalized) != len(dates):
        raise SectorDataBuildContractError("selected denominator has an empty trading day")
    return normalized


def _opportunity_digest(symbols_by_date: Mapping[date, tuple[str, ...]]) -> str:
    digest = hashlib.sha256()
    for trade_date in sorted(symbols_by_date):
        for symbol in symbols_by_date[trade_date]:
            digest.update(
                canonical_json_bytes(
                    {
                        "schema_version": SECTOR_DATA_OPPORTUNITY_SCHEMA,
                        "trade_date": trade_date.isoformat(),
                        "canonical_symbol": symbol,
                    }
                )
                + b"\n"
            )
    return digest.hexdigest()


def _rows_as_mappings(cur) -> list[Mapping[str, Any]]:
    columns = [item[0] for item in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _insert_source_row(
    target: dict[str, Mapping[str, Any]],
    *,
    source: str,
    identity: str,
    row: Mapping[str, Any],
) -> None:
    previous = target.get(identity)
    if previous is not None and previous != row:
        raise SectorDataBuildContractError(
            f"{source} contains conflicting rows for the same trade-date identity: {identity}"
        )
    target[identity] = row


def _source_days(
    conn,
    *,
    dates: Sequence[date],
    symbols_by_date: Mapping[date, tuple[str, ...]],
    chunk_trading_days: int,
) -> Iterator[SectorDataSourceDay]:
    if chunk_trading_days <= 0:
        raise SectorDataBuildContractError("chunk_trading_days must be positive")
    for offset in range(0, len(dates), chunk_trading_days):
        chunk = dates[offset : offset + chunk_trading_days]
        start, end = chunk[0], chunk[-1]
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT trade_date,ts_code,{','.join(SW_DAILY_FIELDS)}
                FROM market.sw_daily
                WHERE trade_date BETWEEN %s AND %s
                ORDER BY trade_date,ts_code
                """,
                (start, end),
            )
            sw_rows = _rows_as_mappings(cur)
            cur.execute(
                f"""
                SELECT trade_date,ts_code,{','.join(MONEYFLOW_FIELDS)}
                FROM market.moneyflow_ts
                WHERE trade_date BETWEEN %s AND %s
                ORDER BY trade_date,ts_code
                """,
                (start, end),
            )
            moneyflow_rows = _rows_as_mappings(cur)
        sw_by_day: dict[date, dict[str, Mapping[str, Any]]] = defaultdict(dict)
        for row in sw_rows:
            trade_date = row["trade_date"]
            symbol = str(row["ts_code"])
            _insert_source_row(
                sw_by_day[trade_date],
                source="market.sw_daily",
                identity=symbol,
                row={field: row[field] for field in SW_DAILY_FIELDS},
            )
        moneyflow_by_day: dict[date, dict[str, Mapping[str, Any]]] = defaultdict(dict)
        for row in moneyflow_rows:
            trade_date = row["trade_date"]
            symbol = str(row["ts_code"])
            _insert_source_row(
                moneyflow_by_day[trade_date],
                source="market.moneyflow_ts",
                identity=symbol,
                row={field: row[field] for field in MONEYFLOW_FIELDS},
            )
        for trade_date in chunk:
            eligible = symbols_by_date[trade_date]
            eligible_set = set(eligible)
            yield SectorDataSourceDay(
                trade_date=trade_date,
                symbols=eligible,
                sw_daily_by_index_l2=sw_by_day.get(trade_date, {}),
                moneyflow_by_symbol={
                    symbol: row
                    for symbol, row in moneyflow_by_day.get(trade_date, {}).items()
                    if symbol in eligible_set
                },
            )


def _candidate_days(builder, source_days, *, progress_every: int):
    if progress_every < 0:
        raise SectorDataBuildContractError("progress_every_trading_days cannot be negative")
    for index, source_day in enumerate(source_days, start=1):
        result = builder.build_day(source_day)
        if progress_every and index % progress_every == 0:
            print(
                f"sector candidate progress: trading_days={index} latest={source_day.trade_date}",
                file=sys.stderr,
                flush=True,
            )
        yield result


def _producer_guarded_days(days, *, expected_producer: Mapping[str, Any]):
    yield from days
    observed = _git_identity()
    if observed.get("dirty") or any(
        observed.get(field) != expected_producer.get(field) for field in ("commit", "tree")
    ):
        raise SectorDataBuildContractError("producer worktree changed during candidate construction")


def build(args: argparse.Namespace) -> Mapping[str, Any]:
    producer = _git_identity()
    if not args.dry_run and producer["dirty"]:
        raise SectorDataBuildContractError("refusing to write candidate from a dirty producer worktree")
    load_dotenv(args.db_env_file, override=False)
    forbidden_roots = (ROOT, ROOT.parent / "AIstock_worktrees")
    authority_bundle = read_candidate_bundle(
        artifact_root=args.industry_candidate_root,
        forbidden_roots=forbidden_roots,
    )
    builder = SectorDataCandidateBuilder(authority_bundle=authority_bundle)
    with get_conn() as conn:
        conn.set_session(readonly=True, autocommit=False)
        denominator = _read_frozen_denominator(
            conn,
            contract=_denominator_contract(authority_bundle),
        )
        classification_receipt = authority_bundle.classification_receipt
        index_receipt = authority_bundle.index_membership_receipt
        if (
            denominator.digest != classification_receipt.denominator_digest
            or denominator.digest != index_receipt.denominator_digest
            or denominator.total_opportunities != classification_receipt.frozen_denominator
            or denominator.total_opportunities != index_receipt.frozen_denominator
        ):
            raise SectorDataBuildContractError("database denominator differs from dual authority receipt")
        selected_dates = _selected_dates(denominator, args)
        selected_symbols = frozenset(require_symbol(value) for value in (args.symbol or ()))
        symbols_by_date = _symbols_by_date(
            denominator,
            selected_dates,
            selected_symbols=selected_symbols,
        )
        expected = sum(len(values) for values in symbols_by_date.values())
        expected_opportunity_digest = _opportunity_digest(symbols_by_date)
        full_scope = (
            not selected_symbols
            and args.start_date is None
            and args.end_date is None
            and args.max_trading_days is None
            and expected == denominator.total_opportunities
        )
        source_days = _source_days(
            conn,
            dates=selected_dates,
            symbols_by_date=symbols_by_date,
            chunk_trading_days=args.chunk_trading_days,
        )
        candidate_days = _candidate_days(
            builder,
            source_days,
            progress_every=args.progress_every_trading_days,
        )
        if args.dry_run:
            assignments = 0
            facts = 0
            status_counts: Counter[str] = Counter()
            alignment_counts: Counter[str] = Counter()
            reason_counts: Counter[str] = Counter()
            for day in candidate_days:
                assignments += len(day.assignments)
                facts += len(day.sector_facts)
                for row in day.assignments:
                    status_counts[str(row["status"])] += 1
                    alignment_counts[str(row["alignment_state"])] += 1
                    reason_counts.update(map(str, row["unavailable_reasons"]))
            if assignments != expected:
                raise SectorDataBuildContractError("dry-run denominator closure failed")
            return {
                "status": "PASS_DRY_RUN",
                "candidate_scope": "full" if full_scope else "sample",
                "artifact_written": False,
                "expected_opportunities": expected,
                "assignment_rows": assignments,
                "sector_fact_rows": facts,
                "status_counts": dict(sorted(status_counts.items())),
                "alignment_counts": dict(sorted(alignment_counts.items())),
                "unavailable_by_reason": dict(sorted(reason_counts.items())),
                "source_denominator_digest": denominator.digest,
                "opportunity_digest": expected_opportunity_digest,
                "database_access": "read_only",
                "database_writes": 0,
                "production_activation": False,
            }
        readback = write_sector_data_candidate(
            artifact_root=args.artifact_root,
            forbidden_roots=forbidden_roots,
            authority_bundle=authority_bundle,
            days=_producer_guarded_days(candidate_days, expected_producer=producer),
            expected_opportunities=expected,
            expected_opportunity_digest=expected_opportunity_digest,
            candidate_scope="full" if full_scope else "sample",
            producer_commit=str(producer["commit"]),
            producer_tree=str(producer["tree"]),
        )
    return {
        "status": "PASS_CANDIDATE_ONLY",
        "candidate_scope": readback.manifest["candidate_scope"],
        "artifact_written": True,
        "artifact_root": str(readback.artifact_root),
        "candidate_hash": readback.manifest["candidate_hash"],
        "assignment_rows": readback.assignment_rows,
        "sector_fact_rows": readback.sector_fact_rows,
        "source_denominator_digest": readback.manifest["source_denominator_digest"],
        "opportunity_digest": readback.manifest["opportunity_digest"],
        "database_access": "read_only",
        "database_writes": 0,
        "production_activation": False,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--industry-candidate-root", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--db-env-file", type=Path, default=os.environ.get("AISTOCK_DB_ENV_FILE"))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--max-trading-days", type=int)
    parser.add_argument("--chunk-trading-days", type=int, default=5)
    parser.add_argument("--progress-every-trading-days", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = build(args)
    except (IndustryPitContractError, SectorDataBuildContractError, OSError, ValueError) as exc:
        print(json.dumps({"status": "BLOCKED_CONTRACT", "error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
