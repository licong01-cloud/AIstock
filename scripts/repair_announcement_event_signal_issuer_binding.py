"""Plan or apply BUG-1114 announcement issuer-binding suppression.

The repair never deletes or rewrites raw announcements/classifications.  It
replays the checked-in adapters so non-exact bindings become auditable
``SUPPRESSED`` signals and ``UNKNOWN``/``SUPERSEDED`` facts.  Production apply
is deliberately a separate, exact-target DML gate.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

import psycopg2
from dotenv import dotenv_values


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.event_signal.announcement_adapter import (  # noqa: E402
    finish_run,
    start_run,
    upsert_facts,
    upsert_signals,
)
from backend.services.event_signal.announcement_issuer_binding import (  # noqa: E402
    attach_announcement_issuer_bindings,
)
from backend.services.event_signal.st_announcement_adapter import (  # noqa: E402
    ANNOUNCEMENT_RULE_VERSION,
    ST_UNIFIED_RULE_VERSION,
    attach_st_cross_checks,
    build_run_id,
    fetch_st_classification_batch,
    fetch_stock_st_events_for_rows,
    seed_st_first_rule_set,
)


REPAIR_SCHEMA_VERSION = "announcement_issuer_binding_repair_receipt_v1"
CONFIRMATIONS = {
    "dev": "APPLY_BUG_1114_ISSUER_SUPPRESSION_DEV",
    "production": "APPLY_BUG_1114_ISSUER_SUPPRESSION_PRODUCTION",
}


@dataclass(frozen=True)
class RepairPlan:
    schema_version: str
    target: str
    source_rule_version: str
    rule_version: str
    time_mode: str
    start_date: str | None
    end_date: str | None
    row_count: int
    repair_row_count: int
    binding_counts: dict[str, int]
    event_type_binding_counts: dict[str, dict[str, int]]
    plan_digest: str
    raw_rows_deleted: int = 0
    signal_rows_deleted: int = 0


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _target_config(env_file: Path, target: str) -> dict[str, Any]:
    values = {str(key): str(value) for key, value in dotenv_values(env_file).items() if value is not None}
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    required = [f"{prefix}{suffix}" for suffix in ("HOST", "PORT", "NAME", "USER", "PASSWORD")]
    missing = [key for key in required if not values.get(key)]
    if missing:
        raise RuntimeError("BUG1114_DB_ENV_MISSING: " + ",".join(missing))
    config = {
        "host": values[f"{prefix}HOST"],
        "port": int(values[f"{prefix}PORT"]),
        "dbname": values[f"{prefix}NAME"],
        "user": values[f"{prefix}USER"],
        "password": values[f"{prefix}PASSWORD"],
        "connect_timeout": 10,
        "application_name": f"aistock_bug1114_{target}_issuer_binding_repair",
    }
    if target == "dev" and (config["port"] != 5433 or "dev" not in config["dbname"].lower()):
        raise RuntimeError("BUG1114_DEV_TARGET_IDENTITY_INVALID")
    if target == "production" and (
        config["port"] == 5433 or "dev" in config["dbname"].lower()
    ):
        raise RuntimeError("BUG1114_PRODUCTION_TARGET_IDENTITY_INVALID")
    return config


def _iter_enriched_batches(
    conn: Any,
    *,
    start_date: dt.date | None,
    end_date: dt.date | None,
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
    last_classification_id = 0
    while True:
        rows = fetch_st_classification_batch(
            conn,
            source_rule_version=ANNOUNCEMENT_RULE_VERSION,
            rule_version=ST_UNIFIED_RULE_VERSION,
            time_mode="backtest",
            last_classification_id=last_classification_id,
            batch_size=batch_size,
            start_date=start_date,
            end_date=end_date,
            missing_only=False,
        )
        if not rows:
            return
        last_classification_id = int(rows[-1]["classification_id"])
        stock_st_events = fetch_stock_st_events_for_rows(conn, rows)
        rows, _matched = attach_st_cross_checks(rows, stock_st_events)
        rows, _counts = attach_announcement_issuer_bindings(
            rows,
            require_terminal_cross_check=True,
        )
        yield rows


def build_plan_from_batches(
    batches: Iterable[Iterable[Mapping[str, Any]]],
    *,
    target: str,
    start_date: dt.date | None,
    end_date: dt.date | None,
) -> RepairPlan:
    digest = hashlib.sha256()
    counts: Counter[str] = Counter()
    event_type_counts: dict[str, Counter[str]] = {}
    row_count = 0
    repair_row_count = 0
    for batch in batches:
        for row in batch:
            decision = dict(row["issuer_binding_decision"])
            item = {
                "classification_id": int(row["classification_id"]),
                "ann_id": int(row["ann_id"]),
                "source_ts_code": str(row["ts_code"]),
                "binding_digest": decision["binding_digest"],
                "status": decision["status"],
                "fact_status": decision["fact_status"],
                "signal_status": decision["signal_status"],
            }
            digest.update((_canonical_json(item) + "\n").encode("utf-8"))
            counts[str(decision["status"])] += 1
            event_type = str(row.get("event_type") or "UNKNOWN")
            event_type_counts.setdefault(event_type, Counter())[str(decision["status"])] += 1
            if str(decision["status"]) != "EXACT":
                repair_row_count += 1
            row_count += 1
    return RepairPlan(
        schema_version=REPAIR_SCHEMA_VERSION,
        target=target,
        source_rule_version=ANNOUNCEMENT_RULE_VERSION,
        rule_version=ST_UNIFIED_RULE_VERSION,
        time_mode="backtest",
        start_date=start_date.isoformat() if start_date else None,
        end_date=end_date.isoformat() if end_date else None,
        row_count=row_count,
        repair_row_count=repair_row_count,
        binding_counts=dict(sorted(counts.items())),
        event_type_binding_counts={
            event_type: dict(sorted(values.items()))
            for event_type, values in sorted(event_type_counts.items())
        },
        plan_digest=digest.hexdigest(),
    )


def _build_plan(
    conn: Any,
    *,
    target: str,
    start_date: dt.date | None,
    end_date: dt.date | None,
    batch_size: int,
) -> RepairPlan:
    return build_plan_from_batches(
        _iter_enriched_batches(
            conn,
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
        ),
        target=target,
        start_date=start_date,
        end_date=end_date,
    )


def select_repair_rows(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Return only fail-closed rows that the repair is allowed to rewrite.

    ``EXACT`` rows can already carry a legitimate lifecycle status such as
    ``RESOLVED`` or ``EXPIRED``.  Replaying those rows through the ST adapter
    would reset that business state to ``ACTIVE``, so they are deliberately
    excluded from the repair write set.
    """

    return [
        row
        for row in rows
        if str(row["issuer_binding_decision"]["status"]) != "EXACT"
    ]


def _apply(
    conn: Any,
    *,
    plan: RepairPlan,
    start_date: dt.date | None,
    end_date: dt.date | None,
    batch_size: int,
) -> dict[str, Any]:
    run_id = build_run_id(
        source_rule_version=ANNOUNCEMENT_RULE_VERSION,
        time_mode="backtest",
        run_mode="repair",
    )
    seed_st_first_rule_set(conn)
    start_run(
        conn,
        run_id=run_id,
        rule_version=ST_UNIFIED_RULE_VERSION,
        source_rule_version=ANNOUNCEMENT_RULE_VERSION,
        time_mode="backtest",
        run_mode="repair",
        date_from=start_date,
        date_to=end_date,
    )
    processed = facts = signals = 0
    binding_counts: Counter[str] = Counter()
    event_type_binding_counts: dict[str, Counter[str]] = {}
    try:
        for rows in _iter_enriched_batches(
            conn,
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
        ):
            for row in rows:
                status = str(row["issuer_binding_decision"]["status"])
                binding_counts[status] += 1
                event_type = str(row.get("event_type") or "UNKNOWN")
                event_type_binding_counts.setdefault(event_type, Counter())[status] += 1
            repair_rows = select_repair_rows(rows)
            if repair_rows:
                event_ids = upsert_facts(
                    conn,
                    repair_rows,
                    run_id=run_id,
                    rule_version=ST_UNIFIED_RULE_VERSION,
                )
                signals += upsert_signals(
                    conn,
                    repair_rows,
                    event_ids_by_key=event_ids,
                    run_id=run_id,
                    rule_version=ST_UNIFIED_RULE_VERSION,
                    time_mode="backtest",
                )
            processed += len(rows)
            facts += len(repair_rows)
        observed_event_type_counts = {
            event_type: dict(sorted(values.items()))
            for event_type, values in sorted(event_type_binding_counts.items())
        }
        if (
            processed != plan.row_count
            or dict(sorted(binding_counts.items())) != plan.binding_counts
            or observed_event_type_counts != plan.event_type_binding_counts
            or facts != plan.repair_row_count
        ):
            raise RuntimeError("BUG1114_APPLY_READBACK_DIFFERS_FROM_PLAN")
        finish_run(
            conn,
            run_id=run_id,
            status="SUCCESS",
            source_input_rows=processed,
            fact_rows=facts,
            signal_rows=signals,
            metrics={
                "repair_schema_version": REPAIR_SCHEMA_VERSION,
                "plan_digest": plan.plan_digest,
                "binding_counts": dict(sorted(binding_counts.items())),
                "event_type_binding_counts": observed_event_type_counts,
                "repair_row_count": plan.repair_row_count,
                "exact_rows_untouched": plan.row_count - plan.repair_row_count,
                "raw_rows_deleted": 0,
                "signal_rows_deleted": 0,
            },
        )
    except Exception:
        # A database exception leaves PostgreSQL transactions aborted.  Do not
        # issue a second SQL statement that can mask the original failure;
        # roll back every repair write and let the caller surface the cause.
        conn.rollback()
        raise
    return {
        **asdict(plan),
        "run_id": run_id,
        "processed_rows": processed,
        "fact_rows": facts,
        "signal_rows": signals,
        "apply_status": "applied",
    }


def _date(value: str | None) -> dt.date | None:
    return dt.date.fromisoformat(value) if value else None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "apply"))
    parser.add_argument("--target", choices=("dev", "production"), required=True)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date", default="2026-07-31")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--expected-plan-digest")
    parser.add_argument("--confirm")
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.batch_size <= 0 or args.batch_size > 5000:
        raise RuntimeError("BUG1114_BATCH_SIZE_OUT_OF_RANGE")
    start_date = _date(args.start_date)
    end_date = _date(args.end_date)
    config = _target_config(args.env_file.resolve(), args.target)
    conn = psycopg2.connect(**config)
    try:
        if args.command == "plan":
            conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
            plan = _build_plan(
                conn,
                target=args.target,
                start_date=start_date,
                end_date=end_date,
                batch_size=args.batch_size,
            )
            conn.rollback()
            result: dict[str, Any] = {**asdict(plan), "apply_status": "not_requested"}
        else:
            if args.confirm != CONFIRMATIONS[args.target]:
                raise RuntimeError("BUG1114_APPLY_CONFIRMATION_INVALID")
            if not args.expected_plan_digest:
                raise RuntimeError("BUG1114_EXPECTED_PLAN_DIGEST_REQUIRED")
            conn.set_session(isolation_level="REPEATABLE READ")
            plan = _build_plan(
                conn,
                target=args.target,
                start_date=start_date,
                end_date=end_date,
                batch_size=args.batch_size,
            )
            if plan.plan_digest != args.expected_plan_digest:
                raise RuntimeError("BUG1114_PLAN_DIGEST_DRIFT")
            result = _apply(
                conn,
                plan=plan,
                start_date=start_date,
                end_date=end_date,
                batch_size=args.batch_size,
            )
            conn.commit()
        output = _canonical_json(result)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(output + os.linesep, encoding="utf-8")
        print(output)
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
