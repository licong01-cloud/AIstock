from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.hmm_risk.stock_fact_gap_repair import (  # noqa: E402
    CONFIRM_APPLY,
    CONFIRM_ROLLBACK,
    DAILY_BASIC_COLUMNS,
    DATASET_ORDER,
    MONEYFLOW_COLUMNS,
    GapKey,
    PostgresGapStore,
    RepairSpec,
    StockFactGapRepairError,
    apply_plan,
    build_plan,
    plan_keys,
    readback_receipt,
    rollback_receipt,
    verify_plan,
    verify_receipt,
)

DEFAULT_UNIVERSE = "shsz_st_pit_qe_dataset_qlib_st_pit_active_h5_daily_candidate_20180801_20260630_moneyflow_v2"
DEFAULT_START = dt.date(2022, 1, 1)
DEFAULT_END = dt.date(2024, 6, 30)
PROVIDER_MAX_ATTEMPTS = 3
PROVIDER_RETRY_SECONDS = (0.5, 1.0)


def _date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from exc


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StockFactGapRepairError(f"cannot read {path}: {type(exc).__name__}: {exc}") from exc
    if not isinstance(value, dict):
        raise StockFactGapRepairError(f"JSON root must be an object: {path}")
    return value


def _render(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"


def _prepare_output(value: Mapping[str, Any], output: Path) -> tuple[Path, str]:
    text = _render(value)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    return temporary, text


def _finalize_output(temporary: Path, output: Path) -> None:
    os.replace(temporary, output)


def _emit(value: Mapping[str, Any], output: Path | None) -> None:
    if output is None:
        print(_render(value), end="")
        return
    temporary, text = _prepare_output(value, output)
    try:
        _finalize_output(temporary, output)
        print(text, end="")
    finally:
        if temporary.exists():
            temporary.unlink()


def _db_config(target: str) -> dict[str, Any]:
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    values = {name: (os.getenv(prefix + name) or "").strip() for name in ("HOST", "PORT", "USER", "PASSWORD", "NAME")}
    missing = sorted(name for name, value in values.items() if not value)
    if missing:
        raise StockFactGapRepairError(f"database target {target} lacks required {prefix} settings: {missing}")
    return {
        "host": values["HOST"],
        "port": int(values["PORT"]),
        "user": values["USER"],
        "password": values["PASSWORD"],
        "dbname": values["NAME"],
        "application_name": "AIstock-HMM-B3-stock-fact-gap-repair",
        "options": "-c client_encoding=utf8 -c statement_timeout=60000",
    }


def _connect(target: str) -> Any:
    return psycopg2.connect(**_db_config(target))


def _provider() -> Any:
    token = (os.getenv("TUSHARE_TOKEN") or "").strip()
    if not token:
        raise StockFactGapRepairError("TUSHARE_TOKEN is not configured")
    import tushare as ts

    return ts.pro_api(token)


def _frame_records(frame: Any) -> list[dict[str, Any]]:
    if frame is None or bool(getattr(frame, "empty", True)):
        return []
    return list(frame.to_dict(orient="records"))


def _provider_query(operation: str, identity: str, query: Any) -> Any:
    errors: list[str] = []
    for attempt in range(1, PROVIDER_MAX_ATTEMPTS + 1):
        try:
            return query()
        except Exception as exc:
            errors.append(f"attempt={attempt}:{type(exc).__name__}:{exc}")
            if attempt < PROVIDER_MAX_ATTEMPTS:
                time.sleep(PROVIDER_RETRY_SECONDS[attempt - 1])
    raise StockFactGapRepairError(
        f"Tushare {operation} failed for {identity} after {PROVIDER_MAX_ATTEMPTS} attempts: " + " | ".join(errors)
    )


def _fetch_daily_basic(pro: Any, keys: Sequence[GapKey]) -> list[dict[str, Any]]:
    grouped: dict[dt.date, set[str]] = defaultdict(set)
    for key in keys:
        grouped[key.trade_date].add(key.ts_code)
    fields = ",".join(DAILY_BASIC_COLUMNS)
    result: list[dict[str, Any]] = []
    for trade_date, expected_symbols in sorted(grouped.items()):
        offset = 0
        while True:
            frame = _provider_query(
                "daily_basic",
                trade_date.isoformat(),
                lambda: pro.daily_basic(
                    trade_date=trade_date.strftime("%Y%m%d"),
                    fields=fields,
                    limit=6000,
                    offset=offset,
                ),
            )
            records = _frame_records(frame)
            for row in records:
                if str(row.get("ts_code") or "").strip() in expected_symbols:
                    result.append(row)
            if len(records) < 6000:
                break
            offset += 6000
    return result


def _fetch_moneyflow(pro: Any, keys: Sequence[GapKey]) -> list[dict[str, Any]]:
    grouped: dict[dt.date, set[str]] = defaultdict(set)
    for key in keys:
        grouped[key.trade_date].add(key.ts_code)
    fields = ",".join(MONEYFLOW_COLUMNS)
    result: list[dict[str, Any]] = []
    for trade_date, expected_symbols in sorted(grouped.items()):
        offset = 0
        while True:
            frame = _provider_query(
                "moneyflow",
                trade_date.isoformat(),
                lambda: pro.moneyflow(
                    trade_date=trade_date.strftime("%Y%m%d"),
                    fields=fields,
                    limit=6000,
                    offset=offset,
                ),
            )
            records = _frame_records(frame)
            for row in records:
                if str(row.get("ts_code") or "").strip() in expected_symbols:
                    result.append(row)
            if len(records) < 6000:
                break
            offset += 6000
    return result


def fetch_provider_rows(plan: Mapping[str, Any], *, pro: Any | None = None) -> dict[str, list[dict[str, Any]]]:
    verify_plan(plan)
    keys = plan_keys(plan)
    provider = _provider() if pro is None else pro
    return {
        "daily_basic": _fetch_daily_basic(provider, keys["daily_basic"]),
        "moneyflow_ts": _fetch_moneyflow(provider, keys["moneyflow_ts"]),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Guarded HMM B3 daily-basic/moneyflow gap repair")
    parser.add_argument("mode", choices=("preflight", "apply", "readback", "rollback"))
    parser.add_argument("--universe-key", default=DEFAULT_UNIVERSE)
    parser.add_argument("--source-start", type=_date, default=DEFAULT_START)
    parser.add_argument("--source-end", type=_date, default=DEFAULT_END)
    parser.add_argument("--dataset", action="append", choices=("daily_basic", "moneyflow_ts"))
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--target", choices=("dev", "production"), required=True)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--confirm", default="")
    return parser


def _require_cli(condition: bool, message: str) -> None:
    if not condition:
        raise StockFactGapRepairError(message)


def run(argv: Iterable[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    load_dotenv(args.env_file, override=True)
    conn = None
    commit_attempted = False
    committed = False
    result: dict[str, Any] | None = None
    pending_receipt: Path | None = None
    try:
        _require_cli(
            args.mode == "preflight" or not args.dataset,
            "--dataset is only valid for preflight; apply scope is bound by the plan hash",
        )
        conn = _connect(args.target)
        conn.autocommit = False
        store = PostgresGapStore(conn)
        if args.mode == "preflight":
            with conn.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
            requested_datasets = args.dataset or list(DATASET_ORDER)
            _require_cli(
                len(requested_datasets) == len(set(requested_datasets)),
                "preflight contains duplicate --dataset values",
            )
            result = build_plan(
                store,
                RepairSpec(
                    args.universe_key,
                    args.source_start,
                    args.source_end,
                    tuple(dataset for dataset in DATASET_ORDER if dataset in requested_datasets),
                ),
            )
            conn.rollback()
        elif args.mode == "apply":
            _require_cli(args.plan is not None, "apply requires --plan")
            _require_cli(args.output is not None, "apply requires a durable --output receipt")
            _require_cli(args.confirm == CONFIRM_APPLY, f"apply requires --confirm {CONFIRM_APPLY}")
            plan = _load_json(args.plan)
            rows = fetch_provider_rows(plan)
            result = apply_plan(store, plan, rows)
        elif args.mode == "readback":
            _require_cli(args.receipt is not None, "readback requires --receipt")
            with conn.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
            result = readback_receipt(store, _load_json(args.receipt))
            conn.rollback()
        else:
            _require_cli(args.receipt is not None, "rollback requires --receipt")
            _require_cli(args.output is not None, "rollback requires a durable --output receipt")
            _require_cli(
                args.confirm == CONFIRM_ROLLBACK,
                f"rollback requires --confirm {CONFIRM_ROLLBACK}",
            )
            receipt = _load_json(args.receipt)
            verify_receipt(receipt)
            result = rollback_receipt(store, receipt)
        result = {**result, "target": args.target, "env_file": str(args.env_file.resolve())}
        if args.mode in {"apply", "rollback"}:
            _require_cli(args.output is not None, f"{args.mode} requires a durable --output receipt")
            pending_receipt, _text = _prepare_output(result, args.output)
            commit_attempted = True
            conn.commit()
            committed = True
            _finalize_output(pending_receipt, args.output)
            pending_receipt = None
        else:
            _emit(result, args.output)
        return 0
    except Exception as exc:
        rollback_error = None
        if conn is not None and not committed:
            try:
                conn.rollback()
            except Exception as rollback_exc:
                rollback_error = f"{type(rollback_exc).__name__}: {rollback_exc}"
        if pending_receipt is not None and not committed and pending_receipt.exists():
            pending_receipt.unlink()
            pending_receipt = None
        _emit(
            {
                "schema_version": "hmm_risk_b3_stock_fact_gap_repair_error_v1",
                "status": "failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "target": args.target,
                "env_file": str(args.env_file.resolve()),
                "db_writes": (
                    bool(result and result.get("db_writes") is True)
                    if committed
                    else None
                    if commit_attempted
                    else False
                ),
                "database_commit_status": "committed"
                if committed
                else "unknown"
                if commit_attempted
                else "not_attempted",
                "rollback_error": rollback_error,
                "pending_receipt_path": None if pending_receipt is None else str(pending_receipt.resolve()),
                "ddl": False,
                "runtime_action_performed": False,
            },
            args.output,
        )
        return 2
    finally:
        if conn is not None:
            conn.close()


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
