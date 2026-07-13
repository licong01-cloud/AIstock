"""Explicit SIM-only manual rebuild path for a MiniQMT TCA receipt."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime
from typing import Callable, Sequence

from backend.services.qmt_strategy_ledger.tca_rebuild import (
    ExecutionTcaRebuildService,
    TcaRebuildRequest,
)
from backend.services.qmt_strategy_ledger.tca_repository import ExecutionTcaRebuildScope
from backend.services.qmt_strategy_ledger.tca_read_service import TcaReadError, TcaReadRuntimeConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rebuild one MiniQMT SIM TCA evidence receipt.")
    parser.add_argument("--binding-id", required=True)
    parser.add_argument("--trade-date", required=True, help="ISO YYYY-MM-DD")
    parser.add_argument("--account-id", required=True, help="internal SIM account identity; never printed")
    parser.add_argument("--as-of", required=True, help="ISO timestamp with UTC offset")
    parser.add_argument("--snapshot-kind", default="RECONCILED_FINAL")
    parser.add_argument("--code-commit", required=True)
    parser.add_argument("--operator-pseudonym", required=True)
    parser.add_argument("--execute", action="store_true", help="perform immutable TCA materialization")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    rebuild_service_factory: Callable[[], ExecutionTcaRebuildService] = ExecutionTcaRebuildService,
    config_provider: Callable[[], TcaReadRuntimeConfig] = TcaReadRuntimeConfig.from_environ,
) -> int:
    args = build_parser().parse_args(argv)
    try:
        trade_date = _parse_trade_date(args.trade_date)
        as_of_time = _parse_as_of(args.as_of)
        account_id = _required_text(args.account_id, "account_id")
        config = config_provider()
        pseudonymizer = config.require_pseudonymizer()
        request = TcaRebuildRequest(
            scope=ExecutionTcaRebuildScope(
                binding_ids=(_required_text(args.binding_id, "binding_id"),),
                trade_date_from=trade_date,
                trade_date_to=trade_date,
                account_ids=(account_id,),
                environment="SIM",
            ),
            snapshot_kind=_required_text(args.snapshot_kind, "snapshot_kind"),
            as_of_time=as_of_time,
            account_pseudonyms={account_id: pseudonymizer.pseudonymize(account_id)},
            account_pseudonym_key_version=pseudonymizer.key_version,
            code_commit=_required_text(args.code_commit, "code_commit"),
            operator_pseudonym=_required_text(args.operator_pseudonym, "operator_pseudonym"),
        )
        if not args.execute:
            print(json.dumps(_request_summary(request, dry_run=True), ensure_ascii=False, sort_keys=True))
            return 0
        outcome = rebuild_service_factory().rebuild(request)
    except TcaReadError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - CLI must return a loud but non-secret failure receipt.
        payload = {
            "error_code": "ADAPTIVE_IS_TCA_MANUAL_REBUILD_FAILED",
            "message": "manual SIM TCA rebuild failed",
            "context": {"reason_code": "ADAPTIVE_IS_TCA_MANUAL_REBUILD_FAILED", "error_type": type(exc).__name__},
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 3
    print(
        json.dumps(
            {
                **_request_summary(request, dry_run=False),
                "receipt_id": outcome.receipt_id,
                "receipt_status": outcome.receipt_status,
                "receipt_generation": outcome.receipt_generation,
                "result_ids": list(outcome.result_ids),
                "reused": outcome.reused,
                "reason_code": outcome.reason_code,
                "stage": outcome.stage,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


def _request_summary(request: TcaRebuildRequest, *, dry_run: bool) -> dict[str, object]:
    return {
        "ok": True,
        "dry_run": dry_run,
        "environment": request.scope.environment,
        "binding_ids": list(request.scope.binding_ids),
        "trade_date_from": request.scope.trade_date_from.isoformat(),
        "trade_date_to": request.scope.trade_date_to.isoformat(),
        "account_pseudonyms": sorted(request.account_pseudonyms.values()),
        "account_pseudonym_key_version": request.account_pseudonym_key_version,
        "snapshot_kind": request.snapshot_kind,
        "as_of_time": request.as_of_time.isoformat(),
        "code_commit": request.code_commit,
        "operator_pseudonym": request.operator_pseudonym,
    }


def _parse_trade_date(value: str) -> date:
    try:
        return date.fromisoformat(_required_text(value, "trade_date"))
    except ValueError as exc:
        raise TcaReadError(
            "ADAPTIVE_IS_TCA_MANUAL_REBUILD_REQUEST_INVALID",
            "trade_date must be ISO YYYY-MM-DD",
            http_status=400,
            stage="TCA_MANUAL_REBUILD",
            context={"field": "trade_date"},
        ) from exc


def _parse_as_of(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(_required_text(value, "as_of").replace("Z", "+00:00"))
    except ValueError as exc:
        raise TcaReadError(
            "ADAPTIVE_IS_TCA_MANUAL_REBUILD_REQUEST_INVALID",
            "as_of must be ISO timestamp with UTC offset",
            http_status=400,
            stage="TCA_MANUAL_REBUILD",
            context={"field": "as_of"},
        ) from exc
    if parsed.tzinfo is None:
        raise TcaReadError(
            "ADAPTIVE_IS_TCA_MANUAL_REBUILD_REQUEST_INVALID",
            "as_of must include a UTC offset",
            http_status=400,
            stage="TCA_MANUAL_REBUILD",
            context={"field": "as_of"},
        )
    return parsed.astimezone(UTC)


def _required_text(value: str, field: str) -> str:
    text = str(value or "").strip()
    if text:
        return text
    raise TcaReadError(
        "ADAPTIVE_IS_TCA_MANUAL_REBUILD_REQUEST_INVALID",
        f"{field} must not be empty",
        http_status=400,
        stage="TCA_MANUAL_REBUILD",
        context={"field": field},
    )


if __name__ == "__main__":
    raise SystemExit(main())
