"""Plan, apply, and verify canonical PIT monthly cutoff coverage.

This is the only operator entrypoint for extending the rolling canonical PIT
state used by monthly dataset preparation.  Plan and verify are zero-write.
Apply is target-explicit and followed by an exact readback.  Production apply
is authorization-bound and additionally requires a successful DEV apply
receipt for the same cutoff and operator contract.

The operator never activates an authority pointer, exports a dataset, calls a
provider, or controls a service process.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import json
import math
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.canonical_equity_pit import (  # noqa: E402
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SCOPE,
    CANONICAL_PIT_UNIVERSE_KEY,
)
from backend.services.dataset_release.profile import load_dataset_profile  # noqa: E402
from backend.services.stock_universe_pit_service import (  # noqa: E402
    DEFAULT_ST_PIT_START_DATE,
    StockUniversePitService,
)


SCHEMA_VERSION = "canonical_pit_monthly_operator_v1"
RECEIPT_SCHEMA_VERSION = "canonical_pit_monthly_operator_receipt_v1"
_TARGETS = frozenset({"dev", "production"})
_MODES = frozenset({"plan", "apply", "verify"})
_AUTHORIZATION_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/#-]{2,255}$")
_DATABASE_ENV_KEYS = (
    "TDX_DB_HOST",
    "TDX_DB_PORT",
    "TDX_DB_USER",
    "TDX_DB_PASSWORD",
    "TDX_DB_NAME",
)


class CanonicalPitMonthlyOperatorError(RuntimeError):
    """Fail-closed operator error whose message contains no credential value."""


def _canonical(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        raise CanonicalPitMonthlyOperatorError("canonical receipt cannot contain a non-finite number")
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _canonical(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


OPERATOR_CONTRACT = {
    "schema_version": SCHEMA_VERSION,
    "universe_key": CANONICAL_PIT_UNIVERSE_KEY,
    "rule_version": CANONICAL_PIT_RULE_VERSION,
    "scope": CANONICAL_PIT_SCOPE,
    "start_date": DEFAULT_ST_PIT_START_DATE.isoformat(),
    "knowledge_policy": "canonical_source_fingerprint_exact",
    "production_requires_matching_dev_apply_receipt": True,
    "pointer_activation": False,
}
OPERATOR_CONTRACT_DIGEST = _digest(OPERATOR_CONTRACT)


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    target: str
    host: str
    port: int
    user: str
    password: str
    dbname: str
    credential_location: str

    @property
    def identity_digest(self) -> str:
        return _digest(
            {
                "schema_version": "canonical_pit_database_target_identity_v1",
                "target": self.target,
                "host": self.host,
                "port": self.port,
                "user": self.user,
                "dbname": self.dbname,
            }
        )


def _load_database_config(target: str, env_file: Path) -> DatabaseConfig:
    if target not in _TARGETS:
        raise CanonicalPitMonthlyOperatorError("database target must be dev or production")
    resolved = env_file.resolve(strict=True)
    values = dotenv_values(resolved)
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    names = {
        "host": f"{prefix}HOST",
        "port": f"{prefix}PORT",
        "user": f"{prefix}USER",
        "password": f"{prefix}PASSWORD",
        "dbname": f"{prefix}NAME",
    }
    missing = [key for key, name in names.items() if not str(values.get(name) or "").strip()]
    if missing:
        raise CanonicalPitMonthlyOperatorError(
            f"{target} database credential location is missing required keys: {missing}"
        )
    try:
        port = int(str(values[names["port"]]))
    except (TypeError, ValueError) as exc:
        raise CanonicalPitMonthlyOperatorError(f"{target} database port is invalid") from exc
    if not 1 <= port <= 65535:
        raise CanonicalPitMonthlyOperatorError(f"{target} database port is invalid")
    return DatabaseConfig(
        target=target,
        host=str(values[names["host"]]).strip(),
        port=port,
        user=str(values[names["user"]]).strip(),
        password=str(values[names["password"]]),
        dbname=str(values[names["dbname"]]).strip(),
        credential_location=str(resolved),
    )


@contextlib.contextmanager
def _database_target_environment(config: DatabaseConfig) -> Iterator[None]:
    """Bind the fresh operator process to one explicit database target."""

    before = {key: os.environ.get(key) for key in _DATABASE_ENV_KEYS}
    values = {
        "TDX_DB_HOST": config.host,
        "TDX_DB_PORT": str(config.port),
        "TDX_DB_USER": config.user,
        "TDX_DB_PASSWORD": config.password,
        "TDX_DB_NAME": config.dbname,
    }
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in before.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _read_dev_receipt(path: Path, *, cutoff: dt.date, profile: Any) -> dict[str, Any]:
    try:
        value = json.loads(path.resolve(strict=True).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CanonicalPitMonthlyOperatorError("DEV receipt is unreadable or invalid JSON") from exc
    claimed_digest = value.get("receipt_digest")
    semantic = {key: item for key, item in value.items() if key != "receipt_digest"}
    required = (
        value.get("schema_version") == RECEIPT_SCHEMA_VERSION
        and value.get("database_target") == "dev"
        and isinstance(value.get("database_identity_digest"), str)
        and len(value["database_identity_digest"]) == 64
        and value.get("mode") == "apply"
        and value.get("status") == "PASS"
        and value.get("profile") == profile.profile
        and value.get("profile_config_digest") == profile.config_digest
        and value.get("cutoff") == cutoff.isoformat()
        and value.get("operator_contract_digest") == OPERATOR_CONTRACT_DIGEST
        and value.get("operation") in {"REBUILT_AND_VERIFIED", "NO_OP_VERIFIED"}
        and value.get("ready_for_monthly") is True
        and value.get("readback", {}).get("coverage_satisfied") is True
        and value.get("readback", {}).get("needs_rebuild") is False
        and isinstance(claimed_digest, str)
        and claimed_digest == _digest(semantic)
    )
    if not required:
        raise CanonicalPitMonthlyOperatorError(
            "DEV receipt does not authorize this production cutoff and operator contract"
        )
    return value


def _require_apply_authorization(
    *,
    target: str,
    authorization_ref: str | None,
    dev_receipt: Path | None,
    cutoff: dt.date,
    profile: Any,
) -> str | None:
    if target != "production":
        return None
    if not authorization_ref or not _AUTHORIZATION_REF.fullmatch(authorization_ref):
        raise CanonicalPitMonthlyOperatorError(
            "production apply requires a bounded non-secret --authorization-ref"
        )
    if dev_receipt is None:
        raise CanonicalPitMonthlyOperatorError(
            "production apply requires --dev-receipt from a successful matching DEV apply/readback"
        )
    return _digest(_read_dev_receipt(dev_receipt, cutoff=cutoff, profile=profile))


def _write_receipt(path: Path, value: Mapping[str, Any], *, allowed_root: Path) -> None:
    resolved = path.resolve(strict=False)
    root = allowed_root.resolve(strict=False)
    try:
        resolved.relative_to(REPO_ROOT.resolve())
    except ValueError:
        pass
    else:
        raise CanonicalPitMonthlyOperatorError("receipt path must be repo-external")
    if resolved.parent != root:
        raise CanonicalPitMonthlyOperatorError("receipt path must be a direct child of operator_receipts root")
    root.mkdir(parents=True, exist_ok=True)
    if resolved.exists():
        raise CanonicalPitMonthlyOperatorError("receipt path already exists; receipts are immutable")
    raw = (json.dumps(_canonical(value), ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode(
        "utf-8"
    )
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{resolved.name}.", suffix=".partial", dir=root)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, resolved)
    finally:
        temporary.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default=str(REPO_ROOT / "configs/datasets/qe_backtest_monthly_v2.yaml"))
    parser.add_argument("--database", choices=sorted(_TARGETS), required=True)
    parser.add_argument("--mode", choices=sorted(_MODES), default="plan")
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--env-file", default=str(REPO_ROOT / ".env"))
    parser.add_argument("--authorization-ref")
    parser.add_argument("--dev-receipt")
    parser.add_argument("--receipt-path", required=True)
    return parser


def _receipt(
    *,
    mode: str,
    target: DatabaseConfig,
    profile: Any,
    cutoff: dt.date,
    authorization_ref: str | None,
    dev_receipt_digest: str | None,
    preflight: Mapping[str, Any],
    readback: Mapping[str, Any],
    operation: str,
) -> dict[str, Any]:
    ready = bool(readback.get("coverage_satisfied")) and not bool(readback.get("needs_rebuild"))
    status = "PASS" if mode == "plan" or ready else "BLOCKED"
    semantic = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "status": status,
        "mode": mode,
        "database_target": target.target,
        "database_identity_digest": target.identity_digest,
        "credential_location": target.credential_location,
        "profile": profile.profile,
        "profile_config_digest": profile.config_digest,
        "cutoff": cutoff.isoformat(),
        "operator_contract_digest": OPERATOR_CONTRACT_DIGEST,
        "authorization_ref": authorization_ref,
        "dev_receipt_digest": dev_receipt_digest,
        "operation": operation,
        "ready_for_monthly": ready,
        "preflight": preflight,
        "readback": readback,
        "safety": {
            "database_dml_executed": mode == "apply" and operation == "REBUILT_AND_VERIFIED",
            "database_ddl_migration": False,
            "authority_pointer_changes": 0,
            "dataset_writes": 0,
            "provider_calls": 0,
            "service_process_controls": 0,
        },
    }
    return {**_canonical(semantic), "receipt_digest": _digest(semantic)}


def main(
    argv: Sequence[str] | None = None,
    *,
    profile_loader: Callable[[str], Any] = load_dataset_profile,
    config_loader: Callable[[str, Path], DatabaseConfig] = _load_database_config,
    service_factory: Callable[[], StockUniversePitService] = StockUniversePitService,
) -> int:
    args = _parser().parse_args(argv)
    cutoff = dt.date.fromisoformat(args.cutoff)
    if cutoff < DEFAULT_ST_PIT_START_DATE:
        raise CanonicalPitMonthlyOperatorError("cutoff precedes the fixed canonical PIT start date")
    profile = profile_loader(args.profile)
    target = config_loader(args.database, Path(args.env_file))
    dev_receipt_digest = None
    if args.mode == "apply":
        dev_receipt_digest = _require_apply_authorization(
            target=args.database,
            authorization_ref=args.authorization_ref,
            dev_receipt=Path(args.dev_receipt) if args.dev_receipt else None,
            cutoff=cutoff,
            profile=profile,
        )

    with _database_target_environment(target):
        service = service_factory()
        preflight = service.plan_canonical_pit_universe(
            start_date=DEFAULT_ST_PIT_START_DATE,
            end_date=cutoff,
        )
        operation = "PLAN_ONLY"
        readback = preflight
        if args.mode == "apply":
            if preflight.get("reason") == "schema_contract_missing":
                raise CanonicalPitMonthlyOperatorError(
                    "canonical PIT schema contract is missing; operator apply cannot perform DDL"
                )
            if preflight.get("needs_rebuild"):
                apply_result = service.ensure_canonical_pit_universe(
                    start_date=DEFAULT_ST_PIT_START_DATE,
                    end_date=cutoff,
                    force=False,
                    strict=True,
                    rebuild_if_stale=True,
                )
                operation = "REBUILT_AND_VERIFIED" if apply_result.get("rebuilt") is True else "NO_OP_VERIFIED"
            else:
                operation = "NO_OP_VERIFIED"
            readback = service.plan_canonical_pit_universe(
                start_date=DEFAULT_ST_PIT_START_DATE,
                end_date=cutoff,
            )
            if readback.get("needs_rebuild") or not readback.get("coverage_satisfied"):
                raise CanonicalPitMonthlyOperatorError("canonical PIT apply readback does not satisfy the cutoff")
        elif args.mode == "verify":
            operation = "READBACK_VERIFIED" if preflight.get("coverage_satisfied") else "READBACK_BLOCKED"

    value = _receipt(
        mode=args.mode,
        target=target,
        profile=profile,
        cutoff=cutoff,
        authorization_ref=args.authorization_ref,
        dev_receipt_digest=dev_receipt_digest,
        preflight=preflight,
        readback=readback,
        operation=operation,
    )
    _write_receipt(
        Path(args.receipt_path),
        value,
        allowed_root=Path(profile.control_root) / "operator_receipts",
    )
    print(_canonical_json(value))
    return 0 if value["status"] == "PASS" else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (CanonicalPitMonthlyOperatorError, OSError, ValueError) as exc:
        print(
            _canonical_json(
                {
                    "schema_version": "canonical_pit_monthly_operator_error_v1",
                    "status": "ERROR",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                }
            ),
            file=sys.stderr,
        )
        raise SystemExit(2)
