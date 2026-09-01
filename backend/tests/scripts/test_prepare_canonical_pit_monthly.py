from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import prepare_canonical_pit_monthly as operator


CUTOFF = "2026-08-31"


def _profile(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        profile="qe_hmm_full_v2",
        config_digest="a" * 64,
        control_root=str(tmp_path),
    )


def _config(target: str) -> operator.DatabaseConfig:
    return operator.DatabaseConfig(
        target=target,
        host="127.0.0.1",
        port=5432,
        user="operator",
        password="",
        dbname=f"aistock_{target}",
        credential_location="F:/Dev/AIstock/.env",
    )


def _plan(*, needs_rebuild: bool) -> dict[str, object]:
    return {
        "schema_version": "canonical_pit_monthly_coverage_plan_v1",
        "universe_key": operator.CANONICAL_PIT_UNIVERSE_KEY,
        "rule_version": operator.CANONICAL_PIT_RULE_VERSION,
        "scope": operator.CANONICAL_PIT_SCOPE,
        "start_date": "2018-08-01",
        "requested_end_date": CUTOFF,
        "effective_end_date": CUTOFF,
        "source_fingerprint_sha256": "b" * 64,
        "needs_rebuild": needs_rebuild,
        "reason": "end_coverage_insufficient" if needs_rebuild else "ready",
        "decision": "REBUILD_REQUIRED" if needs_rebuild else "NO_OP_VERIFIED",
        "coverage_satisfied": not needs_rebuild,
        "zero_write": True,
        "state": {"status": "ready", "dirty": False},
    }


def _run(
    tmp_path: Path,
    service: object,
    *args: str,
    target: str = "dev",
) -> tuple[int, Path]:
    receipt = tmp_path / "operator_receipts" / f"{target}-receipt.json"
    code = operator.main(
        [
            "--database",
            target,
            "--cutoff",
            CUTOFF,
            "--receipt-path",
            str(receipt),
            *args,
        ],
        profile_loader=lambda _path: _profile(tmp_path),
        config_loader=lambda database, _path: _config(database),
        service_factory=lambda: service,
    )
    return code, receipt


def test_plan_is_zero_write_and_does_not_call_ensure(tmp_path: Path) -> None:
    class Service:
        def plan_canonical_pit_universe(self, **_kwargs):
            return _plan(needs_rebuild=True)

        def ensure_canonical_pit_universe(self, **_kwargs):
            raise AssertionError("plan must not rebuild")

    code, receipt = _run(tmp_path, Service(), "--mode", "plan")

    value = json.loads(receipt.read_text(encoding="utf-8"))
    assert code == 0
    assert value["operation"] == "PLAN_ONLY"
    assert value["ready_for_monthly"] is False
    assert value["safety"]["database_dml_executed"] is False
    assert "password" not in receipt.read_text(encoding="utf-8").lower()


def test_dev_apply_rebuilds_once_and_requires_exact_readback(tmp_path: Path) -> None:
    class Service:
        def __init__(self):
            self.plans = iter((_plan(needs_rebuild=True), _plan(needs_rebuild=False)))
            self.ensure_calls = 0

        def plan_canonical_pit_universe(self, **_kwargs):
            return next(self.plans)

        def ensure_canonical_pit_universe(self, **kwargs):
            self.ensure_calls += 1
            assert kwargs["force"] is False
            assert kwargs["strict"] is True
            return {"status": "ready", "rebuilt": True}

    service = Service()
    code, receipt = _run(
        tmp_path,
        service,
        "--mode",
        "apply",
    )

    value = json.loads(receipt.read_text(encoding="utf-8"))
    assert code == 0
    assert service.ensure_calls == 1
    assert value["operation"] == "REBUILT_AND_VERIFIED"
    assert value["ready_for_monthly"] is True
    assert value["readback"]["needs_rebuild"] is False


def test_same_cutoff_apply_is_idempotent_noop(tmp_path: Path) -> None:
    class Service:
        def plan_canonical_pit_universe(self, **_kwargs):
            return _plan(needs_rebuild=False)

        def ensure_canonical_pit_universe(self, **_kwargs):
            raise AssertionError("ready same-cutoff apply must remain a no-op")

    code, receipt = _run(
        tmp_path,
        Service(),
        "--mode",
        "apply",
        "--authorization-ref",
        "user:BUG-1243:dev",
    )

    value = json.loads(receipt.read_text(encoding="utf-8"))
    assert code == 0
    assert value["operation"] == "NO_OP_VERIFIED"
    assert value["safety"]["database_dml_executed"] is False


def test_production_apply_requires_matching_dev_receipt_before_database_access(tmp_path: Path) -> None:
    with pytest.raises(operator.CanonicalPitMonthlyOperatorError, match="requires --dev-receipt"):
        _run(
            tmp_path,
            object(),
            "--mode",
            "apply",
            "--authorization-ref",
            "user:BUG-1243:production",
            target="production",
        )


def test_production_apply_accepts_only_same_cutoff_contract_receipt(tmp_path: Path) -> None:
    dev_receipt = tmp_path / "dev.json"
    dev_value = {
        "schema_version": operator.RECEIPT_SCHEMA_VERSION,
        "database_target": "dev",
        "database_identity_digest": "d" * 64,
        "mode": "apply",
        "status": "PASS",
        "profile": "qe_hmm_full_v2",
        "profile_config_digest": "a" * 64,
        "cutoff": CUTOFF,
        "operator_contract_digest": operator.OPERATOR_CONTRACT_DIGEST,
        "operation": "NO_OP_VERIFIED",
        "ready_for_monthly": True,
        "readback": {"coverage_satisfied": True, "needs_rebuild": False},
    }
    dev_value["receipt_digest"] = operator._digest(dev_value)
    dev_receipt.write_text(
        json.dumps(dev_value),
        encoding="utf-8",
    )

    class Service:
        def plan_canonical_pit_universe(self, **_kwargs):
            return _plan(needs_rebuild=False)

        def ensure_canonical_pit_universe(self, **_kwargs):
            raise AssertionError("ready production state must not rebuild")

    code, receipt = _run(
        tmp_path,
        Service(),
        "--mode",
        "apply",
        "--authorization-ref",
        "user:BUG-1243:production",
        "--dev-receipt",
        str(dev_receipt),
        target="production",
    )

    value = json.loads(receipt.read_text(encoding="utf-8"))
    assert code == 0
    assert value["database_target"] == "production"
    assert value["dev_receipt_digest"]
    assert value["operation"] == "NO_OP_VERIFIED"


def test_production_apply_rejects_tampered_dev_receipt(tmp_path: Path) -> None:
    dev_receipt = tmp_path / "tampered-dev.json"
    dev_receipt.write_text(
        json.dumps(
            {
                "schema_version": operator.RECEIPT_SCHEMA_VERSION,
                "database_target": "dev",
                "database_identity_digest": "d" * 64,
                "mode": "apply",
                "status": "PASS",
                "profile": "qe_hmm_full_v2",
                "profile_config_digest": "a" * 64,
                "cutoff": CUTOFF,
                "operator_contract_digest": operator.OPERATOR_CONTRACT_DIGEST,
                "operation": "NO_OP_VERIFIED",
                "ready_for_monthly": True,
                "readback": {"coverage_satisfied": True, "needs_rebuild": False},
                "receipt_digest": "0" * 64,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(operator.CanonicalPitMonthlyOperatorError, match="does not authorize"):
        _run(
            tmp_path,
            object(),
            "--mode",
            "apply",
            "--authorization-ref",
            "user:BUG-1243:production",
            "--dev-receipt",
            str(dev_receipt),
            target="production",
        )


def test_verify_blocks_when_cutoff_is_not_covered(tmp_path: Path) -> None:
    class Service:
        def plan_canonical_pit_universe(self, **_kwargs):
            return _plan(needs_rebuild=True)

    code, receipt = _run(tmp_path, Service(), "--mode", "verify")

    value = json.loads(receipt.read_text(encoding="utf-8"))
    assert code == 2
    assert value["status"] == "BLOCKED"
    assert value["operation"] == "READBACK_BLOCKED"


def test_apply_refuses_to_create_missing_schema_contract(tmp_path: Path) -> None:
    class Service:
        def plan_canonical_pit_universe(self, **_kwargs):
            return {**_plan(needs_rebuild=True), "reason": "schema_contract_missing"}

        def ensure_canonical_pit_universe(self, **_kwargs):
            raise AssertionError("missing schema must fail before ensure")

    with pytest.raises(operator.CanonicalPitMonthlyOperatorError, match="cannot perform DDL"):
        _run(
            tmp_path,
            Service(),
            "--mode",
            "apply",
            "--authorization-ref",
            "user:BUG-1243:dev",
        )
