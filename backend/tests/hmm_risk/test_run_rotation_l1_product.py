from __future__ import annotations

import json
from argparse import Namespace
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from scripts.hmm_risk import run_rotation_l1_product as subject


MODEL_HASH = "a" * 64
ACCEPTANCE_HASH = "b" * 64


def _write(path: Path, value: dict[str, Any]) -> Path:
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def _arguments(tmp_path: Path, *, write_database: bool = False) -> Namespace:
    child_hashes = ["c" * 64, "d" * 64]
    acceptance = {
        "acceptance_sha256": ACCEPTANCE_HASH,
        "contract_version": subject.V16_CONTRACT_VERSION,
        "status": "development_complete",
        "tail_accessed": False,
        "child_sha256s": child_hashes,
    }
    payload = {
        "contract_version": subject.V16_CONTRACT_VERSION,
        "tail_access_gate": {"passed": True},
        "tail_accessed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
        "forward_power_status": "INSUFFICIENT",
        "final_model": {
            "model_kind": "deterministic_cross_section_rank",
            "model_sha256": MODEL_HASH,
            "scoring_contract_sha256": MODEL_HASH,
            "training_performed": False,
        },
        "profile": {"model_kind": "deterministic_cross_section_rank", "fit_required": False},
        "development_summary": {
            "mean_rank_ic": 0.03,
            "hac_lower_two_sided_95pct": -0.01,
            "hac_upper_two_sided_95pct": 0.07,
        },
    }
    first = {
        "report_sha256": child_hashes[0],
        "final_model_text": "{}",
        "reproducibility_payload": payload,
    }
    second = {
        "report_sha256": child_hashes[1],
        "final_model_text": "{}",
        "reproducibility_payload": payload,
    }
    input_root = tmp_path / "input"
    input_root.mkdir()
    work_parent = tmp_path / "work"
    work_parent.mkdir()
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    return Namespace(
        development_acceptance=_write(tmp_path / "acceptance.json", acceptance),
        development_acceptance_sha256=ACCEPTANCE_HASH,
        development_child_1=_write(tmp_path / "child1.json", first),
        development_child_2=_write(tmp_path / "child2.json", second),
        v14_process_file=_write(tmp_path / "v14.json", {"authority": "v1.4"}),
        input_root=input_root,
        direct_v2_candidate_root=candidate,
        security_identity_manifest=_write(tmp_path / "security.json", {}),
        provider_absence_manifest=_write(tmp_path / "absence.json", {}),
        industry_pit_authority=_write(tmp_path / "industry.json", {"authority": "PIT"}),
        trade_date=date(2026, 8, 31),
        as_of_date=date(2026, 8, 28),
        work_parent=work_parent,
        output_receipt=tmp_path / "execution.json",
        write_database=write_database,
        expected_database_name="aistock_prod" if write_database else None,
    )


def _rows(args: Namespace) -> list[dict[str, Any]]:
    return [
        {
            "trade_date": args.trade_date,
            "as_of_date": args.as_of_date,
            "sector_code": f"80{index:04d}.SI",
            "model_hash": MODEL_HASH,
            "input_hash": "1" * 64,
            "mapping_snapshot_hash": "2" * 64,
            "availability": "available",
            "rotation_l1_capability_status": subject.RESEARCH_CAPABILITY,
            "forward_power_status": "INSUFFICIENT",
            "forward_confirmation": "PENDING_INSUFFICIENT_POWER",
            "validation_basis": "single_date_frozen_model",
            "tail_accessed": False,
        }
        for index in range(31)
    ]


def _install_valid_authorities(monkeypatch: pytest.MonkeyPatch, args: Namespace) -> None:
    acceptance = json.loads(args.development_acceptance.read_text(encoding="utf-8"))
    monkeypatch.setattr(subject, "validate_v14_process_reference", lambda _value: None)
    monkeypatch.setattr(subject, "read_input_bundle", lambda *_args, **_kwargs: {"bundle": {"frozen": True}})
    monkeypatch.setattr(subject, "close_processes", lambda *_args, **_kwargs: acceptance)


def _install_prediction(monkeypatch: pytest.MonkeyPatch, args: Namespace) -> None:
    def predict(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["trade_date"] == args.trade_date
        assert kwargs["as_of_date"] == args.as_of_date
        assert kwargs["capability_status"] == subject.RESEARCH_CAPABILITY
        assert kwargs["forward_power_status"] == "INSUFFICIENT"
        assert kwargs["forward_confirmation"] == "PENDING_INSUFFICIENT_POWER"
        assert kwargs["forbidden_roots"] == (subject.ROOT,)
        return {
            "rows": _rows(args),
            "source_receipt": {"receipt_sha256": "e" * 64, "target_columns_read": False},
        }

    monkeypatch.setattr(subject, "predict_single_date_from_assets", predict)


def test_dry_run_binds_frozen_authorities_and_never_writes_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _arguments(tmp_path)
    _install_valid_authorities(monkeypatch, args)
    _install_prediction(monkeypatch, args)
    monkeypatch.setattr(
        subject,
        "_repository",
        lambda _target: pytest.fail("dry run must not construct a database repository"),
    )

    receipt = subject.execute(args)

    assert receipt["mode"] == "dry_run"
    assert receipt["row_count"] == 31
    assert receipt["sector_count"] == 31
    assert receipt["available_count"] == 31
    assert receipt["model_sha256"] == MODEL_HASH
    assert receipt["input_sha256"] == "1" * 64
    assert receipt["mapping_snapshot_sha256"] == "2" * 64
    assert receipt["development_process_sha256s"] == ["c" * 64, "d" * 64]
    assert receipt["tail_accessed"] is False
    assert receipt["target_columns_read"] is False
    assert receipt["model_fit_count"] == 0
    assert receipt["database_write_performed"] is False
    assert receipt["runtime_action_performed"] is False
    stored = json.loads(args.output_receipt.read_text(encoding="utf-8"))
    assert stored == receipt
    assert receipt["receipt_sha256"] == subject.canonical_sha256(
        {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    )


def test_database_mode_requires_exact_target_and_persists_repository_readback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _arguments(tmp_path, write_database=True)
    _install_valid_authorities(monkeypatch, args)
    _install_prediction(monkeypatch, args)
    calls: list[list[dict[str, Any]]] = []

    class Repository:
        def write_rows(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
            calls.append(rows)
            return {
                "row_count": 31,
                "canonical_row_sha256": subject.canonical_sha256(subject._canonical_rows(rows)),
                "idempotency_verified": True,
            }

    def repository(target: str) -> Repository:
        assert target == "aistock_prod"
        return Repository()

    monkeypatch.setattr(subject, "_repository", repository)

    receipt = subject.execute(args)

    assert len(calls) == 1
    assert len(calls[0]) == 31
    assert receipt["mode"] == "database_write"
    assert receipt["database_target"] == "aistock_prod"
    assert receipt["database_write_performed"] is True
    assert receipt["database_write_receipt"]["idempotency_verified"] is True


def test_database_readback_mismatch_is_typed_after_committed_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _arguments(tmp_path, write_database=True)
    _install_valid_authorities(monkeypatch, args)
    _install_prediction(monkeypatch, args)

    class Repository:
        def write_rows(self, _rows: list[dict[str, Any]]) -> dict[str, Any]:
            return {"row_count": 31, "canonical_row_sha256": "f" * 64, "idempotency_verified": True}

    monkeypatch.setattr(subject, "_repository", lambda _target: Repository())

    with pytest.raises(subject.RotationL1ProductExecutorError) as caught:
        subject.execute(args)

    assert caught.value.reason_code == subject.REASON_EXECUTOR_RESULT
    assert caught.value.context == {"database_write_performed": True}
    failure = subject._failure(caught.value)
    assert failure["database_write_performed"] is True


def test_database_connection_rejects_a_different_actual_target(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.db import pg_pool

    class Cursor:
        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def execute(self, sql: str) -> None:
            assert sql == "SELECT current_database()"

        def fetchone(self) -> tuple[str]:
            return ("aistock_dev",)

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

    @contextmanager
    def get_conn(**kwargs: Any):
        assert kwargs == {"autocommit": False, "manage_transaction": True}
        yield Connection()

    monkeypatch.setattr(pg_pool, "get_conn", get_conn)

    with pytest.raises(subject.RotationL1ProductExecutorError) as caught:
        with subject._database_connection("aistock_prod"):
            pytest.fail("a mismatched database target must fail before yielding the connection")

    assert caught.value.reason_code == subject.REASON_EXECUTOR_DATABASE
    assert caught.value.context == {"expected_database": "aistock_prod", "actual_database": "aistock_dev"}


@pytest.mark.parametrize(
    ("write_database", "expected_database_name"),
    [(True, None), (False, "aistock_prod")],
)
def test_database_mode_and_target_must_be_explicitly_coupled(
    tmp_path: Path,
    write_database: bool,
    expected_database_name: str | None,
) -> None:
    args = _arguments(tmp_path, write_database=write_database)
    args.expected_database_name = expected_database_name

    with pytest.raises(subject.RotationL1ProductExecutorError, match="expected-database-name"):
        subject.execute(args)


def test_fresh_process_or_acceptance_drift_fails_before_prediction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _arguments(tmp_path)
    monkeypatch.setattr(subject, "validate_v14_process_reference", lambda _value: None)
    monkeypatch.setattr(subject, "read_input_bundle", lambda *_args, **_kwargs: {"bundle": {}})
    monkeypatch.setattr(subject, "close_processes", lambda *_args, **_kwargs: {"different": True})
    monkeypatch.setattr(
        subject,
        "predict_single_date_from_assets",
        lambda **_kwargs: pytest.fail("authority mismatch must fail before prediction"),
    )

    with pytest.raises(subject.RotationL1ProductExecutorError) as caught:
        subject.execute(args)

    assert caught.value.reason_code == subject.REASON_EXECUTOR_AUTHORITY


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("contract_version", "unknown"),
        ("tail_accessed", True),
        ("database_write_performed", True),
        ("runtime_action_performed", True),
    ],
)
def test_non_v16_or_mutating_development_authority_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: Any,
) -> None:
    args = _arguments(tmp_path)
    first = json.loads(args.development_child_1.read_text(encoding="utf-8"))
    first["reproducibility_payload"][field] = value
    _write(args.development_child_1, first)
    acceptance = json.loads(args.development_acceptance.read_text(encoding="utf-8"))
    monkeypatch.setattr(subject, "validate_v14_process_reference", lambda _value: None)
    monkeypatch.setattr(subject, "read_input_bundle", lambda *_args, **_kwargs: {"bundle": {}})
    monkeypatch.setattr(subject, "close_processes", lambda *_args, **_kwargs: acceptance)

    with pytest.raises(subject.RotationL1ProductExecutorError) as caught:
        subject.execute(args)

    assert caught.value.reason_code == subject.REASON_EXECUTOR_AUTHORITY


@pytest.mark.parametrize("row_mutation", ["short", "duplicate", "mixed_model", "wrong_date"])
def test_result_requires_exact_31_sector_single_date_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    row_mutation: str,
) -> None:
    args = _arguments(tmp_path)
    _install_valid_authorities(monkeypatch, args)
    rows = _rows(args)
    if row_mutation == "short":
        rows.pop()
    elif row_mutation == "duplicate":
        rows[-1]["sector_code"] = rows[0]["sector_code"]
    elif row_mutation == "mixed_model":
        rows[-1]["model_hash"] = "f" * 64
    else:
        rows[-1]["trade_date"] = date(2026, 8, 28)
    monkeypatch.setattr(
        subject,
        "predict_single_date_from_assets",
        lambda **_kwargs: {"rows": rows, "source_receipt": {"receipt_sha256": "e" * 64}},
    )

    with pytest.raises(subject.RotationL1ProductExecutorError) as caught:
        subject.execute(args)

    assert caught.value.reason_code == subject.REASON_EXECUTOR_RESULT


def test_cli_hash_mismatch_writes_typed_failure_and_no_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    args = _arguments(tmp_path)
    argv = [
        "--development-acceptance",
        str(args.development_acceptance),
        "--development-acceptance-sha256",
        "0" * 64,
        "--development-child-1",
        str(args.development_child_1),
        "--development-child-2",
        str(args.development_child_2),
        "--v14-process-file",
        str(args.v14_process_file),
        "--input-root",
        str(args.input_root),
        "--direct-v2-candidate-root",
        str(args.direct_v2_candidate_root),
        "--security-identity-manifest",
        str(args.security_identity_manifest),
        "--provider-absence-manifest",
        str(args.provider_absence_manifest),
        "--industry-pit-authority",
        str(args.industry_pit_authority),
        "--trade-date",
        args.trade_date.isoformat(),
        "--as-of-date",
        args.as_of_date.isoformat(),
        "--work-parent",
        str(args.work_parent),
        "--output-receipt",
        str(args.output_receipt),
    ]
    monkeypatch.setattr(
        subject,
        "predict_single_date_from_assets",
        lambda **_kwargs: pytest.fail("hash mismatch must fail before prediction"),
    )

    assert subject.main(argv) == 2
    assert not args.output_receipt.exists()
    failures = list(tmp_path.glob(f".{args.output_receipt.name}.failure.*.json"))
    assert len(failures) == 1
    failure = json.loads(failures[0].read_text(encoding="utf-8"))
    assert failure["reason_code"] == subject.REASON_EXECUTOR_AUTHORITY
    assert failure["database_write_performed"] is False
    assert failure["tail_accessed"] is False
    assert failure["runtime_action_performed"] is False


def test_output_must_be_external_and_create_exclusive(tmp_path: Path) -> None:
    args = _arguments(tmp_path)
    args.output_receipt.write_text("occupied", encoding="utf-8")
    with pytest.raises(subject.RotationL1ProductExecutorError, match="already exists"):
        subject.execute(args)

    args.output_receipt = subject.ROOT / "tmp" / "forbidden.json"
    with pytest.raises(subject.RotationL1ProductExecutorError, match="outside the repository"):
        subject.execute(args)
