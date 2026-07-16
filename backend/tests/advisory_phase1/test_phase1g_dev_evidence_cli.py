from __future__ import annotations

import json
import logging

from backend.services.advisory_phase1.phase1g_dev_evidence_contract import (
    EvidenceKind,
    Phase1GDevEvidenceRef,
    REASON_L4_PLAN_STALE,
    REASON_UNEXPECTED_ERROR,
)
from backend.services.advisory_phase1.phase1g_dev_evidence_store import (
    Phase1GDevEvidenceStore,
)
from backend.tests.advisory_phase1.test_phase1g_dev_evidence_contract import (
    _inventory,
)
from scripts import advisory_phase1g_dev_evidence as cli


def _write(path, payload) -> None:  # type: ignore[no-untyped-def]
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_verify_evidence_is_offline_and_canonical(tmp_path, capsys) -> None:  # type: ignore[no-untyped-def]
    store = Phase1GDevEvidenceStore(root=tmp_path / "g5")
    stored = store.publish(_inventory())
    ref_path = tmp_path / "ref.json"
    _write(ref_path, stored.ref.model_dump(mode="json"))
    exit_code = cli.main(
        [
            "verify-evidence",
            "--evidence-ref",
            str(ref_path),
            "--g5-evidence-root",
            str(store.root),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert len(output["reference_closure_hash"]) == 64
    assert output == {
        "command": "verify-evidence",
        "db_readback": False,
        "evidence_kind": "inventory",
        "model_schema_version": "advisory_phase1g_g5_dev_evidence_v1",
        "ok": True,
        "reference_closure_hash": output["reference_closure_hash"],
        "referenced_readback_hash": None,
        "semantic_content_hash": stored.ref.semantic_content_hash,
    }


def test_verify_rejects_ref_tamper_without_traceback_on_stdout(tmp_path, capsys) -> None:  # type: ignore[no-untyped-def]
    store = Phase1GDevEvidenceStore(root=tmp_path / "g5")
    ref = Phase1GDevEvidenceRef(
        evidence_kind=EvidenceKind.INVENTORY,
        relative_path="inventories/aa/" + "a" * 64 + ".json",
        semantic_content_hash="a" * 64,
        file_sha256="b" * 64,
    )
    ref_path = tmp_path / "ref.json"
    _write(ref_path, ref.model_dump(mode="json"))
    exit_code = cli.main(
        [
            "verify-evidence",
            "--evidence-ref",
            str(ref_path),
            "--g5-evidence-root",
            str(store.root),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    assert exit_code == 70
    assert output["ok"] is False
    assert output["reason_code"] == "ADVISORY_PHASE1G_G5_EVIDENCE_STORE_FAILED"
    assert "Traceback" not in json.dumps(output)


def test_parser_has_no_production_approval_force_or_cleanup_options() -> None:
    help_text = cli._parser().format_help().lower()
    for forbidden in (
        "target-db",
        "production",
        "approval",
        "backup",
        "force",
        "cleanup",
        "delete",
        "skip",
    ):
        assert forbidden not in help_text


def test_l4_plan_stale_uses_persistent_failure_exit() -> None:
    assert cli._exit_for_reason(REASON_L4_PLAN_STALE) == cli.EXIT_PERSISTENT_FAILED


def test_receipt_exit_uses_declared_precedence() -> None:
    assert (
        cli._exit_with_precedence(
            (REASON_L4_PLAN_STALE, REASON_UNEXPECTED_ERROR),
            fallback=cli.EXIT_L3_FAILED,
        )
        == cli.EXIT_INTERNAL
    )


def test_unexpected_error_traceback_redacts_original_message(
    tmp_path, capsys, caplog, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    def fail(_args) -> int:  # type: ignore[no-untyped-def]
        raise RuntimeError("password=should-not-appear dsn=should-not-appear")

    monkeypatch.setattr(cli, "_verify", fail)
    with caplog.at_level(logging.ERROR, logger=cli.LOGGER.name):
        exit_code = cli.main(
            [
                "verify-evidence",
                "--evidence-ref",
                str(tmp_path / "unused-ref.json"),
                "--g5-evidence-root",
                str(tmp_path / "unused-root"),
            ]
        )
    output = capsys.readouterr().out
    assert exit_code == cli.EXIT_INTERNAL
    assert "password=should-not-appear" not in output
    assert "dsn=should-not-appear" not in output
    assert "password=should-not-appear" not in caplog.text
    assert "dsn=should-not-appear" not in caplog.text
    assert "exception_type=RuntimeError" in caplog.text
    assert "redacted_traceback=" in caplog.text
