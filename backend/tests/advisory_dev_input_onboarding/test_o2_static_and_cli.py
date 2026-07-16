from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from backend.services.advisory_dev_input_onboarding.contracts import (
    RealDevOnboardingError,
    REASON_IMPORT_COMMIT_NOT_OBSERVED,
    REASON_IMPORT_COMMIT_STATE_UNKNOWN,
)
from backend.services.advisory_dev_input_onboarding.dev_importer import (
    TARGET_INSERT_SQL,
    TARGET_READ_SQL,
)
import scripts.advisory_real_dev_onboarding as cli


def test_o2_sql_registry_is_fixed_insert_select_only() -> None:
    assert set(TARGET_INSERT_SQL) == {"strategy_pkg.package", "strategy_pkg.package_asset"}
    for relation, statement in TARGET_INSERT_SQL.items():
        normalized = f" {' '.join(statement.split()).upper()} "
        assert normalized.strip().startswith(f"INSERT INTO {relation.upper()} ")
        assert " ON CONFLICT " in normalized and " DO NOTHING " in normalized
        assert all(token not in normalized for token in (" UPDATE ", " DELETE ", " TRUNCATE ", " ALTER ", " CREATE ", " DROP ", " COPY "))
    for statement in TARGET_READ_SQL.values():
        normalized = f" {' '.join(statement.split()).upper()} "
        assert normalized.strip().startswith("SELECT ")
        assert all(token not in normalized for token in (" INSERT ", " UPDATE ", " DELETE ", " TRUNCATE "))


def test_o2_source_has_no_bypass_or_unrequested_approval_flags() -> None:
    root = Path(__file__).resolve().parents[2]
    paths = (
        root / "services" / "advisory_dev_input_onboarding" / "dev_importer.py",
        root / "services" / "advisory_dev_input_onboarding" / "production_projection.py",
        root.parents[0] / "scripts" / "advisory_real_dev_onboarding.py",
    )
    forbidden = (
        "session_replication_role",
        "setval(",
        "copy from",
        "batch_a_import_real_data",
        "--confirm",
        "--force",
        "--skip",
        "approval_token",
        "backup_gate",
    )
    for path in paths:
        source = path.read_text(encoding="utf-8-sig")
        lowered = source.lower()
        assert not [value for value in forbidden if value in lowered], path
        ast.parse(source, filename=str(path))


def test_cli_exposes_o2_commands_without_confirmation_arguments() -> None:
    parser = cli._parser()
    help_text = parser.format_help()
    for command in ("export-bundle", "plan-import", "import-dev", "verify-import"):
        assert command in help_text
    for forbidden in ("--confirm", "--force", "--skip", "--approval"):
        assert forbidden not in help_text


def test_cli_contract_validation_redacts_input_values(tmp_path: Path, capsys) -> None:
    sensitive_value = f"review-{id(tmp_path)}"
    request = tmp_path / "request.json"
    request.write_text(json.dumps({"password": sensitive_value}), encoding="utf-8")
    code = cli.main(
        [
            "export-bundle",
            "--request",
            str(request),
            "--inventory-ref",
            str(tmp_path / "inventory.json"),
            "--env-file",
            str(tmp_path / ".env"),
            "--evidence-root",
            str(tmp_path / "evidence"),
            "--source-package-asset-root",
            str(tmp_path / "source"),
            "--target-package-asset-root",
            str(tmp_path / "target"),
        ]
    )
    output = capsys.readouterr()
    assert code == 2
    assert sensitive_value not in output.out
    assert sensitive_value not in output.err
    assert "input contract validation failed" in output.out


@pytest.mark.parametrize(
    ("reason_code", "expected"),
    ((REASON_IMPORT_COMMIT_NOT_OBSERVED, 4), (REASON_IMPORT_COMMIT_STATE_UNKNOWN, 5)),
)
def test_cli_preserves_commit_uncertainty_exit_contract(monkeypatch, reason_code: str, expected: int) -> None:
    def fail(_args):
        raise RealDevOnboardingError(reason_code, "redacted")

    monkeypatch.setattr(cli, "_import_dev", fail)
    code = cli.main(
        [
            "import-dev",
            "--bundle-ref",
            "bundle.json",
            "--plan",
            "plan.json",
            "--env-file",
            ".env",
            "--release-receipt-root",
            "release",
            "--evidence-root",
            "evidence",
            "--source-package-asset-root",
            "source",
            "--target-package-asset-root",
            "target",
        ]
    )
    assert code == expected
