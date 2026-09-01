from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from backend.services.industry_pit.contracts import IndustryPitContractError
from scripts import build_industry_pit_candidates as builder


def test_help_is_fresh_process_and_has_no_database_or_artifact_side_effect(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "scripts/build_industry_pit_candidates.py", "--help"],
        cwd=Path(__file__).resolve().parents[3],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "--artifact-root" in result.stdout
    assert list(tmp_path.iterdir()) == []


def test_cli_always_includes_the_four_approved_mandatory_regressions(tmp_path: Path) -> None:
    args = builder.parse_args(
        [
            "--source-root",
            str(tmp_path),
            "--db-env-file",
            str(tmp_path / "db.env"),
            "--artifact-root",
            str(tmp_path / "candidate"),
        ]
    )
    assert set(builder.MANDATORY_REGRESSION_SYMBOLS).issubset(args.mandatory_symbol)


def test_snapshot_crosscheck_uses_20210730_classification_not_update_date() -> None:
    history = [
        {
            "stock_code": "605077",
            "classification_valid_from": "2021-07-30 00:00:00",
            "industry_code": "220315",
            "source_last_updated_at": "2022-08-21 19:46:00",
        }
    ]
    snapshot = [{"canonical_symbol": "605077.SH", "industry_code": "220315"}]
    assert builder._validate_snapshot_crosscheck(
        history, snapshot, mandatory_symbols=("605077.SH",)
    ) == {
        "checked_20210730_rows": 1,
        "current_snapshot_difference_count": 0,
        "mandatory_checked": 1,
        "mandatory_mismatch_count": 0,
    }


def test_index_evidence_requires_explicit_schema_and_contributes_source_hash(tmp_path: Path) -> None:
    path = tmp_path / "index.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "sw_index_membership_evidence_v1",
                "rows": [
                    {
                        "canonical_symbol": "300741.SZ",
                        "industry_code": "220315",
                        "membership_enter_date": "2021-12-13",
                        "membership_exit_date_exclusive": None,
                        "known_from": "2021-12-13",
                        "source_sha256": "a" * 64,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    rows, hashes = builder._load_index_evidence(path)
    assert rows[0]["membership_enter_date"] == "2021-12-13"
    assert "a" * 64 in hashes
    assert len(hashes) == 2

    path.write_text(json.dumps({"schema_version": "wrong", "rows": []}), encoding="utf-8")
    with pytest.raises(IndustryPitContractError, match="schema"):
        builder._load_index_evidence(path)


def test_exact_source_hash_contract_is_frozen() -> None:
    assert builder.EXPECTED_SOURCE_HASHES == {
        "catalog": "923492f4bcf3c7056904385a0769e4dda561904a29ecd9243f942680cef68c81",
        "classification_history": "15979d9cf8a3b83ccc8dadc967de52f35e667b4f4da5e4e4e3dd5a8bb1f17402",
        "latest_snapshot": "b242ab04e0f68357cf90772e3f15367644d3e74c08a767eb9c5edcf21467fcbb",
        "taxonomy_standard": "18fb07fafda072dad39e274371660706e21678045ae8204931958db9906faa1a",
    }
    assert builder.EXPECTED_CONFLICT_SYMBOLS == 23
    assert builder.EXPECTED_CONFLICT_OPPORTUNITIES == 23_326


def test_approved_historical_conflict_inventory_is_frozen_independently() -> None:
    inventory = builder._approved_regression_inventory()

    assert set(inventory) == {
        "000016.SZ",
        "000716.SZ",
        "002481.SZ",
        "002507.SZ",
        "002557.SZ",
        "002582.SZ",
        "002597.SZ",
        "002719.SZ",
        "002738.SZ",
        "003030.SZ",
        "300699.SZ",
        "300741.SZ",
        "300777.SZ",
        "300783.SZ",
        "300858.SZ",
        "300892.SZ",
        "300915.SZ",
        "300972.SZ",
        "603020.SH",
        "603077.SH",
        "603697.SH",
        "605077.SH",
        "605300.SH",
    }
    assert sum(
        row["legacy_conflict_opportunities"] for row in inventory.values()
    ) == 23_326
    assert {
        row["observation_basis"] for row in inventory.values()
    } == {"approved_c013_historical_baseline"}
    assert all(row["diagnostic_only_not_authority_source"] for row in inventory.values())


def test_repaired_current_conflict_inventory_is_a_non_blocking_observation() -> None:
    observation = builder._current_legacy_conflict_observation([])

    assert observation == {
        "symbol_count": 0,
        "opportunity_count": 0,
        "by_symbol": {},
        "diagnostic_only_not_authority_source": True,
        "blocks_candidate_preparation": False,
    }
    assert len(builder._approved_regression_inventory()) == 23


def test_current_conflict_observation_is_order_invariant_and_rejects_duplicates() -> None:
    forward = builder._current_legacy_conflict_observation(
        [("300741.SZ", 3), ("300858.SZ", 2)]
    )
    reverse = builder._current_legacy_conflict_observation(
        [("300858.SZ", 2), ("300741.SZ", 3)]
    )

    assert forward == reverse
    with pytest.raises(IndustryPitContractError, match="diagnostic is invalid"):
        builder._current_legacy_conflict_observation(
            [("300741.SZ", 3), ("300741.SZ", 3)]
        )
