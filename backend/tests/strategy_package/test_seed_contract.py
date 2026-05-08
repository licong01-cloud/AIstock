from __future__ import annotations

import re
from pathlib import Path

import pytest

from backend.services.strategy_package.seed_contract import (
    MAX_LIBRARY_SEED,
    SeedContractError,
    SeedPolicy,
    build_master_seed_contract,
    build_master_seed_contract_from_manifest,
    derive_dataloader_worker_seed,
)


def test_fixed_master_seed_derivation_is_deterministic() -> None:
    first = build_master_seed_contract(master_seed=20260509, seed_policy="fixed")
    second = build_master_seed_contract(master_seed=20260509, seed_policy=SeedPolicy.FIXED)

    assert first == second
    assert first.seed_sequence == (20260509,)
    runtime_kwargs = first.to_runtime_seed_kwargs()
    assert set(runtime_kwargs) == {
        "python_seed",
        "numpy_seed",
        "torch_seed",
        "torch_cuda_seed",
        "lightgbm_seed",
        "xgboost_random_state",
        "catboost_random_seed",
        "dataloader_worker_seed_base",
    }
    assert all(0 <= value <= MAX_LIBRARY_SEED for value in runtime_kwargs.values())
    assert len(set(runtime_kwargs.values())) == len(runtime_kwargs)
    assert first.python_hash_seed is not None and first.python_hash_seed.isdigit()


def test_different_master_seed_changes_derived_children() -> None:
    left = build_master_seed_contract(master_seed=20260509).to_runtime_seed_kwargs()
    right = build_master_seed_contract(master_seed=20260510).to_runtime_seed_kwargs()

    assert left != right


def test_unset_legacy_records_audit_only_without_silent_runtime_fallback() -> None:
    contract = build_master_seed_contract(master_seed=None, seed_policy="unset_legacy")

    assert contract.is_unset_legacy
    assert contract.master_seed is None
    assert contract.seed_sequence == ()
    assert contract.reproducibility_level == "audit_only"
    assert contract.to_manifest_dict()["seed_policy"] == "unset_legacy"
    with pytest.raises(SeedContractError, match="no runtime seeds|unavailable"):
        contract.to_runtime_seed_kwargs()


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        ({"seed_policy": "fixed", "master_seed": None}, "master_seed is required"),
        ({"seed_policy": "fixed", "master_seed": -1}, "between 0"),
        ({"seed_policy": "fixed", "master_seed": True}, "must be an integer"),
        ({"seed_policy": "fixed", "master_seed": 2**63}, "between 0"),
        ({"seed_policy": "fixed", "master_seed": 7, "seed_sequence": [8]}, "requires seed_sequence"),
        ({"seed_policy": "unset_legacy", "master_seed": 7}, "must not provide master_seed"),
        ({"seed_policy": "surprise", "master_seed": 7}, "seed_policy must be one of"),
    ],
)
def test_invalid_seed_contract_inputs_fail_fast(payload: dict[str, object], match: str) -> None:
    with pytest.raises(SeedContractError, match=match):
        build_master_seed_contract(**payload)


def test_manifest_parser_requires_explicit_seed_policy() -> None:
    with pytest.raises(SeedContractError, match="seed_policy is required"):
        build_master_seed_contract_from_manifest({"master_seed": 20260509})


def test_multi_seed_sequence_and_worker_seed_are_stable() -> None:
    contract = build_master_seed_contract(
        master_seed=101,
        seed_policy="multi_seed",
        seed_sequence=[101, 202, 303],
        nondeterministic_flags=["cuda_atomic_add"],
    )

    assert contract.seed_sequence == (101, 202, 303)
    assert contract.nondeterministic_flags == ("cuda_atomic_add",)
    assert derive_dataloader_worker_seed(contract, 0) == contract.dataloader_worker_seed_base
    assert derive_dataloader_worker_seed(contract, 3) == (contract.dataloader_worker_seed_base + 3) % (MAX_LIBRARY_SEED + 1)
    with pytest.raises(SeedContractError, match="worker_id"):
        derive_dataloader_worker_seed(contract, -1)


def test_phase4_seed_contract_ddl_comments_cover_new_tables_and_columns() -> None:
    sql_path = Path("backend/migrations/qe_phase4_master_seed_contract_20260509.sql")
    sql = sql_path.read_text(encoding="utf-8")

    assert "CREATE SCHEMA IF NOT EXISTS strategy_pkg" in sql
    assert " public." not in sql.lower()
    assert "DROP COLUMN" not in sql.upper()
    assert "ALTER COLUMN" not in sql.upper()

    created_tables = set(re.findall(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-z0-9_]+\.[a-z0-9_]+)\s*\(", sql, re.I))
    table_comments = set(re.findall(r"COMMENT\s+ON\s+TABLE\s+([a-z0-9_]+\.[a-z0-9_]+)\s+IS", sql, re.I))
    assert created_tables <= table_comments

    created_columns: set[str] = set()
    for table_match in re.finditer(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-z0-9_]+\.[a-z0-9_]+)\s*\((.*?)\n\);",
        sql,
        re.I | re.S,
    ):
        table = table_match.group(1)
        for raw_line in table_match.group(2).splitlines():
            line = raw_line.strip().rstrip(",")
            if not line or line.upper().startswith(("CONSTRAINT ", "PRIMARY ", "UNIQUE ", "CHECK ", "FOREIGN ")):
                continue
            column = line.split()[0]
            created_columns.add(f"{table}.{column}")

    altered_columns = {
        f"{match.group(1)}.{match.group(2)}"
        for match in re.finditer(
            r"ALTER\s+TABLE\s+([a-z0-9_]+\.[a-z0-9_]+)\s+ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+([a-z0-9_]+)",
            sql,
            re.I,
        )
    }
    column_comments = set(re.findall(r"COMMENT\s+ON\s+COLUMN\s+([a-z0-9_]+\.[a-z0-9_]+\.[a-z0-9_]+)\s+IS", sql, re.I))

    assert (created_columns | altered_columns) <= column_comments


def test_phase4_seed_contract_add_constraints_are_idempotent() -> None:
    sql_path = Path("backend/migrations/qe_phase4_master_seed_contract_20260509.sql")
    sql = sql_path.read_text(encoding="utf-8")

    assert "ADD CONSTRAINT IF NOT EXISTS" not in sql.upper()
    assert "FROM pg_constraint" in sql
    assert "package_seed_policy_check" in sql
    assert "package_master_seed_range_check" in sql


def test_phase1_promotion_review_has_standalone_additive_migration() -> None:
    sql_path = Path("backend/migrations/strategy_pkg_promotion_review_20260509.sql")
    sql = sql_path.read_text(encoding="utf-8")

    assert "CREATE SCHEMA IF NOT EXISTS strategy_pkg" in sql
    assert "CREATE TABLE IF NOT EXISTS strategy_pkg.promotion_review" in sql
    assert " public." not in sql.lower()
    assert "DROP COLUMN" not in sql.upper()
    assert "COMMENT ON TABLE strategy_pkg.promotion_review" in sql

    table_match = re.search(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+strategy_pkg\.promotion_review\s*\((.*?)\n\);",
        sql,
        re.I | re.S,
    )
    assert table_match is not None
    created_columns = set()
    for raw_line in table_match.group(1).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.upper().startswith(("UNIQUE ", "CONSTRAINT ", "PRIMARY ", "FOREIGN ", "CHECK ")):
            continue
        created_columns.add(line.split()[0])
    column_comments = {
        match.group(1)
        for match in re.finditer(
            r"COMMENT\s+ON\s+COLUMN\s+strategy_pkg\.promotion_review\.([a-z0-9_]+)\s+IS",
            sql,
            re.I,
        )
    }

    assert created_columns <= column_comments
