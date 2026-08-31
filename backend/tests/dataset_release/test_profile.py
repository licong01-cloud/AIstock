from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from backend.services.dataset_release.errors import ProfileValidationError
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.profile import (
    GIB,
    DatasetProfile,
    ResourcePolicy,
    load_dataset_profile,
    validate_resource_policy,
)
from backend.services.dataset_release.stock_schema import (
    QLIB_STOCK_FIELDS,
    QLIB_STOCK_SCHEMA_VERSION,
    qlib_stock_schema_digest,
)
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_UNIVERSE_KEY,
    PitAuthorityStatus,
)


ROOT = Path(__file__).resolve().parents[3]
PROFILE_PATH = ROOT / "configs" / "datasets" / "qe_backtest_monthly_v1.yaml"
CANONICAL_PROFILE_PATH = ROOT / "configs" / "datasets" / "qe_backtest_monthly_v2.yaml"


def test_legacy_profile_identity_remains_byte_semantic_compatible_for_reproduction() -> None:
    profile = load_dataset_profile(PROFILE_PATH)
    assert profile.config_digest == "aef6dff42371e0891cec0d73e8495551119f4601a614e85acfb4f8f43aed139c"
    assert profile.semantic_profile_digest == "fa75a5bec2251d9a39146789fd5c898caeb72c8400d02b4bb92037fdc8a100d5"
    assert profile.pit_authority_status == PitAuthorityStatus.DEPLOYED_LEGACY_PENDING_MIGRATION.value


def test_canonical_profile_is_an_explicit_overlay_with_the_same_resource_limits() -> None:
    legacy = load_dataset_profile(PROFILE_PATH)
    canonical = load_dataset_profile(CANONICAL_PROFILE_PATH)
    assert canonical.profile == "qe_hmm_full_v2"
    assert canonical.universe_key == CANONICAL_PIT_UNIVERSE_KEY
    assert canonical.universe_rule_version == CANONICAL_PIT_RULE_VERSION
    assert canonical.pit_authority_status == PitAuthorityStatus.ACTIVE_CANONICAL.value
    assert canonical.pit_scope == "canonical_all_listed"
    assert legacy.pit_scope == "st_only_active"
    assert canonical.resource_policy == legacy.resource_policy
    assert canonical.semantic_profile_digest != legacy.semantic_profile_digest


def test_profile_yaml_parser_failure_is_typed_and_path_safe(tmp_path: Path) -> None:
    malformed = tmp_path / "broken-profile.yaml"
    malformed.write_text("semantic: [unterminated", encoding="utf-8")
    with pytest.raises(ProfileValidationError) as error:
        load_dataset_profile(malformed)
    assert str(error.value) == "dataset release profile could not be parsed or validated"


def test_profile_freezes_semantic_and_resource_contract(dataset_profile: DatasetProfile) -> None:
    assert dataset_profile.start_date.isoformat() == "2018-08-01"
    assert dataset_profile.minute_start_date.isoformat() == "2024-01-02"
    assert dataset_profile.source_content_probe_ttl_seconds == 86_400
    assert dataset_profile.reconcile_catchup_months == 3
    assert dataset_profile.reconcile_lease_ttl_seconds == 300
    assert dataset_profile.worker_heartbeat_ttl_seconds == 30
    assert dataset_profile.source_audit_reuse_policy == "require_complete_for_reuse_v1"
    assert dataset_profile.indices == DOMESTIC_INDEX_DEFINITIONS
    assert dataset_profile.index_codes[2] == "000300.SH"
    assert dataset_profile.moneyflow_contract == "tushare_moneyflow_shares_yuan_v1"
    assert dataset_profile.static_column_count == 121
    assert dataset_profile.static_default_numeric_dtype == "float32"
    assert dataset_profile.qlib_stock_schema_version == QLIB_STOCK_SCHEMA_VERSION
    assert dataset_profile.qlib_stock_schema_digest == qlib_stock_schema_digest()
    assert dataset_profile.qlib_stock_fields == QLIB_STOCK_FIELDS
    assert dataset_profile.l2_code_id_dtype == "int16"
    assert dataset_profile.l2_code_id_missing == -1
    assert dataset_profile.resource_policy.aggregate_private_commit_bytes == 12 * GIB
    assert dataset_profile.resource_policy.windows_job_commit_bytes == 8 * GIB
    assert dataset_profile.resource_policy.hybrid_job_commit_bytes == 4 * GIB
    assert dataset_profile.resource_policy.wsl_swap_max_bytes == 0
    assert dataset_profile.pressure_ladder["h5_batch"] == (100, 50, 20)
    assert dataset_profile.qlib_toolchain.schema_version == "qe_qlib_toolchain_v1"
    assert dataset_profile.qlib_toolchain.dump_script_sha256 == (
        "b8f34c57ce1ef4b1772f3909735e66058f21b25bd7ab8a5f16318822401fe53f"
    )


@pytest.mark.parametrize(
    ("source", "overrides"),
    [
        ("cli", {"aggregate_private_commit_bytes": 12 * GIB + 1}),
        ("env", {"host_start_available_bytes": 15 * GIB}),
        ("cli", {"date_chunk_months": 4}),
        ("env", {"wsl_swap_max_bytes": 1}),
        ("cli", {"db_pool_size": 5}),
        ("cli", {"enforcement_sample_seconds": 0}),
    ],
)
def test_cli_and_env_cannot_weaken_hard_limits(
    dataset_profile: DatasetProfile,
    source: str,
    overrides: dict[str, int | float],
) -> None:
    with pytest.raises(ProfileValidationError, match=f"unsafe {source}"):
        dataset_profile.with_resource_overrides(overrides, source=source)


def test_safe_physical_tuning_does_not_change_semantic_identity(
    dataset_profile: DatasetProfile,
) -> None:
    tuned = dataset_profile.with_resource_overrides(
        {
            "h5_load_batch_size": 50,
            "minute_code_batch_size": 10,
            "host_start_available_bytes": 20 * GIB,
        },
        source="cli",
    )
    assert tuned.semantic_profile_digest == dataset_profile.semantic_profile_digest
    assert tuned.resource_policy_digest != dataset_profile.resource_policy_digest


def test_later_override_cannot_weaken_an_already_tightened_policy(
    dataset_profile: DatasetProfile,
) -> None:
    tuned = dataset_profile.with_resource_overrides(
        {"h5_load_batch_size": 50, "host_start_available_bytes": 20 * GIB},
        source="profile",
    )
    with pytest.raises(ProfileValidationError, match="weakens effective policy"):
        tuned.with_resource_overrides({"h5_load_batch_size": 75}, source="cli")
    with pytest.raises(ProfileValidationError, match="weakens effective policy"):
        tuned.with_resource_overrides({"host_start_available_bytes": 18 * GIB}, source="env")


@pytest.mark.parametrize(
    "policy",
    [
        ResourcePolicy(wait_deadline_seconds=0),
        ResourcePolicy(db_pool_size=True),
        ResourcePolicy(wsl_memory_high_bytes=8 * GIB),
        ResourcePolicy(hybrid_job_commit_bytes=5 * GIB, wsl_memory_max_bytes=8 * GIB),
    ],
)
def test_direct_resource_policy_construction_cannot_bypass_invariants(
    policy: ResourcePolicy,
) -> None:
    with pytest.raises(ProfileValidationError):
        validate_resource_policy(policy)


def test_profile_rejects_any_index_contract_drift(tmp_path: Path) -> None:
    raw = yaml.safe_load(PROFILE_PATH.read_text(encoding="utf-8"))
    raw["semantic"]["index_context"]["codes"][0]["semantic_role"] = "popular_index"
    path = tmp_path / "bad-profile.yaml"
    path.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")
    with pytest.raises(ProfileValidationError, match="exactly match"):
        load_dataset_profile(path)


def test_profile_rejects_qlib_stock_field_order_drift(tmp_path: Path) -> None:
    raw = yaml.safe_load(PROFILE_PATH.read_text(encoding="utf-8"))
    raw["semantic"]["qlib_stock_authority"]["daily_fields"] = list(
        reversed(raw["semantic"]["qlib_stock_authority"]["daily_fields"])
    )
    path = tmp_path / "bad-stock-profile.yaml"
    path.write_text(
        yaml.safe_dump(raw, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    with pytest.raises(ProfileValidationError, match="12-field Qlib stock"):
        load_dataset_profile(path)


def test_profile_rejects_unversioned_or_unpinned_qlib_toolchain(tmp_path: Path) -> None:
    raw = yaml.safe_load(PROFILE_PATH.read_text(encoding="utf-8"))
    raw["runtime"]["qlib_toolchain"]["dump_script_sha256"] = "latest"
    path = tmp_path / "bad-toolchain-profile.yaml"
    path.write_text(
        yaml.safe_dump(raw, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    with pytest.raises(ProfileValidationError, match="toolchain values"):
        load_dataset_profile(path)
