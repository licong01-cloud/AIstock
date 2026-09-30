from __future__ import annotations

import pytest

from backend.services.dataset_release.component_artifact_manifest import (
    ComponentArtifactManifestError,
    MutationRuleEvidence,
)


def _rule(**updates) -> MutationRuleEvidence:
    values = {
        "rule_id": "monthly-tail",
        "datasets": ("kline_daily_raw",),
        "replace_existing_targets": ("shared.bin",),
        "create_new_targets": (),
        "create_target_templates": ("features/{instrument}/close.day.bin",),
        "writer_targets_by_instrument": {"000001.SZ": ("features/000001.sz/close.day.bin",)},
        "writer_target_policy": "explicit_by_instrument_v1",
        "dependency_edges": ("daily->daily_bin",),
        "rule_identity": "a" * 64,
    }
    values.update(updates)
    return MutationRuleEvidence(**values)


def test_mutation_rule_returns_exact_existing_and_new_instrument_targets() -> None:
    replace, create = _rule().targets_for_instruments(
        ["000001.sz"],
        create_for_instruments=["600000.SH"],
    )
    assert replace == ("features/000001.sz/close.day.bin", "shared.bin")
    assert create == ("features/600000.sh/close.day.bin",)


def test_mutation_rule_never_uses_immutable_lineage_as_fallback_target() -> None:
    rule = _rule(
        writer_targets_by_instrument={},
        writer_target_policy="artifact_file_instrument_index_v1",
    )
    with pytest.raises(ComponentArtifactManifestError, match="lacks exact"):
        rule.targets_for_instruments(
            ["000001.SZ"],
            instrument_file_targets={
                "000001.SZ": (
                    "csv_deltas/000001.sz/2026-08.csv",
                    "csv_overrides/000001.sz/2026-08.csv",
                )
            },
        )
