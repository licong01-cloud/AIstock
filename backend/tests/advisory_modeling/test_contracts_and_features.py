from __future__ import annotations

import base64
import hashlib
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_modeling.feature_builder import (
    ShortReboundFeatureFormulaKernelV1,
    frozen_formula_registry_v1,
)
from backend.services.advisory_modeling.feature_schema import (
    REQUIRED_FORMULA_IDS,
    REQUIRED_QUERY_TEMPLATE_IDS,
    FeatureDefinitionV1,
    FeatureQueryTemplateV1,
    FeatureSchemaV1,
    FrozenFeatureQueryRegistryV1,
)
from backend.services.advisory_modeling.label_policy import (
    RankingGroupStatus,
    RankingGroupIdentityV1,
    RankingLabelInputV1,
    RankingLabelPolicyV1,
    build_ranking_labels,
)
from backend.services.advisory_modeling.style_profile import (
    SHORT_REBOUND_HORIZONS,
    SHORT_REBOUND_TARGET_PACKAGE_ID,
    StrategyStyleProfileV1,
)
from backend.services.advisory_modeling.training_view import DatasetBuildRequestV1


_HASH = "a" * 64


def _profile() -> StrategyStyleProfileV1:
    return StrategyStyleProfileV1(
        profile_id="short-rebound-target-package-v1",
        profile_version="1",
        package_id=SHORT_REBOUND_TARGET_PACKAGE_ID,
        package_manifest_sha256=_HASH,
        package_asset_closure_hash="b" * 64,
        selection_runtime_semantics_hash="c" * 64,
        effective_package_oos_cutoff=date(2026, 7, 1),
    )


def test_style_profile_closes_exact_short_rebound_identity() -> None:
    profile = _profile()

    assert profile.supported_horizons == SHORT_REBOUND_HORIZONS
    assert profile.profile_payload_sha256 == canonical_json_sha256(
        profile.model_dump(mode="python", exclude={"profile_payload_sha256"})
    )
    assert StrategyStyleProfileV1.model_validate(profile.model_dump(mode="json")) == profile

    payload = profile.model_dump(mode="python")
    payload["supported_horizons"] = (1, 5)
    payload["profile_payload_sha256"] = None
    with pytest.raises(ValidationError, match="supported_horizons must equal"):
        StrategyStyleProfileV1.model_validate(payload)


def test_formula_registry_contains_full_canonical_payload_not_hash_only() -> None:
    registry = frozen_formula_registry_v1()

    assert tuple(item.formula_id for item in registry.formulas) == REQUIRED_FORMULA_IDS
    assert all(item.expression and item.input_roles and item.pit_constraints for item in registry.formulas)
    assert registry.registry_hash == canonical_json_sha256(
        registry.model_dump(mode="python", exclude={"registry_hash"})
    )


def test_query_registry_freezes_sql_and_schemas_for_all_existing_templates() -> None:
    commit = "b3cd8375be1038a6820f4cbea54db5e0d44831d0"
    templates = tuple(
        FeatureQueryTemplateV1(
            query_template_id=query_id,
            template_version="1",
            sql_bytes_base64=base64.b64encode(
                f"SELECT trade_date, symbol FROM app.{query_id} WHERE trade_date = %(day)s".encode()
            ).decode("ascii"),
            sql_bytes_sha256=hashlib.sha256(
                f"SELECT trade_date, symbol FROM app.{query_id} WHERE trade_date = %(day)s".encode()
            ).hexdigest(),
            parameter_schema={"day": "date"},
            result_schema=(("trade_date", "date"), ("symbol", "string")),
            repository_commit=commit,
        )
        for query_id in REQUIRED_QUERY_TEMPLATE_IDS
    )
    registry = FrozenFeatureQueryRegistryV1(
        templates=templates,
        source_repository_commit=commit,
    )

    assert registry.registry_hash
    payload = registry.model_dump(mode="python")
    payload["templates"] = payload["templates"][:-1]
    payload["registry_hash"] = None
    with pytest.raises(ValidationError, match="frozen v1 templates"):
        FrozenFeatureQueryRegistryV1.model_validate(payload)


def test_feature_schema_rejects_hash_only_or_missing_required_feature() -> None:
    definition = FeatureDefinitionV1(
        name="selection_effective_rank",
        dtype="int64",
        unit="rank",
        source_role="base_stage_evidence",
        formula_id="candidate_rank_percentile_v1",
        formula_version="1",
        missing_policy="REQUIRED_FAIL_GROUP",
    )
    schema = FeatureSchemaV1(
        feature_schema_id="short-rebound-feature-schema-v1",
        definitions=(definition,),
        required_identity_features=(),
        required_rank_features=(definition.name,),
        required_source_features=(),
    )
    assert schema.feature_schema_hash

    payload = schema.model_dump(mode="python")
    payload["required_source_features"] = ("missing_source",)
    payload["feature_schema_hash"] = None
    with pytest.raises(ValidationError, match="must exist"):
        FeatureSchemaV1.model_validate(payload)


def test_pure_feature_formulas_preserve_frozen_math_and_missing_semantics() -> None:
    builder = ShortReboundFeatureFormulaKernelV1()

    assert builder.candidate_rank_percentile(rank=1, candidate_count=20) == Decimal(1)
    assert builder.candidate_rank_percentile(rank=20, candidate_count=20) == Decimal(0)
    assert builder.candidate_rank_percentile(rank=1, candidate_count=1) == Decimal("0.5")
    assert builder.adjusted_return((Decimal("10"), Decimal("11"))) == Decimal("0.1")
    assert builder.adjusted_return((Decimal("0"), Decimal("11"))) is None
    consensus = builder.multi_alpha_consensus(
        scores=(Decimal("1"), Decimal("-0.5")),
        weights=(Decimal("0.75"), Decimal("0.25")),
    )
    assert consensus["weighted_mean"] == Decimal("0.625")
    assert consensus["max_leg_dominance"] == Decimal("0.8571428571428571428571428571")
    with pytest.raises(ValueError, match="positive-weight"):
        builder.multi_alpha_consensus(
            scores=(Decimal("1"), Decimal("-0.5")),
            weights=(Decimal("1"), Decimal("-1")),
        )


def _label(symbol: str, *, value: str) -> RankingLabelInputV1:
    return RankingLabelInputV1(
        symbol=symbol,
        return_5=Decimal(value),
        executable_mfe_5=Decimal("0"),
        executable_mae_5=Decimal("0"),
        label_source_closure_hash=_HASH,
    )


def _group(policy: RankingLabelPolicyV1 | None = None) -> RankingGroupIdentityV1:
    active_policy = policy or RankingLabelPolicyV1()
    return RankingGroupIdentityV1(
        decision_as_of_trade_date=date(2026, 7, 1),
        target_trade_date=date(2026, 7, 2),
        stable_signal_semantics_hash="9" * 64,
        label_policy_hash=str(active_policy.label_policy_hash),
    )


def test_label_policy_uses_dense_ties_and_never_baseline_rank() -> None:
    policy = RankingLabelPolicyV1()
    result = build_ranking_labels(
        (
            _label("000003.SZ", value="0.03"),
            _label("000001.SZ", value="0.01"),
            _label("000002.SZ", value="0.01"),
        ),
        group_identity=_group(policy),
        policy=policy,
    )

    assert result.status is RankingGroupStatus.MODELABLE
    by_symbol = {item.symbol: item for item in result.labels}
    assert by_symbol["000001.SZ"].relevance == by_symbol["000002.SZ"].relevance == 0
    assert by_symbol["000003.SZ"].relevance == 4
    assert "selection_effective_rank" not in policy.model_dump(mode="json")


def test_label_policy_reports_no_variation_and_too_small_group_explicitly() -> None:
    equal = build_ranking_labels(
        (_label("000001.SZ", value="0.01"), _label("000002.SZ", value="0.01")),
        group_identity=_group(),
    )
    singleton = build_ranking_labels(
        (_label("000001.SZ", value="0.01"),),
        group_identity=_group(),
    )

    assert equal.status is RankingGroupStatus.NO_LABEL_VARIATION
    assert {item.relevance for item in equal.labels} == {0}
    assert singleton.status is RankingGroupStatus.GROUP_NOT_MODELABLE

    wrong_group_payload = _group().model_dump(mode="python")
    wrong_group_payload["label_policy_hash"] = "8" * 64
    wrong_group_payload["group_identity_hash"] = None
    with pytest.raises(ValueError, match="differs from active label policy"):
        build_ranking_labels(
            (_label("000001.SZ", value="0.01"),),
            group_identity=RankingGroupIdentityV1.model_validate(wrong_group_payload),
        )


def test_dataset_request_freezes_full_static_identity() -> None:
    profile = _profile()
    request = DatasetBuildRequestV1(
        style_profile_id=profile.profile_id,
        style_profile_hash=str(profile.profile_payload_sha256),
        package_id=profile.package_id,
        package_manifest_sha256=profile.package_manifest_sha256,
        package_asset_closure_hash=profile.package_asset_closure_hash,
        selection_runtime_semantics_hash=profile.selection_runtime_semantics_hash,
        multi_alpha_parent_contract_version="advisory_historical_range_candidate_component_lineage_v1",
        multi_alpha_component_identity_set_hash="d" * 64,
        decision_date_start=date(2021, 1, 1),
        decision_date_end=date(2026, 7, 1),
        feature_schema_id="schema-v1",
        feature_schema_hash="e" * 64,
        feature_formula_registry_hash="f" * 64,
        feature_query_registry_hash="1" * 64,
        market_regime_policy_template_id="regime-v1",
        market_regime_policy_template_hash="2" * 64,
        label_policy_id="label-v1",
        label_policy_hash="3" * 64,
        source_revision_set_id="sources-v1",
        source_revision_set_hash="4" * 64,
        universe_policy_set_id="universe-v1",
        universe_policy_set_hash="5" * 64,
        calendar_version="cn-trading-v1",
        calendar_hash="6" * 64,
        repository_commit="b3cd8375be1038a6820f4cbea54db5e0d44831d0",
        final_fit_as_of=datetime(2026, 7, 31, 23, 59, tzinfo=UTC),
    )

    assert request.evidence_scope == "RETROSPECTIVE_RESEARCH_ONLY"
    assert request.request_semantic_hash == canonical_json_sha256(
        request.model_dump(mode="python", exclude={"request_semantic_hash"})
    )
