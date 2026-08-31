from __future__ import annotations

import ast
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from backend.services.selection_center.models import SelectionCandidate, SelectionMode
from backend.services.selection_center.prospective_evidence import (
    CandidateStageName,
    REASON_VALID_NO_CANDIDATE,
    SelectionStageTrace,
    StageReceiptStatus,
    build_stage_receipt,
)
from backend.services.selection_center.risk_policy import StockRiskPolicyService
from backend.services.selection_center.runtime_profile import parse_selection_runtime_profile
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.models import AlphaMode
from backend.services.strategy_package.selection_computation import (
    PreparedPackageComponentLineageV1,
    PreparedPackageSignalV1,
    SelectionArtifactHeaderV1,
    StrategyPackageSelectionComputation,
    StrategyPackageSelectionComputationRequestV1,
    StrategyPackageSelectionReadOnlyProvidersV1,
    aggregate_selection_candidates,
    selection_runtime_profile_sha256,
)
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError


class _NoSuspensions:
    def get_suspended_symbols(self, symbols: list[str], trade_date: date) -> dict[str, dict]:
        return {}


class _FailingRiskPolicy:
    def evaluate(self, **_kwargs):
        raise DataUnavailableError("risk policy source unavailable")

    def apply_to_candidates_with_receipt(self, **_kwargs):
        raise AssertionError("apply must not run after evaluate fails")


def _candidate(symbol: str, score: float, rank: int) -> SelectionCandidate:
    return SelectionCandidate(symbol=symbol, score=score, rank=rank, reason="alpha_score")


def _prepared_signal(
    package_id: str,
    candidates: list[SelectionCandidate],
    *,
    valid_no_candidate: bool = False,
    hmm_enabled: bool = False,
) -> PreparedPackageSignalV1:
    hmm_metadata = (
        {"enabled": True, "status": "COMPLETE", "generation_mode": "EXACT_SNAPSHOT"}
        if hmm_enabled
        else {"enabled": False, "status": "NOT_APPLICABLE", "generation_mode": "NOT_APPLICABLE"}
    )
    hmm_receipt = build_stage_receipt(
        stage=CandidateStageName.HMM_ADJUSTED,
        status=StageReceiptStatus.COMPLETE if hmm_enabled else StageReceiptStatus.NOT_APPLICABLE,
        input_count=len(candidates),
        candidates=candidates if hmm_enabled else [],
        semantic_payload={
            key: hmm_metadata[key]
            for key in ("enabled", "status", "generation_mode")
            if key != "status" or hmm_enabled
        },
    )
    return PreparedPackageSignalV1(
        package_id=package_id,
        package_version="1.0.0",
        manifest_sha256="a" * 64,
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        component_lineage=(
            PreparedPackageComponentLineageV1(
                component_id=f"{package_id}_alpha",
                component_weight=1.0,
                factor_ids=("factor_1",),
                score_normalization="rank",
            ),
        ),
        alpha_raw_candidates=tuple(candidates),
        hmm_adjusted_candidates=tuple(candidates),
        hmm_receipt=hmm_receipt,
        hmm_metadata=hmm_metadata,
        artifact_header=SelectionArtifactHeaderV1(
            artifact_id=f"artifact_{package_id}",
            artifact_sha256="b" * 64,
            package_id=package_id,
            manifest_sha256="a" * 64,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config_hash="9" * 64,
            artifact_payload_sha256="c" * 64,
            artifact_contract_version="selection_score_artifact_v2",
            artifact_input_context_hash="d" * 64,
            source_revision_set_hash="e" * 64,
            asset_closure_hash="8" * 64,
            universe_identity_hash="f" * 64,
        ),
        input_context_hash="d" * 64,
        source_revision_set_hash="e" * 64,
        universe_identity_hash="f" * 64,
        valid_no_candidate=valid_no_candidate,
        no_candidate_reason=REASON_VALID_NO_CANDIDATE if valid_no_candidate else None,
    )


def _request(
    package_ids: tuple[str, ...],
    mode: SelectionMode,
    *,
    hmm_enabled: bool = False,
) -> StrategyPackageSelectionComputationRequestV1:
    profile = parse_selection_runtime_profile(
        {
            "runtime_profile": {
                "selection": {"top_k": 2},
                "tradability": {"exclude_suspended": False},
                "risk_policy": {"enabled": False},
                "hmm": (
                    {
                        "enabled": True,
                        "model_snapshot_id": "hmm_snapshot_1",
                        "signal_preset": "preset_1",
                    }
                    if hmm_enabled
                    else {"enabled": False}
                ),
            }
        }
    )
    return StrategyPackageSelectionComputationRequestV1(
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        selection_mode=mode,
        ordered_package_ids=package_ids,
        package_runtime_profiles={package_id: profile for package_id in package_ids},
        package_runtime_profile_hashes={
            package_id: selection_runtime_profile_sha256(profile) for package_id in package_ids
        },
        package_top_k={package_id: 2 for package_id in package_ids},
        package_weights=(
            {package_id: 1.0 for package_id in package_ids}
            if mode is SelectionMode.WEIGHTED_FUSION
            else None
        ),
    )


def _providers(risk_policy=None) -> StrategyPackageSelectionReadOnlyProvidersV1:
    return StrategyPackageSelectionReadOnlyProvidersV1(
        risk_policy=risk_policy or StockRiskPolicyService(),
        tradability=TradabilityFilter(_NoSuspensions()),
    )


def test_single_package_computation_preserves_candidate_and_stage_semantics() -> None:
    package_id = "pkg_single"
    candidates = [_candidate("000001.SZ", 0.9, 1), _candidate("000002.SZ", 0.8, 2)]
    request = _request((package_id,), SelectionMode.SINGLE_PACKAGE)
    prepared = _prepared_signal(package_id, candidates)
    providers = _providers()

    result = StrategyPackageSelectionComputation().compute(
        request=request,
        prepared_signals={package_id: prepared},
        providers=providers,
    )

    profile = request.package_runtime_profiles[package_id]
    risk_decisions = providers.risk_policy.evaluate(
        symbols=[item.symbol for item in prepared.hmm_adjusted_candidates],
        trade_date=request.trade_date,
        profile=profile.risk_policy,
    )
    legacy_risk = providers.risk_policy.apply_to_candidates_with_receipt(
        candidates=list(prepared.hmm_adjusted_candidates),
        decisions=risk_decisions,
        trade_date=request.trade_date,
        top_k=request.package_top_k[package_id],
        package_id=package_id,
        manifest_sha256=prepared.manifest_sha256,
        profile=profile.risk_policy,
        allow_empty=True,
    )
    legacy_selection = providers.tradability.select_top_k_with_receipt(
        candidates=legacy_risk.candidates,
        top_k=request.package_top_k[package_id],
        trade_date=request.trade_date,
        package_id=package_id,
        manifest_sha256=prepared.manifest_sha256,
    )
    legacy_trace = SelectionStageTrace(
        alpha_raw=build_stage_receipt(
            stage=CandidateStageName.ALPHA_RAW,
            status=StageReceiptStatus.COMPLETE,
            input_count=len(prepared.alpha_raw_candidates),
            candidates=list(prepared.alpha_raw_candidates),
            semantic_payload=prepared.artifact_header.stage_semantic_payload(
                package_id=package_id,
                manifest_sha256=prepared.manifest_sha256,
            ),
        ),
        hmm_adjusted=prepared.hmm_receipt,
        risk_policy_adjusted=legacy_risk.receipt,
        selection_effective=legacy_selection.receipt,
        hmm_metadata=dict(prepared.hmm_metadata),
        risk_metadata=legacy_risk.risk_metadata,
        universe_metadata=legacy_selection.universe_metadata,
    )

    assert [item.model_dump(mode="json") for item in result.package_results[package_id]] == [
        item.model_dump(mode="json") for item in legacy_selection.candidates
    ]
    assert [item.model_dump(mode="json") for item in result.aggregate_results] == [
        item.model_dump(mode="json") for item in legacy_selection.candidates
    ]
    assert [item.model_dump(mode="json") for item in result.excluded_results[package_id]] == [
        item.model_dump(mode="json") for item in [*legacy_risk.exclusions, *legacy_selection.exclusions]
    ]
    assert result.stage_trace_by_package[package_id].model_dump(mode="json") == legacy_trace.model_dump(mode="json")
    assert result.manifest_sha256_by_package == {package_id: prepared.manifest_sha256}
    assert result.candidate_outcome_by_package == {package_id: "CANDIDATES_PRESENT"}
    assert result.valid_no_candidate is False
    assert result.no_candidate_reason is None


def test_explicit_valid_no_candidate_is_the_only_empty_success_path() -> None:
    package_id = "pkg_empty"

    result = StrategyPackageSelectionComputation().compute(
        request=_request((package_id,), SelectionMode.SINGLE_PACKAGE),
        prepared_signals={package_id: _prepared_signal(package_id, [], valid_no_candidate=True)},
        providers=_providers(),
    )

    assert result.package_results[package_id] == ()
    assert result.aggregate_results == ()
    assert result.valid_no_candidate is True
    assert result.no_candidate_reason == REASON_VALID_NO_CANDIDATE


def test_unexplained_empty_prepared_signal_is_rejected() -> None:
    with pytest.raises(RuntimeConfigInvalidError, match="requires candidates or valid_no_candidate"):
        _prepared_signal("pkg_invalid_empty", [])


def test_prepared_artifact_header_requires_explicit_identity() -> None:
    with pytest.raises(RuntimeConfigInvalidError, match="requires artifact_id"):
        SelectionArtifactHeaderV1(
            artifact_id="",
            artifact_sha256="b" * 64,
            package_id="pkg_identity",
            manifest_sha256="a" * 64,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config_hash="9" * 64,
        )
    prepared = _prepared_signal("pkg_legacy_header", [_candidate("000001.SZ", 0.9, 1)])
    with pytest.raises(RuntimeConfigInvalidError, match="cannot carry v2 identity fields"):
        replace(prepared.artifact_header, artifact_contract_version=None)


def test_valid_no_candidate_rejects_alpha_raw_candidates() -> None:
    prepared = _prepared_signal("pkg_inconsistent_empty", [], valid_no_candidate=True)

    with pytest.raises(RuntimeConfigInvalidError, match="cannot contain alpha or HMM candidates"):
        replace(
            prepared,
            alpha_raw_candidates=(_candidate("000001.SZ", 0.9, 1),),
        )


def test_hmm_receipt_candidates_must_match_adjusted_candidates() -> None:
    package_id = "pkg_hmm_mismatch"
    prepared = _prepared_signal(
        package_id,
        [_candidate("000001.SZ", 0.9, 1)],
        hmm_enabled=True,
    )
    foreign_receipt = build_stage_receipt(
        stage=CandidateStageName.HMM_ADJUSTED,
        status=StageReceiptStatus.COMPLETE,
        input_count=1,
        candidates=[_candidate("000002.SZ", 0.8, 1)],
        semantic_payload=dict(prepared.hmm_receipt.semantic_payload),
    )

    with pytest.raises(RuntimeConfigInvalidError, match="candidates do not match"):
        replace(prepared, hmm_receipt=foreign_receipt)


def test_hmm_receipt_stage_must_be_hmm_adjusted() -> None:
    package_id = "pkg_hmm_stage"
    prepared = _prepared_signal(package_id, [_candidate("000001.SZ", 0.9, 1)])
    wrong_stage_receipt = build_stage_receipt(
        stage=CandidateStageName.ALPHA_RAW,
        status=StageReceiptStatus.NOT_APPLICABLE,
        input_count=1,
        candidates=[],
        semantic_payload=dict(prepared.hmm_receipt.semantic_payload),
    )

    with pytest.raises(RuntimeConfigInvalidError, match="invalid stage"):
        replace(prepared, hmm_receipt=wrong_stage_receipt)


def test_hmm_metadata_and_runtime_profile_must_match_prepared_receipt() -> None:
    package_id = "pkg_hmm_metadata"
    prepared = _prepared_signal(package_id, [_candidate("000001.SZ", 0.9, 1)])

    with pytest.raises(RuntimeConfigInvalidError, match="metadata does not match receipt semantics"):
        replace(
            prepared,
            hmm_metadata={**prepared.hmm_metadata, "generation_mode": "UNRELATED_MODE"},
        )
    with pytest.raises(RuntimeConfigInvalidError, match="does not match runtime profile"):
        StrategyPackageSelectionComputation().compute(
            request=_request((package_id,), SelectionMode.SINGLE_PACKAGE, hmm_enabled=True),
            prepared_signals={package_id: prepared},
            providers=_providers(),
        )


def test_enabled_hmm_prepared_signal_remains_computable() -> None:
    package_id = "pkg_hmm_enabled"
    result = StrategyPackageSelectionComputation().compute(
        request=_request((package_id,), SelectionMode.SINGLE_PACKAGE, hmm_enabled=True),
        prepared_signals={
            package_id: _prepared_signal(
                package_id,
                [_candidate("000001.SZ", 0.9, 1)],
                hmm_enabled=True,
            )
        },
        providers=_providers(),
    )

    assert result.package_results[package_id][0].symbol == "000001.SZ"
    assert result.stage_trace_by_package[package_id].hmm_adjusted.status is StageReceiptStatus.COMPLETE


def test_v2_identity_hashes_must_be_complete_and_consistent() -> None:
    prepared = _prepared_signal("pkg_identity_closure", [_candidate("000001.SZ", 0.9, 1)])

    with pytest.raises(RuntimeConfigInvalidError, match="source revision hash does not match"):
        replace(prepared, source_revision_set_hash="1" * 64)
    with pytest.raises(RuntimeConfigInvalidError, match="identity closure is incomplete"):
        replace(
            prepared,
            artifact_header=replace(prepared.artifact_header, universe_identity_hash=None),
            universe_identity_hash=None,
        )


def test_artifact_header_must_match_package_date_and_data_source() -> None:
    package_id = "pkg_artifact_context"
    prepared = _prepared_signal(package_id, [_candidate("000001.SZ", 0.9, 1)])

    with pytest.raises(RuntimeConfigInvalidError, match="artifact package identity mismatch"):
        replace(
            prepared,
            artifact_header=replace(prepared.artifact_header, package_id="pkg_other"),
        )
    with pytest.raises(RuntimeConfigInvalidError, match="artifact trade date mismatch"):
        StrategyPackageSelectionComputation().compute(
            request=_request((package_id,), SelectionMode.SINGLE_PACKAGE),
            prepared_signals={
                package_id: replace(
                    prepared,
                    artifact_header=replace(prepared.artifact_header, trade_date=date(2024, 1, 3)),
                )
            },
            providers=_providers(),
        )
    with pytest.raises(RuntimeConfigInvalidError, match="artifact data source mismatch"):
        StrategyPackageSelectionComputation().compute(
            request=_request((package_id,), SelectionMode.SINGLE_PACKAGE),
            prepared_signals={
                package_id: replace(
                    prepared,
                    artifact_header=replace(prepared.artifact_header, data_source="OTHER_SOURCE"),
                )
            },
            providers=_providers(),
        )


def test_runtime_profile_hash_must_match_profile_payload() -> None:
    request = _request(("pkg_profile_hash",), SelectionMode.SINGLE_PACKAGE)

    with pytest.raises(RuntimeConfigInvalidError, match="hash does not match profile payload"):
        replace(request, package_runtime_profile_hashes={"pkg_profile_hash": "9" * 64})


def test_sha256_identity_rejects_noncanonical_uppercase() -> None:
    prepared = _prepared_signal("pkg_upper_hash", [_candidate("000001.SZ", 0.9, 1)])

    with pytest.raises(RuntimeConfigInvalidError, match="lowercase sha256"):
        replace(prepared.artifact_header, artifact_sha256="A" * 64)


def test_provider_data_error_remains_visible() -> None:
    package_id = "pkg_provider_error"

    with pytest.raises(DataUnavailableError, match="risk policy source unavailable"):
        StrategyPackageSelectionComputation().compute(
            request=_request((package_id,), SelectionMode.SINGLE_PACKAGE),
            prepared_signals={package_id: _prepared_signal(package_id, [_candidate("000001.SZ", 0.9, 1)])},
            providers=_providers(_FailingRiskPolicy()),
        )


def test_existing_multi_package_aggregation_semantics_are_preserved() -> None:
    package_results = {
        "pkg_a": [
            _candidate("000001.SZ", 0.9, 1),
            _candidate("000002.SZ", 0.8, 2),
            _candidate("000003.SZ", 0.7, 3),
        ],
        "pkg_b": [
            _candidate("000003.SZ", 0.95, 1),
            _candidate("000002.SZ", 0.85, 2),
            _candidate("000001.SZ", 0.75, 3),
        ],
    }

    intersection = aggregate_selection_candidates(
        mode=SelectionMode.INTERSECTION,
        package_results=package_results,
    )
    union = aggregate_selection_candidates(mode=SelectionMode.UNION, package_results=package_results)
    weighted = aggregate_selection_candidates(
        mode=SelectionMode.WEIGHTED_FUSION,
        package_results=package_results,
        package_weights={"pkg_a": 1.0, "pkg_b": 1.0},
    )

    assert {item.symbol for item in intersection} == {"000001.SZ", "000002.SZ", "000003.SZ"}
    assert {item.symbol for item in union} == {"000001.SZ", "000002.SZ", "000003.SZ"}
    assert [item.symbol for item in weighted] == ["000001.SZ", "000003.SZ", "000002.SZ"]
    assert weighted[0].component_scores["fusion_method"] == "weighted_rank_fusion"
    assert weighted[0].component_scores["fusion_policy_sha256"]


def test_computation_module_has_no_runtime_or_persistence_imports() -> None:
    module_path = Path(__file__).parents[2] / "services" / "strategy_package" / "selection_computation.py"
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported_modules = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_modules.update(
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    )
    forbidden_fragments = (
        "backend.db",
        "repository",
        "simulation_runtime",
        "advisory",
        "paper_trading",
    )

    assert not any(fragment in module for module in imported_modules for fragment in forbidden_fragments)
