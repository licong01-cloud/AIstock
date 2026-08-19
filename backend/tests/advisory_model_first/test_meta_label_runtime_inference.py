from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import model_inference
from backend.services.advisory_model_first.feature_schema_v1 import MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.model_binding_resolution import (
    AdvisoryModelBindingResolutionV1,
    META_LABEL_MODEL_ROLE,
)
from backend.services.advisory_model_first.model_inference import AdvisoryModelShadowService
from backend.services.selection_center.models import SelectionRunStatus
from backend.tests.advisory_model_first.test_model_inference import (
    BINDING_VERSION_ID,
    FUND_LEG_ID,
    LSTM_LEG_ID,
    MANIFEST_SHA256,
    PACKAGE_ID,
    PROGRAM_ID,
    RUNTIME_SEMANTICS_HASH,
    STYLE_PROFILE_ID,
    _ProgramService,
    _ReviewSource,
    _candidate,
)


class _MetaBindingResolver:
    def is_configured(self, **_: object) -> bool:
        return True

    def resolve(self, **_: object) -> AdvisoryModelBindingResolutionV1:
        return AdvisoryModelBindingResolutionV1(
            program_id=PROGRAM_ID,
            binding_version_id=BINDING_VERSION_ID,
            package_id=PACKAGE_ID,
            manifest_sha256=MANIFEST_SHA256,
            style_profile_id=STYLE_PROFILE_ID,
            style_profile_hash="8e8226885af25dbf1830403ea2ba768ec4a135a35680f827ad30994c0369904b",
            selection_runtime_semantics_hash=RUNTIME_SEMANTICS_HASH,
            feature_schema_version="advisory_feature_schema_v1",
            feature_schema_hash="e56adb47d444df26e35eb327d3aacacd273477edf67c4c1db201ea5b4c3bd49c",
            bundle_id="e" * 64,
            bundle_manifest_sha256="f" * 64,
            component_roles={"lstm": LSTM_LEG_ID, "fund": FUND_LEG_ID},
            descriptor_sha256="d" * 64,
            model_role=META_LABEL_MODEL_ROLE,
            shadow_policy_sha256="1" * 64,
            terminal_weights={LSTM_LEG_ID: 0.6966591521, FUND_LEG_ID: 0.3033408479},
        )


class _Selection20Service:
    def __init__(self, *, candidate_count: int = 20) -> None:
        self.run = SimpleNamespace(
            status=SelectionRunStatus.SUCCEEDED,
            trade_date=pd.Timestamp("2026-07-21").date(),
            package_ids=[PACKAGE_ID],
            runtime_config={},
            manifest_sha256_by_package={PACKAGE_ID: MANIFEST_SHA256},
            aggregate_results=[
                _candidate(f"{rank:06d}.SZ", rank=rank, score=1.0 - rank / 100.0)
                for rank in range(1, candidate_count + 1)
            ],
        )

    def get_run(self, run_id: str):
        assert run_id == "selection-1"
        return self.run


class _FeatureSource:
    def __init__(self) -> None:
        self.last_kwargs: dict[str, object] = {}

    def load(self, **kwargs: object):
        self.last_kwargs = dict(kwargs)
        return SimpleNamespace(
            candidate_daily=None,
            candidate_static=None,
            market_daily=None,
            benchmark_daily=None,
            suspend_rows=None,
            hmm_states=None,
            hmm_unavailable=(),
        )


def _meta_bundle() -> dict[str, object]:
    return {
        "manifest": {
            "schema_version": "advisory_meta_label_bundle_v1",
            "model_role": META_LABEL_MODEL_ROLE,
            "status": "EXPERIMENTAL_MODEL",
            "calibration_state": "UNCALIBRATED",
            "program_id": PROGRAM_ID,
            "binding_version_id": BINDING_VERSION_ID,
            "package_id": PACKAGE_ID,
            "manifest_sha256": MANIFEST_SHA256,
            "style_profile_id": STYLE_PROFILE_ID,
            "style_profile_hash": "8e8226885af25dbf1830403ea2ba768ec4a135a35680f827ad30994c0369904b",
            "shadow_policy_sha256": "1" * 64,
            "feature_schema_version": "advisory_feature_schema_v1",
            "feature_schema_hash": "e56adb47d444df26e35eb327d3aacacd273477edf67c4c1db201ea5b4c3bd49c",
            "bundle_id": "e" * 64,
            "request_id": "advmetareq_test",
        },
        "feature_schema": {
            "schema_version": "advisory_feature_schema_v1",
            "feature_schema_hash": "e56adb47d444df26e35eb327d3aacacd273477edf67c4c1db201ea5b4c3bd49c",
            "trained_feature_names": list(MODEL_FEATURE_COLUMNS),
            "categorical_vocabulary": {"l2_code_id": [1, 2]},
        },
        "manifest_file_sha256": "f" * 64,
        "continuation_cutoff": "2026-02-02",
        "hmm_models": {"schema_version": "fresh_sector_hmm_bundle_v1", "models": {"1": {}}},
        "baselines": {"selection_top5": {"mean_net_excess_return_bps": 1.0}},
    }


def _fake_feature_build(*, candidates: pd.DataFrame, **_: object):
    features = candidates.copy()
    features["parent_combined_score"] = features["combined_score"]
    missing = [column for column in MODEL_FEATURE_COLUMNS if column not in features]
    features = pd.concat(
        [features, pd.DataFrame(0.0, index=features.index, columns=missing)],
        axis=1,
    )
    return SimpleNamespace(
        features=features,
        coverage=pd.DataFrame([{"status": "available", "required_missing_columns": []}]),
    )


def _reverse_rank_scorer(_bundle: object, features: pd.DataFrame) -> pd.DataFrame:
    output = features[
        ["decision_as_of_trade_date", "target_trade_date", "instrument", "selection_effective_rank"]
    ].copy()
    output["take_probability"] = output["selection_effective_rank"] / 20.0
    output["skip_probability"] = 1.0 - output["take_probability"]
    output["advisory_model_confidence"] = abs(output["take_probability"] - 0.5) * 2.0
    output = output.sort_values(
        ["take_probability", "selection_effective_rank", "instrument"],
        ascending=[False, True, True],
    )
    output["entry_priority_rank"] = range(1, len(output) + 1)
    output["selection_exit_rank"] = output["selection_effective_rank"]
    output["model_status"] = "EXPERIMENTAL_MODEL"
    output["calibration_state"] = "UNCALIBRATED"
    return output.reset_index(drop=True)


def test_meta_label_runtime_reorders_top20_entry_only_without_legacy_fallback(monkeypatch) -> None:
    monkeypatch.setattr(model_inference, "build_advisory_feature_matrix", _fake_feature_build)
    feature_source = _FeatureSource()

    def _legacy_loader(**_: object):
        raise AssertionError("legacy quality bundle loader must not be called")

    service = AdvisoryModelShadowService(
        program_service=_ProgramService(target_count=20),
        selection_service=_Selection20Service(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model-root",
        bundle_loader=_legacy_loader,
        meta_label_bundle_loader=lambda **_: _meta_bundle(),
        meta_label_scorer=_reverse_rank_scorer,
        outcome_bundle_loader=_legacy_loader,
        price_range_bundle_loader=_legacy_loader,
        binding_resolver=_MetaBindingResolver(),
    )

    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "EXPERIMENTAL_SHADOW"
    assert result["model_role"] == META_LABEL_MODEL_ROLE
    assert result["candidate_count"] == 20
    assert result["shortlist_count"] == 5
    assert result["candidates"][0]["symbol"] == "000020.SZ"
    assert result["candidates"][0]["advisory_model_rank"] == 1
    assert result["candidates"][0]["selection_effective_rank"] == 20
    assert result["candidates"][0]["selection_exit_rank"] == 20
    assert result["candidates"][-1]["symbol"] == "000001.SZ"
    assert result["outcome"]["status"] == "OUTCOME_UNAVAILABLE"
    assert result["price_range"]["status"] == "PRICE_RANGE_UNAVAILABLE"
    assert len(feature_source.last_kwargs["symbols"]) == 20
    assert feature_source.last_kwargs["continuation_cutoff"].isoformat() == "2026-02-02"


def test_meta_label_runtime_rejects_non_top20_program_before_feature_access() -> None:
    feature_source = _FeatureSource()
    service = AdvisoryModelShadowService(
        program_service=_ProgramService(target_count=5),
        selection_service=_Selection20Service(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model-root",
        meta_label_bundle_loader=lambda **_: _meta_bundle(),
        binding_resolver=_MetaBindingResolver(),
    )

    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "MODEL_UNAVAILABLE"
    assert result["reason_code"] == "ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH"
    assert feature_source.last_kwargs == {}


def test_meta_label_runtime_rejects_incomplete_selection_top20_before_feature_access() -> None:
    feature_source = _FeatureSource()
    service = AdvisoryModelShadowService(
        program_service=_ProgramService(target_count=20),
        selection_service=_Selection20Service(candidate_count=19),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model-root",
        meta_label_bundle_loader=lambda **_: _meta_bundle(),
        binding_resolver=_MetaBindingResolver(),
    )

    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "MODEL_UNAVAILABLE"
    assert result["reason_code"] == "ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE"
    assert feature_source.last_kwargs == {}


@pytest.mark.parametrize(
    "mutation",
    (
        lambda scored: scored.__setitem__(
            "entry_priority_rank", scored["entry_priority_rank"].astype(float) + 0.5
        ),
        lambda scored: scored.__setitem__("advisory_model_confidence", 0.0),
        lambda scored: scored.__setitem__("decision_as_of_trade_date", pd.Timestamp("2026-07-19")),
    ),
)
def test_meta_label_runtime_rejects_rank_confidence_or_clock_drift(monkeypatch, mutation) -> None:
    monkeypatch.setattr(model_inference, "build_advisory_feature_matrix", _fake_feature_build)

    def _mutated_scorer(bundle: object, features: pd.DataFrame) -> pd.DataFrame:
        scored = _reverse_rank_scorer(bundle, features)
        mutation(scored)
        return scored

    service = AdvisoryModelShadowService(
        program_service=_ProgramService(target_count=20),
        selection_service=_Selection20Service(),
        review_source=_ReviewSource(),
        feature_source=_FeatureSource(),
        model_root_provider=lambda: "/model-root",
        meta_label_bundle_loader=lambda **_: _meta_bundle(),
        meta_label_scorer=_mutated_scorer,
        binding_resolver=_MetaBindingResolver(),
    )

    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "MODEL_UNAVAILABLE"
    assert result["reason_code"] == "ADVISORY_META_LABEL_SCORING_INVALID"


def test_meta_label_runtime_rejects_feature_schema_identity_drift_before_feature_access() -> None:
    feature_source = _FeatureSource()
    bundle = _meta_bundle()
    bundle["feature_schema"]["feature_schema_hash"] = "9" * 64
    service = AdvisoryModelShadowService(
        program_service=_ProgramService(target_count=20),
        selection_service=_Selection20Service(),
        review_source=_ReviewSource(),
        feature_source=feature_source,
        model_root_provider=lambda: "/model-root",
        meta_label_bundle_loader=lambda **_: bundle,
        binding_resolver=_MetaBindingResolver(),
    )

    result = service.model_shadow(
        program_id=PROGRAM_ID,
        target_trade_date=pd.Timestamp("2026-07-21").date(),
    )

    assert result["status"] == "MODEL_UNAVAILABLE"
    assert result["reason_code"] == "ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH"
    assert feature_source.last_kwargs == {}
