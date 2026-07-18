from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.advisory_dev_input_onboarding.contracts import (
    ArtifactStorePolicyArtifact,
    O4ArtifactKind,
    O4_ARTIFACT_STORE_POLICY_HASH,
    RealDevOnboardingError,
)
from backend.services.advisory_dev_input_onboarding.phase1e_orchestration import (
    AdvisoryPhase1EOrchestrationService,
)
from backend.services.advisory_phase1.source_observer import SOURCE_QUERY_TEMPLATES, registered_source_observer_configs
from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.strategy_package.models import AlphaMode


ROOT = Path(__file__).resolve().parents[3]


def test_common_o4_runtime_artifacts_are_exact_typed_and_full_readable(tmp_path: Path) -> None:
    service = AdvisoryPhase1EOrchestrationService(repository_root=ROOT)
    env_file = tmp_path / ".env"
    dataset_root = tmp_path / "advisory-datasets"
    dataset_root.mkdir()
    env_file.write_text(f"AISTOCK_ADVISORY_DATASET_STORE_ROOT={dataset_root}\n", encoding="utf-8")
    artifact_root = tmp_path / "o4-artifacts"
    store = service._input_store(artifact_root=artifact_root, env_file=env_file, code_commit="fixture")
    config = registered_source_observer_configs()[("phase1e_advisory_inputs_dev_v2", "v2")]

    refs = service._publish_common_artifacts(
        store=store,
        config=config,
        store_backend_root=dataset_root,
        policy=None,
    )

    assert {ref.artifact_kind for ref in refs.values()} == {
        O4ArtifactKind.SOURCE_QUERY_REGISTRY.value,
        O4ArtifactKind.OBSERVER_CONFIG.value,
        O4ArtifactKind.PARTITION_POLICY.value,
        O4ArtifactKind.STORE_BACKEND_POLICY.value,
        O4ArtifactKind.ARTIFACT_STORE_POLICY.value,
    }
    artifact_policy_ref = refs["artifact_store_policy_ref"]
    assert artifact_policy_ref.semantic_hash == O4_ARTIFACT_STORE_POLICY_HASH
    policy = store.load(ref=artifact_policy_ref, model_type=ArtifactStorePolicyArtifact)
    assert policy.content_hash == O4_ARTIFACT_STORE_POLICY_HASH


def test_explicit_artifact_root_conflict_fails_without_process_env_fallback(tmp_path: Path) -> None:
    service = AdvisoryPhase1EOrchestrationService(repository_root=ROOT)
    configured_root = tmp_path / "configured"
    explicit_root = tmp_path / "explicit"
    env_file = tmp_path / ".env"
    env_file.write_text(f"AISTOCK_ADVISORY_PHASE1E_ARTIFACT_ROOT={configured_root}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="conflicts"):
        service._input_store(artifact_root=explicit_root, env_file=env_file, code_commit=None)


def test_pre_observation_lineage_matches_live_inference_calendar_contract() -> None:
    service = AdvisoryPhase1EOrchestrationService(repository_root=ROOT)
    decision_date = date(2026, 7, 17)
    trading_dates = tuple(decision_date - timedelta(days=offset) for offset in reversed(range(80)))
    repository = SimpleNamespace(list_trading_days=lambda **_kwargs: trading_dates)
    projection = SimpleNamespace(
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        legs=(SimpleNamespace(alpha_component_id="single", required_window=60),),
    )

    (lineage,) = service._pre_observation_lineages(
        projection=projection,
        decision_trade_date=decision_date,
        calendar_reader=repository,
    )

    selected = trading_dates[-65:]
    calendar_identity_hash = canonical_json_sha256(
        {
            "dataset_id": "market.trading_calendar",
            "effective_trade_date": selected[-1].isoformat(),
            "calendar_version": "market.trading_calendar.v1",
            "calendar_source": "market.trading_calendar",
        }
    )
    assert lineage.window_start_date == selected[0]
    assert lineage.trading_dates == selected
    assert lineage.window_resolution == "trading_calendar"
    assert lineage.window_lineage_hash == canonical_json_sha256(
        {
            "calendar_identity_hash": calendar_identity_hash,
            "window_start_date": selected[0].isoformat(),
            "required_window": 60,
            "window_resolution": "trading_calendar",
        }
    )


def test_dataset_schema_fingerprint_is_closed_from_compiled_template_schemas() -> None:
    expected = canonical_json_sha256(
        [
            {
                "template_id": template.template_id,
                "template_version": template.template_version,
                "schema_fingerprint": template.schema_fingerprint,
            }
            for template in sorted(
                SOURCE_QUERY_TEMPLATES.values(),
                key=lambda item: (item.template_id, item.template_version),
            )
        ]
    )

    assert AdvisoryPhase1EOrchestrationService._dataset_schema_fingerprint() == expected


def test_pre_observation_cutoff_is_the_next_trade_date_entry_deadline() -> None:
    decision_date = date(2026, 7, 17)
    target_date = date(2026, 7, 20)
    calendar = SimpleNamespace(list_trading_days=lambda **_kwargs: [target_date])

    cutoff = AdvisoryPhase1EOrchestrationService._prospective_decision_cutoff(
        calendar_reader=calendar,
        decision_trade_date=decision_date,
    )

    assert cutoff.isoformat() == "2026-07-20T09:25:00+08:00"


def test_source_only_workload_uses_authoritative_universe_count_and_rejects_drift() -> None:
    service = AdvisoryPhase1EOrchestrationService(repository_root=ROOT)
    payload = {
        "point_in_time_context": {"parent_input_universe_count": 4200},
        "phase0a_universe_evidence": {
            "layers": [{"layer": "package_eligible_universe", "output_count": 4200}]
        },
    }
    evidence = SimpleNamespace(
        selection_evidence=SimpleNamespace(evidence_payload_json=payload),
        selection_artifact=SimpleNamespace(universe_count=4200, scores_json=[{"symbol": "000001.SZ"}]),
    )
    context = {
        "program_request_evidence": evidence,
        "program_date": SimpleNamespace(
            program_id="program_o4",
            decision_trade_date=date(2026, 7, 17),
            style_family="trend",
            package_id="pkg_o4",
            manifest_sha256="a" * 64,
            alpha_mode=AlphaMode.MULTI_ALPHA,
        ),
    }

    workload = service._source_only_workload(context=context, requirement_set_hash="b" * 64)
    assert workload.input_universe_count == 4200
    assert workload.candidate_depth == 1

    payload["phase0a_universe_evidence"]["layers"][0]["output_count"] = 4199
    with pytest.raises(RealDevOnboardingError, match="package-eligible universe evidence"):
        service._source_only_workload(context=context, requirement_set_hash="b" * 64)
