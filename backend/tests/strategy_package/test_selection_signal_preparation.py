from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.selection_center.hmm_runtime import SectorHMMRuntime
from backend.services.strategy_package.models import AlphaMode, SelectionScoreArtifactStatus
from backend.services.strategy_package.selection_artifact import (
    SelectionScoreArtifact,
    StrategyPackageSelectionArtifactService,
)
from backend.services.strategy_package.selection_signal_preparation import (
    StrategyPackageSelectionSignalPreparation,
    _ForbiddenSelectionArtifactRepository,
    build_historical_strategy_package_signal_preparation,
)
from backend.services.strategy_package.live_inference import WslStrategyPackageInferenceProvider
from backend.services.trading_core.errors import HMMRuntimeUnavailableError, RuntimeConfigInvalidError


TRADE_DATE = date(2026, 6, 2)
SHA = "a" * 64


@dataclass
class _Record:
    package_version: str
    manifest: object

    def current_manifest(self):  # noqa: ANN202
        return self.manifest


class _PackageReader:
    def __init__(self, record: _Record) -> None:
        self.record = record

    def get(self, package_id: str) -> _Record:
        assert package_id == "pkg-single"
        return self.record


class _RawPreparer:
    def __init__(self, *, empty: bool = False) -> None:
        self.calls: list[dict] = []
        self.empty = empty

    def prepare_from_live_inference_dates(self, **kwargs):  # noqa: ANN003, ANN202
        self.calls.append(kwargs)
        rows = [] if self.empty else [
            {
                "symbol": "000001.SZ",
                "score": 0.25,
                "rank": 1,
                "target_weight": 0.2,
                "reference_price": None,
                "component_scores": {"alpha": {"raw_score": 0.25}},
                "reason": "live_inference_score",
            }
        ]
        count = len(rows)
        context = {
            "calendar_hash": "b" * 64,
            "universe_input_hash": "c" * 64,
        }
        return [
            SelectionScoreArtifact(
                artifact_id=f"ssa_operational_{len(self.calls)}",
                package_id="pkg-single",
                manifest_sha256=SHA,
                trade_date=TRADE_DATE,
                data_source="DB_HISTORICAL",
                runtime_config_hash="d" * 64,
                scores_json=rows,
                score_count=count,
                universe_count=5000,
                top_score_symbol=rows[0]["symbol"] if rows else None,
                status=SelectionScoreArtifactStatus.SUCCEEDED,
                metadata={
                    "authority_scope": "authoritative_selection",
                    "candidate_outcome": "CANDIDATES_PRESENT" if rows else "VALID_NO_CANDIDATE",
                    "empty_stage": None if rows else "alpha_raw",
                    "provider_semantics_id": "strategy_package_live_inference_v2",
                    "provider_semantics_hash": "e" * 64,
                    "artifact_input_context": context,
                    "source_read_receipts": [
                        {
                            "source_role": "pit_universe",
                            "dataset_id": "market.stock_universe_pit",
                            "content_hash": "f" * 64,
                            "first_observed_at": (
                                datetime(2026, 7, 20, tzinfo=UTC) + timedelta(seconds=len(self.calls))
                            ).isoformat(),
                        }
                    ],
                },
                artifact_contract_version="selection_score_artifact_v2",
                artifact_input_context_hash=canonical_json_sha256(context),
                source_revision_set_hash="1" * 64,
                asset_closure_hash="2" * 64,
            )
        ]


def _service(*, empty: bool = False):  # noqa: ANN202
    component = SimpleNamespace(
        alpha_id="alpha",
        component_weight=1.0,
        factor_ids=["factor_a"],
        score_normalization="none",
    )
    manifest = SimpleNamespace(
        package_id="pkg-single",
        package_version="1.0.0",
        manifest_sha256=SHA,
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        alpha_components=[component],
    )
    raw = _RawPreparer(empty=empty)
    service = StrategyPackageSelectionSignalPreparation(
        package_reader=_PackageReader(_Record(package_version="1.0.0", manifest=manifest)),
        raw_artifact_preparer=raw,
        hmm_runtime=SectorHMMRuntime(),
    )
    return service, raw


def test_historical_signal_preparation_is_repository_free_and_deterministic() -> None:
    service, raw = _service()
    config = {"runtime_profile": {"selection": {"top_k": 5}, "hmm": {"enabled": False}}}

    first = service.prepare_historical(package_id="pkg-single", trade_date=TRADE_DATE, runtime_config=config)
    second = service.prepare_historical(package_id="pkg-single", trade_date=TRADE_DATE, runtime_config=config)

    assert first.raw.signal_id == second.raw.signal_id
    assert first.raw.signal_id.startswith("ahrsig_")
    assert first.prepared_signal.artifact_header.artifact_id == first.raw.signal_id
    assert [item.symbol for item in first.prepared_signal.alpha_raw_candidates] == ["000001.SZ"]
    assert first.prepared_signal.hmm_adjusted_candidates == first.prepared_signal.alpha_raw_candidates
    assert all(call["historical_read_only"] is True for call in raw.calls)
    assert all(call["include_reference_price"] is False for call in raw.calls)
    assert all(call["cutoff_date"] == TRADE_DATE for call in raw.calls)


def test_historical_signal_preparation_accepts_only_evidenced_raw_empty() -> None:
    service, _raw = _service(empty=True)
    result = service.prepare_historical(
        package_id="pkg-single",
        trade_date=TRADE_DATE,
        runtime_config={"runtime_profile": {"selection": {"top_k": 5}, "hmm": {"enabled": False}}},
    )

    assert result.prepared_signal.valid_no_candidate is True
    assert result.prepared_signal.alpha_raw_candidates == ()
    assert result.raw.raw_inference_receipt["universe_count"] == 5000


def test_historical_hmm_raw_empty_still_preflights_exact_frozen_artifacts() -> None:
    service, _raw = _service(empty=True)

    class _HMM:
        def __init__(self) -> None:
            self.delegate = SectorHMMRuntime()
            self.preflight_calls = []

        def adjust_candidates_with_receipt(self, **kwargs):  # noqa: ANN003, ANN201
            return self.delegate.adjust_candidates_with_receipt(**kwargs)

        def preflight_coefficients(self, **kwargs):  # noqa: ANN003, ANN201
            self.preflight_calls.append(kwargs)
            return {
                "enabled": True,
                "model_snapshot_id": "snapshot-1",
                "signal_preset": "preset-1",
                "generation_mode": "EXACT_SNAPSHOT",
                "model_artifact_sha256": "3" * 64,
                "coefficient_sha256": "4" * 64,
                "input_data_max_dates_hash": canonical_json_sha256({"market": TRADE_DATE.isoformat()}),
                "snapshot_trained_at": TRADE_DATE.isoformat(),
                "available_at": f"{TRADE_DATE.isoformat()}T00:00:00+00:00",
                "training_information_cutoff": TRADE_DATE.isoformat(),
                "as_of_trade_date": TRADE_DATE.isoformat(),
                "effective_trade_date": TRADE_DATE.isoformat(),
            }

    hmm = _HMM()
    service._hmm_runtime = hmm
    metadata = {
        "model_snapshot_id": "snapshot-1",
        "signal_preset": "preset-1",
        "model_artifact_sha256": "3" * 64,
        "coefficient_sha256": "4" * 64,
        "snapshot_trained_at": TRADE_DATE.isoformat(),
        "available_at": f"{TRADE_DATE.isoformat()}T00:00:00+00:00",
        "training_information_cutoff": TRADE_DATE.isoformat(),
        "as_of_trade_date": TRADE_DATE.isoformat(),
        "effective_trade_date": TRADE_DATE.isoformat(),
        "generation_mode": "EXACT_SNAPSHOT",
        "input_data_max_dates": {"market": TRADE_DATE.isoformat()},
    }
    result = service.prepare_historical(
        package_id="pkg-single",
        trade_date=TRADE_DATE,
        runtime_config={
            "runtime_profile": {
                "selection": {"top_k": 5},
                "hmm": {
                    "enabled": True,
                    "model_snapshot_id": "snapshot-1",
                    "signal_preset": "preset-1",
                },
            },
            "phase0a_hmm_metadata": metadata,
        },
    )

    assert hmm.preflight_calls[0]["require_frozen_snapshot"] is True
    assert result.prepared_signal.hmm_metadata["coefficient_sha256"] == "4" * 64
    assert result.prepared_signal.hmm_receipt.semantic_payload["generation_mode"] == "EXACT_SNAPSHOT"


def test_historical_hmm_unavailable_error_keeps_original_context() -> None:
    service, _raw = _service(empty=True)

    class _UnavailableHMM:
        def adjust_candidates_with_receipt(self, **_kwargs):  # noqa: ANN003, ANN201
            raise HMMRuntimeUnavailableError(
                "missing coefficients",
                context={"reason_code": "ADVISORY_PHASE0A2C_HMM_RECEIPT_INCOMPLETE", "path": "missing.json"},
            )

    service._hmm_runtime = _UnavailableHMM()
    metadata = {
        "model_snapshot_id": "snapshot-1",
        "signal_preset": "preset-1",
        "model_artifact_sha256": "3" * 64,
        "coefficient_sha256": "4" * 64,
        "snapshot_trained_at": TRADE_DATE.isoformat(),
        "available_at": f"{TRADE_DATE.isoformat()}T00:00:00+00:00",
        "training_information_cutoff": TRADE_DATE.isoformat(),
        "as_of_trade_date": TRADE_DATE.isoformat(),
        "effective_trade_date": TRADE_DATE.isoformat(),
        "generation_mode": "EXACT_SNAPSHOT",
        "input_data_max_dates": {"market": TRADE_DATE.isoformat()},
    }

    with pytest.raises(HMMRuntimeUnavailableError) as exc_info:
        service.prepare_historical(
            package_id="pkg-single",
            trade_date=TRADE_DATE,
            runtime_config={
                "runtime_profile": {
                    "selection": {"top_k": 5},
                    "hmm": {
                        "enabled": True,
                        "model_snapshot_id": "snapshot-1",
                        "signal_preset": "preset-1",
                    },
                },
                "phase0a_hmm_metadata": metadata,
            },
        )

    assert exc_info.value.context["reason_code"] == "ADVISORY_HR_HMM_INPUT_UNAVAILABLE"
    assert exc_info.value.context["original_reason_code"] == "ADVISORY_PHASE0A2C_HMM_RECEIPT_INCOMPLETE"


def test_historical_signal_repository_sentinel_fails_loudly() -> None:
    repository = _ForbiddenSelectionArtifactRepository()

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        repository.save(object())

    assert exc_info.value.context == {
        "reason_code": "ADVISORY_HR_SELECTION_ARTIFACT_REPOSITORY_FORBIDDEN",
        "operation": "save",
    }


def test_current_selection_adapter_keeps_default_policy_and_saves_each_artifact_once() -> None:
    class _ArtifactRepository:
        def __init__(self) -> None:
            self.saved = []

        def save(self, artifact):  # noqa: ANN001, ANN201
            self.saved.append(artifact)
            return artifact

    artifact_repository = _ArtifactRepository()
    service = StrategyPackageSelectionArtifactService(
        package_repository=object(),
        artifact_repository=artifact_repository,
        runtime_asset_resolver=object(),
        live_inference_provider=object(),
    )
    prepared = [object(), object()]
    captured = {}

    def prepare(**kwargs):  # noqa: ANN003, ANN201
        captured.update(kwargs)
        return prepared

    service.prepare_from_live_inference_dates = prepare  # type: ignore[method-assign]
    result = service.generate_from_live_inference_dates(
        package_id="pkg-current",
        trade_dates=[TRADE_DATE],
    )

    assert result == prepared
    assert artifact_repository.saved == prepared
    assert captured["historical_read_only"] is False
    assert captured["include_reference_price"] is True


def test_historical_composition_identifies_injected_wsl_backend(tmp_path) -> None:  # noqa: ANN001
    package_reader = object()
    wsl = WslStrategyPackageInferenceProvider(repo_root=tmp_path)
    preparation = build_historical_strategy_package_signal_preparation(
        package_reader=package_reader,
        package_asset_store=object(),
        runtime_root=tmp_path / "runtime",
        repository_root=tmp_path,
        hmm_snapshot_provider=None,
        wsl_inference_provider=wsl,
    )

    provider, backend = preparation._raw_artifact_preparer._resolve_live_provider({})
    assert provider is wsl
    assert backend == "wsl"


def test_historical_composition_binds_only_its_explicit_runtime_root(tmp_path) -> None:  # noqa: ANN001
    runtime_root = tmp_path / "phase1r-runtime"
    preparation = build_historical_strategy_package_signal_preparation(
        package_reader=object(),
        package_asset_store=object(),
        runtime_root=runtime_root,
        repository_root=tmp_path,
        hmm_snapshot_provider=None,
    )

    provider, backend = preparation._raw_artifact_preparer._resolve_live_provider({})

    assert isinstance(provider, WslStrategyPackageInferenceProvider)
    assert provider.safe_artifact_roots == (runtime_root.resolve(),)
    assert backend == "wsl"
