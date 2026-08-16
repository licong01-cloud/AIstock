from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SNAPSHOT_PREFIX,
    CANONICAL_PIT_UNIVERSE_KEY,
    PitAuthorityStatus,
    canonical_rule_parameters_digest,
)
from backend.services.canonical_pit_dataset_consumer import (
    CanonicalPitDatasetConsumerError,
    FormalDatasetUsage,
)
from backend.services.dataset_release.cas_store import canonical_json_bytes
from backend.services.dataset_release.pit import (
    DATASET_CANDIDATE_MANIFEST_SCHEMA,
    DATASET_PIT_BINDING_SCHEMA,
    FrozenPitSnapshot,
    PitSnapshotError,
    freeze_pit_snapshot,
)
from backend.services.quantevolver.config_composer import (
    ConfigComposer,
    QE_FORMAL_DATASET_BINDING_FILE,
    RDAGENT_DEFAULT_DATA_SPLIT,
)
from backend.services.quantevolver.experiment_config import ExperimentConfig
from backend.services.quantevolver.factor_universe_mask_service import (
    FactorUniverseMaskService,
)
from backend.services.quantevolver.long_trend_data_reader import (
    QELongTrendDatasetReader,
    inspect_qe_snapshot_identity,
)
from backend.services.quantevolver.long_trend_evaluation_contract import (
    QELongTrendError,
)
from backend.services.quantevolver.qe_dataset_contract import (
    QE_FORMAL_DATASET_REQUEST_PARAM,
    QEFormalDatasetBinding,
    QEFormalDatasetRequest,
    QEFormalRuntimePins,
    require_qe_formal_dataset_binding,
)


def _snapshot() -> FrozenPitSnapshot:
    return freeze_pit_snapshot(
        [
            {
                "ts_code": "000001.SZ",
                "eligible_start": "2018-08-01",
                "eligible_end": "2026-07-31",
                "entry_reason": "warmup_complete",
                "exit_reason": None,
            },
            {
                "ts_code": "600462.SH",
                "eligible_start": "2018-08-01",
                "eligible_end": "2025-07-18",
                "entry_reason": "warmup_complete",
                "exit_reason": "delisted",
            },
        ],
        universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        scope_start=date(2018, 8, 1),
        cutoff=date(2026, 7, 31),
        state_identity="fixture-state",
        source_fingerprint_sha256="b" * 64,
        parameter_hash=canonical_rule_parameters_digest(),
    )


def _manifest(snapshot: FrozenPitSnapshot, *, scope: str = "full") -> dict:
    release_id = "qe-hmm-v2-20260731"
    return {
        "schema_version": DATASET_CANDIDATE_MANIFEST_SCHEMA,
        "release_id": release_id,
        "cutoff": snapshot.cutoff.isoformat(),
        "scope": scope,
        "artifact_root": "c" * 64,
        "pit_binding": {
            "schema_version": DATASET_PIT_BINDING_SCHEMA,
            "authority_id": CANONICAL_PIT_AUTHORITY_ID,
            "authority_status": PitAuthorityStatus.ACTIVE_CANONICAL.value,
            "scope": scope,
            "rolling_universe_key": CANONICAL_PIT_UNIVERSE_KEY,
            "frozen_universe_key": f"{CANONICAL_PIT_SNAPSHOT_PREFIX}{release_id}",
            "rule_version": CANONICAL_PIT_RULE_VERSION,
            "rule_parameters_digest": canonical_rule_parameters_digest(),
            "cutoff": snapshot.cutoff.isoformat(),
            "rolling_cutoff_spans_sha256": snapshot.spans_sha256,
            "frozen_snapshot_digest": snapshot.spans_sha256,
            "release_id": release_id,
        },
    }


def _digest(value: dict) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _binding(
    *,
    usage_mode: FormalDatasetUsage = FormalDatasetUsage.TRAINING,
) -> tuple[QEFormalDatasetBinding, FrozenPitSnapshot, dict]:
    snapshot = _snapshot()
    manifest = _manifest(snapshot)
    binding = require_qe_formal_dataset_binding(
        manifest,
        usage_mode=usage_mode,
        expected_manifest_digest=_digest(manifest),
    )
    return binding, snapshot, manifest


def _request(
    *,
    usage_mode: FormalDatasetUsage = FormalDatasetUsage.TRAINING,
) -> tuple[QEFormalDatasetRequest, QEFormalDatasetBinding, FrozenPitSnapshot, dict]:
    binding, snapshot, manifest = _binding(usage_mode=usage_mode)
    request = QEFormalDatasetRequest.from_release_manifest(
        manifest,
        usage_mode=usage_mode,
        expected_manifest_digest=_digest(manifest),
        runtime_pins=QEFormalRuntimePins(
            artifact_root=manifest["artifact_root"],
            qlib_bin_snapshot_id="qe-hmm-v2-20260731-daily",
            qlib_instruments_sha256="d" * 64,
            qlib_calendar_sha256="e" * 64,
            qlib_meta_export_sha256="f" * 64,
            suspend_dataset_id="qe-hmm-v2-20260731-suspend",
            suspend_parquet_sha256="1" * 64,
            suspend_manifest_sha256="2" * 64,
            suspend_source_contract="tushare_suspend_d_shsz_S_v1",
        ),
    )
    return request, binding, snapshot, manifest


def test_formal_qe_binding_roundtrips_through_experiment_config_without_default_drift(
    monkeypatch,
) -> None:
    import backend.services.canonical_equity_pit as canonical_pit

    monkeypatch.setattr(
        canonical_pit,
        "get_conn",
        lambda *args, **kwargs: pytest.fail("formal QE binding must not query PIT DB"),
    )
    request, binding, _, _ = _request()
    config = ExperimentConfig(
        factor_names=["DemoFactor"],
        model_id="lgbm",
        canonical_pit_dataset=request,
    )

    params = config.build_custom_params()
    assert params[QE_FORMAL_DATASET_REQUEST_PARAM] == request.as_dict()
    restored = ExperimentConfig.model_validate(config.model_dump(mode="json"))
    assert restored.canonical_pit_dataset == request
    assert restored.build_custom_params()[QE_FORMAL_DATASET_REQUEST_PARAM] == request.as_dict()

    legacy = ExperimentConfig(factor_names=["DemoFactor"], model_id="lgbm")
    assert QE_FORMAL_DATASET_REQUEST_PARAM not in legacy.build_custom_params()
    with pytest.raises(ValueError, match="is reserved"):
        ExperimentConfig(
            factor_names=["DemoFactor"],
            model_id="lgbm",
            extra_params={QE_FORMAL_DATASET_REQUEST_PARAM: request.as_dict()},
        )
    tampered = request.as_dict()
    tampered["release_manifest"]["artifact_root"] = "d" * 64
    with pytest.raises(ValueError, match="digest differs from immutable reference"):
        ExperimentConfig(
            factor_names=["DemoFactor"],
            model_id="lgbm",
            canonical_pit_dataset=tampered,
        )
    wrong_root = request.as_dict()
    wrong_root["runtime_pins"]["artifact_root"] = "d" * 64
    with pytest.raises(ValueError, match="artifact_root differs"):
        ExperimentConfig(
            factor_names=["DemoFactor"],
            model_id="lgbm",
            canonical_pit_dataset=wrong_root,
        )
    wrong_pin_schema = request.as_dict()
    wrong_pin_schema["runtime_pins"]["schema_version"] = "legacy_runtime_pins_v0"
    with pytest.raises(ValueError, match="runtime pins schema_version"):
        ExperimentConfig(
            factor_names=["DemoFactor"],
            model_id="lgbm",
            canonical_pit_dataset=wrong_pin_schema,
        )
    unsafe_sidecar = request.as_dict()
    unsafe_sidecar["runtime_pins"]["suspend_dataset_id"] = "../outside"
    with pytest.raises(ValueError, match="suspend_dataset_id is not canonical"):
        ExperimentConfig(
            factor_names=["DemoFactor"],
            model_id="lgbm",
            canonical_pit_dataset=unsafe_sidecar,
        )
    coerced_pin = request.as_dict()
    coerced_pin["runtime_pins"]["suspend_source_contract"] = 123
    with pytest.raises(ValueError, match="runtime pins values must be strings"):
        QEFormalRuntimePins.from_mapping(coerced_pin["runtime_pins"])
    noncanonical_digest = request.as_dict()
    noncanonical_digest["expected_manifest_digest"] = request.expected_manifest_digest.upper()
    with pytest.raises(ValueError, match="expected_manifest_digest is not canonical"):
        ExperimentConfig(
            factor_names=["DemoFactor"],
            model_id="lgbm",
            canonical_pit_dataset=noncanonical_digest,
        )


def test_formal_qe_binding_rejects_sample_and_legacy_reproduction() -> None:
    snapshot = _snapshot()
    sample = _manifest(snapshot, scope="sample")
    with pytest.raises(CanonicalPitDatasetConsumerError, match="sample PIT binding cannot drive"):
        require_qe_formal_dataset_binding(
            sample,
            usage_mode=FormalDatasetUsage.TRAINING,
            expected_manifest_digest=_digest(sample),
        )

    legacy = {"universe_key": "shsz_st_pit_active_v1", "reproduction_mode": True}
    with pytest.raises(CanonicalPitDatasetConsumerError, match="legacy/v1 manifests are forbidden"):
        require_qe_formal_dataset_binding(
            legacy,
            usage_mode=FormalDatasetUsage.PREDICTION,
            expected_manifest_digest=_digest(legacy),
        )


def test_formal_factor_universe_reads_only_frozen_snapshot(monkeypatch) -> None:
    import backend.services.quantevolver.factor_universe_mask_service as mask_module

    monkeypatch.setattr(
        mask_module,
        "get_conn",
        lambda *args, **kwargs: pytest.fail("formal factor universe must not query PIT DB"),
    )
    binding, snapshot, manifest = _binding()
    service = FactorUniverseMaskService.from_formal_release(
        release_manifest=manifest,
        expected_manifest_digest=_digest(manifest),
        usage_mode=FormalDatasetUsage.TRAINING,
        frozen_snapshot=snapshot.as_dict(),
    )

    metadata = service.metadata(start_date="2025-07-17", end_date="2025-07-21")
    assert metadata["release_id"] == binding.release_id
    assert metadata["universe_fingerprint_sha256"] == binding.frozen_snapshot_digest
    assert metadata["release_manifest_digest"] == binding.manifest_digest
    dates = pd.to_datetime(["2025-07-17", "2025-07-18", "2025-07-21"])
    mask = service.build_eligible_mask(dates, ["000001.SZ", "600462.SH"])
    assert mask.tolist() == [[True, True], [True, True], [True, False]]
    eligible = service.build_eligible_index(
        start_date="2025-07-17",
        end_date="2025-07-21",
        trading_dates=dates,
    )
    assert list(eligible) == [
        (pd.Timestamp("2025-07-17"), "000001.SZ"),
        (pd.Timestamp("2025-07-17"), "600462.SH"),
        (pd.Timestamp("2025-07-18"), "000001.SZ"),
        (pd.Timestamp("2025-07-18"), "600462.SH"),
        (pd.Timestamp("2025-07-21"), "000001.SZ"),
    ]
    with pytest.raises(ValueError, match="exceeds the formal frozen dataset cutoff"):
        service.ensure_ready(start_date="2026-07-31", end_date="2026-08-01")
    with pytest.raises(ValueError, match="exceeds the formal frozen dataset cutoff"):
        service.load_spans(
            start_date="2026-07-31",
            end_date="2026-08-01",
            ensure=False,
        )
    with pytest.raises(ValueError, match="does not accept a universe_key override"):
        service.build_eligible_index(
            start_date="2025-07-17",
            end_date="2025-07-21",
            universe_key="unapproved_override",
            trading_dates=dates,
        )
    with pytest.raises(ValueError, match="exceeds the formal frozen dataset cutoff"):
        service.build_eligible_index(
            start_date="2026-08-01",
            end_date="2026-08-01",
            trading_dates=[],
        )


def test_formal_factor_universe_rejects_snapshot_identity_drift() -> None:
    binding, snapshot, manifest = _binding()
    tampered = snapshot.as_dict()
    tampered["spans"][0]["eligible_end"] = "2026-07-30"
    with pytest.raises(PitSnapshotError, match="identity/digest differs"):
        FactorUniverseMaskService.from_formal_release(
            release_manifest=manifest,
            expected_manifest_digest=_digest(manifest),
            usage_mode=FormalDatasetUsage.TRAINING,
            frozen_snapshot=tampered,
        )


def _patch_composer(monkeypatch, composer: ConfigComposer) -> None:
    monkeypatch.setattr(
        composer,
        "_get_factors_info",
        lambda *_args, **_kwargs: [
            {
                "factor_name": "DemoFactor",
                "source": "custom",
                "code_text": "def calculate_DemoFactor(instruments, start_date, end_date):\n    return None\n",
            }
        ],
    )
    monkeypatch.setattr(composer, "_get_model_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(composer, "_get_strategy_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        composer,
        "_fetch_workspace_config",
        lambda *_args, **_kwargs: {
            "workspace_base": "/tmp/qe_workspace",
            "qlib_data_path": "/tmp/qlib_day",
            "qlib_minute_path": "/tmp/qlib_minute",
            "factor_data_dir": "/tmp/factor_data",
        },
    )
    monkeypatch.setattr(
        composer,
        "_prepare_risk_policy_runtime",
        lambda **kwargs: (kwargs["custom_params"], None),
    )
    monkeypatch.setattr(
        composer,
        "_prepare_suspend_filter_runtime",
        lambda **kwargs: (kwargs["custom_params"], None),
    )
    monkeypatch.setattr(composer, "_get_read_exp_res_content", lambda: "# read")


def test_composer_persists_binding_and_factor_cache_identity(monkeypatch, tmp_path: Path) -> None:
    request, binding, _, _ = _request()
    config = ExperimentConfig(
        factor_names=["DemoFactor"],
        model_id="lgbm",
        canonical_pit_dataset=request,
    )
    split = dict(
        RDAGENT_DEFAULT_DATA_SPLIT,
        test_end="2026-07-31",
        backtest_end="2026-07-31",
    )
    risk_params, risk_spec_json = ConfigComposer()._prepare_risk_policy_runtime(
        custom_params=config.build_custom_params(),
        data_split=split,
        qlib_data_path="/candidate/daily_bin/qlib",
    )
    assert risk_params["risk_policy"]["st_universe_key"] == binding.qe_runtime_universe_key
    risk_spec = json.loads(risk_spec_json)
    assert risk_spec["dataset"]["contract_id"] == binding.release_id
    assert risk_spec["dataset"]["canonical_frozen_universe_key"] == binding.frozen_universe_key
    assert risk_spec["pins"]["universe_key"] == binding.frozen_universe_key
    assert risk_spec["pins"]["instruments_sha256"] == request.runtime_pins.qlib_instruments_sha256
    assert risk_spec["suspend"]["universe_key"] == binding.frozen_universe_key
    assert risk_spec["suspend"]["manifest_sha256"] == request.runtime_pins.suspend_manifest_sha256
    composer = ConfigComposer()
    _patch_composer(monkeypatch, composer)

    result = composer.compose_experiment_in_memory(
        factor_names=config.factor_names,
        model_id=None,
        data_split=split,
        custom_params=config.build_custom_params(),
        skip_db_save=True,
        execution_algo="CLOSE_PRICE",
        execution_algo_params={},
    )

    assert json.loads(result["experiment_files"][QE_FORMAL_DATASET_BINDING_FILE]) == binding.as_dict()
    assert result["canonical_pit_dataset_binding"] == binding.as_dict()
    assert QE_FORMAL_DATASET_REQUEST_PARAM not in result["experiment_files"]["conf.yaml"]
    prepare = result["experiment_files"]["prepare_factors.py"]
    compile(prepare, "prepare_factors.py", "exec")
    namespace: dict[str, object] = {}
    exec(prepare, namespace)
    expected_meta = dict(namespace["QE_DATASET_EXPECTED_META"])
    assert expected_meta["canonical_pit_dataset_binding"] == binding.as_dict()
    factor_root = tmp_path / "factor_data"
    factor_root.mkdir()
    (factor_root / "meta.json").write_text(json.dumps(expected_meta), encoding="utf-8")
    namespace["FACTOR_DATA_DIR"] = str(factor_root)
    namespace["_validate_factor_data_dataset_contract"]()
    tampered_meta = dict(expected_meta)
    tampered_meta["canonical_pit_dataset_binding"] = dict(
        tampered_meta["canonical_pit_dataset_binding"]
    )
    tampered_meta["canonical_pit_dataset_binding"]["manifest_digest"] = "d" * 64
    (factor_root / "meta.json").write_text(json.dumps(tampered_meta), encoding="utf-8")
    with pytest.raises(RuntimeError, match="dataset contract mismatch"):
        namespace["_validate_factor_data_dataset_contract"]()
    expected_universe = dict(namespace["FACTOR_CACHE_EXPECTED_UNIVERSE_META"])
    cached = dict(expected_universe)
    assert namespace["_cache_universe_mismatch"](cached, expected_universe) == ""
    cached["release_manifest_digest"] = "d" * 64
    assert namespace["_cache_universe_mismatch"](cached, expected_universe) == "release_manifest_digest"
    cached = dict(expected_universe)
    cached["coverage_semantics"] = "legacy_coverage"
    assert namespace["_cache_universe_mismatch"](cached, expected_universe) == "coverage_semantics"
    cached = dict(expected_universe)
    cached["data_freshness_profile"] = "legacy_freshness"
    assert namespace["_cache_universe_mismatch"](cached, expected_universe) == "data_freshness_profile"

    legacy_result = composer.compose_experiment_in_memory(
        factor_names=config.factor_names,
        model_id=None,
        data_split=split,
        custom_params=ExperimentConfig(
            factor_names=["DemoFactor"],
            model_id="lgbm",
        ).build_custom_params(),
        skip_db_save=True,
        execution_algo="CLOSE_PRICE",
        execution_algo_params={},
    )
    assert "canonical_pit_dataset_binding" not in legacy_result


def test_long_trend_reader_requires_same_formal_release_identity(tmp_path: Path) -> None:
    binding, _, _ = _binding()
    workspace = tmp_path / "workspace"
    factor_root = tmp_path / "factor_data"
    workspace.mkdir()
    factor_root.mkdir()
    meta = {
        "snapshot_id": binding.release_id,
        "start": "2018-08-01",
        "end": binding.cutoff.isoformat(),
        "lineage_parent_ids": [],
        "canonical_pit_dataset_binding": binding.as_dict(),
    }
    (factor_root / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    (factor_root / "daily_pv.h5").write_bytes(b"daily")
    (factor_root / "sector_data.h5").write_bytes(b"sector")
    snapshot_identity = inspect_qe_snapshot_identity(
        factor_root,
        formal_dataset_binding=binding,
    )

    reader = QELongTrendDatasetReader(
        factor_data_dir=factor_root,
        qe_workspace_root=workspace,
        qe_dataset_contract_id=binding.release_id,
        snapshot_identity=snapshot_identity,
        formal_dataset_binding=binding,
    )
    assert reader.formal_dataset_binding == binding

    wrong = binding.as_dict()
    wrong["manifest_digest"] = "d" * 64
    with pytest.raises(QELongTrendError, match="differs from the requested release"):
        QELongTrendDatasetReader(
            factor_data_dir=factor_root,
            qe_workspace_root=workspace,
            qe_dataset_contract_id=binding.release_id,
            snapshot_identity=snapshot_identity,
            formal_dataset_binding=QEFormalDatasetBinding.from_mapping(wrong),
        )
