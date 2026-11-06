import asyncio
from copy import deepcopy

import pytest
from pydantic import ValidationError

from backend.services.quantevolver.frozen_inference_contract import FrozenInferenceSpec, validate_mode
from backend.services.quantevolver.qe_batch_execution_policy import is_pure_backtest


def spec():
    return dict(schema_version="qe_frozen_inference_v1", model_sha256="a" * 64,
                dataset_sha256="b" * 64, reference_prediction_sha256="c" * 64,
                source_training_manifest_sha256="d" * 64, input_manifest_sha256="e" * 64,
                source_model_config_sha256="f" * 64, code_sha256={"qe_custom_loaders.py": "1" * 64},
                model_module_sha256={"qlib.contrib.model.gbdt": "2" * 64}, feature_names=["F"],
                excluded_reference_columns=["R"], normalizer_fit_start="2022-03-15",
                normalizer_fit_end="2025-12-31", dataset_class="DatasetH", step_len=1)


def request():
    return dict(backtest_only=True, model_source_task_id="original", model_source_loop_index=1,
                frozen_inference=spec(), alpha_mode="single")


def test_opt_in_is_explicit_and_inference_is_not_pure_replay():
    assert validate_mode({}) is None
    assert validate_mode(request()).dataset_class == "DatasetH"
    assert is_pure_backtest(request()) is False
    assert is_pure_backtest({"backtest_only": True}) is True


@pytest.mark.parametrize("change", [dict(backtest_only=False), dict(prediction_replay=True),
                                      dict(model_source_task_id=None), dict(model_source_loop_index=0),
                                      dict(alpha_mode="multi"), dict(feature_processing={"clip_outlier": False}),
                                      dict(extra_params={"feature_processing": {"clip_outlier": False}})])
def test_invalid_modes_fail_closed(change):
    value = request()
    value.update(change)
    with pytest.raises(ValueError, match="QE_FROZEN_MODE_INVALID"):
        validate_mode(value, allow_extra_params=True)


@pytest.mark.parametrize("field", ["model_params", "strategy_params", "runtime_flags", "extra_params"])
def test_hidden_opt_in_never_activates(field):
    with pytest.raises(ValueError, match="MUST_BE_PUBLIC"):
        validate_mode({field: {"nested": [{"frozen_inference": None}]}}, allow_extra_params=True)


@pytest.mark.parametrize("change", [dict(step_len=20), dict(feature_names=["F", "F"]),
                                      dict(code_sha256={"../qe_custom_loaders.py": "1" * 64}),
                                      dict(excluded_reference_columns=["F"]), dict(unrecognized=True)])
def test_identity_schema_rejects_ambiguous_pins(change):
    value = spec()
    value.update(change)
    with pytest.raises(ValidationError):
        FrozenInferenceSpec.model_validate(value)


def test_public_schema_builder_roundtrip():
    from backend.routers.quantevolver_evolution import CustomEvoLoopConfig
    from backend.services.quantevolver.experiment_config_builders import build_config_from_custom_evo_loop

    value = request()
    value.update(factor_keys=["F||manual", "R||manual"], model_id="LGBModel", strategy_id="score_weighted_topk_v2")
    public = CustomEvoLoopConfig.model_validate(value)
    payload = public.model_dump()
    payload["source_label_horizon"] = 20
    built = build_config_from_custom_evo_loop(payload, {"label_horizon": 20})
    assert built.frozen_inference == public.frozen_inference
    assert "frozen_inference" not in built.build_custom_params()
    assert "frozen_inference" not in built.build_strategy_params()
    default = deepcopy(value)
    default.pop("frozen_inference")
    payload = CustomEvoLoopConfig.model_validate(default).model_dump()
    payload["source_label_horizon"] = 20
    assert build_config_from_custom_evo_loop(payload, {"label_horizon": 20}).frozen_inference is None


def test_source_transport_requires_actual_weight_dataset_prediction_and_code_hashes():
    import hashlib
    import io
    import json
    import tarfile
    from unittest.mock import AsyncMock
    import yaml
    from backend.services.quantevolver.frozen_inference_contract import canonical_sha, source_assets

    model = {"class": "LGBModel", "module_path": "qlib.contrib.model.gbdt", "kwargs": {}}
    files = {"mlruns/1/run/artifacts/dataset": b"original-fitted-dataset",
             "mlruns/1/run/artifacts/pred.pkl": b"original-prediction",
             "qe_custom_loaders.py": b"original-code\n",
             "conf.yaml": yaml.safe_dump({"task": {"model": model, "dataset": {"kwargs": {
                 "segments": {"test": ["2026-01-05", "2026-06-30"]}}}}}).encode()}
    payload = spec()
    payload.update(model_sha256=hashlib.sha256(b"original-model").hexdigest(),
                   dataset_sha256=hashlib.sha256(files["mlruns/1/run/artifacts/dataset"]).hexdigest(),
                   reference_prediction_sha256=hashlib.sha256(files["mlruns/1/run/artifacts/pred.pkl"]).hexdigest(),
                   code_sha256={"qe_custom_loaders.py": hashlib.sha256(files["qe_custom_loaders.py"]).hexdigest()},
                   source_model_config_sha256=canonical_sha(model))
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as tar:
        info = tarfile.TarInfo("mlruns/1/run/artifacts/params.pkl")
        info.size = len(b"original-model")
        tar.addfile(info, io.BytesIO(b"original-model"))
    client = AsyncMock()
    client.list_workspace_files.return_value = {"catalog_completeness": "complete", "files": [
        {"relative_path": key, "size_bytes": len(value)} for key, value in files.items()]}
    client.download_workspace_file_bytes.side_effect = lambda task, loop, path: files[path]
    result = asyncio.run(source_assets(client, "original", "Loop1", FrozenInferenceSpec.model_validate(payload), stream.getvalue()))
    assert json.loads(result["qe_frozen_source_assets.json"])["source_model_config"] == model
    files["qe_custom_loaders.py"] = b"changed-code!\n"
    with pytest.raises(ValueError, match="SHA_OR_SIZE_MISMATCH"):
        asyncio.run(source_assets(client, "original", "Loop1", FrozenInferenceSpec.model_validate(payload), stream.getvalue()))


def test_executor_cannot_convert_frozen_inference_to_training():
    from unittest.mock import AsyncMock, MagicMock
    from backend.services.quantevolver.executors.backtest import BacktestExecutor, BacktestMode
    from backend.services.quantevolver.executors.base import ExecutionContext
    from backend.services.quantevolver.experiment_config import ExperimentConfig

    value = request()
    value.update(factor_names=["F", "R"], model_id="LGBModel", label_horizon=20)
    config = ExperimentConfig.model_validate(value)
    composer = MagicMock()
    executor = BacktestExecutor(composer, AsyncMock())
    ctx = ExecutionContext(task_id="test", loop_index=1, experiment_name="test/Loop1")
    with pytest.raises(ValueError, match="REQUIRES_BACKTEST_ONLY_SOURCE"):
        asyncio.run(executor.submit(config, ctx, mode=BacktestMode.FULL_TRAIN))
    composer.compose_experiment_in_memory.assert_not_called()


@pytest.mark.parametrize("change", [{"fit_calls": 1}, {"fit_calls": False}, {"source_bytes_unchanged": False},
                                      {"processor_state_after": "b" * 64}, {"reference_equivalent": False},
                                      {"output_end": "2026-09-29 00:00:00"}])
def test_completion_rejects_refit_state_drift_or_shortened_window(change):
    from backend.services.quantevolver.qe_evolution_service import AutoEvolutionScheduler

    config = request()
    config["data_split"] = {"test_start": "2026-01-05", "test_end": "2026-09-30"}
    receipt = dict(schema_version="qe_frozen_inference_result_v1", status="verified", mode="frozen_inference",
                   spec=spec(), fit_calls=0, source_bytes_unchanged=True, reference_equivalent=True,
                   source_task_id="original", source_loop_index=1, processor_state_before="a" * 64,
                   processor_state_after="a" * 64, model_weights_before="a" * 64, model_weights_after="a" * 64,
                   output_rows=3, reference_rows=2, executable_prediction_panel_sha256="c" * 64,
                   feature_names=["F"], step_len=1, output_start="2026-01-05 00:00:00", output_end="2026-09-30 00:00:00")
    AutoEvolutionScheduler._validate_frozen_inference_result(receipt, config=config)
    receipt.update(change)
    with pytest.raises(ValueError, match="QE_FROZEN_RESULT_"):
        AutoEvolutionScheduler._validate_frozen_inference_result(receipt, config=config)
