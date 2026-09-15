from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.mcp.modules.qe_experiment import _validate_experiment_config
from backend.routers.quantevolver_evolution import CustomEvoLoopConfig
from backend.services.quantevolver.executors.backtest import BacktestExecutor, BacktestMode
from backend.services.quantevolver.executors.base import ExecutionContext, PredictionReplaySource
from backend.services.quantevolver.experiment_config import ExperimentConfig
from backend.services.quantevolver.experiment_config_builders import build_config_from_custom_evo_loop
from backend.services.quantevolver.payload_summary import compact_config_summary, compact_loop_row
from backend.services.quantevolver.qe_evolution_service import AutoEvolutionScheduler


class _Coordinator:
    def __init__(self) -> None:
        self.source = None
        self.payload = None

    async def submit(self, *, client, source, payload):
        self.source = source
        self.payload = payload
        return SimpleNamespace(
            loop_id="Loop1",
            state="submitted",
            reservation_id="reservation-1",
            reservation_status="submitting",
            remote_status="reserved",
            active_count=1,
            node_capacity=2,
            duplicate_replay=False,
            remote_acceptance_unknown=False,
            detail={},
        )


def _source(*, digest: str = "a" * 64, size: int = 4) -> PredictionReplaySource:
    return PredictionReplaySource(
        source_task_id="qe_source",
        source_loop_index=2,
        source_node_id="wsl2-5080",
        catalog_path="mlruns/1/run/artifacts/pred.pkl",
        sha256=digest,
        size_bytes=size,
    )


def _config(*, digest: str = "a" * 64) -> ExperimentConfig:
    return ExperimentConfig(
        factor_names=["f1"],
        model_id="model_lstm_v1",
        label_horizon=20,
        prediction_replay=True,
        prediction_source_task_id="qe_source",
        prediction_source_loop_index=2,
        prediction_source_sha256=digest,
    )


def _context(source: PredictionReplaySource, *, command_file: str = "frozen_prediction.pkl.b64") -> ExecutionContext:
    return ExecutionContext(
        task_id="qe_target",
        loop_index=1,
        experiment_name="qe_target/Loop1",
        node_id="wsl2-5080",
        prediction_replay_source=source,
        extra_experiment_files={command_file: "cHJlZA=="},
        submission_source_kind="qe_evolution_loop",
        submission_source_execution_id="qe_target_Loop1",
    )


def _executor(command: str = "cd /workspace && python qrun_limit_minute.py conf.yaml"):
    composer = MagicMock()
    composer.compose_experiment_in_memory.return_value = {
        "experiment_files": {"conf.yaml": "not-a-generated-config"},
        "wsl_command": command,
    }
    coordinator = _Coordinator()
    return BacktestExecutor(
        composer,
        AsyncMock(),
        submission_coordinator=coordinator,
    ), coordinator


def test_public_and_mcp_contract_normalize_replay_identity() -> None:
    loop = CustomEvoLoopConfig(
        factor_keys=["f1||official"],
        model_id="model_lstm_v1",
        label_horizon=20,
        prediction_replay=True,
        prediction_source_task_id=" qe_source ",
        prediction_source_loop_index=2,
        prediction_source_sha256="A" * 64,
    )
    assert loop.prediction_source_task_id == "qe_source"
    assert loop.prediction_source_sha256 == "a" * 64

    result = _validate_experiment_config(
        "custom_evo",
        {"loops": [loop.model_dump()], "engine_mode": "unified"},
        include_normalized=True,
    )
    assert result["valid"] is True
    assert result["normalized_config"]["loops"][0]["prediction_replay"] is True


@pytest.mark.parametrize(
    "updates, message",
    [
        ({"backtest_only": True}, "mutually exclusive"),
        ({"prediction_source_task_id": None}, "prediction_source_task_id"),
        ({"prediction_replay": "true"}, "valid boolean"),
        ({"prediction_source_loop_index": 0}, "must be >= 1"),
        ({"prediction_source_loop_index": 2.5}, "valid integer"),
        ({"prediction_source_sha256": "wrong"}, "64 hex"),
    ],
)
def test_public_contract_fails_closed(updates: dict, message: str) -> None:
    values = {
        "factor_keys": ["f1||official"],
        "model_id": "model_lstm_v1",
        "prediction_replay": True,
        "prediction_source_task_id": "qe_source",
        "prediction_source_loop_index": 2,
    }
    values.update(updates)
    with pytest.raises(ValueError, match=message):
        CustomEvoLoopConfig(**values)


def test_config_builder_keeps_replay_identity_out_of_executable_params() -> None:
    cfg = build_config_from_custom_evo_loop(
        {
            "factor_keys": ["f1||official"],
            "model_id": "model_lstm_v1",
            "prediction_replay": True,
            "prediction_source_task_id": "qe_source",
            "prediction_source_loop_index": 2,
            "prediction_source_sha256": "a" * 64,
            "source_label_horizon": 20,
            "label_horizon": 20,
        },
        {},
    )
    assert cfg.prediction_replay is True
    assert cfg.prediction_source_sha256 == "a" * 64
    params = cfg.build_custom_params()
    assert not any(key.startswith("prediction_") for key in params)


def test_executor_injects_exact_prediction_replay_command_and_backtest_capacity() -> None:
    source = _source()
    executor, coordinator = _executor()
    result = asyncio.run(
        executor.submit(_config(), _context(source), mode=BacktestMode.PREDICTION_REPLAY)
    )
    assert result.wsl_command.endswith(
        "python qrun_limit_minute.py conf.yaml --pred-backtest frozen_prediction.pkl"
    )
    assert result.wsl_command.count("--pred-backtest") == 1
    assert "--backtest-only" not in result.wsl_command
    assert coordinator.payload.model_source is None
    assert coordinator.source.backtest_only is True
    assert coordinator.source.parallel_training_eligible is False
    assert coordinator.payload.experiment_files["frozen_prediction.pkl.b64"] == "cHJlZA=="
    requested = result.detail["execution_manifest"]["requested"]
    assert requested["mode"] == "prediction_replay"
    assert requested["prediction_replay_source"]["sha256"] == "a" * 64


@pytest.mark.parametrize(
    "command",
    [
        "python qrun_limit.py conf.yaml",
        "python qrun_limit_minute.py conf.yaml --backtest-only",
        "python qrun_limit_minute.py conf.yaml && python qrun_limit_minute.py other.yaml",
    ],
)
def test_executor_rejects_non_minute_or_ambiguous_commands(command: str) -> None:
    executor, _ = _executor(command)
    with pytest.raises(ValueError, match="PREDICTION_REPLAY"):
        asyncio.run(
            executor.submit(
                _config(),
                _context(_source()),
                mode=BacktestMode.PREDICTION_REPLAY,
            )
        )


def test_executor_rejects_missing_or_conflicting_resolved_identity() -> None:
    executor, _ = _executor()
    with pytest.raises(ValueError, match="frozen_prediction"):
        asyncio.run(
            executor.submit(
                _config(),
                _context(_source(), command_file="other.b64"),
                mode=BacktestMode.PREDICTION_REPLAY,
            )
        )
    with pytest.raises(ValueError, match="does not match"):
        asyncio.run(
            executor.submit(
                _config(),
                _context(_source(digest="b" * 64)),
                mode=BacktestMode.PREDICTION_REPLAY,
            )
        )


@pytest.mark.parametrize("source_node_id", ["wsl2-5080", "rdagent-node1"])
def test_source_resolver_accepts_one_complete_catalog_on_either_node(
    source_node_id: str,
) -> None:
    prediction = b"pred"
    digest = hashlib.sha256(prediction).hexdigest()
    client = AsyncMock()
    client.list_workspace_files.return_value = {
        "catalog_completeness": "complete",
        "files": [
            {"relative_path": "run.log", "size_bytes": 1},
            {
                "relative_path": "mlruns/1/run/artifacts/pred.pkl",
                "size_bytes": len(prediction),
                "file_type": "file",
            },
        ],
    }
    client.download_workspace_file_bytes.return_value = prediction
    scheduler = object.__new__(AutoEvolutionScheduler)

    source_ref, files = asyncio.run(
        scheduler._build_prediction_replay_payload(
            client,
            "qe_source",
            2,
            source_node_id=source_node_id,
            expected_sha256=digest,
        )
    )
    assert source_ref["source_node_id"] == source_node_id
    assert source_ref["sha256"] == digest
    assert base64.b64decode(files["frozen_prediction.pkl.b64"]) == prediction
    assert json.loads(files["qe_prediction_replay_source_ref.json"])["catalog_path"].endswith(
        "/artifacts/pred.pkl"
    )


@pytest.mark.parametrize(
    "catalog, error",
    [
        ({"catalog_completeness": "partial", "files": []}, "CATALOG_INCOMPLETE"),
        ({"catalog_completeness": "complete", "files": []}, "ARTIFACT_CARDINALITY"),
        (
            {
                "catalog_completeness": "complete",
                "files": [
                    {"relative_path": "a/artifacts/pred.pkl", "size_bytes": 4},
                    {"relative_path": "b/artifacts/pred.pkl", "size_bytes": 4},
                ],
            },
            "ARTIFACT_CARDINALITY",
        ),
    ],
)
def test_source_resolver_rejects_incomplete_or_ambiguous_catalog(catalog, error) -> None:
    client = AsyncMock()
    client.list_workspace_files.return_value = catalog
    scheduler = object.__new__(AutoEvolutionScheduler)
    with pytest.raises(ValueError, match=error):
        asyncio.run(
            scheduler._build_prediction_replay_payload(
                client,
                "qe_source",
                2,
                source_node_id="wsl2-5080",
                expected_sha256=None,
            )
        )


def test_source_resolver_rejects_digest_and_size_drift() -> None:
    client = AsyncMock()
    client.list_workspace_files.return_value = {
        "catalog_completeness": "complete",
        "files": [{"relative_path": "a/artifacts/pred.pkl", "size_bytes": 4}],
    }
    client.download_workspace_file_bytes.return_value = b"pred"
    scheduler = object.__new__(AutoEvolutionScheduler)
    with pytest.raises(ValueError, match="SHA256_MISMATCH"):
        asyncio.run(
            scheduler._build_prediction_replay_payload(
                client,
                "qe_source",
                2,
                source_node_id="wsl2-5080",
                expected_sha256="b" * 64,
            )
        )
    client.download_workspace_file_bytes.return_value = b"short"
    with pytest.raises(ValueError, match="SIZE_MISMATCH"):
        asyncio.run(
            scheduler._build_prediction_replay_payload(
                client,
                "qe_source",
                2,
                source_node_id="wsl2-5080",
                expected_sha256=None,
            )
        )


def test_compact_summary_exposes_replay_source_without_executable_kwargs() -> None:
    summary = compact_config_summary(
        {
            "prediction_replay": True,
            "prediction_source_task_id": "qe_source",
            "prediction_source_loop_index": 2,
            "prediction_source_sha256": "a" * 64,
            "prediction_replay_source": _source().model_dump(),
            "model_params": {"topk": 50},
        }
    )
    assert summary["prediction_replay"] is True
    assert summary["prediction_replay_source"]["sha256"] == "a" * 64
    assert not any(key.startswith("prediction_") for key in summary.get("strategy_params", {}))


def test_loop_summary_keeps_source_and_executable_prediction_digests_distinct() -> None:
    compact = compact_loop_row(
        {
            "loop_id": "qe_target_Loop1",
            "config_json": {
                "prediction_replay": True,
                "prediction_source_sha256": "a" * 64,
            },
            "metrics_json": {
                "prediction_replay_result": {
                    "source_prediction_sha256": "a" * 64,
                    "executable_prediction_panel_sha256": "b" * 64,
                    "source_prediction_rows": 10,
                    "executable_prediction_rows": 8,
                    "excluded_prediction_rows": 2,
                }
            },
        }
    )
    assert compact["config_summary"]["prediction_source_sha256"] == "a" * 64
    assert compact["prediction_replay_result"]["source_prediction_sha256"] == "a" * 64
    assert compact["prediction_replay_result"]["executable_prediction_panel_sha256"] == "b" * 64


def test_result_receipt_requires_exact_digests_and_consistent_counts() -> None:
    scheduler = object.__new__(AutoEvolutionScheduler)
    receipt = {
        "schema_version": "qe_prediction_replay_result_v1",
        "source_prediction_sha256": "a" * 64,
        "executable_prediction_panel_sha256": "b" * 64,
        "source_prediction_rows": 10,
        "executable_prediction_rows": 8,
        "excluded_prediction_rows": 2,
    }
    assert scheduler._validate_prediction_replay_result(
        receipt,
        expected_source_sha256="a" * 64,
    ) == receipt

    for updates, error in (
        ({"source_prediction_sha256": "c" * 64}, "SOURCE_SHA256_MISMATCH"),
        ({"executable_prediction_panel_sha256": "invalid"}, "EXECUTABLE_SHA256_INVALID"),
        ({"excluded_prediction_rows": -1}, "COUNT_INVALID"),
        ({"excluded_prediction_rows": 1}, "COUNT_MISMATCH"),
    ):
        with pytest.raises(ValueError, match=error):
            scheduler._validate_prediction_replay_result(
                {**receipt, **updates},
                expected_source_sha256="a" * 64,
            )


class _CursorContext:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, *_args):
        return None

    def fetchone(self):
        return self.row


class _ConnectionContext:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return _CursorContext(self.row)


def test_source_loop_must_be_completed_and_node_bound() -> None:
    scheduler = object.__new__(AutoEvolutionScheduler)
    with patch(
        "backend.services.quantevolver.qe_evolution_service.get_conn",
        return_value=_ConnectionContext(
            {"status": "completed", "loop_node_id": "wsl2-5080", "task_node_id": None}
        ),
    ):
        assert scheduler._get_prediction_replay_source_loop("qe_source", 2) == {
            "status": "completed",
            "node_id": "wsl2-5080",
        }

    with patch(
        "backend.services.quantevolver.qe_evolution_service.get_conn",
        return_value=_ConnectionContext(
            {"status": "running", "loop_node_id": "wsl2-5080", "task_node_id": None}
        ),
    ), pytest.raises(ValueError, match="SOURCE_NOT_COMPLETED"):
        scheduler._get_prediction_replay_source_loop("qe_source", 2)

    with patch(
        "backend.services.quantevolver.qe_evolution_service.get_conn",
        return_value=_ConnectionContext(
            {"status": "completed", "loop_node_id": None, "task_node_id": None}
        ),
    ), pytest.raises(ValueError, match="SOURCE_NODE_MISSING"):
        scheduler._get_prediction_replay_source_loop("qe_source", 2)


def _load_runner_replay_helpers():
    runner = Path(__file__).resolve().parents[3] / "scripts" / "qrun_limit_minute.py"
    source = runner.read_text(encoding="utf-8")
    start = source.index("def _prediction_panel_sha256")
    end = source.index("\ndef main()", start)
    namespace = {
        "Any": object,
        "Path": Path,
        "hashlib": hashlib,
        "json": json,
        "pd": pytest.importorskip("pandas"),
    }
    exec(source[start:end], namespace)  # noqa: S102 - trusted first-party function slice
    return namespace


def test_runner_replay_receipt_distinguishes_source_bytes_and_executable_panel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pd = pytest.importorskip("pandas")
    helpers = _load_runner_replay_helpers()
    pred_path = tmp_path / "frozen_prediction.pkl"
    pred_path.write_bytes(b"immutable-prediction-bytes")
    source_sha = hashlib.sha256(pred_path.read_bytes()).hexdigest()
    (tmp_path / "qe_prediction_replay_source_ref.json").write_text(
        json.dumps(
            {
                "schema_version": "qe_prediction_replay_source_ref_v1",
                "sha256": source_sha,
                "size_bytes": pred_path.stat().st_size,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    assert helpers["_load_prediction_replay_source_ref"](pred_path)["sha256"] == source_sha
    panel = pd.DataFrame(
        {"score": [0.2, 0.1]},
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2026-08-28"), "000001.SZ"), (pd.Timestamp("2026-08-28"), "000002.SZ")],
            names=["datetime", "instrument"],
        ),
    )
    executable_sha = helpers["_prediction_panel_sha256"](panel)
    assert executable_sha == helpers["_prediction_panel_sha256"](panel.copy())
    assert executable_sha != source_sha


def test_frontend_exposes_replay_without_manual_identity_input() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    page = (
        repo_root / "frontend/src/app/quantevolver/evolution/page.tsx"
    ).read_text(encoding="utf-8")
    detail = (
        repo_root
        / "frontend/src/app/quantevolver/evolution/components/LoopDetailPanel.tsx"
    ).read_text(encoding="utf-8")

    assert "冻结预测回放（不训练、不重新推理，仅分钟线回测）" in page
    assert "SHA256 由服务端目录解析并钉住" in page
    assert "源预测 SHA256" in detail
    assert "可执行面板 SHA256" in detail
    assert 'value={loop.prediction_source_task_id}' not in page
    assert 'value={loop.prediction_source_sha256}' not in page
