"""Tests for live inference cold-start preflight (Task #33 / P0-F / P0-4).

Covers ``QEExperimentRuntimeAssetResolver.preflight_for_strategy_package``
plus the ``require_preflight_or_raise`` wrapper. The historical incident
behind this feature is the 30+ live-inference cold-start failures audited
in ``docs/analysis/paper_v2_user_requirement_audit_20260507.md`` §0/§7
P0-4: deep failures inside ``prepare_workspace`` would only surface 30
minutes into a run. The preflight must reject early on each of the five
documented missing-asset paths.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from backend.services.strategy_package.live_inference import (
    LiveInferencePreflightCheck,
    LiveInferencePreflightError,
    LiveInferencePreflightResult,
    PREFLIGHT_CHECK_CONF_YAML,
    PREFLIGHT_CHECK_FACTOR_SOURCE,
    PREFLIGHT_CHECK_MODEL_PARAMS,
    PREFLIGHT_CHECK_NAMES,
    PREFLIGHT_CHECK_QE_NODE,
    PREFLIGHT_CHECK_QE_SOURCE,
    PREFLIGHT_STATUS_BLOCKED,
    PREFLIGHT_STATUS_PASS,
    QEExperimentRuntimeAssetResolver,
    QEExperimentRuntimeSource,
)


class _OneRowCursor:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self.row = row

    def __enter__(self) -> "_OneRowCursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def fetchone(self) -> dict[str, Any] | None:
        return self.row


class _OneRowConn:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self.row = row

    def __enter__(self) -> "_OneRowConn":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def cursor(self, *_args: Any, **_kwargs: Any) -> _OneRowCursor:
        return _OneRowCursor(self.row)


def _seed_full_workspace(workspace: Path) -> None:
    """Materialize a minimal but complete QE asset workspace under ``workspace``."""

    workspace.mkdir(parents=True, exist_ok=True)
    factors_dir = workspace / "factors"
    artifacts_dir = workspace / "mlruns" / "1" / "artifacts"
    factors_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "params.pkl").write_bytes(b"fake model bytes")
    for factor_name in ("factor_a",):
        (factors_dir / f"{factor_name}.py").write_text(
            "import pandas as pd\n"
            "def calculate():\n"
            "    return pd.DataFrame()\n",
            encoding="utf-8",
        )
    (workspace / "conf.yaml").write_text(
        "task:\n  model:\n    class: dummy\n",
        encoding="utf-8",
    )


def _make_runtime_source(workspace: Path, **overrides: Any) -> QEExperimentRuntimeSource:
    base = dict(
        experiment_id="qe_preflight_exp",
        db_workspace_path=workspace,
        asset_workspace_path=workspace,
        factor_names=["factor_a"],
        custom_params={},
        data_split={},
        qe_task_id="qe_task_a",
        qe_loop_id="loop_1",
        execution_node_id="node_a",
    )
    base.update(overrides)
    return QEExperimentRuntimeSource(**base)


class _StubResolver(QEExperimentRuntimeAssetResolver):
    """Resolver subclass that returns a canned source instead of hitting DB / node."""

    def __init__(self, *, source: QEExperimentRuntimeSource | None, error: Exception | None = None) -> None:
        super().__init__(conn_factory=lambda: iter([]), cache_root=source.asset_workspace_path if source else Path("."))
        self._source = source
        self._error = error
        self.calls: list[dict[str, Any]] = []

    def load_source_for_strategy_package(  # type: ignore[override]
        self,
        *,
        source_type: str,
        source_id: str,
        loop_id: str | None = None,
        run_id: str | None = None,
    ) -> QEExperimentRuntimeSource:
        self.calls.append(
            {
                "source_type": source_type,
                "source_id": source_id,
                "loop_id": loop_id,
                "run_id": run_id,
            }
        )
        if self._error is not None:
            raise self._error
        assert self._source is not None
        return self._source


def test_preflight_happy_path_returns_five_pass_checks(tmp_path) -> None:
    workspace = tmp_path / "workspace_happy"
    _seed_full_workspace(workspace)
    resolver = _StubResolver(source=_make_runtime_source(workspace))

    result = resolver.preflight_for_strategy_package(
        source_type="qe_evolution_loop",
        source_id="qe_task_a",
        loop_id="loop_1",
    )

    assert isinstance(result, LiveInferencePreflightResult)
    assert result.passed is True
    assert [check.name for check in result.checks] == list(PREFLIGHT_CHECK_NAMES)
    assert all(check.status == PREFLIGHT_STATUS_PASS for check in result.checks)
    assert result.blocked_check is None


def test_candidate_strategy_package_resolves_underlying_qe_loop(monkeypatch) -> None:
    candidate_row = {
        "candidate_id": "csp_1",
        "source_type": "qe_evolution_loop",
        "source_id": "qe_task_a_Loop1",
        "source_task_id": "qe_task_a",
        "source_loop_id": "qe_task_a_Loop1",
        "source_experiment_id": "qe_exp_a",
        "status": "ACTIVE",
    }
    resolver = QEExperimentRuntimeAssetResolver(conn_factory=lambda: _OneRowConn(candidate_row))
    captured: dict[str, str] = {}

    def fake_load_experiment_row_by_task_loop(*, qe_task_id: str, qe_loop_id: str) -> dict[str, Any]:
        captured["qe_task_id"] = qe_task_id
        captured["qe_loop_id"] = qe_loop_id
        return {
            "experiment_id": "qe_exp_a",
            "status": "completed",
            "qe_task_id": qe_task_id,
            "qe_loop_id": qe_loop_id,
            "factor_names": ["factor_a"],
            "custom_params": {"execution_node_id": "node_a"},
            "data_split": {},
            "result_metrics": {},
        }

    def fake_source_from_experiment_row(row: dict[str, Any], *, source_lookup: dict[str, Any]) -> QEExperimentRuntimeSource:
        return _make_runtime_source(
            Path("."),
            experiment_id=row["experiment_id"],
            qe_task_id=source_lookup["qe_task_id"],
            qe_loop_id=source_lookup["qe_loop_id"],
        )

    monkeypatch.setattr(resolver, "_load_experiment_row_by_task_loop", fake_load_experiment_row_by_task_loop)
    monkeypatch.setattr(resolver, "_source_from_experiment_row", fake_source_from_experiment_row)
    source = resolver.load_source_for_strategy_package(
        source_type="candidate_strategy_package",
        source_id="csp_1",
    )

    assert captured == {"qe_task_id": "qe_task_a", "qe_loop_id": "Loop1"}
    assert source.experiment_id == "qe_exp_a"


def test_preflight_qe_source_failure_short_circuits_remaining_checks(tmp_path) -> None:
    from backend.services.trading_core.errors import DataUnavailableError

    resolver = _StubResolver(
        source=None,
        error=DataUnavailableError(
            "QE evolution loop does not exist for live inference",
            context={"qe_task_id": "qe_missing"},
        ),
    )

    result = resolver.preflight_for_strategy_package(
        source_type="qe_evolution_loop",
        source_id="qe_missing",
        loop_id="loop_1",
    )

    assert result.passed is False
    assert [check.name for check in result.checks] == list(PREFLIGHT_CHECK_NAMES)
    qe_source_check = result.checks[0]
    assert qe_source_check.name == PREFLIGHT_CHECK_QE_SOURCE
    assert qe_source_check.status == PREFLIGHT_STATUS_BLOCKED
    assert "QE evolution loop" in qe_source_check.message
    # downstream checks marked as skipped due to the short-circuit
    for check in result.checks[1:]:
        assert check.status == PREFLIGHT_STATUS_BLOCKED
        assert check.context["skipped_due_to"] == PREFLIGHT_CHECK_QE_SOURCE


def test_preflight_blocks_when_execution_node_id_missing(tmp_path) -> None:
    workspace = tmp_path / "workspace_no_node"
    _seed_full_workspace(workspace)
    source = _make_runtime_source(workspace, execution_node_id="")
    resolver = _StubResolver(source=source)

    result = resolver.preflight_for_strategy_package(
        source_type="qe_evolution_loop",
        source_id="qe_task_a",
        loop_id="loop_1",
    )

    assert result.passed is False
    assert result.checks[0].name == PREFLIGHT_CHECK_QE_SOURCE
    assert result.checks[0].status == PREFLIGHT_STATUS_PASS
    node_check = result.checks[1]
    assert node_check.name == PREFLIGHT_CHECK_QE_NODE
    assert node_check.status == PREFLIGHT_STATUS_BLOCKED
    assert "execution_node_id" in node_check.message


def test_preflight_blocks_when_conf_yaml_missing(tmp_path) -> None:
    workspace = tmp_path / "workspace_no_conf"
    _seed_full_workspace(workspace)
    (workspace / "conf.yaml").unlink()  # remove conf.yaml to trigger blocker
    resolver = _StubResolver(source=_make_runtime_source(workspace))

    result = resolver.preflight_for_strategy_package(
        source_type="qe_evolution_loop",
        source_id="qe_task_a",
        loop_id="loop_1",
    )

    assert result.passed is False
    conf_check = next(c for c in result.checks if c.name == PREFLIGHT_CHECK_CONF_YAML)
    assert conf_check.status == PREFLIGHT_STATUS_BLOCKED
    assert "conf.yaml" in conf_check.message
    # checks before this one passed
    qe_source_check = next(c for c in result.checks if c.name == PREFLIGHT_CHECK_QE_SOURCE)
    qe_node_check = next(c for c in result.checks if c.name == PREFLIGHT_CHECK_QE_NODE)
    assert qe_source_check.status == PREFLIGHT_STATUS_PASS
    assert qe_node_check.status == PREFLIGHT_STATUS_PASS


def test_preflight_blocks_when_factor_source_dir_missing(tmp_path) -> None:
    workspace = tmp_path / "workspace_no_factors"
    _seed_full_workspace(workspace)
    # Remove the factors directory entirely
    factors_dir = workspace / "factors"
    for child in factors_dir.iterdir():
        child.unlink()
    factors_dir.rmdir()
    resolver = _StubResolver(source=_make_runtime_source(workspace))

    result = resolver.preflight_for_strategy_package(
        source_type="qe_evolution_loop",
        source_id="qe_task_a",
        loop_id="loop_1",
    )

    assert result.passed is False
    factor_check = next(c for c in result.checks if c.name == PREFLIGHT_CHECK_FACTOR_SOURCE)
    assert factor_check.status == PREFLIGHT_STATUS_BLOCKED
    assert "factor source" in factor_check.message.lower()
    model_check = next(c for c in result.checks if c.name == PREFLIGHT_CHECK_MODEL_PARAMS)
    assert model_check.status == PREFLIGHT_STATUS_BLOCKED
    assert model_check.context["skipped_due_to"] == PREFLIGHT_CHECK_FACTOR_SOURCE


def test_preflight_blocks_when_declared_factor_files_missing(tmp_path) -> None:
    workspace = tmp_path / "workspace_no_factor_files"
    _seed_full_workspace(workspace)
    # Remove the per-factor python file but keep the directory
    (workspace / "factors" / "factor_a.py").unlink()
    resolver = _StubResolver(source=_make_runtime_source(workspace))

    result = resolver.preflight_for_strategy_package(
        source_type="qe_evolution_loop",
        source_id="qe_task_a",
        loop_id="loop_1",
    )

    assert result.passed is False
    factor_check = next(c for c in result.checks if c.name == PREFLIGHT_CHECK_FACTOR_SOURCE)
    assert factor_check.status == PREFLIGHT_STATUS_BLOCKED
    assert "missing_factor_samples" in factor_check.context
    assert "factor_a" in factor_check.context["missing_factor_samples"]


def test_preflight_blocks_when_model_params_missing(tmp_path) -> None:
    workspace = tmp_path / "workspace_no_model"
    _seed_full_workspace(workspace)
    # Remove the model params artifact
    (workspace / "mlruns" / "1" / "artifacts" / "params.pkl").unlink()
    resolver = _StubResolver(source=_make_runtime_source(workspace))

    result = resolver.preflight_for_strategy_package(
        source_type="qe_evolution_loop",
        source_id="qe_task_a",
        loop_id="loop_1",
    )

    assert result.passed is False
    model_check = next(c for c in result.checks if c.name == PREFLIGHT_CHECK_MODEL_PARAMS)
    assert model_check.status == PREFLIGHT_STATUS_BLOCKED
    assert "params.pkl" in model_check.message
    # check 4 (factor_source) was the last to pass before this
    factor_check = next(c for c in result.checks if c.name == PREFLIGHT_CHECK_FACTOR_SOURCE)
    assert factor_check.status == PREFLIGHT_STATUS_PASS


def test_require_preflight_or_raise_returns_result_when_passed(tmp_path) -> None:
    workspace = tmp_path / "workspace_pass"
    _seed_full_workspace(workspace)
    resolver = _StubResolver(source=_make_runtime_source(workspace))

    result = resolver.require_preflight_or_raise(
        source_type="qe_evolution_loop",
        source_id="qe_task_a",
        loop_id="loop_1",
    )
    assert result.passed is True


def test_require_preflight_or_raise_raises_typed_error_on_block(tmp_path) -> None:
    workspace = tmp_path / "workspace_no_conf_for_raise"
    _seed_full_workspace(workspace)
    (workspace / "conf.yaml").unlink()
    resolver = _StubResolver(source=_make_runtime_source(workspace))

    with pytest.raises(LiveInferencePreflightError) as exc_info:
        resolver.require_preflight_or_raise(
            source_type="qe_evolution_loop",
            source_id="qe_task_a",
            loop_id="loop_1",
        )

    err = exc_info.value
    assert err.error_code == "LIVE_INFERENCE_PREFLIGHT_FAILED"
    assert err.context["blocked_check"] == PREFLIGHT_CHECK_CONF_YAML
    payload = err.context["preflight"]
    assert payload["passed"] is False
    assert any(check["name"] == PREFLIGHT_CHECK_CONF_YAML and check["status"] == "BLOCKED"
               for check in payload["checks"])


def test_preflight_rejects_invalid_runtime_config_shape() -> None:
    resolver = _StubResolver(source=None)
    result = resolver.preflight_for_strategy_package(
        source_type="qe_evolution_loop",
        source_id="qe_task_a",
        loop_id="loop_1",
        runtime_config={"selection_artifact_config": "not-a-dict"},
    )
    assert result.passed is False
    qe_check = result.checks[0]
    assert qe_check.status == PREFLIGHT_STATUS_BLOCKED
    assert "object" in qe_check.message
