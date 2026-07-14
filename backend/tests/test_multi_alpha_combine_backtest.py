from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
import yaml

import backend.services.multi_alpha.combine_backtest as combine_backtest_module
from backend.routers.multi_alpha import CombineBacktestRunRequest
from backend.services.multi_alpha.combine_backtest import (
    COMBINE_BACKTEST_CONFIRM,
    COMBINE_BACKTEST_STALE_FAIL_CONFIRM,
    DEFAULT_PRED_BACKTEST_TIMEOUT_SECONDS,
    DEFAULT_WEIGHTING_SCHEMES,
    InMemoryCombineBacktestRepository,
    MultiAlphaCombineBacktestError,
    MultiAlphaCombineBacktestService,
    RANK_FUSION_WEIGHTING_SCHEMES,
    apply_pred_backtest_overrides,
    ShellPredBacktestExecutor,
    maybe_upload_combined_prediction,
    parse_request,
    run_command,
)
from backend.services.multi_alpha.panels import MultiAlphaPanelBuilder


DATES = [pd.Timestamp("2026-01-02").date(), pd.Timestamp("2026-01-03").date(), pd.Timestamp("2026-01-04").date()]
INSTRUMENTS = ["A", "B", "C"]


class FakeExecutor:
    def __init__(self, *, sleep_seconds: float = 0.0, fail_names: set[str] | None = None) -> None:
        self.calls: list[dict] = []
        self.sleep_seconds = sleep_seconds
        self.fail_names = set(fail_names or set())
        self.active = 0
        self.max_active = 0
        self.active_observations: list[int] = []
        self._lock = threading.Lock()

    def execute_pred_backtest(self, *, workspace: Path, pred_pkl: Path, node_id: str, backtest_config: dict) -> dict:
        name = workspace.name
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.active_observations.append(self.active)
        try:
            if self.sleep_seconds:
                time.sleep(self.sleep_seconds)
            if name in self.fail_names:
                raise MultiAlphaCombineBacktestError(
                    f"injected fake executor failure for {name}",
                    reason_code="fake_pred_backtest_failed",
                    context={"backtest_name": name},
                )
            frame = pd.read_pickle(pred_pkl).reset_index()
            score_sum = float(frame["score"].sum())
            metrics = {
                "cagr": 1.0 + score_sum / 1000.0,
                "max_drawdown": -0.15,
                "sharpe": 2.0 + score_sum / 1000.0,
                "calmar": 6.0 + score_sum / 1000.0,
                "topk_return_20": 0.05,
                "topk_hit_rate_20": 0.6,
                "turnover": 20.0,
                "name": name,
            }
            with self._lock:
                self.calls.append({"workspace": workspace, "node_id": node_id, "metrics": metrics, "backtest_config": dict(backtest_config)})
            (workspace / "qlib_results_enhanced.json").write_text(json.dumps({"absolute_returns": metrics}), encoding="utf-8")
            return metrics
        finally:
            with self._lock:
                self.active -= 1


class FakeCapacityChecker:
    def __init__(self, active_count: int = 0) -> None:
        self.active_count = active_count
        self.calls: list[dict] = []
        self.releases: list[dict] = []

    def ensure_slot_available(self, *, node_id: str, limit: int, run_id: str, backtest_name: str) -> dict:
        self.calls.append({"node_id": node_id, "limit": limit, "run_id": run_id, "backtest_name": backtest_name})
        if self.active_count >= limit:
            raise MultiAlphaCombineBacktestError(
                "node saturated",
                reason_code="node_capacity_exhausted",
                context={"node_id": node_id, "limit": limit, "active_count": self.active_count},
            )
        return {"node_id": node_id, "limit": limit, "active_count": self.active_count, "available": True}

    def release_slot(self, capacity: dict) -> None:
        self.releases.append(capacity)


class FakeArchiveEventCapture:
    def __init__(self, *, fail: bool = False, on_enqueue=None) -> None:  # type: ignore[no-untyped-def]
        self.fail = fail
        self.on_enqueue = on_enqueue
        self.events: list[dict] = []

    def enqueue_multi_alpha_combine_completed_result(self, **kwargs):  # type: ignore[no-untyped-def]
        self.events.append(dict(kwargs))
        if self.on_enqueue is not None:
            self.on_enqueue(dict(kwargs))
        if self.fail:
            raise RuntimeError("archive outbox unavailable")
        return {
            "inserted": True,
            "event_id": f"evt_{kwargs['run_id']}",
            "event_type": "qe.multi_alpha.combine.completed",
            "source_system": "multi_alpha",
            "source_id": kwargs["run_id"],
            "source_sub_id": kwargs["run_id"],
            "duplicate": False,
        }


def _pred(offset: float) -> pd.DataFrame:
    rows = []
    for d_idx, trade_date in enumerate(DATES):
        for i_idx, instrument in enumerate(INSTRUMENTS):
            rows.append({"trade_date": trade_date, "instrument": instrument, "score": offset + d_idx + i_idx})
    return pd.DataFrame(rows)


def _label() -> pd.DataFrame:
    rows = []
    for d_idx, trade_date in enumerate(DATES):
        for i_idx, instrument in enumerate(INSTRUMENTS):
            rows.append(
                {
                    "trade_date": trade_date,
                    "instrument": instrument,
                    "forward_return": (0.01 * (d_idx + i_idx + 1)) + (0.003 if d_idx == 1 and instrument == "C" else 0.0),
                }
            )
    return pd.DataFrame(rows)


def _payload() -> dict:
    return {
        "roster": [
            {"leg_id": "leg_a", "seed_run_ids": ["a1", "a2"]},
            {"leg_id": "leg_b", "seed_run_ids": ["b1", "b2"]},
        ],
        "oos_start": "2026-01-02",
        "oos_end": "2026-01-04",
        "weighting_schemes": ["equal", "ic_weighted", "risk_parity"],
        "normalize_method": "rank",
        "walk_forward": {"enabled": True, "window": 2, "min_periods": 2},
        "backtest_config": {"node_id": "wsl2-5080", "node_parallelism": {"wsl2-5080": 2}},
        "baseline_leg_id": "leg_a",
        "topk": 1,
        "min_date_coverage": 1.0,
        "run_async": False,
    }


def _payload_three_legs() -> dict:
    payload = _payload()
    payload["roster"] = [
        {"leg_id": "leg_a", "seed_run_ids": ["a1", "a2"]},
        {"leg_id": "leg_b", "seed_run_ids": ["b1", "b2"]},
        {"leg_id": "leg_c", "seed_run_ids": ["c1", "c2"]},
    ]
    payload["weighting_schemes"] = ["equal"]
    return payload


def _service(
    tmp_path: Path,
    *,
    capacity_checker: FakeCapacityChecker | None = None,
    executor: FakeExecutor | None = None,
) -> tuple[MultiAlphaCombineBacktestService, InMemoryCombineBacktestRepository, FakeExecutor, FakeCapacityChecker]:
    preds = {
        "a1": _pred(1.0),
        "a2": _pred(2.0),
        "b1": _pred(-1.0),
        "b2": _pred(-2.0),
        "c1": _pred(3.0),
        "c2": _pred(4.0),
    }
    labels = {run_id: _label() for run_id in preds}
    repo = InMemoryCombineBacktestRepository()
    executor = executor or FakeExecutor()
    checker = capacity_checker or FakeCapacityChecker()
    service = MultiAlphaCombineBacktestService(
        panel_builder=MultiAlphaPanelBuilder(
            prediction_loader=lambda run_id: preds[run_id],
            label_loader=lambda run_id: labels[run_id],
        ),
        executor=executor,
        repository=repo,
        capacity_checker=checker,
        workspace_root=tmp_path / "macb",
        clock=lambda: datetime(2026, 1, 5, tzinfo=timezone.utc),
    )
    service._archive_event_capture = FakeArchiveEventCapture()  # type: ignore[attr-defined]
    return service, repo, executor, checker


def _runtime_template(tmp_path: Path) -> Path:
    template = tmp_path / "runtime_template"
    template.mkdir()
    conf = {
        "port_analysis_config": {
            "strategy": {
                "class": "ScoreWeightedTopkStrategyV2",
                "kwargs": {"topk": 25, "n_drop": 2},
            },
        },
    }
    (template / "conf.yaml").write_text(yaml.safe_dump(conf, sort_keys=False), encoding="utf-8")
    for runtime_file in ("qrun_limit_minute.py", "read_exp_res.py"):
        (template / runtime_file).write_text("# test runtime placeholder\n", encoding="utf-8")
    return template


def test_submit_preflights_missing_runtime_template_before_persisting_run(tmp_path: Path) -> None:
    repo = InMemoryCombineBacktestRepository()
    service = MultiAlphaCombineBacktestService(
        panel_builder=RaisingPanelBuilder(),  # type: ignore[arg-type]
        prediction_loader=lambda _run_id: _pred(1.0),
        repository=repo,
        workspace_root=tmp_path / "macb",
    )
    payload = _payload()
    payload["backtest_config"] = {
        "node_id": "wsl2-5080",
        "node_parallelism": {"wsl2-5080": 1},
        "runtime_template_dir": str(tmp_path / "missing-runtime"),
    }

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        service.submit_run(payload)

    assert excinfo.value.reason_code == "pred_backtest_runtime_template_missing"
    assert repo.runs == {}


def test_prepare_runtime_template_routes_unreadable_wsl_link_through_wsl_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    template = _runtime_template(tmp_path)
    inaccessible = template / "bak_basic.h5"
    inaccessible.write_text("test stand-in", encoding="utf-8")
    workspace = tmp_path / "workspace"
    copied: list[tuple[Path, Path]] = []

    monkeypatch.setattr(
        combine_backtest_module,
        "_find_unreadable_runtime_template_entry",
        lambda _src: (inaccessible, OSError(1920, "inaccessible WSL reparse point")),
    )

    def fake_wsl_copy(*, src: Path, workspace: Path, backtest_config: dict) -> None:
        copied.append((src, workspace))
        workspace.mkdir(parents=True, exist_ok=True)
        for item in src.iterdir():
            if item.is_file():
                shutil.copy2(item, workspace / item.name)

    monkeypatch.setattr(combine_backtest_module, "_copy_runtime_template_via_wsl", fake_wsl_copy)

    combine_backtest_module.prepare_pred_backtest_workspace(
        workspace=workspace,
        backtest_config={"runtime_template_dir": str(template)},
    )

    assert copied == [(template, workspace)]
    assert (workspace / "conf.yaml").exists()
    assert (workspace / "bak_basic.h5").read_text(encoding="utf-8") == "test stand-in"


def test_default_local_pred_backtest_commands_use_configured_wsl_on_windows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(combine_backtest_module, "_is_windows_host", lambda: True)
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    qrun_command, read_command, read_env = combine_backtest_module._default_local_pred_backtest_commands(
        workspace=workspace,
        pred_name="combined_prediction.pkl",
        backtest_config={
            "wsl_distro": "Ubuntu",
            "wsl_conda_sh": "/home/test/miniconda3/etc/profile.d/conda.sh",
            "wsl_conda_env": "rdagent-gpu",
        },
    )

    assert qrun_command[:4] == ["wsl", "-d", "Ubuntu", "bash"]
    assert "conda activate rdagent-gpu" in qrun_command[-1]
    assert ". ./.factor_env" in qrun_command[-1]
    assert "python qrun_limit_minute.py conf.yaml --pred-backtest combined_prediction.pkl" in qrun_command[-1]
    assert read_command[:4] == ["wsl", "-d", "Ubuntu", "bash"]
    assert "QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py" in read_command[-1]
    assert read_env is None


def test_prepare_runtime_template_preserves_real_drvfs_linux_symlink(tmp_path: Path) -> None:
    if sys.platform != "win32" or shutil.which("wsl") is None:
        pytest.skip("Windows + WSL is required for the DrvFS symlink regression")
    if subprocess.run(["wsl", "-d", "Ubuntu", "true"], capture_output=True, check=False).returncode != 0:
        pytest.skip("Ubuntu WSL distro is unavailable")

    template = _runtime_template(tmp_path)
    workspace = tmp_path / "workspace"
    source_link = template / "bak_basic.h5"
    copied_link = workspace / "bak_basic.h5"
    source_link_wsl = combine_backtest_module.win_to_wsl_path(str(source_link))
    copied_link_wsl = combine_backtest_module.win_to_wsl_path(str(copied_link))
    subprocess.run(
        [
            "wsl",
            "-d",
            "Ubuntu",
            "bash",
            "-lc",
            f"ln -s /etc/hosts {combine_backtest_module.shlex.quote(source_link_wsl)}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        combine_backtest_module.prepare_pred_backtest_workspace(
            workspace=workspace,
            backtest_config={"runtime_template_dir": str(template), "wsl_distro": "Ubuntu"},
        )
        readlink = subprocess.run(
            ["wsl", "-d", "Ubuntu", "readlink", copied_link_wsl],
            check=True,
            capture_output=True,
            text=True,
        )
        assert readlink.stdout.strip() == "/etc/hosts"
    finally:
        subprocess.run(
            ["wsl", "-d", "Ubuntu", "rm", "-f", source_link_wsl, copied_link_wsl],
            check=False,
            capture_output=True,
        )


class RaisingPanelBuilder:
    def build_combiner_legs(self, **_kwargs):
        raise AssertionError("rank-fusion-only combine-backtest must not build label panels")


def _rank_fusion_service(
    tmp_path: Path,
    *,
    executor: FakeExecutor | None = None,
    panel_builder: object | None = None,
) -> tuple[MultiAlphaCombineBacktestService, InMemoryCombineBacktestRepository, FakeExecutor, FakeCapacityChecker]:
    preds = {
        "a1": _pred(1.0),
        "a2": _pred(2.0),
        "b1": _pred(-1.0),
        "b2": _pred(-2.0),
        "c1": _pred(3.0),
        "c2": _pred(4.0),
    }
    repo = InMemoryCombineBacktestRepository()
    executor = executor or FakeExecutor()
    checker = FakeCapacityChecker()
    service = MultiAlphaCombineBacktestService(
        panel_builder=panel_builder,  # type: ignore[arg-type]
        prediction_loader=lambda run_id: preds[run_id],
        executor=executor,
        repository=repo,
        capacity_checker=checker,
        workspace_root=tmp_path / "macb",
        clock=lambda: datetime(2026, 1, 5, tzinfo=timezone.utc),
    )
    service._archive_event_capture = FakeArchiveEventCapture()  # type: ignore[attr-defined]
    return service, repo, executor, checker


def test_combine_backtest_runs_ic_weighted_and_risk_parity_and_persists(tmp_path: Path) -> None:
    service, repo, executor, checker = _service(tmp_path)

    result = service.submit_run(_payload(), run_async=False)
    run = service.get_run(result["run_id"])

    assert run["run"]["status"] == "succeeded"
    schemes = {row["weighting_scheme"]: row for row in run["scheme_results"]}
    assert set(schemes) == {"equal", "ic_weighted", "risk_parity"}
    assert all(not row["skipped"] for row in schemes.values())
    assert schemes["ic_weighted"]["sharpe"] is not None
    assert schemes["risk_parity"]["calmar"] is not None
    assert len(run["loo"]) == 0
    assert {call["node_id"] for call in executor.calls} == {"wsl2-5080"}
    assert {call["limit"] for call in checker.calls} == {2}
    assert len(checker.releases) == len(executor.calls)
    assert repo.runs[result["run_id"]]["roster_hash"]


def test_combine_backtest_heartbeat_updates_phase_and_updated_at(tmp_path: Path) -> None:
    service, repo, _executor, _checker = _service(tmp_path)

    result = service.submit_run(_payload(), run_async=False)
    run = repo.runs[result["run_id"]]

    assert run["status"] == "succeeded"
    assert run["updated_at"] is not None
    assert run["reason"] is None


def test_archive_event_enqueue_failure_is_sidecar_and_persisted(tmp_path: Path) -> None:
    service, repo, _executor, _checker = _service(tmp_path)
    service._archive_event_capture = FakeArchiveEventCapture(fail=True)  # type: ignore[attr-defined]

    result = service.submit_run(_payload(), run_async=False)
    run = repo.runs[result["run_id"]]

    assert run["status"] == "succeeded"
    assert result["archive_event"]["queued"] is False
    assert "archive outbox unavailable" in result["archive_event"]["error"]
    assert run["reason"]["logical_status"] == "succeeded"
    assert run["reason"]["archive_event"]["run_id"] == result["run_id"]
    assert "archive outbox unavailable" in run["reason"]["archive_event"]["error"]


def test_async_daemon_exception_is_persisted_as_failed_not_silent(tmp_path: Path) -> None:
    service, repo, _executor, _checker = _service(tmp_path)
    payload = _payload()
    payload["baseline_leg_id"] = "missing_leg"
    request = parse_request(payload)
    run_id = "macb_async_failure"
    repo.create_run(run_id=run_id, request=request, roster_hash="hash")

    service._execute_run_thread(run_id, request)

    run = repo.runs[run_id]
    assert run["status"] == "failed"
    assert run["updated_at"] is not None
    assert run["reason"]["reason_code"] == "baseline_leg_missing"
    assert run["reason"]["logical_status"] == "failed"
    assert run["reason"]["run_id"] == run_id


def test_unhandled_execute_run_exception_is_persisted_with_explicit_reason(tmp_path: Path) -> None:
    service, repo, _executor, _checker = _service(tmp_path)
    request = parse_request(_payload())
    run_id = "macb_unhandled_failure"
    repo.create_run(run_id=run_id, request=request, roster_hash="hash")

    def boom(*_args, **_kwargs):
        raise RuntimeError("unexpected boom")

    service._execute_run = boom  # type: ignore[method-assign]

    service._execute_run_thread(run_id, request)

    run = repo.runs[run_id]
    assert run["status"] == "failed"
    assert run["reason"]["reason_code"] == "combine_backtest_unhandled_exception"
    assert "unexpected boom" in run["reason"]["message"]


def test_parse_request_sets_reasonable_timeout_defaults_and_topk(tmp_path: Path) -> None:
    payload = _payload()
    payload["topk"] = 50

    request = parse_request(payload)

    assert request.topk == 50
    assert request.scheme_timeout_seconds == DEFAULT_PRED_BACKTEST_TIMEOUT_SECONDS
    assert request.backtest_config["timeout_seconds"] == DEFAULT_PRED_BACKTEST_TIMEOUT_SECONDS
    assert request.backtest_config["topk"] == 50
    assert request.run_timeout_seconds is not None
    assert request.run_timeout_seconds < 6 * 60 * 60


def test_topk_reaches_executor_backtest_config(tmp_path: Path) -> None:
    service, _repo, executor, _checker = _service(tmp_path)
    payload = _payload()
    payload["topk"] = 50

    service.submit_run(payload, run_async=False)

    assert executor.calls
    assert {call["backtest_config"]["topk"] for call in executor.calls} == {50}


def test_topk_override_updates_qrun_conf_yaml(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    conf = {
        "port_analysis_config": {
            "strategy": {
                "class": "ScoreWeightedTopkStrategyV2",
                "kwargs": {"topk": 25, "n_drop": 2},
            },
        },
    }
    (workspace / "conf.yaml").write_text(yaml.safe_dump(conf, sort_keys=False), encoding="utf-8")

    apply_pred_backtest_overrides(workspace=workspace, backtest_config={"topk": 100})

    updated = yaml.safe_load((workspace / "conf.yaml").read_text(encoding="utf-8"))
    assert updated["port_analysis_config"]["strategy"]["kwargs"]["topk"] == 100
    assert updated["port_analysis_config"]["strategy"]["kwargs"]["n_drop"] == 2


def test_shell_executor_command_path_applies_topk_and_strategy_overrides(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    template = tmp_path / "runtime_template"
    template.mkdir()
    (template / "conf.yaml").write_text(
        """
model:
  kwargs:
    pt_model_kwargs: { "num_features": {{ num_features }}, "num_timesteps": {{ num_timesteps }} }
port_analysis_config:
  strategy:
    class: ScoreWeightedTopkStrategyV2
    kwargs:
      topk: 25
      n_drop: 2
      max_n_drop: 2
      min_n_drop: 0
""".lstrip(),
        encoding="utf-8",
    )
    for runtime_file in ("qrun_limit_minute.py", "read_exp_res.py"):
        (template / runtime_file).write_text("# test runtime placeholder\n", encoding="utf-8")
    script = tmp_path / "emit_metrics.py"
    script.write_text(
        "import json\n"
        "from pathlib import Path\n"
        "Path('command_was_run').write_text('yes', encoding='utf-8')\n"
        "Path('qlib_results_enhanced.json').write_text(json.dumps({'absolute_returns': "
        "{'cagr': 1.0, 'max_drawdown': -0.1, 'sharpe': 2.0, 'calmar': 10.0}, "
        "'prediction_diagnostics': {'topk_return_20': 0.07, 'topk_hit_rate_20': 0.66}, "
        "'trade_diagnostics': {'avg_turnover': 12.0}}))\n",
        encoding="utf-8",
    )
    pred_pkl = tmp_path / "combined_prediction.pkl"
    pd.DataFrame({"score": [1.0]}).to_pickle(pred_pkl)
    workspace = tmp_path / "workspace"
    caplog.set_level("INFO", logger="backend.services.multi_alpha.combine_backtest")

    metrics = ShellPredBacktestExecutor().execute_pred_backtest(
        workspace=workspace,
        pred_pkl=pred_pkl,
        node_id="wsl2-5080",
        backtest_config={
            "runtime_template_dir": str(template),
            "command": [sys.executable, str(script)],
            "topk": 50,
            "strategy_kwargs": {"n_drop": 5, "hold_thresh": 0.12},
            "timeout_seconds": 30,
        },
    )

    updated = (workspace / "conf.yaml").read_text(encoding="utf-8")
    assert re.search(r"(?m)^      topk: 50$", updated)
    assert re.search(r"(?m)^      n_drop: 5$", updated)
    assert re.search(r"(?m)^      hold_thresh: 0\.12$", updated)
    assert re.search(r"(?m)^      max_n_drop: 2$", updated)
    assert re.search(r"(?m)^      min_n_drop: 0$", updated)
    assert "{{ num_features }}" in updated
    assert "{{ num_timesteps }}" in updated
    assert (workspace / "command_was_run").read_text(encoding="utf-8") == "yes"
    assert metrics["sharpe"] == 2.0
    override_logs = [record for record in caplog.records if record.message == "Applied pred-backtest conf overrides"]
    assert override_logs
    assert override_logs[-1].workspace == str(workspace)
    assert override_logs[-1].effective_topk == 50
    assert override_logs[-1].strategy_kwargs_keys == ["hold_thresh", "n_drop"]


def test_shell_executor_command_path_override_failure_is_loud(tmp_path: Path) -> None:
    template = tmp_path / "runtime_template"
    template.mkdir()
    (template / "conf.yaml").write_text(
        """
model:
  kwargs:
    pt_model_kwargs: { "num_features": {{ num_features }}, "num_timesteps": {{ num_timesteps }} }
port_analysis_config:
  strategy:
    class: ScoreWeightedTopkStrategyV2
    kwargs:
      n_drop: 2
      max_n_drop: 2
      min_n_drop: 0
""".lstrip(),
        encoding="utf-8",
    )
    for runtime_file in ("qrun_limit_minute.py", "read_exp_res.py"):
        (template / runtime_file).write_text("# test runtime placeholder\n", encoding="utf-8")
    script = tmp_path / "should_not_run.py"
    script.write_text(
        "from pathlib import Path\nPath('command_was_run').write_text('unexpected', encoding='utf-8')\n",
        encoding="utf-8",
    )
    pred_pkl = tmp_path / "combined_prediction.pkl"
    pd.DataFrame({"score": [1.0]}).to_pickle(pred_pkl)
    workspace = tmp_path / "workspace"

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        ShellPredBacktestExecutor().execute_pred_backtest(
            workspace=workspace,
            pred_pkl=pred_pkl,
            node_id="wsl2-5080",
            backtest_config={
                "runtime_template_dir": str(template),
                "command": [sys.executable, str(script)],
                "topk": 50,
                "timeout_seconds": 30,
            },
        )

    assert excinfo.value.reason_code == "pred_backtest_conf_invalid"
    assert excinfo.value.context["field"] == "port_analysis_config.strategy.kwargs.topk"
    assert not (workspace / "command_was_run").exists()


def test_shell_executor_command_path_missing_kwargs_block_is_loud(tmp_path: Path) -> None:
    template = tmp_path / "runtime_template"
    template.mkdir()
    (template / "conf.yaml").write_text(
        """
model:
  kwargs:
    pt_model_kwargs: { "num_features": {{ num_features }}, "num_timesteps": {{ num_timesteps }} }
port_analysis_config:
  strategy:
    class: ScoreWeightedTopkStrategyV2
""".lstrip(),
        encoding="utf-8",
    )
    for runtime_file in ("qrun_limit_minute.py", "read_exp_res.py"):
        (template / runtime_file).write_text("# test runtime placeholder\n", encoding="utf-8")
    script = tmp_path / "should_not_run.py"
    script.write_text(
        "from pathlib import Path\nPath('command_was_run').write_text('unexpected', encoding='utf-8')\n",
        encoding="utf-8",
    )
    pred_pkl = tmp_path / "combined_prediction.pkl"
    pd.DataFrame({"score": [1.0]}).to_pickle(pred_pkl)
    workspace = tmp_path / "workspace"

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        ShellPredBacktestExecutor().execute_pred_backtest(
            workspace=workspace,
            pred_pkl=pred_pkl,
            node_id="wsl2-5080",
            backtest_config={
                "runtime_template_dir": str(template),
                "command": [sys.executable, str(script)],
                "topk": 50,
                "timeout_seconds": 30,
            },
        )

    assert excinfo.value.reason_code == "pred_backtest_conf_invalid"
    assert excinfo.value.context["field"] == "port_analysis_config.strategy.kwargs"
    assert not (workspace / "command_was_run").exists()


def test_async_run_exposes_running_phase_heartbeat(tmp_path: Path) -> None:
    executor = FakeExecutor(sleep_seconds=0.2)
    service, repo, _executor, _checker = _service(tmp_path, executor=executor)
    payload = _payload()

    result = service.submit_run(payload, run_async=True)
    run_id = result["run_id"]

    deadline = time.time() + 5
    observed_phase = None
    while time.time() < deadline:
        reason = repo.runs[run_id].get("reason") or {}
        observed_phase = reason.get("phase")
        if observed_phase in {"backtest_submitted", "backtests_running", "backtest_finished"}:
            break
        time.sleep(0.02)

    assert observed_phase in {"backtest_submitted", "backtests_running", "backtest_finished"}
    assert repo.runs[run_id]["updated_at"] is not None
    deadline = time.time() + 5
    while time.time() < deadline and repo.runs[run_id]["status"] == "running":
        time.sleep(0.02)
    assert repo.runs[run_id]["status"] == "succeeded"


def test_scheme_timeout_fails_run_loud_without_waiting_for_default_six_hours(tmp_path: Path) -> None:
    executor = FakeExecutor(sleep_seconds=2.0)
    service, repo, _executor, _checker = _service(tmp_path, executor=executor)
    payload = _payload()
    payload["weighting_schemes"] = ["equal"]
    payload["scheme_timeout_seconds"] = 1
    payload["run_timeout_seconds"] = 10

    started_at = time.monotonic()
    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        service.submit_run(payload, run_async=False)

    assert time.monotonic() - started_at < 5
    assert excinfo.value.reason_code == "combine_backtest_scheme_timeout"
    run_id = next(iter(repo.runs))
    run = service.get_run(run_id)
    assert run["run"]["status"] == "failed"
    assert run["run"]["reason"]["reason_code"] == "combine_backtest_scheme_timeout"
    assert run["run"]["reason"]["context"]["child_task"] in {"baseline_leg_a", "combined_equal"}
    assert run["run"]["reason"]["context"]["scheme_timeout_seconds"] == 1


def test_run_timeout_marks_run_failed_loud(tmp_path: Path) -> None:
    executor = FakeExecutor(sleep_seconds=2.0)
    service, repo, _executor, _checker = _service(tmp_path, executor=executor)
    payload = _payload()
    payload["weighting_schemes"] = ["equal"]
    payload["scheme_timeout_seconds"] = 30
    payload["run_timeout_seconds"] = 1

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        service.submit_run(payload, run_async=False)

    assert excinfo.value.reason_code == "combine_backtest_run_timeout"
    run_id = next(iter(repo.runs))
    run = repo.runs[run_id]
    assert run["status"] == "failed"
    assert run["reason"]["reason_code"] == "combine_backtest_run_timeout"
    assert run["reason"]["logical_status"] == "failed"


def test_stale_running_cleanup_is_dry_run_by_default_and_confirmation_gated(tmp_path: Path) -> None:
    service, repo, _executor, _checker = _service(tmp_path)
    request = parse_request(_payload())
    repo.create_run(run_id="macb_stale", request=request, roster_hash="hash")
    repo.runs["macb_stale"]["updated_at"] = "2026-01-01T00:00:00+00:00"

    preview = service.mark_stale_running_runs_failed(max_age_seconds=1, dry_run=True)

    assert preview["dry_run"] is True
    assert preview["candidate_count"] == 1
    assert repo.runs["macb_stale"]["status"] == "running"
    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        service.mark_stale_running_runs_failed(max_age_seconds=1, dry_run=False)
    assert excinfo.value.reason_code == "stale_cleanup_confirmation_required"

    result = service.mark_stale_running_runs_failed(
        max_age_seconds=1,
        dry_run=False,
        confirmation=COMBINE_BACKTEST_STALE_FAIL_CONFIRM,
    )

    assert result["updated_count"] == 1
    assert repo.runs["macb_stale"]["status"] == "failed"
    assert repo.runs["macb_stale"]["reason"]["reason_code"] == "combine_backtest_stale_timeout"


def test_combine_backtest_persists_loo_for_three_or_more_legs(tmp_path: Path) -> None:
    service, _repo, _executor, _checker = _service(tmp_path)

    result = service.submit_run(_payload_three_legs(), run_async=False)
    run = service.get_run(result["run_id"])

    assert len(run["loo"]) == 3
    assert {row["dropped_leg_id"] for row in run["loo"]} == {"leg_a", "leg_b", "leg_c"}


def test_combine_backtest_limits_intra_run_parallelism_to_node_cap(tmp_path: Path) -> None:
    executor = FakeExecutor(sleep_seconds=0.05)
    service, _repo, _executor, _checker = _service(tmp_path, executor=executor)

    result = service.submit_run(_payload_three_legs(), run_async=False)
    run = service.get_run(result["run_id"])

    assert run["run"]["status"] == "succeeded"
    assert executor.max_active == 2
    assert max(executor.active_observations) <= 2
    assert len(executor.calls) == 5


def test_shell_executor_runs_two_real_wsl_children_concurrently_without_hang(tmp_path: Path) -> None:
    if shutil.which("wsl") is None:
        pytest.skip("WSL is required for the real concurrent subprocess smoke")
    executor = FakeExecutor(sleep_seconds=0.05)
    service, _repo, _fake_executor, _checker = _service(tmp_path, executor=executor)
    command = [
        "wsl",
        "bash",
        "-lc",
        "python3 - <<'PY'\n"
        "import json\n"
        "import time\n"
        "from pathlib import Path\n"
        "start = time.time()\n"
        "time.sleep(5)\n"
        "Path('qlib_results_enhanced.json').write_text(json.dumps({'absolute_returns': "
        "{'cagr': 1.23, 'max_drawdown': -0.12, 'sharpe': 2.34, 'calmar': 10.25, "
        "'topk_return_20': 0.07, 'topk_hit_rate_20': 0.66, 'turnover': 12.0}}))\n"
        "print(f'ok real-wsl start={start:.6f} end={time.time():.6f} cwd={Path.cwd()}')\n"
        "PY",
    ]
    service._executor = ShellPredBacktestExecutor()  # exercise real subprocesses instead of the fake executor
    payload = _payload()
    payload["weighting_schemes"] = ["equal"]
    payload["backtest_config"] = {
        "node_id": "wsl2-5080",
        "node_parallelism": {"wsl2-5080": 2},
        "runtime_template_dir": str(_runtime_template(tmp_path)),
        "command": command,
        "timeout_seconds": 30,
    }

    result = service.submit_run(payload, run_async=False)
    run = service.get_run(result["run_id"])

    assert run["run"]["status"] == "succeeded"
    workspaces = sorted((tmp_path / "macb" / result["run_id"]).iterdir())
    assert len({path.resolve() for path in workspaces}) == 2
    assert {"baseline_leg_a", "combined_equal"} == {path.name for path in workspaces}
    intervals: list[tuple[float, float]] = []
    for workspace in workspaces:
        stdout = (workspace / "pred_backtest_stdout.log").read_text(encoding="utf-8")
        assert "ok real-wsl" in stdout
        match = re.search(r"start=(?P<start>\d+\.\d+) end=(?P<end>\d+\.\d+)", stdout)
        assert match is not None, stdout
        intervals.append((float(match.group("start")), float(match.group("end"))))
    assert len(intervals) == 2
    assert max(start for start, _end in intervals) < min(end for _start, end in intervals)
    assert run["loo"] == []


def test_parallel_and_serial_results_match(tmp_path: Path) -> None:
    serial_payload = _payload_three_legs()
    serial_payload["backtest_config"] = {"node_id": "wsl2-5080", "node_parallelism": {"wsl2-5080": 1}}
    parallel_payload = _payload_three_legs()
    parallel_payload["backtest_config"] = {"node_id": "wsl2-5080", "node_parallelism": {"wsl2-5080": 2}}
    serial_service, _serial_repo, _serial_executor, _serial_checker = _service(tmp_path / "serial")
    parallel_service, _parallel_repo, _parallel_executor, _parallel_checker = _service(tmp_path / "parallel")

    serial = serial_service.get_run(serial_service.submit_run(serial_payload, run_async=False)["run_id"])
    parallel = parallel_service.get_run(parallel_service.submit_run(parallel_payload, run_async=False)["run_id"])

    serial_schemes = {row["weighting_scheme"]: row for row in serial["scheme_results"]}
    parallel_schemes = {row["weighting_scheme"]: row for row in parallel["scheme_results"]}
    assert set(serial_schemes) == set(parallel_schemes)
    for scheme, serial_row in serial_schemes.items():
        parallel_row = parallel_schemes[scheme]
        assert parallel_row["weights_json"] == serial_row["weights_json"]
        assert parallel_row["per_window_weights_json"] == serial_row["per_window_weights_json"]
        for key in ("cagr", "max_drawdown", "sharpe", "calmar", "vs_baseline_sharpe_delta", "vs_baseline_calmar_delta"):
            assert parallel_row[key] == pytest.approx(serial_row[key])

    serial_loo = {(row["weighting_scheme"], row["dropped_leg_id"]): row for row in serial["loo"]}
    parallel_loo = {(row["weighting_scheme"], row["dropped_leg_id"]): row for row in parallel["loo"]}
    assert set(serial_loo) == set(parallel_loo)
    for key, serial_row in serial_loo.items():
        parallel_row = parallel_loo[key]
        assert parallel_row["marginal_sharpe"] == pytest.approx(serial_row["marginal_sharpe"])
        assert parallel_row["marginal_calmar"] == pytest.approx(serial_row["marginal_calmar"])
        assert parallel_row["marginal_cagr"] == pytest.approx(serial_row["marginal_cagr"])


def test_combine_backtest_deterministic_combined_prediction(tmp_path: Path) -> None:
    service, _repo, _executor, _checker = _service(tmp_path)
    first = service.submit_run(_payload(), run_async=False)
    second = service.submit_run(_payload(), run_async=False)

    first_pred = pd.read_pickle(tmp_path / "macb" / first["run_id"] / "combined_equal" / "combined_prediction.pkl")
    second_pred = pd.read_pickle(tmp_path / "macb" / second["run_id"] / "combined_equal" / "combined_prediction.pkl")

    pd.testing.assert_frame_equal(first_pred, second_pred)


def test_parse_request_respects_confirmation_constant_name() -> None:
    assert COMBINE_BACKTEST_CONFIRM == "MULTI_ALPHA_COMBINE_BACKTEST_RUN"
    assert DEFAULT_WEIGHTING_SCHEMES == ("equal", "orthogonality_aware", "ic_weighted", "risk_parity")
    assert RANK_FUSION_WEIGHTING_SCHEMES == ("rank_fusion_rrf", "rank_fusion_borda")
    request = parse_request(_payload())
    assert request.weighting_schemes == ("equal", "ic_weighted", "risk_parity")


def test_rank_fusion_schemes_are_opt_in_not_default() -> None:
    payload = _payload()
    payload.pop("weighting_schemes")

    request = parse_request(payload)

    assert request.weighting_schemes == DEFAULT_WEIGHTING_SCHEMES
    assert not any(scheme in request.weighting_schemes for scheme in RANK_FUSION_WEIGHTING_SCHEMES)


def test_rest_request_model_preserves_rank_fusion_options() -> None:
    payload = _payload()
    payload["weighting_schemes"] = ["rank_fusion_rrf"]
    payload["rank_fusion"] = {"rrf_k": 42, "leg_weights": {"leg_a": 2.0}}

    request = CombineBacktestRunRequest(**payload)

    assert request.model_dump()["rank_fusion"] == {"rrf_k": 42, "leg_weights": {"leg_a": 2.0}}


def test_rest_request_model_accepts_timeout_controls() -> None:
    payload = _payload()
    payload["scheme_timeout_seconds"] = 1800
    payload["run_timeout_seconds"] = 7200

    request = CombineBacktestRunRequest(**payload)

    dumped = request.model_dump()
    assert dumped["scheme_timeout_seconds"] == 1800
    assert dumped["run_timeout_seconds"] == 7200


def test_rank_fusion_rrf_and_borda_run_and_persist_metadata(tmp_path: Path) -> None:
    service, _repo, _executor, _checker = _rank_fusion_service(tmp_path)
    payload = _payload_three_legs()
    payload["weighting_schemes"] = ["rank_fusion_rrf", "rank_fusion_borda"]
    payload["rank_fusion"] = {"rrf_k": 42}

    result = service.submit_run(payload, run_async=False)
    run = service.get_run(result["run_id"])

    assert run["run"]["status"] == "succeeded"
    schemes = {row["weighting_scheme"]: row for row in run["scheme_results"]}
    assert set(schemes) == {"rank_fusion_rrf", "rank_fusion_borda"}
    assert all(not row["skipped"] for row in schemes.values())
    assert all(row["sharpe"] is not None and row["calmar"] is not None for row in schemes.values())
    assert schemes["rank_fusion_rrf"]["weights_json"]["method"] == "rrf"
    assert schemes["rank_fusion_rrf"]["weights_json"]["rrf_k"] == 42
    assert schemes["rank_fusion_rrf"]["per_window_weights_json"][0]["rank_fusion"] is True
    assert schemes["rank_fusion_borda"]["weights_json"]["method"] == "borda"
    assert "rrf_k" not in schemes["rank_fusion_borda"]["weights_json"]
    assert schemes["rank_fusion_borda"]["per_window_weights_json"][0]["method"] == "borda"
    assert len(run["loo"]) == 6
    assert {
        (row["weighting_scheme"], row["dropped_leg_id"])
        for row in run["loo"]
    } == {
        ("rank_fusion_rrf", "leg_a"),
        ("rank_fusion_rrf", "leg_b"),
        ("rank_fusion_rrf", "leg_c"),
        ("rank_fusion_borda", "leg_a"),
        ("rank_fusion_borda", "leg_b"),
        ("rank_fusion_borda", "leg_c"),
    }


def test_rank_fusion_only_path_does_not_require_label_panels(tmp_path: Path) -> None:
    service, _repo, _executor, _checker = _rank_fusion_service(
        tmp_path,
        panel_builder=RaisingPanelBuilder(),
    )
    payload = _payload()
    payload["weighting_schemes"] = ["rank_fusion_rrf"]

    result = service.submit_run(payload, run_async=False)
    run = service.get_run(result["run_id"])

    assert run["run"]["status"] == "succeeded"
    assert run["scheme_results"][0]["weighting_scheme"] == "rank_fusion_rrf"
    assert run["scheme_results"][0]["skipped"] is False


def test_rank_fusion_migration_allows_new_schemes_and_rollback_restores_old_set() -> None:
    path = Path("backend/migrations/multi_alpha_combine_backtest_rankfusion_schemes_20260621.sql")
    text = path.read_text(encoding="utf-8")
    forward, rollback = text.split("-- Rollback", maxsplit=1)
    old_schemes = {"equal", "orthogonality_aware", "ic_weighted", "risk_parity"}

    for scheme in (*old_schemes, "rank_fusion_rrf", "rank_fusion_borda"):
        assert f"'{scheme}'" in forward
    assert "ck_macb_scheme_supported" in forward
    assert "ck_macb_loo_scheme_supported" in forward
    assert "DROP CONSTRAINT IF EXISTS ck_macb_scheme_supported" in forward
    assert "DROP CONSTRAINT IF EXISTS ck_macb_loo_scheme_supported" in forward
    for scheme in old_schemes:
        assert f"'{scheme}'" in rollback
    assert "'rank_fusion_rrf'" not in rollback
    assert "'rank_fusion_borda'" not in rollback


def test_node_parallelism_must_cover_selected_node(tmp_path: Path) -> None:
    service, _repo, _executor, _checker = _service(tmp_path)
    payload = _payload()
    payload["backtest_config"] = {"node_id": "rdagent-node1", "node_parallelism": {"wsl2-5080": 2}}

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        service.submit_run(payload, run_async=False)

    assert excinfo.value.reason_code == "node_parallelism_missing_node"


def test_node_capacity_exhaustion_fails_loud_before_executor(tmp_path: Path) -> None:
    service, repo, executor, _checker = _service(tmp_path, capacity_checker=FakeCapacityChecker(active_count=2))
    status_seen_by_archive_emit: list[str] = []
    service._archive_event_capture = FakeArchiveEventCapture(  # type: ignore[attr-defined]
        on_enqueue=lambda event: status_seen_by_archive_emit.append(repo.runs[event["run_id"]]["status"])
    )

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        service.submit_run(_payload(), run_async=False)

    assert excinfo.value.reason_code == "node_capacity_exhausted"
    run_id = next(iter(repo.runs))
    assert repo.runs[run_id]["status"] == "failed"
    assert status_seen_by_archive_emit == ["failed"]
    assert executor.calls == []


def test_noncritical_child_failure_records_reason_and_continues(tmp_path: Path) -> None:
    service, repo, executor, _checker = _service(
        tmp_path,
        executor=FakeExecutor(fail_names={"loo_equal_drop_leg_b"}),
    )

    result = service.submit_run(_payload_three_legs(), run_async=False)
    run_id = result["run_id"]
    run = service.get_run(run_id)

    assert run["run"]["status"] == "partial_failed"
    assert run["run"]["reason"]["logical_status"] == "partial_failed"
    failed = run["run"]["reason"]["failed_child_tasks"]
    assert failed["loo_equal_drop_leg_b"]["reason_code"] == "fake_pred_backtest_failed"
    assert len(run["scheme_results"]) == 1
    assert len(run["loo"]) == 2
    assert {row["dropped_leg_id"] for row in run["loo"]} == {"leg_a", "leg_c"}
    assert repo.runs[run_id]["status"] == "partial_failed"
    assert {call["workspace"].name for call in executor.calls} >= {"baseline_leg_a", "combined_equal", "loo_equal_drop_leg_a", "loo_equal_drop_leg_c"}


def test_equal_scheme_failure_fails_run_and_marks_scheme_skipped(tmp_path: Path) -> None:
    service, repo, _executor, _checker = _service(
        tmp_path,
        executor=FakeExecutor(fail_names={"combined_equal"}),
    )

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        service.submit_run(_payload_three_legs(), run_async=False)

    assert excinfo.value.reason_code == "fake_pred_backtest_failed"
    run_id = next(iter(repo.runs))
    run = service.get_run(run_id)
    assert run["run"]["status"] == "failed"
    assert run["run"]["reason"]["logical_status"] == "failed"
    scheme = run["scheme_results"][0]
    assert scheme["weighting_scheme"] == "equal"
    assert scheme["skipped"] is True
    assert "fake_pred_backtest_failed" in scheme["skipped_reason"]
    assert run["loo"] == []


def test_run_command_decodes_utf8_subprocess_output_on_windows_codepages(tmp_path: Path) -> None:
    script = tmp_path / "emit_utf8.py"
    script.write_text(
        "import sys\n"
        "sys.stdout.buffer.write(b'out prefix \\xe2\\x8f\\xb1 utf8\\\\n')\n"
        "sys.stderr.buffer.write(b'err prefix \\xe2\\x8f\\xb1 utf8\\\\n')\n",
        encoding="utf-8",
    )

    completed = run_command(
        [sys.executable, str(script)],
        cwd=tmp_path,
        timeout_seconds=30,
        log_prefix="utf8_capture",
    )
    marker = "\u23f1"

    assert completed.returncode == 0
    assert f"out prefix {marker} utf8" in completed.stdout
    assert f"err prefix {marker} utf8" in completed.stderr
    assert f"out prefix {marker} utf8" in (tmp_path / "utf8_capture_stdout.log").read_text(encoding="utf-8")
    assert f"err prefix {marker} utf8" in (tmp_path / "utf8_capture_stderr.log").read_text(encoding="utf-8")


def test_run_command_timeout_fails_loud_and_keeps_context(tmp_path: Path) -> None:
    script = tmp_path / "hang.py"
    script.write_text(
        "import sys, time\n"
        "print('starting hang', flush=True)\n"
        "sys.stderr.write('stderr before timeout\\n')\n"
        "sys.stderr.flush()\n"
        "time.sleep(100000)\n",
        encoding="utf-8",
    )

    started_at = time.monotonic()
    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        run_command(
            [sys.executable, str(script)],
            cwd=tmp_path,
            timeout_seconds=1,
            log_prefix="timeout_capture",
            error_context={"workspace": str(tmp_path), "node_id": "wsl2-5080", "backtest_name": "combined_equal"},
        )

    assert time.monotonic() - started_at < 10
    assert excinfo.value.reason_code == "pred_backtest_timeout"
    assert excinfo.value.context["workspace"] == str(tmp_path)
    assert excinfo.value.context["node_id"] == "wsl2-5080"
    assert excinfo.value.context["backtest_name"] == "combined_equal"
    assert "starting hang" in excinfo.value.context["stdout_tail"]
    assert "stderr before timeout" in excinfo.value.context["stderr_tail"]
    assert "starting hang" in (tmp_path / "timeout_capture_stdout.log").read_text(encoding="utf-8")
    assert "stderr before timeout" in (tmp_path / "timeout_capture_stderr.log").read_text(encoding="utf-8")


def test_run_command_detaches_stdin_from_service_process(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, object] = {}

    def fake_run(*args, **kwargs):
        observed["stdin"] = kwargs.get("stdin")
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="ok\n", stderr="")

    monkeypatch.setattr("backend.services.multi_alpha.combine_backtest.subprocess.run", fake_run)

    completed = run_command(
        [sys.executable, "-c", "print('ok')"],
        cwd=tmp_path,
        timeout_seconds=30,
        log_prefix="stdin_capture",
    )

    assert completed.returncode == 0
    assert observed["stdin"] is subprocess.DEVNULL


def test_shell_executor_timeout_marks_critical_child_failed_without_hanging(tmp_path: Path) -> None:
    service, repo, _executor, _checker = _service(tmp_path)
    payload = _payload_three_legs()
    payload["backtest_config"] = {
        "node_id": "wsl2-5080",
        "node_parallelism": {"wsl2-5080": 2},
        "runtime_template_dir": str(_runtime_template(tmp_path)),
        "command": [
            sys.executable,
            "-c",
            "import sys, time; print('child started', flush=True); sys.stderr.write('child stderr\\n'); sys.stderr.flush(); time.sleep(100000)",
        ],
        "timeout_seconds": 1,
    }
    service._executor = ShellPredBacktestExecutor()

    started_at = time.monotonic()
    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        service.submit_run(payload, run_async=False)

    assert time.monotonic() - started_at < 20
    assert excinfo.value.reason_code == "pred_backtest_timeout"
    run_id = next(iter(repo.runs))
    run = service.get_run(run_id)
    assert run["run"]["status"] == "failed"
    failed_children = run["run"]["reason"]["failed_child_tasks"]
    assert failed_children["baseline_leg_a"]["reason_code"] == "pred_backtest_timeout"
    assert failed_children["baseline_leg_a"]["context"]["backtest_name"] == "baseline_leg_a"
    assert "child stderr" in failed_children["baseline_leg_a"]["context"]["stderr_tail"]


def test_shell_executor_failure_keeps_utf8_stderr_tail(tmp_path: Path) -> None:
    script = tmp_path / "fail_utf8.py"
    script.write_text(
        "import sys\n"
        "sys.stderr.buffer.write(b'failure stderr \\xe2\\x8f\\xb1 diagnostic\\\\n')\n"
        "sys.exit(7)\n",
        encoding="utf-8",
    )
    pred_pkl = tmp_path / "combined_prediction.pkl"
    pd.DataFrame({"score": [1.0]}).to_pickle(pred_pkl)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for runtime_file in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"):
        (workspace / runtime_file).write_text("# test runtime placeholder\n", encoding="utf-8")
    marker = "\u23f1"

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        ShellPredBacktestExecutor().execute_pred_backtest(
            workspace=workspace,
            pred_pkl=pred_pkl,
            node_id="wsl2-5080",
            backtest_config={"command": [sys.executable, str(script)], "timeout_seconds": 30},
        )

    assert excinfo.value.reason_code == "pred_backtest_failed"
    assert f"failure stderr {marker} diagnostic" in excinfo.value.context["stderr_tail"]
    assert f"failure stderr {marker} diagnostic" in (workspace / "pred_backtest_stderr.log").read_text(encoding="utf-8")


def test_prediction_store_upload_is_explicit_and_fail_loud(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pred_path = tmp_path / "combined_prediction.pkl"
    pd.DataFrame({"score": [1.0]}).to_pickle(pred_path)
    monkeypatch.setenv("AISTOCK_PREDICTION_STORE_UPLOAD_URL", "http://backend/api/v1/prediction-store/artifacts/{run_key}")

    class Response:
        status_code = 200
        text = "{}"

        @staticmethod
        def json() -> dict:
            return {"status": "success", "data": {"manifest": {"artifacts": [{"artifact_type": "prediction"}]}}}

    def fake_post(url: str, **kwargs):
        assert url.endswith("/macb_123_combined_equal")
        assert "files" in kwargs and "pred" in kwargs["files"]
        return Response()

    monkeypatch.setattr("backend.services.multi_alpha.combine_backtest.requests.post", fake_post)

    manifest = maybe_upload_combined_prediction(
        run_id="macb_123",
        backtest_name="combined_equal",
        pred_pkl=pred_path,
        node_id="wsl2-5080",
        backtest_config={"node_id": "wsl2-5080"},
    )

    assert manifest == {"artifacts": [{"artifact_type": "prediction"}]}
