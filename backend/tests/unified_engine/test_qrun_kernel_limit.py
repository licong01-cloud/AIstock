from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
RUNNER_CASES = (
    (PROJECT_ROOT / "scripts" / "qrun_limit.py", "day"),
    (PROJECT_ROOT / "scripts" / "qrun_limit_minute.py", "1min"),
)


class _FakeQlibConfig(dict):
    def get_kernels(self, freq: str) -> int:
        del freq
        return int(self["kernels"])


def _load_runner(
    monkeypatch: pytest.MonkeyPatch,
    runner_path: Path,
    *,
    apply_requested_kernels: bool = True,
):
    config = _FakeQlibConfig(
        kernels=26,
        exp_manager={"kwargs": {}},
    )
    init_calls: list[dict] = []

    qlib = types.ModuleType("qlib")

    def fake_init(**kwargs) -> None:
        init_calls.append(dict(kwargs))
        # Model QlibConfig.set(): qlib.init resets C before applying kwargs.
        config["kernels"] = 26
        if apply_requested_kernels:
            config["kernels"] = kwargs.get("kernels", 26)

    qlib.init = fake_init
    qlib_model = types.ModuleType("qlib.model")
    qlib_model_trainer = types.ModuleType("qlib.model.trainer")
    qlib_model_trainer.task_train = lambda *args, **kwargs: None
    qlib_workflow = types.ModuleType("qlib.workflow")
    qlib_workflow_cli = types.ModuleType("qlib.workflow.cli")
    qlib_workflow_cli.sys_config = lambda *args, **kwargs: None
    qlib_workflow_cli.task_train = lambda *args, **kwargs: None
    qlib_config = types.ModuleType("qlib.config")
    qlib_config.C = config

    for name, module in {
        "qlib": qlib,
        "qlib.model": qlib_model,
        "qlib.model.trainer": qlib_model_trainer,
        "qlib.workflow": qlib_workflow,
        "qlib.workflow.cli": qlib_workflow_cli,
        "qlib.config": qlib_config,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    spec = importlib.util.spec_from_file_location(
        f"{runner_path.stem}_kernel_limit_test",
        runner_path,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module, config, init_calls


@pytest.mark.parametrize(("runner_path", "freq"), RUNNER_CASES)
def test_qrun_passes_four_kernel_limit_through_qlib_init(
    monkeypatch: pytest.MonkeyPatch,
    runner_path: Path,
    freq: str,
) -> None:
    runner, qlib_config, init_calls = _load_runner(monkeypatch, runner_path)
    config = {
        "qlib_init": {
            "provider_uri": {"day": "/data/day", "1min": "/data/minute"},
            "kernels": 99,
        }
    }
    exp_manager = {"kwargs": {"uri": "file:/tmp/mlruns"}}

    qlib_init_config = runner._qlib_init_config_with_kernel_limit(config)
    runner.qlib.init(**qlib_init_config, exp_manager=exp_manager)
    runner._verify_qlib_kernel_limit(freq)

    assert len(init_calls) == 1
    assert init_calls[0]["kernels"] == 4
    assert init_calls[0]["provider_uri"] == config["qlib_init"]["provider_uri"]
    assert init_calls[0]["exp_manager"] is exp_manager
    assert qlib_config.get_kernels(freq) == 4
    assert config["qlib_init"]["kernels"] == 99


@pytest.mark.parametrize(("runner_path", "freq"), RUNNER_CASES)
def test_qrun_fails_closed_when_effective_kernel_limit_does_not_stick(
    monkeypatch: pytest.MonkeyPatch,
    runner_path: Path,
    freq: str,
) -> None:
    runner, qlib_config, _init_calls = _load_runner(
        monkeypatch,
        runner_path,
        apply_requested_kernels=False,
    )

    with pytest.raises(RuntimeError, match="QE_QLIB_KERNEL_LIMIT_NOT_EFFECTIVE"):
        qlib_init_config = runner._qlib_init_config_with_kernel_limit(
            {"qlib_init": {"provider_uri": "/data/day"}}
        )
        runner.qlib.init(**qlib_init_config, exp_manager={"kwargs": {}})
        runner._verify_qlib_kernel_limit(freq)

    assert qlib_config.get_kernels(freq) == 26


@pytest.mark.parametrize(("runner_path", "_freq"), RUNNER_CASES)
def test_qrun_verifies_kernel_limit_before_provider_read(
    runner_path: Path,
    _freq: str,
) -> None:
    source = runner_path.read_text(encoding="utf-8")
    run_main = source[source.index("def _run_main") :]

    build_at = run_main.index("_qlib_init_config_with_kernel_limit(config)")
    init_at = run_main.index("qlib.init(**qlib_init_config")
    verify_at = run_main.index("_verify_qlib_kernel_limit(")
    provider_read_at = run_main.index("load_benchmark_series(config)")

    assert build_at < init_at < verify_at < provider_read_at
    assert 'C["kernels"] = 4' not in run_main
