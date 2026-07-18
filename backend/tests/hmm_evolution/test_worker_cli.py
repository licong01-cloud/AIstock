from __future__ import annotations

from scripts import hmm_evolution_worker


def test_worker_cli_loads_canonical_env_without_overriding_process_env(
    monkeypatch,
    tmp_path,
) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "HMM_EVOLUTION_RUNTIME_MODE=api_worker\n"
        "HMM_EVOLUTION_WORKER_LOG_LEVEL=DEBUG\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("HMM_EVOLUTION_RUNTIME_MODE", raising=False)
    monkeypatch.setenv("HMM_EVOLUTION_WORKER_LOG_LEVEL", "WARNING")

    loaded_path = hmm_evolution_worker._load_canonical_env(env_path)

    assert loaded_path == env_path.resolve()
    assert hmm_evolution_worker.os.environ["HMM_EVOLUTION_RUNTIME_MODE"] == "api_worker"
    assert hmm_evolution_worker.os.environ["HMM_EVOLUTION_WORKER_LOG_LEVEL"] == "WARNING"


def test_worker_cli_missing_canonical_env_keeps_fail_closed_default(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.delenv("HMM_EVOLUTION_RUNTIME_MODE", raising=False)

    loaded_path = hmm_evolution_worker._load_canonical_env(tmp_path / "missing.env")

    assert loaded_path == (tmp_path / "missing.env").resolve()
    assert "HMM_EVOLUTION_RUNTIME_MODE" not in hmm_evolution_worker.os.environ


def test_worker_cli_fails_closed_when_runtime_is_disabled(monkeypatch) -> None:
    monkeypatch.setenv("HMM_EVOLUTION_RUNTIME_MODE", "disabled")
    assert hmm_evolution_worker.main(["--once", "--owner-id", "test-worker"]) == 2


def test_worker_cli_requires_an_explicit_finite_mode() -> None:
    parser = hmm_evolution_worker.build_parser()
    try:
        parser.parse_args([])
    except SystemExit as exc:
        assert exc.code == 2
    else:  # pragma: no cover - argparse must reject implicit daemon behaviour.
        raise AssertionError("worker CLI accepted an unbounded implicit mode")
