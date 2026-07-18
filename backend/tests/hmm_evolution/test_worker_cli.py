from __future__ import annotations

import signal
from threading import Event

import pytest

from backend.services.hmm_evolution.errors import HMMEvolutionError
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


def test_worker_cli_requires_an_explicit_mode() -> None:
    parser = hmm_evolution_worker.build_parser()
    try:
        parser.parse_args([])
    except SystemExit as exc:
        assert exc.code == 2
    else:  # pragma: no cover - argparse must reject implicit daemon behaviour.
        raise AssertionError("worker CLI accepted an implicit mode")


def test_worker_cli_accepts_explicit_service_mode() -> None:
    args = hmm_evolution_worker.build_parser().parse_args(["--serve", "--poll-seconds", "2.5"])

    assert args.serve is True
    assert args.once is False
    assert args.drain is False
    assert args.poll_seconds == 2.5


def test_worker_cli_poll_interval_prefers_argument_over_env(monkeypatch) -> None:
    monkeypatch.setenv("HMM_EVOLUTION_WORKER_POLL_SECONDS", "9")

    assert hmm_evolution_worker._resolve_poll_seconds(1.5) == 1.5
    assert hmm_evolution_worker._resolve_poll_seconds(None) == 9.0


@pytest.mark.parametrize("value", ["bad", "0", "301", "nan"])
def test_worker_cli_poll_interval_fails_loud(monkeypatch, value: str) -> None:
    monkeypatch.setenv("HMM_EVOLUTION_WORKER_POLL_SECONDS", value)

    with pytest.raises(HMMEvolutionError, match="poll interval"):
        hmm_evolution_worker._resolve_poll_seconds(None)


def test_worker_cli_signal_handlers_request_graceful_stop(monkeypatch) -> None:
    installed: dict[signal.Signals, object] = {}

    def capture(sig: signal.Signals, handler: object) -> None:
        installed[sig] = handler

    monkeypatch.setattr(hmm_evolution_worker.signal, "signal", capture)
    stop_event = Event()

    hmm_evolution_worker._install_shutdown_handlers(stop_event)
    handler = installed[signal.SIGTERM]
    assert callable(handler)
    handler(signal.SIGTERM, None)

    assert stop_event.is_set()
    assert signal.SIGINT in installed


def test_worker_cli_service_owner_id_is_distinct() -> None:
    assert hmm_evolution_worker._default_owner_id(service=True).startswith("service-")
    assert hmm_evolution_worker._default_owner_id().startswith("manual-")


def test_worker_cli_returns_nonzero_for_unexpected_service_boot_failure(monkeypatch) -> None:
    monkeypatch.setenv("HMM_EVOLUTION_RUNTIME_MODE", "api_worker")

    def fail_runtime_build() -> object:
        raise RuntimeError("database connection failed")

    monkeypatch.setattr(hmm_evolution_worker, "build_runtime", fail_runtime_build)

    assert hmm_evolution_worker.main(["--serve", "--owner-id", "test-service"]) == 1
