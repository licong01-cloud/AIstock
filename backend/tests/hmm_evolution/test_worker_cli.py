from __future__ import annotations

from scripts import hmm_evolution_worker


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
