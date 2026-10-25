from __future__ import annotations

import json
import io
import subprocess
import sys
from threading import Event
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import monthly_unified_dataset_release_worker as cli


class _Worker:
    def __init__(self, values: list[dict[str, object] | None]) -> None:
        self.values = list(values)
        self.calls = 0

    def run_once(self):  # type: ignore[no-untyped-def]
        self.calls += 1
        return self.values.pop(0) if self.values else None


class _Runtime:
    def __init__(self, values: list[dict[str, object] | None]) -> None:
        self.worker = _Worker(values)

    def preflight_receipt(self) -> dict[str, object]:
        return {
            "schema_version": "aistock_monthly_release_worker_preflight_v1",
            "status": "PASS",
        }


def _loader(runtime: _Runtime):
    def load(*, project_root: Path):
        assert project_root == cli.PROJECT_ROOT
        return runtime

    return load


def test_preflight_does_not_run_an_operation(capsys: pytest.CaptureFixture[str]) -> None:
    runtime = _Runtime([{"operation_id": "forbidden"}])

    assert cli.main(["--preflight"], runtime_loader=_loader(runtime)) == 0

    assert json.loads(capsys.readouterr().out)["status"] == "PASS"
    assert runtime.worker.calls == 0


def test_supervised_owner_eof_stops_before_next_claim() -> None:
    stop = Event()
    cli._watch_supervisor(io.BytesIO(b""), stop)
    runtime = _Runtime([{"operation_id": "forbidden"}])
    assert cli._run_service(runtime, poll_seconds=0.1, stop_event=stop) == []
    assert runtime.worker.calls == 0


def test_supervised_mode_requires_parent_handshake_before_runtime(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(b"")))
    calls = []
    assert cli.main(["--serve", "--supervised"], runtime_loader=lambda **kw: calls.append(kw)) == 2
    assert calls == []
    assert "supervisor handshake" in json.loads(capsys.readouterr().err)["message"]


def test_supervisor_eof_fresh_process_stops_without_loading_database() -> None:
    code = (
        "from threading import Event; import sys; "
        "from scripts.monthly_unified_dataset_release_worker import _watch_supervisor; "
        "stop=Event(); _watch_supervisor(sys.stdin.buffer, stop); "
        "assert stop.is_set(); print('OWNER_EOF_PASS')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], input=b"", capture_output=True,
        cwd=cli.PROJECT_ROOT, timeout=20, check=True,
    )
    assert result.stdout.strip() == b"OWNER_EOF_PASS"


def test_forced_owner_exit_closes_real_pipe_in_fresh_process() -> None:
    child = (
        "import sys; from threading import Event; "
        "from scripts.monthly_unified_dataset_release_worker import _bind_supervisor; "
        "stop=Event(); _bind_supervisor(sys.stdin.buffer, stop); "
        "assert stop.wait(5); print('OWNER_CRASH_PASS', flush=True)"
    )
    parent = (
        "import os, subprocess, sys; "
        f"child=subprocess.Popen([sys.executable, '-c', {child!r}], stdin=subprocess.PIPE, close_fds=True); "
        "child.stdin.write(b'START\\n'); child.stdin.flush(); os._exit(0)"
    )
    result = subprocess.run(
        [sys.executable, "-c", parent], capture_output=True,
        cwd=cli.PROJECT_ROOT, timeout=20, check=True,
    )
    assert result.stdout.strip() == b"OWNER_CRASH_PASS"


def test_owner_loss_wakes_idle_worker_and_finishes_current_claim(monkeypatch) -> None:
    from threading import Thread
    from queue import Queue

    stop = Event()
    owner_input = Queue()
    started = Event()

    class Pipe:
        def read(self, _size):
            started.set()
            return owner_input.get(timeout=3)

    watcher = Thread(target=cli._watch_supervisor, args=(Pipe(), stop))
    watcher.start()
    assert started.wait(2)
    runtime = _Runtime([])

    def finish_current():
        owner_input.put(b"")
        assert stop.wait(2)
        return {"operation_id": "current", "status": "SOURCE_READY"}

    monkeypatch.setattr(runtime.worker, "run_once", finish_current)
    result = cli._run_service(runtime, poll_seconds=300, stop_event=stop)
    watcher.join(2)
    assert not watcher.is_alive()
    assert len(result) == 1


def test_broken_owner_pipe_revokes_claims() -> None:
    class BrokenPipe:
        def read(self, _size):
            raise OSError("owner exited")
    stop = Event()
    cli._watch_supervisor(BrokenPipe(), stop)
    assert stop.is_set()


def test_once_and_bounded_drain_process_only_durable_pending_operations(
    capsys: pytest.CaptureFixture[str],
) -> None:
    once = _Runtime([{"operation_id": "dmr_one", "status": "SOURCE_READY"}])
    assert cli.main(["--once"], runtime_loader=_loader(once)) == 0
    assert json.loads(capsys.readouterr().out)["processed_operation_count"] == 1

    drain = _Runtime(
        [
            {"operation_id": "dmr_a", "status": "SOURCE_READY"},
            {"operation_id": "dmr_b", "status": "READY_TO_ACTIVATE"},
            {"operation_id": "dmr_c", "status": "forbidden"},
        ]
    )
    assert (
        cli.main(
            ["--drain", "--max-operations", "2"],
            runtime_loader=_loader(drain),
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["processed_operation_count"] == 2
    assert payload["last_operation_id"] == "dmr_b"
    assert drain.worker.calls == 2


@pytest.mark.parametrize(
    "arguments",
    (
        ["--drain"],
        ["--once", "--max-operations", "2"],
        ["--once", "--poll-seconds", "nan"],
        ["--once", "--supervised"],
    ),
)
def test_invalid_bounds_fail_closed(
    arguments: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    runtime = _Runtime([])
    assert cli.main(arguments, runtime_loader=_loader(runtime)) == 2
    assert json.loads(capsys.readouterr().err)["status"] == "FAILED"
    assert runtime.worker.calls == 0


def test_runtime_boot_failure_is_structured_and_nonzero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail(**_kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("missing frozen authority")

    assert cli.main(["--once"], runtime_loader=fail) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["reason_code"] == "MONTHLY_RELEASE_WORKER_FAILED"
    assert "missing frozen authority" in payload["message"]


def test_runtime_boot_failure_preserves_typed_context(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from backend.services.dataset_release.monthly_runtime import (
        MonthlyRuntimeConfigurationError,
    )

    def fail(**_kwargs):  # type: ignore[no-untyped-def]
        raise MonthlyRuntimeConfigurationError(
            "monthly release environment is incomplete",
            context={"missing": ["AISTOCK_MONTHLY_RELEASE_STATE_ROOT"]},
        )

    assert cli.main(["--preflight"], runtime_loader=fail) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["reason_code"] == "MONTHLY_RELEASE_RUNTIME_UNAVAILABLE"
    assert payload["context"] == {
        "missing": ["AISTOCK_MONTHLY_RELEASE_STATE_ROOT"]
    }


def test_activation_verifier_is_wired_into_worker(monkeypatch, tmp_path: Path) -> None:
    from backend.services.dataset_release import monthly_worker_runtime as runtime_module

    settings = SimpleNamespace(
        authorization_root=tmp_path,
        active_profile=tmp_path / "active.json",
        worker_service=lambda registry: "service",
    )
    nodes = object()
    production = object()
    registry = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(runtime_module.MonthlyRuntimeSettings, "from_env", lambda: settings)
    monkeypatch.setattr(runtime_module.MonthlyNodeRuntimeSettings, "from_env", lambda: nodes)
    monkeypatch.setattr(
        runtime_module.MonthlyProductionSettings,
        "from_env",
        lambda **_kwargs: production,
    )
    monkeypatch.setattr(
        runtime_module,
        "build_monthly_production_registry",
        lambda **_kwargs: registry,
    )

    def worker(service, **kwargs):  # type: ignore[no-untyped-def]
        captured["service"] = service
        captured.update(kwargs)
        return "worker"

    monkeypatch.setattr(runtime_module, "MonthlyReleaseWorker", worker)

    value = runtime_module.build_monthly_worker_runtime(project_root=tmp_path)

    assert value.worker == "worker"
    assert captured["service"] == "service"
    assert captured["authorization_store"].root == tmp_path
    assert callable(captured["activation_verifier"])


def test_activation_verifier_requires_exact_running_node_identity(monkeypatch, tmp_path: Path) -> None:
    from backend.services.dataset_release import monthly_worker_runtime as runtime_module

    manifest = "a" * 64
    profile = SimpleNamespace(
        raw={
            "components": {"dataset_manifest_sha256": manifest},
            "node_bindings": {
                "wsl2-5080": {"candidate_root": "/data/wsl/release"},
                "rdagent-node1": {"candidate_root": "/data/node1/release"},
            },
        },
        profile_sha256="b" * 64,
    )
    monkeypatch.setattr(runtime_module, "load_qe_profile", lambda _path: profile)
    monkeypatch.setattr(runtime_module, "validate_controller_snapshot", lambda _profile: None)
    nodes = SimpleNamespace(
        dataset_identity_urls=lambda: {
            "wsl2-5080": "http://wsl/dataset-identity",
            "rdagent-node1": "http://node1/dataset-identity",
        }
    )

    class Response:
        def __init__(self, *, node_id: str, root: str) -> None:
            self.node_id = node_id
            self.root = root

        def raise_for_status(self) -> None:
            return None

        def json(self):  # type: ignore[no-untyped-def]
            return {
                "complete": True,
                "dataset": {
                    "dataset_manifest_sha256": manifest,
                    "resolved_node_id": self.node_id,
                    "resolved_data_root_uri": self.root,
                },
            }

    class Client:
        def __init__(self, **_kwargs) -> None:  # type: ignore[no-untyped-def]
            pass

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args) -> None:  # type: ignore[no-untyped-def]
            return None

        def get(self, _url: str, *, params):  # type: ignore[no-untyped-def]
            return Response(node_id=params["node_id"], root=params["data_root_uri"])

    monkeypatch.setattr(runtime_module.httpx, "Client", Client)

    receipt = runtime_module._activation_verifier(tmp_path / "active.json", nodes)(
        {"dataset_manifest_sha256": manifest}
    )

    assert receipt["status"] == "PASS"
    assert set(receipt["node_dataset_identity_readbacks"]) == {"wsl2-5080", "rdagent-node1"}


def test_activation_verifier_rejects_stale_running_node(monkeypatch, tmp_path: Path) -> None:
    from backend.services.dataset_release import monthly_worker_runtime as runtime_module

    manifest = "a" * 64
    profile = SimpleNamespace(
        raw={
            "components": {"dataset_manifest_sha256": manifest},
            "node_bindings": {"wsl2-5080": {"candidate_root": "/data/wsl/release"}},
        },
        profile_sha256="b" * 64,
    )
    monkeypatch.setattr(runtime_module, "load_qe_profile", lambda _path: profile)
    monkeypatch.setattr(runtime_module, "validate_controller_snapshot", lambda _profile: None)
    nodes = SimpleNamespace(
        dataset_identity_urls=lambda: {"wsl2-5080": "http://wsl/dataset-identity"}
    )

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self):  # type: ignore[no-untyped-def]
            return {
                "complete": True,
                "dataset": {
                    "dataset_manifest_sha256": "c" * 64,
                    "resolved_node_id": "wsl2-5080",
                    "resolved_data_root_uri": "/data/wsl/release",
                },
            }

    class Client:
        def __init__(self, **_kwargs) -> None:  # type: ignore[no-untyped-def]
            pass

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args) -> None:  # type: ignore[no-untyped-def]
            return None

        def get(self, _url: str, *, params):  # type: ignore[no-untyped-def]
            return Response()

    monkeypatch.setattr(runtime_module.httpx, "Client", Client)

    with pytest.raises(RuntimeError, match="running node dataset identity differs"):
        runtime_module._activation_verifier(tmp_path / "active.json", nodes)(
            {"dataset_manifest_sha256": manifest}
        )
