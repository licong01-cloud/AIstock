from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import noxfile  # noqa: E402


def _reset_nox_env_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(noxfile, "_VALIDATION_ENV_LOADED", False)


def test_env_prefers_self_hosted_source_dotenv_without_copying(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _reset_nox_env_loader(monkeypatch)
    source = tmp_path / "source"
    source.mkdir()
    (source / ".env").write_text(
        "TDX_DB_HOST=127.0.0.1\nTDX_DB_PASSWORD=secret-for-test\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("AISTOCK_SELF_HOSTED_SOURCE", str(source))
    monkeypatch.delenv("AISTOCK_ENV_FILE", raising=False)
    monkeypatch.delenv("TDX_DB_PASSWORD", raising=False)

    env = noxfile._env()

    assert env["AISTOCK_ENV_FILE"] == str(source / ".env")
    assert env["TDX_DB_PASSWORD"] == "secret-for-test"
    assert os.environ["TDX_DB_PASSWORD"] == "secret-for-test"


def test_env_uses_canonical_root_dotenv_for_worktree(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _reset_nox_env_loader(monkeypatch)
    canonical = tmp_path / "canonical"
    worktree = tmp_path / "worktree"
    canonical.mkdir()
    worktree.mkdir()
    (canonical / ".env").write_text(
        "TDX_DB_HOST=127.0.0.1\nTDX_DB_PASSWORD=canonical-secret-for-test\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("AISTOCK_ENV_FILE", raising=False)
    monkeypatch.delenv("AISTOCK_SELF_HOSTED_SOURCE", raising=False)
    monkeypatch.delenv("TDX_DB_PASSWORD", raising=False)
    monkeypatch.setattr(noxfile, "CANONICAL_ROOT", canonical)
    monkeypatch.setattr(noxfile, "ROOT", worktree)

    env = noxfile._env()

    assert env["AISTOCK_ENV_FILE"] == str(canonical / ".env")
    assert env["TDX_DB_PASSWORD"] == "canonical-secret-for-test"


def test_managed_backend_refuses_production_port() -> None:
    class DummySession:
        def error(self, message: str) -> None:
            raise RuntimeError(message)

    with pytest.raises(RuntimeError, match="production port 8001"):
        with noxfile._managed_validation_backend(DummySession(), "8001"):
            pass


def test_frontend_node_modules_install_runs_only_when_playwright_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    frontend = tmp_path / "frontend"
    frontend.mkdir()
    caller_cwd = tmp_path / "caller"
    caller_cwd.mkdir()
    monkeypatch.chdir(caller_cwd)
    monkeypatch.setattr(noxfile, "ROOT", tmp_path)
    calls: list[tuple[tuple[str, ...], Path, dict[str, object]]] = []

    class DummySession:
        def log(self, message: str) -> None:
            calls.append((("log", message), Path.cwd(), {}))

        def run(self, *args: str, **kwargs: object) -> None:
            calls.append((tuple(args), Path.cwd(), dict(kwargs)))

    noxfile._ensure_frontend_node_modules(DummySession())
    assert (("npm", "ci"), frontend, {"external": True}) in calls
    assert Path.cwd() == caller_cwd

    calls.clear()
    bin_dir = frontend / "node_modules" / ".bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / ("playwright.cmd" if os.name == "nt" else "playwright")).write_text("", encoding="utf-8")

    noxfile._ensure_frontend_node_modules(DummySession())
    assert calls == []


def test_terminate_process_tree_uses_taskkill_on_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, ...]] = []

    class DummyProc:
        pid = 12345

        def poll(self) -> None:
            return None

        def wait(self, timeout: int) -> int:
            return 0

    monkeypatch.setattr(noxfile.os, "name", "nt")
    monkeypatch.setattr(
        noxfile.subprocess,
        "run",
        lambda args, **_kwargs: calls.append(tuple(args)),
    )

    noxfile._terminate_process_tree(DummyProc())  # type: ignore[arg-type]

    assert calls == [("taskkill", "/PID", "12345", "/T", "/F")]


def test_kill_windows_listeners_on_port_only_targets_listening(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, ...]] = []

    class Result:
        stdout = (
            "  TCP    0.0.0.0:3012   0.0.0.0:0   LISTENING   111\n"
            "  TCP    127.0.0.1:3012 127.0.0.1:50000 TIME_WAIT 0\n"
        )
        returncode = 0

    def fake_run(args: list[str], **_kwargs: object) -> Result:
        calls.append(tuple(args))
        return Result()

    monkeypatch.setattr(noxfile.os, "name", "nt")
    monkeypatch.setattr(noxfile.subprocess, "run", fake_run)

    noxfile._kill_windows_listeners_on_port("3012")

    assert calls[0] == ("netstat", "-ano")
    assert ("taskkill", "/PID", "111", "/T", "/F") in calls


def test_kill_windows_listeners_falls_back_when_taskkill_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, ...]] = []

    class NetstatResult:
        stdout = "  TCP    127.0.0.1:3012   0.0.0.0:0   LISTENING   222\n"
        returncode = 0

    class FailedKillResult:
        stdout = ""
        returncode = 1

    def fake_run(args: list[str], **_kwargs: object) -> NetstatResult | FailedKillResult:
        calls.append(tuple(args))
        return NetstatResult() if args[0] == "netstat" else FailedKillResult()

    monkeypatch.setattr(noxfile.os, "name", "nt")
    monkeypatch.setattr(noxfile.subprocess, "run", fake_run)

    noxfile._kill_windows_listeners_on_port("3012")

    assert ("taskkill", "/PID", "222", "/T", "/F") in calls
    assert any(call[:4] == ("powershell", "-NoProfile", "-Command", "Stop-Process -Id 222 -Force -ErrorAction SilentlyContinue") for call in calls)


def test_reclaim_validation_ports_enabled_in_github_actions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AISTOCK_RECLAIM_VALIDATION_PORTS", raising=False)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert noxfile._reclaim_validation_ports() is True
