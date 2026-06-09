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
