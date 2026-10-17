from __future__ import annotations

import runpy
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("location", ["canonical_root", "indirect_ancestor"])
def test_catalog_freeze_cli_rejects_unsafe_output_before_database(tmp_path, monkeypatch, location):
    import psycopg2

    from backend.services.hmm_risk.formal_state_executor import FormalStateError

    root = Path(__file__).resolve().parents[3]
    script = root / "scripts/hmm_risk/freeze_formal_industry_authority.py"
    namespace = runpy.run_path(str(script))
    authority = tmp_path / "authority"
    authority.mkdir()
    output = (root / "backend/services/hmm_risk/formal_state_executor.py").resolve().parents[3]
    if (root / ".git").is_file():
        gitdir = Path((root / ".git").read_text(encoding="utf-8").strip().removeprefix("gitdir: "))
        output = (gitdir / (gitdir / "commondir").read_text(encoding="utf-8").strip()).resolve().parent
    output = output / "tmp/forbidden-catalog.json"
    if location == "indirect_ancestor":
        ancestor = tmp_path / "indirect"
        output = ancestor / "nested/catalog.json"
        real_symlink = Path.is_symlink
        monkeypatch.setattr(Path, "is_symlink", lambda path: path == ancestor or real_symlink(path))
    monkeypatch.setattr(psycopg2, "connect", lambda **_: pytest.fail("DB accessed before path validation"))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script),
            "--authority-root",
            str(authority),
            "--env-file",
            str(tmp_path / "absent.env"),
            "--output",
            str(output),
        ],
    )
    with pytest.raises(FormalStateError, match="output"):
        namespace["main"]()
    assert not output.exists()
