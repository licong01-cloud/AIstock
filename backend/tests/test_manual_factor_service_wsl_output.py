from __future__ import annotations

from pathlib import Path
from contextlib import contextmanager
import hashlib
from types import SimpleNamespace

import pytest

from backend.services.manual_factor_service import _build_wsl_copy_file_script
from backend.services import manual_factor_service as service


def test_wsl_result_copy_script_uses_windows_mount_not_unc() -> None:
    script = _build_wsl_copy_file_script(
        "/home/lc999/factor_workspace/_factor_demo/result.h5",
        Path("F:/Dev/AIstock/.codex_tmp/result.h5"),
    )

    assert "\\\\wsl" not in script.lower()
    assert "wsl.localhost" not in script.lower()
    assert "/mnt/f/Dev/AIstock/.codex_tmp/result.h5" in script
    assert "cp /home/lc999/factor_workspace/_factor_demo/result.h5" in script


@pytest.mark.parametrize("case", ["insert", "replay", "code_conflict", "source_conflict", "disabled"])
def test_candidate_registration_contract(monkeypatch, case):
    code = "value = 1\n"
    row = (42, "manual", code, "x", case != "disabled")
    if case == "code_conflict":
        row = (42, "manual", "value = 2\n", "x", True)
    if case == "source_conflict":
        row = (42, "rdagent_task_sync", code, "x", True)
    calls, terminal = [], []
    rows = [] if case == "insert" else [row]

    @contextmanager
    def connection(**kwargs):
        assert kwargs == {"autocommit": False, "manage_transaction": True}
        try:
            yield SimpleNamespace(cursor=cursor)
            terminal.append("commit")
        except ValueError:
            terminal.append("rollback")
            raise

    def execute(sql, params):
        calls.append(sql)
        if "INSERT INTO" in sql:
            rows.append(row)
            assert params[0:3] == ("candidate_roc", "manual", "research_candidate_v1")
            assert params[5:8] == ("x", code, "research only")

    @contextmanager
    def cursor():
        yield SimpleNamespace(execute=execute, fetchall=lambda: rows.copy(), fetchone=lambda: (42,))

    monkeypatch.setattr(service, "get_conn", connection)
    monkeypatch.setattr(Path, "write_text", lambda *a, **k: pytest.fail("filesystem write"))
    monkeypatch.setattr(Path, "mkdir", lambda *a, **k: pytest.fail("filesystem mkdir"))
    kwargs = dict(description="research only", expression="x")
    if "conflict" in case:
        with pytest.raises(ValueError, match="identity conflict"):
            service.ManualFactorService().register_candidate("candidate_roc", code, **kwargs)
        assert terminal == ["rollback"]
    else:
        result = service.ManualFactorService().register_candidate("candidate_roc", code, **kwargs)
        assert result["catalog_id"] == 42
        assert result["source_sha256"] == hashlib.sha256(code.encode()).hexdigest()
        assert result["status"] == ("registered" if case == "insert" else "replayed")
        assert result["is_available"] == (case != "disabled")
        assert not any(result[key] for key in ("official_metrics_written", "classification_written", "rating_written"))
        assert terminal == ["commit"]
    assert not any("UPDATE SET" in sql or "DELETE" in sql for sql in calls)
    assert sum("INSERT INTO" in sql for sql in calls) == (case == "insert")


def test_candidate_validation_and_cli_target(monkeypatch, tmp_path, capsys):
    from scripts import register_manual_factor as cli
    from scripts import factor_research

    monkeypatch.setattr(service, "get_conn", lambda **k: pytest.fail("unexpected DB access"))
    for name, code, error in [("bad-name", "x=1", ValueError), ("valid_name", "", ValueError),
                              ("valid_name", "x=", SyntaxError)]:
        with pytest.raises(error):
            service.ManualFactorService().register_candidate(name, code)
    events = []
    monkeypatch.setattr(factor_research, "configure", lambda path, target: events.append(target) or {"target": target})
    monkeypatch.setattr(service.ManualFactorService, "register_candidate",
                        lambda self, name, code, **k: events.append(code) or {"catalog_id": 42})
    code_file = tmp_path / "factor.py"
    code_file.write_bytes(b"x = 1\r\n")
    args = SimpleNamespace(direct_db=False, env_file="explicit.env", target="dev", name="valid_name",
                           code_file=code_file, description=None, expression=None)
    cli.cmd_register_candidate(args)
    assert events == ["dev", "x = 1\r\n"]
    assert '"catalog_id": 42' in capsys.readouterr().out
    args.direct_db = True
    with pytest.raises(ValueError, match="legacy"):
        cli.cmd_register_candidate(args)
