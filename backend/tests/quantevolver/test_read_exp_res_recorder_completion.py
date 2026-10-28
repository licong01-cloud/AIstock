"""The actual extraction entry must reject failed native recorder outputs."""
import ast
from pathlib import Path
import types

import pytest


SOURCE = Path(__file__).resolve().parents[2] / "services/quantevolver/templates/read_exp_res.py"


def _run_selected_recorder(recorder):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    guard = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_require_finished_recorder")
    branch = next(n for n in tree.body if isinstance(n, ast.If) and ast.unparse(n.test) == "latest_recorder is None")
    # Execute the production branch through its first write/export callback.
    write_index = next(i for i, n in enumerate(branch.orelse)
                       if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call)
                       and isinstance(n.value.func, ast.Name) and n.value.func.id == "_write_extracted_recorder_ref")
    writes = []
    module = ast.Module(body=[guard, *branch.orelse[:write_index + 1]], type_ignores=[])
    namespace = {"latest_recorder": recorder, "experiment_name": "workflow", "_binding": {},
                 "_write_extracted_recorder_ref": lambda *args: writes.append(args)}
    exec(compile(ast.fix_missing_locations(module), str(SOURCE), "exec"), namespace)
    return writes


@pytest.mark.parametrize("status", ["FAILED", "RUNNING", "SCHEDULED", "KILLED", None])
def test_failed_or_unfinished_recorder_cannot_be_exported(status):
    with pytest.raises(RuntimeError, match="QE_RESULT_RECORDER_NOT_FINISHED"):
        _run_selected_recorder(types.SimpleNamespace(info={"status": status}))


def test_finished_recorder_reaches_existing_extraction():
    assert len(_run_selected_recorder(types.SimpleNamespace(info={"status": "FINISHED"}))) == 1
