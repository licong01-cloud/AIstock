"""Read-only inventory and deterministic de-duplication of historical RD-Agent factors."""
from __future__ import annotations

import ast
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import threading
import time
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from datetime import date
from pathlib import Path
from typing import Any

from .models import ResearchError


TASK_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
POSIX_PATH_RE = re.compile(r"^/[A-Za-z0-9_./-]+$")
SSH_HOST_RE = re.compile(r"^[A-Za-z0-9_.@-]+$")
EVALUATION_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{2,80}$")
REFERENCE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{2,80}$")
LEGACY_DATA_FILES = frozenset({
    "daily_pv.h5",
    "daily_basic.h5",
    "moneyflow.h5",
    "bak_basic.h5",
    "cyq_perf.h5",
    "sector_data.h5",
    "margin_detail.h5",
    "static_factors.parquet",
    "static_factors_schema.csv",
})
_ALLOWED_IMPORT_ROOTS = frozenset({"numpy", "os", "pandas", "scipy"})
_FORBIDDEN_CALLS = frozenset({
    "builtins.compile",
    "builtins.eval",
    "builtins.exec",
    "builtins.open",
    "builtins.__import__",
    "os.chmod",
    "os.chown",
    "os.link",
    "os.makedirs",
    "os.mkdir",
    "os.popen",
    "os.remove",
    "os.rename",
    "os.replace",
    "os.rmdir",
    "os.symlink",
    "os.system",
    "os.unlink",
    "pathlib.Path.open",
    "pathlib.Path.touch",
    "pathlib.Path.unlink",
    "pathlib.Path.write_bytes",
    "pathlib.Path.write_text",
    "shutil.copy",
    "shutil.copyfile",
    "shutil.move",
    "shutil.rmtree",
    "subprocess.call",
    "subprocess.check_call",
    "subprocess.check_output",
    "subprocess.Popen",
    "subprocess.run",
})
_READ_METHODS = frozenset({"read_csv", "read_hdf", "read_parquet"})


LEGACY_ADAPTER_TEMPLATE = '''\
from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

import pandas as pd

SOURCE = __SOURCE__
ENTRY = __ENTRY__
EVALUATION_NAME = __EVALUATION_NAME__
DATA_FILES = __DATA_FILES__


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--instruments", required=True)
    args = parser.parse_args()
    instruments = json.loads(args.instruments)
    if (
        not isinstance(instruments, list)
        or not instruments
        or any(not isinstance(item, str) or not re.fullmatch(r"\\d{6}\\.(SH|SZ|BJ)", item) for item in instruments)
    ):
        raise ValueError("instruments must be a non-empty JSON list")
    instruments = sorted(set(instruments))
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError(args.output)

    linked: list[Path] = []
    generated = Path("result.h5")
    if generated.exists() or generated.is_symlink():
        raise FileExistsError(generated)
    try:
        for name in DATA_FILES:
            source = args.data_dir / name
            target = Path(name)
            if source.is_symlink() or not source.is_file():
                raise FileNotFoundError(source)
            if target.exists() or target.is_symlink():
                raise FileExistsError(target)
            os.link(source, target)
            linked.append(target)
        original_read_hdf = pd.read_hdf
        original_read_parquet = pd.read_parquet

        def bounded_read_hdf(path, *positional, **keyword):
            name = Path(path).name
            if name in DATA_FILES:
                path = args.data_dir / name
                scope = [
                    f"datetime >= {args.start_date!r}",
                    f"datetime <= {args.end_date!r}",
                    f"instrument={instruments!r}",
                ]
                existing = keyword.get("where")
                if existing is None:
                    keyword["where"] = scope
                elif isinstance(existing, list):
                    keyword["where"] = [*existing, *scope]
                else:
                    keyword["where"] = [existing, *scope]
            return original_read_hdf(path, *positional, **keyword)

        def bounded_read_parquet(path, *positional, **keyword):
            name = Path(path).name
            if name in DATA_FILES:
                path = args.data_dir / name
                scope = [
                    ("datetime", ">=", pd.Timestamp(args.start_date)),
                    ("datetime", "<=", pd.Timestamp(args.end_date)),
                    ("instrument", "in", instruments),
                ]
                existing = keyword.get("filters")
                if existing is None:
                    keyword["filters"] = scope
                elif isinstance(existing, list) and (not existing or isinstance(existing[0], tuple)):
                    keyword["filters"] = [*existing, *scope]
                else:
                    raise RuntimeError("legacy parquet disjunctive filters are unsupported")
            return original_read_parquet(path, *positional, **keyword)

        pd.read_hdf = bounded_read_hdf
        pd.read_parquet = bounded_read_parquet
        namespace = {"__name__": "rdagent_salvage_candidate", "__file__": str(Path(__file__))}
        exec(compile(SOURCE, "<rdagent-salvage-candidate>", "exec"), namespace)
        entry = namespace.get(ENTRY)
        if not callable(entry):
            raise RuntimeError("declared calculate entry is unavailable")
        entry()
        pd.read_hdf = original_read_hdf
        pd.read_parquet = original_read_parquet
        if generated.is_symlink() or not generated.is_file():
            raise RuntimeError("legacy candidate did not create result.h5")
        frame = pd.read_hdf(generated, key="data")
        if not isinstance(frame, pd.DataFrame) or frame.shape[1] != 1:
            raise RuntimeError("legacy candidate must return exactly one factor column")
        if not isinstance(frame.index, pd.MultiIndex) or set(frame.index.names) != {"datetime", "instrument"}:
            raise RuntimeError("legacy candidate index must contain datetime/instrument")
        if frame.index.names != ["datetime", "instrument"]:
            frame = frame.swaplevel("datetime", "instrument")
        dates = pd.to_datetime(frame.index.get_level_values("datetime"), errors="raise")
        symbols = frame.index.get_level_values("instrument")
        selected = (
            (dates >= pd.Timestamp(args.start_date))
            & (dates <= pd.Timestamp(args.end_date))
            & symbols.isin(instruments)
        )
        frame = frame.loc[selected].rename(columns={frame.columns[0]: EVALUATION_NAME}).sort_index()
        if frame.empty:
            raise RuntimeError("legacy candidate has no rows in the requested scope")
        frame.to_hdf(args.output, key="data", mode="w")
    finally:
        if generated.exists() and not generated.is_symlink():
            generated.unlink()
        for target in reversed(linked):
            if target.exists() and not target.is_symlink():
                target.unlink()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
'''


NODE_EXTRACTOR_SOURCE = r'''
from __future__ import annotations
import json
import pickle
import re
import sys
from pathlib import Path, PurePath

class CompatUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        if module == "pathlib" and name in {"PosixPath", "WindowsPath"}:
            return Path
        if module == "pathlib" and name in {"PurePosixPath", "PureWindowsPath"}:
            return PurePath
        return super().find_class(module, name)

def emit(payload):
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), flush=True)

def load(path):
    with path.open("rb") as handle:
        return CompatUnpickler(handle).load()

root = Path(sys.argv[1])
node_id = sys.argv[2]
if not root.is_dir():
    emit({"record_type":"unavailable","source_node":node_id,"reason":"source_root_missing","source_path":str(root)})
    raise SystemExit(2)

for loop_dir in sorted(root.glob("*/Loop_*"), key=lambda item: (item.parent.name, item.name)):
    task_dir = loop_dir.parent
    if not task_dir.is_dir() or not re.fullmatch(r"[A-Za-z0-9_.-]+", task_dir.name):
        continue
    if not loop_dir.is_dir() or not re.fullmatch(r"Loop_\d+", loop_dir.name):
        continue
    loop_id = int(loop_dir.name.split("_", 1)[1])
    pickle_root = loop_dir / "coding" / "coder result"
    pickles = sorted(pickle_root.glob("*/*.pkl")) if pickle_root.is_dir() else []
    if not pickles:
        continue
    pickle_path = pickles[-1]
    try:
        obj = load(pickle_path)
    except Exception as exc:
        emit({"record_type":"unavailable","source_node":node_id,"task_id":task_dir.name,
              "loop_id":loop_id,"reason":"node_extractor_failed","exception_type":type(exc).__name__,
              "source_path":str(pickle_path.relative_to(root))})
        continue
    workspaces = obj if isinstance(obj, list) else (getattr(obj, "sub_workspace_list", None) or [])
    for workspace_index, workspace in enumerate(workspaces):
        if workspace is None:
            continue
        file_dict = getattr(workspace, "file_dict", {}) or {}
        target = getattr(workspace, "target_task", None)
        target_name = None
        if target is not None:
            target_name = getattr(target, "factor_name", None) or getattr(target, "name", None)
        for file_key, code_text in sorted(file_dict.items(), key=lambda item: str(item[0])):
            if not isinstance(file_key, str) or not file_key.endswith(".py") or not isinstance(code_text, str):
                continue
            # Model evolution workspaces share the same coder-result shape. They are not
            # factor candidates and therefore do not belong in either factor denominator.
            if Path(file_key).name == "model.py":
                continue
            emit({"record_type":"factor_source","source_node":node_id,"source_kind":"rdagent_pickle",
                  "task_id":task_dir.name,"loop_id":loop_id,
                  "workspace_key":f"workspace_{workspace_index}/{file_key}",
                  "factor_name":target_name or Path(file_key).stem,"code_text":code_text,
                  "source_path":str(pickle_path.relative_to(root))})
'''


def normalize_code(code_text: str) -> str:
    """Normalize representation only; do not rewrite Python semantics."""
    lines = code_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    return "\n".join(line.rstrip() for line in lines).strip() + "\n"


def code_sha256(code_text: str) -> str:
    return hashlib.sha256(normalize_code(code_text).encode("utf-8")).hexdigest()


def inspect_factor_code(code_text: str) -> dict[str, Any]:
    normalized = normalize_code(code_text)
    try:
        tree = ast.parse(normalized)
    except (SyntaxError, ValueError) as exc:
        return {
            "valid": False,
            "reason": "python_ast_invalid",
            "exception_type": type(exc).__name__,
            "code_sha256": code_sha256(normalized),
        }
    entries = sorted(
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("calculate_")
    )
    canonical = ast.dump(tree, annotate_fields=True, include_attributes=False)
    return {
        "valid": bool(entries),
        "reason": None if entries else "factor_entry_missing",
        "entries": entries,
        "code_sha256": code_sha256(normalized),
        "ast_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    }


def _normalize_name(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").casefold())


def _source_identity(record: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        record.get("source_node"), record.get("task_id"), record.get("loop_id"),
        record.get("workspace_key"), record.get("source_path"),
    )


def prepare_source_record(raw: Mapping[str, Any]) -> dict[str, Any]:
    required = ("source_node", "source_kind", "task_id", "workspace_key", "factor_name", "code_text", "source_path")
    if any(not isinstance(raw.get(key), str) or not raw.get(key) for key in required):
        raise ResearchError("source_record_invalid", "Factor source record is missing a required string field")
    if not TASK_ID_RE.fullmatch(str(raw["task_id"])):
        raise ResearchError("source_record_invalid", "Task identity contains unsupported characters")
    inspected = inspect_factor_code(str(raw["code_text"]))
    result = dict(raw)
    result.update(inspected)
    result["factor_name_normalized"] = _normalize_name(str(raw["factor_name"]))
    result["code_text"] = normalize_code(str(raw["code_text"]))
    return result


def iter_central_export(root: Path, node_id: str = "central_export") -> Iterator[dict[str, Any]]:
    if not root.is_dir():
        yield {"record_type": "unavailable", "source_node": node_id,
               "reason": "source_root_missing", "source_path": str(root)}
        return
    for task_dir in sorted(root.iterdir(), key=lambda item: item.name):
        if not task_dir.is_dir() or not TASK_ID_RE.fullmatch(task_dir.name):
            continue
        factor_order_path = task_dir / "factor_order.json"
        dynamic: set[str] = set()
        if factor_order_path.is_file():
            try:
                payload = json.loads(factor_order_path.read_text(encoding="utf-8-sig"))
                dynamic = {str(name) for name in payload.get("dynamic_factors", [])}
            except (OSError, ValueError, TypeError):
                dynamic = set()
        factors_dir = task_dir / "factors"
        if not factors_dir.is_dir():
            continue
        for path in sorted(factors_dir.glob("*.py"), key=lambda item: item.name):
            try:
                code_text = path.read_text(encoding="utf-8-sig")
            except (OSError, UnicodeError) as exc:
                yield {"record_type": "unavailable", "source_node": node_id, "task_id": task_dir.name,
                       "reason": "source_record_invalid", "exception_type": type(exc).__name__,
                       "source_path": str(path.relative_to(root))}
                continue
            yield {
                "record_type": "factor_source", "source_node": node_id,
                "source_kind": "central_export", "task_id": task_dir.name, "loop_id": None,
                "workspace_key": f"factors/{path.name}", "factor_name": path.stem,
                "code_text": code_text, "source_path": str(path.relative_to(root)),
                "exported_dynamic_factor": path.stem in dynamic,
            }


def _validate_posix_path(value: str, field: str) -> str:
    if not POSIX_PATH_RE.fullmatch(value) or ".." in Path(value).parts:
        raise ResearchError("source_config_invalid", f"{field} must be an absolute safe POSIX path")
    return value


def _node_command(source: Mapping[str, Any]) -> list[str]:
    kind = source.get("kind")
    node_id = str(source.get("node_id") or "")
    root = _validate_posix_path(str(source.get("root") or ""), "root")
    python_path = _validate_posix_path(str(source.get("python") or ""), "python")
    if not TASK_ID_RE.fullmatch(node_id):
        raise ResearchError("source_config_invalid", "node_id contains unsupported characters")
    if kind == "wsl_pickle":
        distro = str(source.get("distro") or "")
        if not TASK_ID_RE.fullmatch(distro):
            raise ResearchError("source_config_invalid", "distro contains unsupported characters")
        return ["wsl.exe", "-d", distro, "--", python_path, "-", root, node_id]
    if kind == "ssh_pickle":
        host = str(source.get("host") or "")
        if host.startswith("-") or host.count("@") > 1 or not SSH_HOST_RE.fullmatch(host):
            raise ResearchError("source_config_invalid", "host contains unsupported characters")
        command = ["ssh", "-F", "NUL", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", host,
                   python_path, "-", root, node_id]
        return command
    raise ResearchError("source_config_invalid", "Unsupported node source kind")


def iter_node_pickle_source(source: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
    node_id = str(source.get("node_id") or "unknown")
    timeout_seconds = source.get("timeout_seconds", 1800)
    if not isinstance(timeout_seconds, int) or isinstance(timeout_seconds, bool) or not 1 <= timeout_seconds <= 86400:
        raise ResearchError("source_config_invalid", "timeout_seconds must be an integer from 1 to 86400")
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stderr_file:
        try:
            command = _node_command(source)
            process = subprocess.Popen(  # noqa: S603 - fixed executable/validated atomized arguments
                command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr_file,
                text=True, encoding="utf-8", errors="replace",
            )
            # A timer owns subprocess timeout/cancellation while stdout remains line-streamed.
        except (OSError, ResearchError) as exc:
            if isinstance(exc, ResearchError):
                raise
            yield {"record_type": "unavailable", "source_node": node_id,
                   "reason": "source_node_unreachable", "exception_type": type(exc).__name__}
            return
        assert process.stdin is not None and process.stdout is not None
        timed_out = threading.Event()

        def kill_on_timeout() -> None:
            timed_out.set()
            process.kill()

        timer = threading.Timer(timeout_seconds, kill_on_timeout)
        timer.daemon = True
        timer.start()
        try:
            process.stdin.write(NODE_EXTRACTOR_SOURCE)
            process.stdin.close()
        except BrokenPipeError:
            pass
        invalid_lines = 0
        emitted_unavailable = 0
        try:
            for line in process.stdout:
                try:
                    payload = json.loads(line)
                except ValueError:
                    invalid_lines += 1
                    continue
                if isinstance(payload, dict):
                    emitted_unavailable += int(payload.get("record_type") == "unavailable")
                    yield payload
                else:
                    invalid_lines += 1
            return_code = process.wait()
        finally:
            timer.cancel()
        stderr_file.seek(0)
        stderr = stderr_file.read()
    if timed_out.is_set():
        yield {
            "record_type": "unavailable", "source_node": node_id,
            "reason": "node_extractor_timeout", "timeout_seconds": timeout_seconds,
        }
    elif invalid_lines or (return_code and not emitted_unavailable):
        yield {
            "record_type": "unavailable", "source_node": node_id,
            "reason": "node_extractor_failed", "return_code": return_code,
            "invalid_output_lines": invalid_lines,
            "stderr_tail": stderr[-500:] if stderr else "",
        }


def load_catalog_snapshot(path: Path | None) -> tuple[list[dict[str, Any]], bool]:
    if path is None:
        return [], False
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, UnicodeError) as exc:
        raise ResearchError("catalog_snapshot_unavailable", "Catalog snapshot is unreadable") from exc
    rows = payload.get("factors") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise ResearchError("catalog_snapshot_unavailable", "Catalog snapshot must contain a factor list")
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("factor_name"), str):
            raise ResearchError("catalog_snapshot_unavailable", "Catalog snapshot row is invalid")
        item = dict(row)
        code = item.get("code_text")
        item["factor_name_normalized"] = _normalize_name(item["factor_name"])
        if isinstance(code, str) and code.strip():
            item.update(inspect_factor_code(code))
        normalized.append(item)
    return normalized, True


def group_candidates(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ast: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_ast[str(record["ast_sha256"])].append(record)
    groups: list[dict[str, Any]] = []
    for ast_hash, members in sorted(by_ast.items()):
        ordered = sorted(members, key=_source_identity)
        names = sorted({str(item["factor_name"]) for item in ordered}, key=lambda value: (value.casefold(), value))
        groups.append({
            "candidate_id": f"ast:{ast_hash}", "ast_sha256": ast_hash,
            "code_sha256s": sorted({str(item["code_sha256"]) for item in ordered}),
            "factor_names": names, "representative_name": names[0],
            "member_count": len(ordered),
            "provenance": [{key: item.get(key) for key in (
                "source_node", "source_kind", "task_id", "loop_id", "workspace_key", "source_path",
                "factor_name", "code_sha256", "exported_dynamic_factor",
            )} for item in ordered],
            "code_text": ordered[0]["code_text"],
        })
    name_to_groups: dict[str, list[str]] = defaultdict(list)
    for group in groups:
        for name in group["factor_names"]:
            name_to_groups[_normalize_name(name)].append(group["candidate_id"])
    for group in groups:
        conflicts = sorted({candidate for name in group["factor_names"]
                            for candidate in name_to_groups[_normalize_name(name)]
                            if candidate != group["candidate_id"]})
        group["source_name_conflicts"] = conflicts
    return groups


def classify_catalog(groups: Iterable[dict[str, Any]], catalog: list[dict[str, Any]], available: bool) -> None:
    by_ast: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in catalog:
        if row.get("ast_sha256"):
            by_ast[str(row["ast_sha256"])].append(row)
        if row.get("code_sha256"):
            by_code[str(row["code_sha256"])].append(row)
        by_name[str(row["factor_name_normalized"])].append(row)
    for group in groups:
        if not available:
            group["catalog_status"] = "catalog_unavailable"
            group["catalog_matches"] = []
            continue
        matches: list[dict[str, Any]] = []
        matches.extend(by_ast.get(group["ast_sha256"], []))
        for digest in group["code_sha256s"]:
            matches.extend(by_code.get(digest, []))
        matches = list({str(row.get("id", row["factor_name"])): row for row in matches}.values())
        if matches:
            group["catalog_status"] = "catalog_exact_match"
        else:
            name_matches = [row for name in group["factor_names"] for row in by_name.get(_normalize_name(name), [])]
            matches = list({str(row.get("id", row["factor_name"])): row for row in name_matches}.values())
            group["catalog_status"] = "catalog_name_conflict" if matches else "catalog_absent"
        matches.sort(key=lambda row: (str(row.get("factor_name", "")).casefold(),
                                      str(row.get("factor_name", "")), str(row.get("id", ""))))
        group["catalog_matches"] = [
            {"id": row.get("id"), "factor_name": row.get("factor_name"), "source": row.get("source")}
            for row in matches
        ]


def _literal_path(node: ast.AST | None, constants: Mapping[str, str]) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.replace("\\", "/")
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    return None


def _call_parts(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.Name):
        return (node.id,)
    if isinstance(node, ast.Attribute):
        return (*_call_parts(node.value), node.attr)
    return ()


def audit_legacy_factor_code(code_text: str) -> dict[str, Any]:
    """Classify whether a legacy zero-argument factor is safe to adapt, without executing it."""
    inspected = inspect_factor_code(code_text)
    if not inspected["valid"]:
        return {"compatible": False, "reasons": [inspected["reason"]], "data_files": [], "entry": None}
    tree = ast.parse(normalize_code(code_text))
    reasons: set[str] = set()
    imports: dict[str, str] = {}
    constants: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".", 1)[0]
                if root not in _ALLOWED_IMPORT_ROOTS:
                    reasons.add("unsupported_import")
                imports[alias.asname or root] = alias.name
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or "").split(".", 1)[0]
            if root not in _ALLOWED_IMPORT_ROOTS:
                reasons.add("unsupported_import")
            for alias in node.names:
                imports[alias.asname or alias.name] = f"{node.module}.{alias.name}"
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = node.value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    if isinstance(target, ast.Name):
                        constants[target.id] = value.value.replace("\\", "/")

    entries = [
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("calculate_")
    ]
    if len(entries) != 1:
        reasons.add("calculate_entry_ambiguous")
    elif isinstance(entries[0], ast.AsyncFunctionDef) or any((
        entries[0].args.args,
        entries[0].args.posonlyargs,
        entries[0].args.kwonlyargs,
        entries[0].args.vararg,
        entries[0].args.kwarg,
    )):
        reasons.add("calculate_entry_not_legacy_zero_arg")

    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if isinstance(node, ast.If):
            # Historical scripts may only invoke the calculate function in the __main__ guard.
            calls = [item.value for item in node.body if isinstance(item, ast.Expr) and isinstance(item.value, ast.Call)]
            is_main_guard = (
                isinstance(node.test, ast.Compare)
                and isinstance(node.test.left, ast.Name)
                and node.test.left.id == "__name__"
                and len(node.test.ops) == 1
                and isinstance(node.test.ops[0], ast.Eq)
                and len(node.test.comparators) == 1
                and isinstance(node.test.comparators[0], ast.Constant)
                and node.test.comparators[0].value == "__main__"
            )
            called_name = _call_parts(calls[0].func)[-1] if len(calls) == 1 and _call_parts(calls[0].func) else None
            expected_name = entries[0].name if len(entries) == 1 else None
            if (not is_main_guard or len(node.body) != 1 or len(calls) != 1 or node.orelse
                    or called_name != expected_name):
                reasons.add("top_level_execution_unsupported")
            continue
        reasons.add("top_level_execution_unsupported")

    data_files: set[str] = set()
    output_calls = 0
    terminal_forbidden = {
        "chmod", "chown", "link", "mkdir", "open", "popen", "remove", "rename", "replace",
        "rmdir", "symlink", "system", "touch", "unlink", "write_bytes", "write_text",
    }
    # replace/rename are valid pandas methods, so only reject them when rooted in an imported OS/path module.
    pandas_safe_terminals = {"replace", "rename"}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        parts = _call_parts(node.func)
        if not parts:
            continue
        root = imports.get(parts[0], parts[0])
        qualified = ".".join((root, *parts[1:]))
        if qualified in _FORBIDDEN_CALLS or parts[-1] in {"__import__", "compile", "eval", "exec"} or (
            parts[-1] in terminal_forbidden
            and parts[-1] not in pandas_safe_terminals
            and root not in {"numpy", "pandas", "np", "pd"}
        ):
            reasons.add("side_effect_or_dynamic_execution")
        if parts[-1] in _READ_METHODS:
            path = _literal_path(node.args[0] if node.args else None, constants)
            if path not in LEGACY_DATA_FILES:
                reasons.add("data_dependency_unsupported")
            else:
                data_files.add(path)
        if parts[-1] in {"to_csv", "to_hdf", "to_parquet", "to_pickle"}:
            path = _literal_path(node.args[0] if node.args else None, constants)
            if parts[-1] != "to_hdf" or path != "result.h5":
                reasons.add("output_side_effect_unsupported")
            else:
                output_calls += 1
    if output_calls != 1:
        reasons.add("result_output_contract_invalid")
    if not data_files:
        reasons.add("data_dependency_missing")
    return {
        "compatible": not reasons,
        "reasons": sorted(reasons),
        "data_files": sorted(data_files),
        "entry": entries[0].name if len(entries) == 1 else None,
        "code_sha256": inspected["code_sha256"],
        "ast_sha256": inspected.get("ast_sha256"),
    }


def evaluation_name(representative_name: str, ast_sha256: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "_", representative_name.casefold()).strip("_")
    if not base or not base[0].isalpha():
        base = f"factor_{base}".rstrip("_")
    suffix = ast_sha256[:10]
    name = f"rdg_{base[:64]}_{suffix}"
    if not EVALUATION_NAME_RE.fullmatch(name):
        raise ResearchError("evaluation_name_invalid", "Could not derive a stable evaluation name")
    return name


def render_legacy_adapter(*, code_text: str, entry: str, name: str, data_files: list[str]) -> str:
    if not EVALUATION_NAME_RE.fullmatch(name):
        raise ResearchError("evaluation_name_invalid", "Evaluation name is invalid")
    return (
        LEGACY_ADAPTER_TEMPLATE
        .replace("__SOURCE__", repr(normalize_code(code_text)))
        .replace("__ENTRY__", repr(entry))
        .replace("__EVALUATION_NAME__", repr(name))
        .replace("__DATA_FILES__", repr(tuple(data_files)))
    )


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if path.is_symlink() or not path.is_file():
        raise ResearchError("inventory_missing", f"Expected regular inventory file: {path.name}")
    rows: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError("row is not an object")
                rows.append(value)
    except (OSError, UnicodeError, ValueError) as exc:
        raise ResearchError("inventory_invalid", f"Inventory file is invalid: {path.name}") from exc
    return rows


def prepare_salvage_evaluation(inventory_root: Path, artifact_root: Path) -> dict[str, Any]:
    """Materialize audited legacy adapters for later research evaluation; do not execute them."""
    inventory_root = _assert_repo_external(inventory_root)
    artifact_root = _assert_repo_external(artifact_root)
    if artifact_root.exists() and (not artifact_root.is_dir() or any(artifact_root.iterdir())):
        raise ResearchError("artifact_root_not_empty", "Artifact root must be absent or an empty directory")
    groups = {row.get("candidate_id"): row for row in _load_jsonl(inventory_root / "candidate_groups.jsonl")}
    review = _load_jsonl(inventory_root / "review_candidates.jsonl")
    artifact_root.mkdir(parents=True, exist_ok=True)
    scripts_root = artifact_root / "scripts"
    scripts_root.mkdir()
    prepared: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []
    names: set[str] = set()
    for compact in sorted(review, key=lambda row: str(row.get("candidate_id"))):
        candidate_id = compact.get("candidate_id")
        group = groups.get(candidate_id)
        if not isinstance(group, dict) or not isinstance(group.get("code_text"), str):
            unavailable.append({"candidate_id": candidate_id, "reason": "candidate_source_missing"})
            continue
        actual = inspect_factor_code(group["code_text"])
        # ast.dump is interpreter-version dependent (notably Python 3.10 versus
        # newer Windows Python). Normalized source identity is the portable
        # preparation/readback check; the inventory AST identity remains the
        # within-scan de-duplication key.
        if actual.get("code_sha256") not in set(group.get("code_sha256s") or []):
            unavailable.append({"candidate_id": candidate_id, "reason": "candidate_identity_mismatch"})
            continue
        audit = audit_legacy_factor_code(group["code_text"])
        if not audit["compatible"]:
            unavailable.append({
                "candidate_id": candidate_id,
                "representative_name": group.get("representative_name"),
                "reason": "legacy_adapter_incompatible",
                "details": audit["reasons"],
            })
            continue
        name = evaluation_name(str(group["representative_name"]), str(group["ast_sha256"]))
        if name in names:
            raise ResearchError("evaluation_name_conflict", "Stable evaluation names are not unique")
        names.add(name)
        script_path = scripts_root / f"{name}.py"
        script_path.write_text(
            render_legacy_adapter(
                code_text=group["code_text"], entry=str(audit["entry"]), name=name,
                data_files=list(audit["data_files"]),
            ),
            encoding="utf-8", newline="\n",
        )
        prepared.append({
            "candidate_id": candidate_id,
            "evaluation_name": name,
            "representative_name": group["representative_name"],
            "factor_names": group["factor_names"],
            "ast_sha256": group["ast_sha256"],
            "code_sha256s": group["code_sha256s"],
            "catalog_status": group["catalog_status"],
            "catalog_matches": group["catalog_matches"],
            "data_files": audit["data_files"],
            "script": str(script_path),
            "provenance": group["provenance"],
        })
    summary = {
        "schema_version": "rdagent_factor_salvage_evaluation_prep_v1",
        "inventory_candidates": len(review),
        "prepared_candidates": len(prepared),
        "unavailable_candidates": len(unavailable),
        "denominator_closed": len(review) == len(prepared) + len(unavailable),
        "candidate_execution_count": 0,
        "database_writes": 0,
        "factor_library_writes": 0,
        "source_mutations": 0,
    }
    _write_jsonl(artifact_root / "evaluation_candidates.jsonl", prepared)
    _write_jsonl(artifact_root / "evaluation_unavailable.jsonl", unavailable)
    _write_json(artifact_root / "evaluation_prep_summary.json", summary)
    return summary


def run_salvage_precheck(spec: Mapping[str, Any], artifact_root: Path) -> dict[str, Any]:
    """Execute audited adapters on an explicit bounded scope; this is not a value evaluation."""
    from .runner import load_values

    artifact_root = _assert_repo_external(artifact_root)
    if artifact_root.exists() and (not artifact_root.is_dir() or any(artifact_root.iterdir())):
        raise ResearchError("artifact_root_not_empty", "Artifact root must be absent or an empty directory")
    prepared_root = _assert_repo_external(Path(str(spec.get("prepared_root") or "")))
    data_dir = _assert_repo_external(Path(str(spec.get("data_dir") or "")))
    if not data_dir.is_dir() or data_dir.is_symlink():
        raise ResearchError("input_missing", "data_dir must be a regular directory")
    try:
        start_date = date.fromisoformat(spec["start_date"])
        end_date = date.fromisoformat(spec["end_date"])
        if start_date > end_date:
            raise ValueError
    except (KeyError, TypeError, ValueError) as exc:
        raise ResearchError("precheck_scope_invalid", "start_date/end_date are invalid") from exc
    start = start_date.isoformat()
    end = end_date.isoformat()
    instruments = spec.get("instruments")
    if not isinstance(instruments, list) or not instruments or any(
        not isinstance(item, str) or not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", item)
        for item in instruments
    ):
        raise ResearchError("precheck_scope_invalid", "Explicit canonical instruments are required")
    instruments = sorted(set(instruments))
    timeout = spec.get("timeout_seconds", 300)
    if type(timeout) is not int or not 1 <= timeout <= 86400:
        raise ResearchError("precheck_scope_invalid", "timeout_seconds must be an integer from 1 to 86400")
    candidates = _load_jsonl(prepared_root / "evaluation_candidates.jsonl")
    requested_ids = spec.get("candidate_ids")
    if requested_ids is not None:
        if not isinstance(requested_ids, list) or not requested_ids or any(
            not isinstance(item, str) or not item.startswith("ast:") for item in requested_ids
        ):
            raise ResearchError("precheck_scope_invalid", "candidate_ids must be a non-empty identity list")
        requested_set = set(requested_ids)
        available_ids = {str(item.get("candidate_id")) for item in candidates}
        missing_ids = requested_set - available_ids
        if missing_ids:
            raise ResearchError("precheck_scope_invalid", "candidate_ids contain unknown identities")
        candidates = [item for item in candidates if item.get("candidate_id") in requested_set]
    artifact_root.mkdir(parents=True, exist_ok=True)
    results_root = artifact_root / "results"
    results_root.mkdir()
    results: list[dict[str, Any]] = []
    for candidate in candidates:
        name = candidate.get("evaluation_name")
        script = Path(str(candidate.get("script") or "")).resolve()
        if not isinstance(name, str) or not EVALUATION_NAME_RE.fullmatch(name):
            results.append({"candidate_id": candidate.get("candidate_id"), "status": "unavailable",
                            "reason": "evaluation_name_invalid"})
            continue
        if script.is_symlink() or not script.is_file() or not script.is_relative_to(prepared_root.resolve()):
            results.append({"candidate_id": candidate.get("candidate_id"), "evaluation_name": name,
                            "status": "unavailable", "reason": "adapter_script_missing"})
            continue
        folder = results_root / name
        folder.mkdir()
        output = folder / "values.h5"
        started = time.perf_counter()
        command = [
            sys.executable, str(script), "--data-dir", str(data_dir), "--output", str(output),
            "--start-date", start, "--end-date", end,
            "--instruments", json.dumps(instruments, ensure_ascii=True, separators=(",", ":")),
        ]
        try:
            with (folder / "stdout.log").open("x", encoding="utf-8") as stdout, (
                    folder / "stderr.log").open("x", encoding="utf-8") as stderr:
                completed = subprocess.run(
                    command, cwd=folder, stdout=stdout, stderr=stderr,
                    timeout=timeout, check=False,
                )
            if completed.returncode:
                results.append({
                    "candidate_id": candidate.get("candidate_id"), "evaluation_name": name,
                    "status": "unavailable", "reason": "candidate_execution_failed",
                    "returncode": completed.returncode,
                    "elapsed_seconds": round(time.perf_counter() - started, 6),
                })
                continue
            frame = load_values(output, name)
            dates = frame.index.get_level_values("datetime")
            symbols = set(frame.index.get_level_values("instrument"))
            if dates.min().date().isoformat() < start or dates.max().date().isoformat() > end:
                raise ResearchError("candidate_scope_mismatch", "Candidate dates exceed the precheck scope")
            if not symbols.issubset(instruments):
                raise ResearchError("candidate_scope_mismatch", "Candidate symbols exceed the precheck scope")
            results.append({
                "candidate_id": candidate.get("candidate_id"), "evaluation_name": name,
                "status": "available", "rows": len(frame),
                "nan_rows": int(frame[name].isna().sum()),
                "actual_start": str(dates.min().date()), "actual_end": str(dates.max().date()),
                "instrument_count": len(symbols),
                "elapsed_seconds": round(time.perf_counter() - started, 6),
                "values": str(output),
            })
        except subprocess.TimeoutExpired:
            results.append({
                "candidate_id": candidate.get("candidate_id"), "evaluation_name": name,
                "status": "unavailable", "reason": "candidate_execution_timeout",
                "elapsed_seconds": round(time.perf_counter() - started, 6),
            })
        except (OSError, ResearchError, ValueError) as exc:
            results.append({
                "candidate_id": candidate.get("candidate_id"), "evaluation_name": name,
                "status": "unavailable", "reason": (
                    exc.code if isinstance(exc, ResearchError) else "candidate_output_invalid"
                ),
                "exception_type": type(exc).__name__,
                "elapsed_seconds": round(time.perf_counter() - started, 6),
            })
    available = sum(row["status"] == "available" for row in results)
    reasons: dict[str, int] = defaultdict(int)
    for row in results:
        if row["status"] != "available":
            reasons[str(row.get("reason") or "unknown")] += 1
    summary = {
        "schema_version": "rdagent_factor_salvage_precheck_v1",
        "scope": "technical_precheck_not_value_evaluation",
        "requested_candidates": len(candidates),
        "available_candidates": available,
        "unavailable_candidates": len(results) - available,
        "unavailable_by_reason": dict(sorted(reasons.items())),
        "denominator_closed": len(candidates) == len(results),
        "start_date": start,
        "end_date": end,
        "instrument_count": len(instruments),
        "database_writes": 0,
        "factor_library_writes": 0,
        "source_mutations": 0,
        "value_conclusions": 0,
    }
    _write_jsonl(artifact_root / "precheck_results.jsonl", results)
    _write_json(artifact_root / "precheck_summary.json", summary)
    return summary


def run_salvage_metric_evaluation(
    spec: Mapping[str, Any],
    artifact_root: Path,
    *,
    prepare=None,
    compute=None,
) -> dict[str, Any]:
    """Evaluate prepared full-history values without writing the factor library.

    Value generation remains a separate, shardable ``salvage-precheck`` step.
    This stage loads the shared official metric context once, then evaluates
    candidates one at a time so the number of historical candidates cannot
    turn into an all-candidate in-memory panel.
    """
    import pandas as pd

    from .full_evaluation import build_standard_windows, compute_candidate_metrics
    from .runner import _normalize_computed_metrics, evaluation_context, load_values

    artifact_root = _assert_repo_external(artifact_root)
    if artifact_root.exists() and (not artifact_root.is_dir() or any(artifact_root.iterdir())):
        raise ResearchError("artifact_root_not_empty", "Artifact root must be absent or an empty directory")
    prepared_root = _assert_repo_external(Path(str(spec.get("prepared_root") or "")))
    value_roots_raw = spec.get("value_roots")
    if not isinstance(value_roots_raw, list) or not value_roots_raw:
        raise ResearchError("evaluation_scope_invalid", "value_roots must be a non-empty list")
    value_roots = [_assert_repo_external(Path(str(item))) for item in value_roots_raw]
    if any(not root.is_dir() or root.is_symlink() for root in value_roots):
        raise ResearchError("input_missing", "Each value root must be a regular directory")

    candidates = _load_jsonl(prepared_root / "evaluation_candidates.jsonl")
    by_id = {str(row.get("candidate_id")): row for row in candidates}
    requested_ids = spec.get("candidate_ids")
    if requested_ids is None:
        selected_ids = sorted(by_id)
    else:
        if not isinstance(requested_ids, list) or not requested_ids or any(
            not isinstance(item, str) or not item.startswith("ast:") for item in requested_ids
        ):
            raise ResearchError("evaluation_scope_invalid", "candidate_ids must be a non-empty identity list")
        selected_ids = sorted(set(requested_ids))
        if set(selected_ids) - set(by_id):
            raise ResearchError("evaluation_scope_invalid", "candidate_ids contain unknown identities")

    value_records: dict[str, dict[str, Any]] = {}
    for root in value_roots:
        for row in _load_jsonl(root / "precheck_results.jsonl"):
            candidate_id = str(row.get("candidate_id") or "")
            if candidate_id not in by_id:
                raise ResearchError("evaluation_identity_mismatch", "Value result has an unknown candidate identity")
            if candidate_id in value_records:
                raise ResearchError("evaluation_identity_mismatch", "Candidate appears in more than one value root")
            value_records[candidate_id] = row
    missing = set(selected_ids) - set(value_records)
    if missing:
        raise ResearchError("evaluation_denominator_open", "Selected candidates are missing value results")

    try:
        read_start = date.fromisoformat(str(spec["read_start"])).isoformat()
        read_end = date.fromisoformat(str(spec["read_end"])).isoformat()
        signal_start = date.fromisoformat(str(spec["signal_start"])).isoformat()
        signal_end = date.fromisoformat(str(spec["signal_end"])).isoformat()
        cutoff = date.fromisoformat(str(spec["cutoff"])).isoformat()
        if not read_start <= signal_start <= signal_end <= read_end <= cutoff:
            raise ValueError
    except (KeyError, TypeError, ValueError) as exc:
        raise ResearchError("evaluation_scope_invalid", "Evaluation dates are invalid or out of order") from exc
    instruments = spec.get("instruments")
    if not isinstance(instruments, list) or not instruments or any(
        not isinstance(item, str) or not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", item)
        for item in instruments
    ):
        raise ResearchError("evaluation_scope_invalid", "Explicit canonical instruments are required")
    instruments = sorted(set(instruments))
    universe_key = spec.get("universe_key")
    if not isinstance(universe_key, str) or not universe_key.strip():
        raise ResearchError("evaluation_scope_invalid", "universe_key is required")
    qlib_bin_path = _assert_repo_external(Path(str(spec.get("qlib_bin_path") or "")))
    if not qlib_bin_path.is_dir() or qlib_bin_path.is_symlink():
        raise ResearchError("input_missing", "qlib_bin_path must be a regular directory")

    if prepare is None or compute is None:
        from backend.services.quantevolver.qe_eval_v2_metric_engine import (
            compute_single_factor_metrics,
            prepare_shared_context,
        )

        prepare = prepare or prepare_shared_context
        compute = compute or compute_single_factor_metrics
    ctx = prepare(
        qlib_bin_path=qlib_bin_path,
        start_date=read_start,
        end_date=read_end,
        instrument_hint=set(instruments),
        load_suspend_d=True,
        load_st_pit_mask=True,
        universe_key=universe_key,
    )
    ctx = evaluation_context(ctx, {"signal_start": signal_start, "signal_end": signal_end})
    windows = build_standard_windows(
        pd.DatetimeIndex(ctx["dates"]),
        signal_start=signal_start,
        signal_end=signal_end,
    )

    artifact_root.mkdir(parents=True, exist_ok=True)
    evaluated = unavailable = nonfinite = 0
    reasons: dict[str, int] = defaultdict(int)
    started = time.perf_counter()
    with (artifact_root / "metric_results.jsonl").open("x", encoding="utf-8", newline="\n") as result_stream, (
        artifact_root / "metric_unavailable.jsonl"
    ).open("x", encoding="utf-8", newline="\n") as unavailable_stream:
        for candidate_id in selected_ids:
            candidate = by_id[candidate_id]
            value_record = value_records[candidate_id]
            name = str(candidate.get("evaluation_name") or "")
            if value_record.get("status") != "available":
                row = {
                    "candidate_id": candidate_id,
                    "evaluation_name": name,
                    "reason": value_record.get("reason") or "candidate_values_unavailable",
                    "stage": "value_generation",
                }
                unavailable_stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
                unavailable += 1
                reasons[str(row["reason"])] += 1
                continue
            try:
                values_path = Path(str(value_record.get("values") or "")).resolve()
                if values_path.is_symlink() or not values_path.is_file() or not any(
                    values_path.is_relative_to(root.resolve()) for root in value_roots
                ):
                    raise ResearchError("candidate_output_missing", "Candidate values are outside declared roots")
                frame = load_values(values_path, name)
                dates = frame.index.get_level_values("datetime")
                symbols = set(frame.index.get_level_values("instrument"))
                if not symbols.issubset(instruments):
                    raise ResearchError("candidate_scope_mismatch", "Candidate contains undeclared symbols")
                selected = frame.loc[
                    (dates >= pd.Timestamp(signal_start)) & (dates <= pd.Timestamp(signal_end))
                ]
                missing_dates = pd.DatetimeIndex(ctx["dates"]).difference(
                    pd.DatetimeIndex(selected.index.get_level_values("datetime")).unique()
                )
                if not missing_dates.empty:
                    raise ResearchError(
                        "candidate_scope_mismatch",
                        "Candidate output silently omits declared signal dates",
                    )
                raw = compute_candidate_metrics(name, selected, ctx, windows, compute=compute)
                metrics, replaced = _normalize_computed_metrics(raw)
                finite = selected.loc[selected[name].notna()]
                row = {
                    "candidate_id": candidate_id,
                    "evaluation_name": name,
                    "representative_name": candidate.get("representative_name"),
                    "catalog_status": candidate.get("catalog_status"),
                    "metrics": metrics,
                    "rows": len(frame),
                    "nan_rows": int(frame[name].isna().sum()),
                    "signal_rows": len(selected),
                    "actual_factor_value_range": (
                        {
                            "start": str(finite.index.get_level_values("datetime").min().date()),
                            "end": str(finite.index.get_level_values("datetime").max().date()),
                        }
                        if not finite.empty
                        else None
                    ),
                    "computed_metric_serialization": {
                        "nonfinite_values_as_null": replaced,
                        "policy": "ieee_nonfinite_to_json_null_no_zero_fill_or_row_removal",
                    },
                }
                result_stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n")
                result_stream.flush()
                evaluated += 1
                nonfinite += replaced
            except Exception as exc:
                reason = exc.code if isinstance(exc, ResearchError) else "candidate_metric_evaluation_failed"
                row = {
                    "candidate_id": candidate_id,
                    "evaluation_name": name,
                    "reason": reason,
                    "stage": "metric_evaluation",
                    "exception_type": type(exc).__name__,
                }
                unavailable_stream.write(
                    json.dumps(row, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n"
                )
                unavailable_stream.flush()
                unavailable += 1
                reasons[reason] += 1
            finally:
                if "frame" in locals():
                    del frame
                if "selected" in locals():
                    del selected

    summary = {
        "schema_version": "rdagent_factor_salvage_metric_evaluation_v1",
        "scope": "complete_pit_candidate_metrics_not_factor_library_delivery",
        "requested_candidates": len(selected_ids),
        "evaluated_candidates": evaluated,
        "unavailable_candidates": unavailable,
        "unavailable_by_reason": dict(sorted(reasons.items())),
        "denominator_closed": len(selected_ids) == evaluated + unavailable,
        "windows": windows,
        "read_start": read_start,
        "read_end": read_end,
        "signal_start": signal_start,
        "signal_end": signal_end,
        "cutoff": cutoff,
        "instrument_count": len(instruments),
        "nonfinite_metric_values_as_null": nonfinite,
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "database_writes": 0,
        "factor_library_writes": 0,
        "source_mutations": 0,
    }
    _write_json(artifact_root / "metric_summary.json", summary)
    return summary


def run_salvage_reference_correlation(
    spec: Mapping[str, Any],
    artifact_root: Path,
) -> dict[str, Any]:
    """Compute bounded candidate-by-library correlation diagnostics.

    The result intentionally retains per-candidate top matches and denominator
    counts instead of a multi-million-row success log.  It does not calculate
    the already-known library-by-library matrix, and it never installs research
    candidates into the official factor cache.
    """
    import pandas as pd

    from backend.services.quantevolver.correlation_engine import CorrelationEngine

    from .full_evaluation import load_reference_values
    from .runner import load_values

    artifact_root = _assert_repo_external(artifact_root)
    if artifact_root.exists() and (not artifact_root.is_dir() or any(artifact_root.iterdir())):
        raise ResearchError("artifact_root_not_empty", "Artifact root must be absent or an empty directory")
    prepared_root = _assert_repo_external(Path(str(spec.get("prepared_root") or "")))
    value_roots_raw = spec.get("value_roots")
    if not isinstance(value_roots_raw, list) or not value_roots_raw:
        raise ResearchError("correlation_scope_invalid", "value_roots must be a non-empty list")
    value_roots = [_assert_repo_external(Path(str(item))) for item in value_roots_raw]
    if any(not root.is_dir() or root.is_symlink() for root in value_roots):
        raise ResearchError("input_missing", "Each value root must be a regular directory")

    candidates = {str(row.get("candidate_id")): row for row in _load_jsonl(
        prepared_root / "evaluation_candidates.jsonl"
    )}
    value_records: dict[str, dict[str, Any]] = {}
    for root in value_roots:
        for row in _load_jsonl(root / "precheck_results.jsonl"):
            candidate_id = str(row.get("candidate_id") or "")
            if candidate_id not in candidates or candidate_id in value_records:
                raise ResearchError("correlation_identity_mismatch", "Value candidate identity is unknown or duplicated")
            value_records[candidate_id] = row
    requested_ids = spec.get("candidate_ids")
    if requested_ids is None:
        selected_ids = sorted(
            candidate_id for candidate_id, row in value_records.items() if row.get("status") == "available"
        )
    else:
        if not isinstance(requested_ids, list) or not requested_ids or any(
            not isinstance(item, str) or not item.startswith("ast:") for item in requested_ids
        ):
            raise ResearchError("correlation_scope_invalid", "candidate_ids must be a non-empty identity list")
        selected_ids = sorted(set(requested_ids))
        if set(selected_ids) - set(candidates) or any(
            value_records.get(item, {}).get("status") != "available" for item in selected_ids
        ):
            raise ResearchError("correlation_scope_invalid", "candidate_ids require available full-history values")
    if not selected_ids:
        raise ResearchError("correlation_scope_invalid", "No available candidates were selected")

    raw_references = spec.get("reference_value_artifacts")
    if not isinstance(raw_references, dict) or not raw_references:
        raise ResearchError("correlation_scope_invalid", "reference_value_artifacts must be a non-empty mapping")
    references: list[tuple[str, Path]] = []
    for name, raw_path in sorted(raw_references.items()):
        if not isinstance(name, str) or not REFERENCE_NAME_RE.fullmatch(name) or not isinstance(raw_path, str):
            raise ResearchError("correlation_scope_invalid", "Reference names and paths are invalid")
        path = Path(raw_path).expanduser().resolve()
        if path.is_relative_to(Path(__file__).resolve().parents[3]):
            raise ResearchError("correlation_scope_invalid", "Reference values must be repo-external artifacts")
        references.append((name, path))
    selected_names = {str(candidates[candidate_id].get("evaluation_name") or "") for candidate_id in selected_ids}
    if selected_names & {name for name, _ in references}:
        raise ResearchError("correlation_scope_invalid", "Candidate and reference names must be distinct")

    raw_windows = spec.get("windows")
    if not isinstance(raw_windows, dict) or not raw_windows:
        raise ResearchError("correlation_scope_invalid", "windows must be a non-empty mapping")
    windows: dict[str, dict[str, str]] = {}
    for name, raw in sorted(raw_windows.items()):
        if not isinstance(name, str) or not isinstance(raw, dict):
            raise ResearchError("correlation_scope_invalid", "Correlation window is invalid")
        try:
            start = date.fromisoformat(str(raw["start"])).isoformat()
            end = date.fromisoformat(str(raw["end"])).isoformat()
            if start > end:
                raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise ResearchError("correlation_scope_invalid", "Correlation window dates are invalid") from exc
        windows[name] = {"start": start, "end": end}

    def positive_int(name: str, default: int) -> int:
        value = spec.get(name, default)
        if type(value) is not int or value < 1:
            raise ResearchError("correlation_scope_invalid", f"{name} must be a positive integer")
        return value

    candidate_batch_size = positive_int("candidate_batch_size", 4)
    reference_batch_size = positive_int("reference_batch_size", 16)
    min_stocks = positive_int("correlation_min_stocks", 100)
    min_days = positive_int("correlation_min_effective_days", 20)
    top_k = positive_int("top_k", 10)
    half_life = spec.get("correlation_half_life", 125)
    if type(half_life) not in (int, float) or half_life <= 0:
        raise ResearchError("correlation_scope_invalid", "correlation_half_life must be positive")

    artifact_root.mkdir(parents=True, exist_ok=True)
    total_requested = total_available = total_unavailable = 0
    global_reasons: dict[str, int] = defaultdict(int)
    started = time.perf_counter()
    with (artifact_root / "reference_correlation_results.jsonl").open(
        "x", encoding="utf-8", newline="\n"
    ) as stream:
        for candidate_offset in range(0, len(selected_ids), candidate_batch_size):
            batch_ids = selected_ids[candidate_offset : candidate_offset + candidate_batch_size]
            candidate_frames = []
            names_by_id: dict[str, str] = {}
            for candidate_id in batch_ids:
                name = str(candidates[candidate_id].get("evaluation_name") or "")
                path = Path(str(value_records[candidate_id].get("values") or "")).resolve()
                if path.is_symlink() or not path.is_file() or not any(
                    path.is_relative_to(root.resolve()) for root in value_roots
                ):
                    raise ResearchError("candidate_output_missing", "Candidate values are outside declared roots")
                candidate_frames.append(load_values(path, name))
                names_by_id[candidate_id] = name
            candidate_panel = pd.concat(candidate_frames, axis=1, join="outer").sort_index()
            candidate_names = list(candidate_panel.columns)
            diagnostics = {
                candidate: {
                    window: {"requested": 0, "available": 0, "unavailable_by_reason": defaultdict(int), "top": []}
                    for window in windows
                }
                for candidate in candidate_names
            }
            unique_dates = pd.DatetimeIndex(candidate_panel.index.get_level_values("datetime")).unique()
            engine = CorrelationEngine(
                object(),
                window=len(unique_dates),
                half_life=float(half_life),
                min_stocks=min_stocks,
                min_days=min_days,
            )
            for reference_offset in range(0, len(references), reference_batch_size):
                reference_batch = references[reference_offset : reference_offset + reference_batch_size]
                reference_frames = []
                loaded_names: list[str] = []
                failed: list[tuple[str, str]] = []
                for reference_name, reference_path in reference_batch:
                    try:
                        reference_frames.append(load_reference_values(reference_path, reference_name))
                        loaded_names.append(reference_name)
                    except Exception as exc:
                        failed.append((
                            reference_name,
                            exc.code if isinstance(exc, ResearchError) else "reference_value_load_failed",
                        ))
                for candidate in candidate_names:
                    for window in windows:
                        diagnostic = diagnostics[candidate][window]
                        diagnostic["requested"] += len(reference_batch)
                        for _, reason in failed:
                            diagnostic["unavailable_by_reason"][reason] += 1
                if not reference_frames:
                    continue
                reference_panel = pd.concat(reference_frames, axis=1, join="outer").sort_index()
                try:
                    daily = engine.compute_selected_daily_submatrix(
                        candidate_panel,
                        reference_panel,
                        as_of_date=max(window["end"] for window in windows.values()),
                    )
                except Exception as exc:
                    reason = exc.code if isinstance(exc, ResearchError) else "correlation_block_failed"
                    for candidate in candidate_names:
                        for window in windows:
                            diagnostics[candidate][window]["unavailable_by_reason"][reason] += len(loaded_names)
                    del reference_frames, reference_panel
                    continue
                for window_name, window in windows.items():
                    try:
                        result = engine.aggregate_selected_daily_submatrix(
                            daily,
                            start_date=window["start"],
                            end_date=window["end"],
                            as_of_date=window["end"],
                        )
                        for row in result.records():
                            diagnostic = diagnostics[row["candidate"]][window_name]
                            if row["status"] == "available":
                                diagnostic["available"] += 1
                                diagnostic["top"].append({
                                    "reference": row["reference"],
                                    "correlation": row["correlation"],
                                    "effective_days": row["effective_days"],
                                    "avg_stocks_per_day": row["avg_stocks_per_day"],
                                })
                                diagnostic["top"].sort(
                                    key=lambda item: (-abs(item["correlation"]), item["reference"])
                                )
                                del diagnostic["top"][top_k:]
                            else:
                                diagnostic["unavailable_by_reason"][str(row.get("reason") or "unknown")] += 1
                    except Exception as exc:
                        reason = exc.code if isinstance(exc, ResearchError) else "correlation_window_failed"
                        for candidate in candidate_names:
                            diagnostics[candidate][window_name]["unavailable_by_reason"][reason] += len(loaded_names)
                del reference_frames, reference_panel, daily

            id_by_name = {name: candidate_id for candidate_id, name in names_by_id.items()}
            for candidate in candidate_names:
                rendered_windows = {}
                for window_name, raw in diagnostics[candidate].items():
                    unavailable_count = sum(raw["unavailable_by_reason"].values())
                    rendered_windows[window_name] = {
                        "requested_pairs": raw["requested"],
                        "available_pairs": raw["available"],
                        "unavailable_pairs": unavailable_count,
                        "unavailable_by_reason": dict(sorted(raw["unavailable_by_reason"].items())),
                        "top_absolute_correlations": raw["top"],
                        "denominator_closed": raw["requested"] == raw["available"] + unavailable_count,
                    }
                    total_requested += raw["requested"]
                    total_available += raw["available"]
                    total_unavailable += unavailable_count
                    for reason, count in raw["unavailable_by_reason"].items():
                        global_reasons[reason] += count
                row = {
                    "candidate_id": id_by_name[candidate],
                    "evaluation_name": candidate,
                    "representative_name": candidates[id_by_name[candidate]].get("representative_name"),
                    "windows": rendered_windows,
                }
                stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n")
                stream.flush()
            del candidate_frames, candidate_panel

    summary = {
        "schema_version": "rdagent_factor_salvage_reference_correlation_v1",
        "scope": "candidate_by_official_library_only",
        "candidate_count": len(selected_ids),
        "reference_count": len(references),
        "window_count": len(windows),
        "requested_pairs": total_requested,
        "available_pairs": total_available,
        "unavailable_pairs": total_unavailable,
        "unavailable_by_reason": dict(sorted(global_reasons.items())),
        "denominator_closed": total_requested == total_available + total_unavailable,
        "reference_reference_pairs_computed": 0,
        "candidate_candidate_pairs_computed": 0,
        "candidate_batch_size": candidate_batch_size,
        "reference_batch_size": reference_batch_size,
        "top_k": top_k,
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "database_writes": 0,
        "factor_library_writes": 0,
    }
    _write_json(artifact_root / "reference_correlation_summary.json", summary)
    return summary


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")


def _assert_repo_external(path: Path) -> Path:
    resolved = path.resolve()
    for parent in (resolved, *resolved.parents):
        if (parent / ".git").exists():
            raise ResearchError("artifact_root_inside_repository", "Artifact root must be outside every Git repository")
    return resolved


def run_salvage(spec: Mapping[str, Any], artifact_root: Path) -> dict[str, Any]:
    artifact_root = _assert_repo_external(artifact_root)
    if artifact_root.exists() and (not artifact_root.is_dir() or any(artifact_root.iterdir())):
        raise ResearchError("artifact_root_not_empty", "Artifact root must be absent or an empty directory")
    artifact_root.mkdir(parents=True, exist_ok=True)
    sources = spec.get("sources")
    if not isinstance(sources, list) or not sources:
        raise ResearchError("source_config_invalid", "At least one source is required")

    source_records: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []
    source_status: dict[str, dict[str, int]] = defaultdict(lambda: {"factor_sources": 0, "unavailable": 0})
    for source in sources:
        if not isinstance(source, dict):
            raise ResearchError("source_config_invalid", "Source entries must be objects")
        kind = source.get("kind")
        node_id = str(source.get("node_id") or "unknown")
        source_status[node_id]
        iterator: Iterable[dict[str, Any]]
        if kind == "central_export":
            iterator = iter_central_export(Path(str(source.get("root") or "")), node_id=node_id)
        else:
            iterator = iter_node_pickle_source(source)
        for raw in iterator:
            if raw.get("record_type") != "factor_source":
                unavailable.append(dict(raw))
                source_status[node_id]["unavailable"] += 1
                continue
            try:
                record = prepare_source_record(raw)
            except ResearchError as exc:
                unavailable.append({
                    "record_type": "unavailable", "source_node": node_id,
                    "reason": exc.code, "source_path": raw.get("source_path"),
                })
                source_status[node_id]["unavailable"] += 1
                continue
            if not record["valid"]:
                unavailable.append({
                    "record_type": "unavailable", "source_node": node_id,
                    "task_id": record.get("task_id"), "loop_id": record.get("loop_id"),
                    "workspace_key": record.get("workspace_key"), "source_path": record.get("source_path"),
                    "factor_name": record.get("factor_name"), "reason": record["reason"],
                    "code_sha256": record["code_sha256"],
                })
                source_status[node_id]["unavailable"] += 1
                continue
            source_records.append(record)
            source_status[node_id]["factor_sources"] += 1

    groups = group_candidates(source_records)
    snapshot_value = spec.get("catalog_snapshot")
    catalog, catalog_available = load_catalog_snapshot(Path(snapshot_value) if snapshot_value else None)
    classify_catalog(groups, catalog, catalog_available)
    review = [group for group in groups if group["catalog_status"] in {"catalog_absent", "catalog_name_conflict"}]
    statuses: dict[str, int] = defaultdict(int)
    for group in groups:
        statuses[str(group["catalog_status"])] += 1
    review_statuses: dict[str, int] = defaultdict(int)
    review_sources: dict[str, int] = defaultdict(int)
    review_with_exported_dynamic = 0
    compact_review: list[dict[str, Any]] = []
    for group in review:
        review_statuses[str(group["catalog_status"])] += 1
        nodes = sorted({str(item["source_node"]) for item in group["provenance"]})
        for node in nodes:
            review_sources[node] += 1
        has_exported_dynamic = any(item.get("exported_dynamic_factor") is True for item in group["provenance"])
        review_with_exported_dynamic += int(has_exported_dynamic)
        compact_review.append({key: value for key, value in group.items() if key != "code_text"} | {
            "source_nodes": nodes,
            "has_exported_dynamic_evidence": has_exported_dynamic,
        })
    source_objects = len(source_records) + len(unavailable)
    operational_reasons = {"source_node_unreachable", "source_root_missing", "node_extractor_failed",
                           "node_extractor_timeout"}
    summary = {
        "schema_version": "rdagent_factor_salvage_dry_run_v1",
        "dry_run": True,
        "source_objects": source_objects,
        "grouped_members": len(source_records),
        "unavailable": len(unavailable),
        "source_denominator_closed": source_objects == len(source_records) + len(unavailable),
        "source_scan_complete": not any(row.get("reason") in operational_reasons for row in unavailable),
        "unique_groups": len(groups),
        "catalog_status_counts": dict(sorted(statuses.items())),
        "group_denominator_closed": len(groups) == sum(statuses.values()),
        "review_candidates": len(review),
        "review_catalog_status_counts": dict(sorted(review_statuses.items())),
        "review_source_presence": dict(sorted(review_sources.items())),
        "review_with_exported_dynamic_evidence": review_with_exported_dynamic,
        "source_status": dict(sorted(source_status.items())),
        "database_writes": 0,
        "factor_library_writes": 0,
        "source_mutations": 0,
    }
    _write_jsonl(artifact_root / "source_inventory.jsonl", source_records)
    _write_jsonl(artifact_root / "candidate_groups.jsonl", groups)
    _write_jsonl(artifact_root / "review_candidates.jsonl", compact_review)
    _write_jsonl(artifact_root / "unavailable.jsonl", unavailable)
    _write_json(artifact_root / "dry_run_summary.json", summary)
    return summary
