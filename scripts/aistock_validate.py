from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import socket
import subprocess
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "tests" / "aistock_validation" / "templates" / "test_run_record.md"
DEFAULT_HISTORY_ROOT = ROOT / "tests" / "aistock_validation" / "history"
RUN_METADATA_SCHEMA_VERSION = "aistock_validation_run_v1"
EVIDENCE_MANIFEST_SCHEMA_VERSION = "aistock_validation_evidence_manifest_v1"
COVERAGE_SNAPSHOT_SCHEMA_VERSION = "aistock_validation_coverage_snapshot_v1"


def _safe_slug(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9._-]+", "-", value)
    return re.sub(r"-+", "-", value).strip("-") or "validation"


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        return "unknown"


def _operator() -> str:
    return os.environ.get("USERNAME") or os.environ.get("USER") or "unknown"


def _now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _path_for_json(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT.resolve()).as_posix()
    except Exception:
        return str(path)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _file_sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _environment_snapshot() -> dict[str, str | None]:
    keys = [
        "BACKEND_PORT",
        "FRONTEND_PORT",
        "TDX_HTTP_PORT",
        "PAPER_V2_API_BASE",
        "QE_API_BASE",
        "QE_ARCHIVE_API_BASE",
        "NEXT_PUBLIC_API_BASE",
        "PAPER_V2_SKIP_REALTIME",
        "QE_READ_L3_SKIP_UI",
        "QE_ARCHIVE_L3_SKIP_UI",
    ]
    return {key.lower(): os.environ.get(key) for key in keys}


def _is_port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def _http_probe(url: str, timeout: float = 5.0) -> tuple[bool, str]:
    request = urllib.request.Request(url, headers={"Accept": "application/json,text/plain,*/*"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read(4096)
            if response.status < 200 or response.status >= 300:
                return False, f"HTTP {response.status}"
            if not body.strip():
                return False, "empty response body"
            return True, f"HTTP {response.status}, {len(body)} bytes"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}: {exc.reason}"
    except urllib.error.URLError as exc:
        return False, f"connection failed: {exc.reason}"
    except TimeoutError:
        return False, "timeout"


def cmd_record(args: argparse.Namespace) -> int:
    if not TEMPLATE.exists():
        raise SystemExit(f"Missing test run template: {TEMPLATE}")
    now = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    module = _safe_slug(args.module)
    level = _safe_slug(args.level.upper())
    title_slug = _safe_slug(args.title)
    history_root = Path(args.history_root).resolve() if args.history_root else DEFAULT_HISTORY_ROOT
    out_dir = history_root / module
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{now}_{level}_{title_slug}.md"
    json_file = Path(args.json_out).resolve() if args.json_out else out_file.with_suffix(".json")

    text = TEMPLATE.read_text(encoding="utf-8")
    created_at = _now_iso()
    replacements = {
        "{{TITLE}}": args.title,
        "{{MODULE}}": args.module,
        "{{LEVEL}}": args.level.upper(),
        "{{DATE}}": created_at,
        "{{GIT_COMMIT}}": _git_commit(),
        "{{OPERATOR}}": _operator(),
    }
    for key, value in replacements.items():
        text = text.replace(key, value)
    out_file.write_text(text, encoding="utf-8")
    print(out_file)
    if not args.no_json:
        metadata = {
            "schema_version": RUN_METADATA_SCHEMA_VERSION,
            "module": args.module,
            "module_slug": module,
            "level": args.level.upper(),
            "level_slug": level,
            "title": args.title,
            "title_slug": title_slug,
            "git_commit": _git_commit(),
            "operator": _operator(),
            "started_at": created_at,
            "finished_at": None,
            "status": args.status,
            "environment": _environment_snapshot(),
            "markdown_path": _path_for_json(out_file),
            "metadata_path": _path_for_json(json_file),
            "steps": [],
            "coverage": {
                "schema_version": COVERAGE_SNAPSHOT_SCHEMA_VERSION,
                "status": "not_collected",
                "line": None,
                "branch": None,
                "diff_line": None,
                "diff_branch": None,
                "snapshot_path": None,
                "quality_gates": [],
            },
            "quality_gates": [],
            "evidence": [],
            "residual_risks": [],
        }
        _write_json(json_file, metadata)
        print(f"metadata: {json_file}")
    return 0


def _evidence_entry(raw_path: str, *, kind: str) -> dict:
    path = Path(raw_path)
    if not path.is_absolute():
        path = (ROOT / path).resolve()
    exists = path.exists()
    is_dir = path.is_dir() if exists else False
    size_bytes = path.stat().st_size if exists and path.is_file() else None
    child_count = None
    if exists and is_dir:
        try:
            child_count = sum(1 for _ in path.iterdir())
        except OSError:
            child_count = None
    return {
        "kind": kind,
        "path": _path_for_json(path),
        "exists": exists,
        "is_dir": is_dir,
        "size_bytes": size_bytes,
        "child_count": child_count,
        "sha256": _file_sha256(path) if exists and path.is_file() else None,
    }


def _parse_kind_path(raw: str) -> tuple[str, str]:
    if "=" not in raw:
        return "file", raw
    kind, value = raw.split("=", 1)
    kind = _safe_slug(kind)
    if not kind:
        kind = "file"
    if not value:
        raise SystemExit(f"Invalid evidence item, empty path: {raw}")
    return kind, value


def cmd_evidence(args: argparse.Namespace) -> int:
    items: list[tuple[str, str]] = []
    for raw in args.item or []:
        items.append(_parse_kind_path(raw))
    for raw in args.include or []:
        items.append(("file", raw))
    for kind, raw_values in {
        "coverage": args.coverage,
        "playwright_report": args.playwright_report,
        "playwright_trace": args.playwright_trace,
        "smoke_json": args.smoke_json,
        "db_smoke": args.db_smoke,
        "artifact": args.artifact,
    }.items():
        for raw in raw_values or []:
            items.append((kind, raw))

    evidence = [_evidence_entry(raw_path, kind=kind) for kind, raw_path in items]
    missing = [item for item in evidence if not item["exists"]]
    manifest = {
        "schema_version": EVIDENCE_MANIFEST_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "module": args.module,
        "level": args.level.upper() if args.level else None,
        "title": args.title,
        "run_id": args.run_id,
        "git_commit": _git_commit(),
        "operator": _operator(),
        "environment": _environment_snapshot(),
        "evidence": evidence,
        "missing_count": len(missing),
        "missing": missing,
    }
    output = Path(args.output).resolve()
    _write_json(output, manifest)
    print(output)
    if missing and args.fail_missing:
        for item in missing:
            print(f"missing evidence: {item['kind']} {item['path']}")
        return 1
    return 0


def _percent(covered: int | None, valid: int | None) -> float | None:
    if covered is None or valid is None or valid <= 0:
        return None
    return round((covered / valid) * 100, 2)


def _normalize_coverage_path(raw_path: str) -> str:
    path = Path(raw_path)
    try:
        if path.is_absolute():
            return path.resolve().relative_to(ROOT.resolve()).as_posix()
    except (OSError, RuntimeError, ValueError):
        pass
    return raw_path.replace("\\", "/").lstrip("./")


def _parse_condition_coverage(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    match = re.search(r"\((\d+)\s*/\s*(\d+)\)", value)
    if not match:
        return None
    covered, valid = int(match.group(1)), int(match.group(2))
    return covered, valid


def _new_file_coverage(path: str) -> dict:
    return {
        "path": path,
        "_line_hits": {},
        "_branch_covered": 0,
        "_branch_valid": 0,
    }


def _finalize_file_coverage(raw_file: dict) -> dict:
    line_hits: dict[int, int] = raw_file["_line_hits"]
    executable_lines = sorted(line_hits)
    covered_lines = sorted(line for line, hits in line_hits.items() if hits > 0)
    missing_lines = sorted(line for line, hits in line_hits.items() if hits <= 0)
    lines_valid = len(executable_lines)
    lines_covered = len(covered_lines)
    branch_valid = raw_file["_branch_valid"] or None
    branch_covered = raw_file["_branch_covered"] if branch_valid is not None else None
    return {
        "path": raw_file["path"],
        "lines_valid": lines_valid,
        "lines_covered": lines_covered,
        "line_percent": _percent(lines_covered, lines_valid),
        "branches_valid": branch_valid,
        "branches_covered": branch_covered,
        "branch_percent": _percent(branch_covered, branch_valid),
        "executable_lines": executable_lines,
        "covered_lines": covered_lines,
        "missing_lines": missing_lines,
    }


def _aggregate_coverage_files(files: list[dict]) -> dict:
    lines_valid = sum(item["lines_valid"] for item in files)
    lines_covered = sum(item["lines_covered"] for item in files)
    branch_values = [item for item in files if item["branches_valid"] is not None]
    branches_valid = sum(item["branches_valid"] for item in branch_values) if branch_values else None
    branches_covered = (
        sum(item["branches_covered"] for item in branch_values) if branch_values else None
    )
    return {
        "lines_valid": lines_valid,
        "lines_covered": lines_covered,
        "line_percent": _percent(lines_covered, lines_valid),
        "branches_valid": branches_valid,
        "branches_covered": branches_covered,
        "branch_percent": _percent(branches_covered, branches_valid),
    }


def _parse_coverage_xml(path: Path) -> tuple[list[dict], dict]:
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        raise SystemExit(f"Invalid coverage XML: {path}: {exc}") from exc

    files_by_path: dict[str, dict] = {}
    for class_node in root.findall(".//class"):
        filename = class_node.attrib.get("filename")
        if not filename:
            continue
        normalized = _normalize_coverage_path(filename)
        raw_file = files_by_path.setdefault(normalized, _new_file_coverage(normalized))
        for line_node in class_node.findall("./lines/line"):
            number_raw = line_node.attrib.get("number")
            if not number_raw:
                continue
            line_number = int(number_raw)
            hits = int(line_node.attrib.get("hits", "0"))
            raw_file["_line_hits"][line_number] = max(
                hits,
                raw_file["_line_hits"].get(line_number, 0),
            )
            branch_counts = _parse_condition_coverage(line_node.attrib.get("condition-coverage"))
            if branch_counts:
                covered, valid = branch_counts
                raw_file["_branch_covered"] += covered
                raw_file["_branch_valid"] += valid

    files = [_finalize_file_coverage(raw_file) for raw_file in files_by_path.values()]
    files.sort(key=lambda item: item["path"])
    totals = _aggregate_coverage_files(files)
    return files, totals


def _parse_coverage_json(path: Path) -> tuple[list[dict], dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid coverage JSON: {path}: {exc}") from exc

    files: list[dict] = []
    for filename, payload in (data.get("files") or {}).items():
        normalized = _normalize_coverage_path(filename)
        executed_lines = set(int(line) for line in payload.get("executed_lines") or [])
        missing_lines = set(int(line) for line in payload.get("missing_lines") or [])
        summary = payload.get("summary") or {}
        if not executed_lines and not missing_lines and summary.get("num_statements"):
            missing_lines = set(int(line) for line in payload.get("missing_lines") or [])
        executable_lines = sorted(executed_lines | missing_lines)
        lines_valid = int(summary.get("num_statements") or len(executable_lines))
        lines_covered = int(summary.get("covered_lines") or len(executed_lines))
        branch_valid = summary.get("num_branches")
        branch_covered = summary.get("covered_branches")
        branch_valid = int(branch_valid) if branch_valid is not None else None
        branch_covered = int(branch_covered) if branch_covered is not None else None
        files.append(
            {
                "path": normalized,
                "lines_valid": lines_valid,
                "lines_covered": lines_covered,
                "line_percent": _percent(lines_covered, lines_valid),
                "branches_valid": branch_valid,
                "branches_covered": branch_covered,
                "branch_percent": _percent(branch_covered, branch_valid),
                "executable_lines": executable_lines,
                "covered_lines": sorted(executed_lines),
                "missing_lines": sorted(missing_lines),
            }
        )

    files.sort(key=lambda item: item["path"])
    totals = _aggregate_coverage_files(files)
    return files, totals


def _parse_diff_patch(patch_text: str) -> dict[str, set[int]]:
    changed_lines: dict[str, set[int]] = {}
    current_path: str | None = None
    new_line: int | None = None
    for raw_line in patch_text.splitlines():
        if raw_line.startswith("+++ "):
            path = raw_line[4:].strip()
            if path == "/dev/null":
                current_path = None
            else:
                current_path = _normalize_coverage_path(re.sub(r"^[ab]/", "", path))
                changed_lines.setdefault(current_path, set())
            new_line = None
            continue
        if raw_line.startswith("@@ "):
            match = re.search(r"\+(\d+)(?:,(\d+))?", raw_line)
            new_line = int(match.group(1)) if match else None
            continue
        if current_path is None or new_line is None:
            continue
        if raw_line.startswith("+") and not raw_line.startswith("+++"):
            changed_lines[current_path].add(new_line)
            new_line += 1
        elif raw_line.startswith(" ") or raw_line == "":
            new_line += 1
        elif raw_line.startswith("-"):
            continue
    return {path: lines for path, lines in changed_lines.items() if lines}


def _git_diff_patch(base: str, paths: list[str]) -> str:
    command = ["git", "diff", "--unified=0", base, "--", *paths]
    return subprocess.check_output(command, cwd=ROOT, text=True, encoding="utf-8")


def _find_coverage_file(path: str, files_by_path: dict[str, dict]) -> tuple[dict | None, str]:
    normalized = _normalize_coverage_path(path)
    if normalized in files_by_path:
        return files_by_path[normalized], "exact"
    suffix_matches = [
        item
        for coverage_path, item in files_by_path.items()
        if coverage_path.endswith(f"/{normalized}") or coverage_path == normalized
    ]
    if len(suffix_matches) == 1:
        return suffix_matches[0], "suffix"
    return None, "missing"


def _calculate_diff_coverage(files: list[dict], changed_lines: dict[str, set[int]]) -> dict:
    files_by_path = {item["path"]: item for item in files}
    diff_files: list[dict] = []
    missing_coverage_files: list[str] = []
    total_valid = 0
    total_covered = 0
    for changed_path, lines in sorted(changed_lines.items()):
        coverage_file, match_mode = _find_coverage_file(changed_path, files_by_path)
        if coverage_file is None:
            missing_coverage_files.append(changed_path)
            diff_files.append(
                {
                    "path": changed_path,
                    "matched_path": None,
                    "match_mode": match_mode,
                    "changed_lines": sorted(lines),
                    "executable_changed_lines": [],
                    "covered_changed_lines": [],
                    "missing_changed_lines": [],
                    "non_executable_changed_lines": sorted(lines),
                    "line_percent": None,
                }
            )
            continue
        executable = set(coverage_file["executable_lines"])
        covered = set(coverage_file["covered_lines"])
        executable_changed = sorted(lines & executable)
        covered_changed = sorted(line for line in executable_changed if line in covered)
        missing_changed = sorted(line for line in executable_changed if line not in covered)
        non_executable = sorted(lines - executable)
        total_valid += len(executable_changed)
        total_covered += len(covered_changed)
        diff_files.append(
            {
                "path": changed_path,
                "matched_path": coverage_file["path"],
                "match_mode": match_mode,
                "changed_lines": sorted(lines),
                "executable_changed_lines": executable_changed,
                "covered_changed_lines": covered_changed,
                "missing_changed_lines": missing_changed,
                "non_executable_changed_lines": non_executable,
                "line_percent": _percent(len(covered_changed), len(executable_changed)),
            }
        )
    return {
        "enabled": True,
        "lines_valid": total_valid,
        "lines_covered": total_covered,
        "line_percent": _percent(total_covered, total_valid),
        "files": diff_files,
        "missing_coverage_files": missing_coverage_files,
    }


def _coverage_gate(metric: str, actual: float | None, threshold: float | None, *, reason: str | None = None) -> dict:
    if threshold is None:
        return {
            "metric": metric,
            "threshold": None,
            "actual": actual,
            "status": "not_configured",
            "reason": None,
        }
    if actual is None:
        return {
            "metric": metric,
            "threshold": threshold,
            "actual": None,
            "status": "failed",
            "reason": reason or "metric unavailable",
        }
    status = "passed" if actual >= threshold else "failed"
    return {
        "metric": metric,
        "threshold": threshold,
        "actual": actual,
        "status": status,
        "reason": None if status == "passed" else f"{actual} < {threshold}",
    }


def _validate_percent_threshold(raw_value: float | None, name: str) -> float | None:
    if raw_value is None:
        return None
    if raw_value < 0 or raw_value > 100:
        raise SystemExit(f"{name} must be between 0 and 100, got {raw_value}")
    return float(raw_value)


def cmd_coverage(args: argparse.Namespace) -> int:
    if args.coverage_xml:
        coverage_path = Path(args.coverage_xml).resolve()
        if not coverage_path.exists():
            raise SystemExit(f"Coverage input does not exist: {coverage_path}")
        files, totals = _parse_coverage_xml(coverage_path)
        source = {"kind": "coverage_xml", "path": _path_for_json(coverage_path)}
    else:
        coverage_path = Path(args.coverage_json).resolve()
        if not coverage_path.exists():
            raise SystemExit(f"Coverage input does not exist: {coverage_path}")
        files, totals = _parse_coverage_json(coverage_path)
        source = {"kind": "coverage_json", "path": _path_for_json(coverage_path)}

    line_threshold = _validate_percent_threshold(args.line_threshold, "--line-threshold")
    branch_threshold = _validate_percent_threshold(args.branch_threshold, "--branch-threshold")
    diff_line_threshold = _validate_percent_threshold(
        args.diff_line_threshold,
        "--diff-line-threshold",
    )

    diff = {"enabled": False, "lines_valid": None, "lines_covered": None, "line_percent": None, "files": []}
    if args.diff_patch:
        patch_path = Path(args.diff_patch).resolve()
        if not patch_path.exists():
            raise SystemExit(f"Diff patch does not exist: {patch_path}")
        changed_lines = _parse_diff_patch(patch_path.read_text(encoding="utf-8"))
        diff = _calculate_diff_coverage(files, changed_lines)
        diff["source"] = {"kind": "diff_patch", "path": _path_for_json(patch_path)}
    elif args.diff_base:
        patch_text = _git_diff_patch(args.diff_base, args.diff_path or [])
        changed_lines = _parse_diff_patch(patch_text)
        diff = _calculate_diff_coverage(files, changed_lines)
        diff["source"] = {
            "kind": "git_diff",
            "base": args.diff_base,
            "paths": args.diff_path or [],
        }

    gates = [
        _coverage_gate("line", totals["line_percent"], line_threshold),
        _coverage_gate(
            "branch",
            totals["branch_percent"],
            branch_threshold,
            reason="branch coverage is unavailable; run pytest with branch coverage enabled",
        ),
    ]
    if diff_line_threshold is not None:
        if not diff["enabled"]:
            gates.append(
                _coverage_gate(
                    "diff_line",
                    None,
                    diff_line_threshold,
                    reason="diff coverage requested but no diff source was provided",
                )
            )
        elif diff["missing_coverage_files"]:
            missing_files_text = json.dumps(diff["missing_coverage_files"], ensure_ascii=False)
            gates.append(
                _coverage_gate(
                    "diff_line",
                    None,
                    diff_line_threshold,
                    reason="changed files are missing from coverage: " + missing_files_text,
                )
            )
        elif diff["lines_valid"] == 0:
            gates.append(
                {
                    "metric": "diff_line",
                    "threshold": diff_line_threshold,
                    "actual": None,
                    "status": "skipped",
                    "reason": "no executable changed lines in diff",
                }
            )
        else:
            gates.append(_coverage_gate("diff_line", diff["line_percent"], diff_line_threshold))

    failed_gates = [gate for gate in gates if gate["status"] == "failed"]
    status = "failed" if failed_gates else "passed"
    output = Path(args.output).resolve()
    snapshot = {
        "schema_version": COVERAGE_SNAPSHOT_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "module": args.module,
        "level": args.level.upper() if args.level else None,
        "title": args.title,
        "run_id": args.run_id,
        "git_commit": _git_commit(),
        "operator": _operator(),
        "status": status,
        "source": source,
        "output_path": _path_for_json(output),
        "totals": totals,
        "diff": diff,
        "quality_gates": gates,
        "failed_gates": failed_gates,
        "files": files,
    }
    _write_json(output, snapshot)
    print(output)
    print(
        "coverage: "
        f"line={totals['line_percent']} "
        f"branch={totals['branch_percent']} "
        f"diff_line={diff.get('line_percent')} "
        f"status={status}"
    )
    if failed_gates:
        for gate in failed_gates:
            print(f"coverage gate failed: {gate['metric']} - {gate['reason']}")
    return 1 if failed_gates and not args.no_fail else 0


def cmd_ports(args: argparse.Namespace) -> int:
    failed = False
    for raw_port in args.ports:
        port = int(raw_port)
        occupied = _is_port_open(port)
        status = "occupied" if occupied else "free"
        print(f"127.0.0.1:{port} {status}")
        if occupied and not args.allow_occupied:
            failed = True
    return 1 if failed else 0


def cmd_services(args: argparse.Namespace) -> int:
    checks = [
        (
            "FastAPI backend",
            f"http://127.0.0.1:{args.backend_port}/openapi.json",
        )
    ]
    if not args.skip_tdx:
        checks.append(
            (
                "TDX realtime minute endpoint",
                f"http://127.0.0.1:{args.tdx_port}/api/kline-all/tdx?code={args.tdx_probe_code}&type=minute1",
            )
        )
    failed = False
    for name, url in checks:
        ok, detail = _http_probe(url, timeout=args.timeout)
        print(f"{name}: {'ok' if ok else 'FAILED'} - {url} - {detail}")
        failed = failed or not ok
    return 1 if failed else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AIstock local validation helper.")
    sub = parser.add_subparsers(dest="command", required=True)

    record = sub.add_parser("record", help="Create a validation run record.")
    record.add_argument("--module", required=True)
    record.add_argument("--level", required=True)
    record.add_argument("--title", required=True)
    record.add_argument("--history-root", default=os.environ.get("AISTOCK_VALIDATION_HISTORY_ROOT"))
    record.add_argument("--json-out")
    record.add_argument("--no-json", action="store_true", help="Keep legacy Markdown-only behavior.")
    record.add_argument(
        "--status",
        default="created",
        choices=["created", "running", "passed", "failed", "partial", "skipped"],
    )
    record.set_defaults(func=cmd_record)

    evidence = sub.add_parser("evidence", help="Create a validation evidence manifest.")
    evidence.add_argument("--module", required=True)
    evidence.add_argument("--level")
    evidence.add_argument("--title")
    evidence.add_argument("--run-id")
    evidence.add_argument("--output", required=True)
    evidence.add_argument("--item", action="append", default=[], help="Evidence item as kind=path.")
    evidence.add_argument("--include", action="append", default=[], help="Generic evidence file or directory.")
    evidence.add_argument("--coverage", action="append", default=[])
    evidence.add_argument("--playwright-report", action="append", default=[])
    evidence.add_argument("--playwright-trace", action="append", default=[])
    evidence.add_argument("--smoke-json", action="append", default=[])
    evidence.add_argument("--db-smoke", action="append", default=[])
    evidence.add_argument("--artifact", action="append", default=[])
    evidence.add_argument("--fail-missing", action="store_true")
    evidence.set_defaults(func=cmd_evidence)

    coverage = sub.add_parser("coverage", help="Parse coverage output and write a gate snapshot.")
    source_group = coverage.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--coverage-xml", help="Coverage.py Cobertura XML report.")
    source_group.add_argument("--coverage-json", help="Coverage.py JSON report.")
    coverage.add_argument("--module", required=True)
    coverage.add_argument("--level")
    coverage.add_argument("--title")
    coverage.add_argument("--run-id")
    coverage.add_argument("--output", required=True)
    coverage.add_argument("--line-threshold", type=float)
    coverage.add_argument("--branch-threshold", type=float)
    coverage.add_argument("--diff-line-threshold", type=float)
    coverage.add_argument("--diff-patch", help="Unified diff patch file for diff coverage.")
    coverage.add_argument("--diff-base", help="Git base ref for changed-line diff coverage.")
    coverage.add_argument(
        "--diff-path",
        action="append",
        default=[],
        help="Restrict git diff coverage to a path; can be repeated.",
    )
    coverage.add_argument(
        "--no-fail",
        action="store_true",
        help="Write the snapshot but return zero even when configured coverage gates fail.",
    )
    coverage.set_defaults(func=cmd_coverage)

    ports = sub.add_parser("ports", help="Check localhost port occupancy.")
    ports.add_argument("--allow-occupied", action="store_true")
    ports.add_argument("ports", nargs="+")
    ports.set_defaults(func=cmd_ports)

    services = sub.add_parser("services", help="Fail-fast check required local validation services.")
    services.add_argument("--backend-port", default=os.environ.get("BACKEND_PORT", "8012"))
    services.add_argument("--tdx-port", default=os.environ.get("TDX_HTTP_PORT", "19080"))
    services.add_argument("--tdx-probe-code", default=os.environ.get("TDX_PROBE_CODE", "SZ000001"))
    services.add_argument("--timeout", type=float, default=5.0)
    services.add_argument(
        "--skip-tdx",
        action="store_true",
        default=os.environ.get("PAPER_V2_SKIP_REALTIME") == "1",
        help="Skip TDX realtime probing for non-realtime validation runs.",
    )
    services.set_defaults(func=cmd_services)

    return parser


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
