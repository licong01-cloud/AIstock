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
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "tests" / "aistock_validation" / "templates" / "test_run_record.md"
DEFAULT_HISTORY_ROOT = ROOT / "tests" / "aistock_validation" / "history"
RUN_METADATA_SCHEMA_VERSION = "aistock_validation_run_v1"
EVIDENCE_MANIFEST_SCHEMA_VERSION = "aistock_validation_evidence_manifest_v1"


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
                "line": None,
                "branch": None,
                "diff_line": None,
                "diff_branch": None,
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
