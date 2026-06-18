from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "aistock_discovery_input_pack_v1"
DIFF_HEADER_PREFIXES = ("--- ", "+++ ", "@@", "diff --git ", "index ")
BOM_MOJIBAKE_PREFIX_REPLACEMENTS = {
    "\u9518\u7e1c": "a",
    "\u9518\u7e1d": "b",
    "\u9518\u7e1e": "c",
    "\u9518\u7e1f": "d",
    "\u9518\u7e20": "e",
    "\u9518\u7e21": "f",
    "\u9518\u7e22": "g",
    "\u9518\u7e23": "h",
    "\u9518\u7e24": "i",
    "\u9518\u7e25": "j",
    "\u9518\u7e26": "k",
    "\u9518\u7e27": "l",
    "\u9518\u7e28": "m",
    "\u9518\u7e29": "n",
    "\u9518\u7e2a": "o",
    "\u9518\u7e2b": "p",
    "\u9518\u7e2c": "q",
    "\u9518\u7e2d": "r",
    "\u9518\u7e2e": "s",
    "\u9518\u7e2f": "t",
    "\u9518\u7e30": "u",
    "\u9518\u7e31": "v",
    "\u9518\u7e32": "w",
    "\u9518\u7e33": "x",
    "\u9518\u7e34": "y",
    "\u9518\u7e35": "z",
    "\u9518\ufffd": "",
}
NOISE_TOKENS = {
    "changes",
    "changes:",
    "files",
    "files:",
    "changed files",
    "changed-files",
    "--- changes ---",
    "no changes",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_json(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def split_csv(values: list[str] | None) -> list[str]:
    result: list[str] = []
    for value in values or []:
        result.extend(item.strip() for item in str(value).split(",") if item.strip())
    return result


def normalize_repo_path(value: str | Path) -> str:
    text = str(value or "").strip().lstrip("\ufeff").replace("\\", "/")
    for prefix, replacement in BOM_MOJIBAKE_PREFIX_REPLACEMENTS.items():
        if text.startswith(prefix):
            text = replacement + text.removeprefix(prefix)
            break
    text = re.sub(r"\s+", " ", text).strip()
    while text.startswith("./"):
        text = text[2:]
    if text.startswith("a/") or text.startswith("b/"):
        text = text[2:]
    return text


def is_probable_repo_path(value: str) -> bool:
    text = normalize_repo_path(value)
    if not text or "\x00" in text:
        return False
    lowered = text.lower().strip()
    if lowered in NOISE_TOKENS:
        return False
    if lowered.startswith(("http://", "https://")):
        return False
    if any(text.startswith(prefix) for prefix in DIFF_HEADER_PREFIXES):
        return False
    if text.startswith(("/", "\\")):
        return False
    first_part = text.split("/", 1)[0]
    if ":" in first_part:
        return False
    if text.startswith(("+", "-")) and not text.startswith(("+.", "-.")):
        return False
    if " " in text and "/" not in text:
        return False
    return bool(Path(text).suffix or "/" in text)


def unique_repo_paths(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        item = normalize_repo_path(value)
        if item and item not in seen and is_probable_repo_path(item):
            seen.add(item)
            result.append(item)
    return result


def read_changed_files_file(path: Path | None) -> list[str]:
    if path is None or not path.exists():
        return []
    try:
        return path.read_text(encoding="utf-8-sig", errors="replace").splitlines()
    except OSError:
        return []


def git_changed_files(base_ref: str | None, *, root: Path = ROOT) -> list[str]:
    if not base_ref:
        return []
    proc = subprocess.run(
        ["git", "diff", "--name-only", str(base_ref), "HEAD"],
        cwd=str(root),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return []
    return proc.stdout.splitlines()


def collect_changed_files(
    *,
    changed_files: list[str] | None = None,
    changed_files_file: Path | None = None,
    base_ref: str | None = None,
    root: Path = ROOT,
) -> list[str]:
    collected: list[str] = []
    collected.extend(split_csv(changed_files))
    collected.extend(read_changed_files_file(changed_files_file))
    collected.extend(git_changed_files(base_ref, root=root))
    return unique_repo_paths(collected)


def git_snapshot(root: Path = ROOT) -> dict[str, Any]:
    def run_git(args: list[str]) -> str | None:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(root),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
        )
        return proc.stdout.strip() if proc.returncode == 0 else None

    dirty = run_git(["status", "--porcelain=v1"]) or ""
    return {
        "branch": run_git(["branch", "--show-current"]),
        "head": run_git(["rev-parse", "HEAD"]),
        "origin_main": run_git(["rev-parse", "origin/main"]),
        "dirty": bool(dirty),
        "dirty_count": len([line for line in dirty.splitlines() if line.strip()]),
    }


def build_discovery_input_pack(
    *,
    run_id: str | None = None,
    changed_files: list[str] | None = None,
    changed_files_file: Path | None = None,
    base_ref: str | None = None,
    module: str | None = None,
    allowed_plan_keys: list[str] | None = None,
    codegraph_freshness_json: Path | None = None,
    code_intelligence_json: Path | None = None,
    root: Path = ROOT,
) -> dict[str, Any]:
    changed = collect_changed_files(
        changed_files=changed_files,
        changed_files_file=changed_files_file,
        base_ref=base_ref,
        root=root,
    )
    snapshot = git_snapshot(root)
    commit = snapshot.get("head")
    run = str(run_id or os.environ.get("GITHUB_RUN_ID") or "local")
    artifact_root = Path("tmp") / "validation" / "nightly_discovery" / run
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run,
        "generated_at": utc_now(),
        "commit": commit,
        "branch": snapshot.get("branch"),
        "root": str(root),
        "module": module or "validation",
        "changed_files": changed,
        "changed_files_count": len(changed),
        "input_quality": {
            "changed_files_status": "ok" if changed else "empty",
            "noise_filtered": True,
            "path_encoding": "utf-8-sig-safe",
        },
        "recent_failures": [],
        "recent_bug_clusters": [],
        "codegraph_refs": {
            "freshness_json": str(codegraph_freshness_json) if codegraph_freshness_json else None,
            "code_intelligence_json": str(code_intelligence_json) if code_intelligence_json else None,
        },
        "understand_anything_refs": {},
        "allowed_plan_keys": allowed_plan_keys or ["l0", "validation_module_registry_l0"],
        "readonly_runtime_targets": [],
        "stop_conditions": [
            "no_production_db_write",
            "no_production_runtime_restart",
            "allowlisted_plans_only",
        ],
        "artifact_refs": {
            "artifact_root": str(artifact_root).replace("\\", "/"),
            "changed_files_txt": str(artifact_root / "nightly-changed-files.txt").replace("\\", "/"),
            "input_pack_json": str(artifact_root / "discovery-input-pack.json").replace("\\", "/"),
        },
        "production_gates": {
            "production_ddl_gate": "noop",
            "production_frontend_dependency_gate": "noop",
            "production_backend_dependency_gate": "noop",
        },
    }


def render_changed_files_text(changed_files: list[str]) -> str:
    return "" if not changed_files else "\n".join(changed_files) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build AIstock Nightly active-discovery input pack.")
    parser.add_argument("--run-id")
    parser.add_argument("--changed-file", action="append", default=None)
    parser.add_argument("--changed-files-file")
    parser.add_argument("--base-ref")
    parser.add_argument("--module", default="validation")
    parser.add_argument("--allowed-plan-key", action="append", default=None)
    parser.add_argument("--codegraph-freshness-json")
    parser.add_argument("--code-intelligence-json")
    parser.add_argument("--root")
    parser.add_argument("--output")
    parser.add_argument("--changed-files-output")
    parser.add_argument("--json", action="store_true", help="Emit compact JSON stdout instead of one-line status.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.root) if args.root else ROOT
    payload = build_discovery_input_pack(
        run_id=args.run_id,
        changed_files=args.changed_file,
        changed_files_file=Path(args.changed_files_file) if args.changed_files_file else None,
        base_ref=args.base_ref,
        module=args.module,
        allowed_plan_keys=list(args.allowed_plan_key or []) or None,
        codegraph_freshness_json=Path(args.codegraph_freshness_json) if args.codegraph_freshness_json else None,
        code_intelligence_json=Path(args.code_intelligence_json) if args.code_intelligence_json else None,
        root=root,
    )
    write_json(Path(args.output) if args.output else None, payload)
    if args.changed_files_output:
        output = Path(args.changed_files_output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(render_changed_files_text(payload["changed_files"]), encoding="utf-8")
    if args.json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")
    else:
        sys.stdout.write(
            "PASS discovery-input-pack "
            f"changed_files={payload['changed_files_count']} "
            f"input_pack={payload.get('artifact_refs', {}).get('input_pack_json')} "
            f"changed_files_ref={payload.get('artifact_refs', {}).get('changed_files_txt')}\n"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
