from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

BUG_ID_RE = re.compile(r"^BUG-(\d{3,})$")
BUG_FILE_RE = re.compile(r"BUG-(\d{3,})")
ALLOWED_CLOSE_SYNC_STATUSES = {"fixed", "closed", "verified"}


def _normalize_path(value: str) -> str:
    normalized = value.replace("\\", "/").strip()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _load_changed_files(path: str | None, values: list[str]) -> list[str]:
    items = [_normalize_path(item) for item in values]
    if path:
        source = Path(path)
        if source.exists():
            items.extend(_normalize_path(line) for line in source.read_text(encoding="utf-8").splitlines())
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item and item not in seen:
            result.append(item)
            seen.add(item)
    return result


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        raise ValueError(f"invalid JSON: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return payload


def _bug_number(value: str | None) -> int | None:
    if not value:
        return None
    match = BUG_ID_RE.match(str(value).strip())
    return int(match.group(1)) if match else None


def _bug_number_from_path(path: str) -> int | None:
    match = BUG_FILE_RE.search(path)
    return int(match.group(1)) if match else None


def _validate_bug_json(path: Path, rel_path: str, *, close_sync_only: bool) -> tuple[dict[str, Any], list[str]]:
    payload = _read_json(path)
    blocking: list[str] = []
    bug_id = str(payload.get("bug_id") or "").strip()
    number = _bug_number(bug_id)
    path_number = _bug_number_from_path(rel_path)
    if number is None:
        blocking.append(f"{rel_path}: missing or invalid bug_id")
    if path_number is not None and number is not None and path_number != number:
        blocking.append(f"{rel_path}: bug_id does not match filename")
    if payload.get("schema_version") != "aistock_validation_bug_v1":
        blocking.append(f"{rel_path}: unsupported schema_version")
    if not str(payload.get("github_issue_number") or "").strip():
        blocking.append(f"{rel_path}: missing github_issue_number")
    if not str(payload.get("github_issue_url") or "").startswith("https://github.com/licong01-cloud/AIstock/issues/"):
        blocking.append(f"{rel_path}: missing or invalid github_issue_url")
    for field in ("production_ddl_gate", "production_frontend_dependency_gate", "production_backend_dependency_gate"):
        if not str(payload.get(field) or "").strip():
            blocking.append(f"{rel_path}: missing {field}")
    if close_sync_only:
        status = str(payload.get("status") or "").strip().lower()
        if status not in ALLOWED_CLOSE_SYNC_STATUSES:
            blocking.append(f"{rel_path}: status={status or 'missing'} is not close-sync metadata")
    return payload, blocking


def check_bug_registry_metadata(
    *,
    repo_root: Path,
    changed_files: list[str],
    close_sync_only: bool = False,
) -> dict[str, Any]:
    bug_files = [
        path
        for path in changed_files
        if path.startswith("tests/aistock_validation/bugs/")
        and path.endswith(".json")
        and not Path(path).name.startswith(".")
    ]
    allocator_changed = "tests/aistock_validation/bugs/.bug_id_allocator.json" in changed_files
    blocking: list[str] = []
    warnings: list[str] = []
    bug_ids: list[str] = []
    bug_numbers: list[int] = []

    for rel_path in bug_files:
        path = repo_root / rel_path
        if not path.exists():
            blocking.append(f"{rel_path}: file does not exist")
            continue
        try:
            payload, file_blocking = _validate_bug_json(path, rel_path, close_sync_only=close_sync_only)
        except ValueError as exc:
            blocking.append(str(exc))
            continue
        blocking.extend(file_blocking)
        bug_id = str(payload.get("bug_id") or "").strip()
        number = _bug_number(bug_id)
        if bug_id:
            bug_ids.append(bug_id)
        if number is not None:
            bug_numbers.append(number)

    allocator_payload: dict[str, Any] | None = None
    if allocator_changed:
        allocator_path = repo_root / "tests/aistock_validation/bugs/.bug_id_allocator.json"
        try:
            allocator_payload = _read_json(allocator_path)
        except ValueError as exc:
            blocking.append(str(exc))
        else:
            last_allocated = int(allocator_payload.get("last_allocated") or 0)
            max_bug_number = max(bug_numbers, default=0)
            if max_bug_number > last_allocated:
                blocking.append(
                    f"allocator last_allocated={last_allocated} is behind changed BUG max={max_bug_number}"
                )
            if close_sync_only:
                warnings.append("allocator changed in close-sync registry lane; this is allowed only for same-task metadata PRs")

    if close_sync_only and allocator_changed and not bug_files:
        blocking.append("close-sync metadata lane changed allocator without BUG JSON")

    return {
        "schema_version": "aistock_bug_registry_metadata_check_v1",
        "workflow_gate": "passed" if not blocking else "blocked",
        "repo_root": str(repo_root),
        "changed_files": changed_files,
        "bug_files": bug_files,
        "bug_ids": bug_ids,
        "allocator_changed": allocator_changed,
        "allocator_last_allocated": (allocator_payload or {}).get("last_allocated") if allocator_payload else None,
        "close_sync_only": close_sync_only,
        "blocking": blocking,
        "warnings": warnings,
    }


def _write_json(path: str, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate AIstock BUG registry metadata for fast CI lanes.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--changed-file", action="append", default=[])
    parser.add_argument("--changed-files-file")
    parser.add_argument("--close-sync-only", action="store_true")
    parser.add_argument("--output-json")
    args = parser.parse_args(argv)

    payload = check_bug_registry_metadata(
        repo_root=Path(args.repo_root),
        changed_files=_load_changed_files(args.changed_files_file, args.changed_file),
        close_sync_only=args.close_sync_only,
    )
    if args.output_json:
        _write_json(args.output_json, payload)
    print(
        "BUG registry metadata check: "
        f"gate={payload['workflow_gate']} files={len(payload['bug_files'])} "
        f"allocator_changed={str(bool(payload['allocator_changed'])).lower()} blocking={len(payload['blocking'])}"
    )
    if payload["blocking"]:
        for item in payload["blocking"][:20]:
            print(f"- {item}", file=sys.stderr)
    return 0 if payload["workflow_gate"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
