from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name


class RequirementDeltaError(RuntimeError):
    """The changed requirements surface cannot be validated safely."""


@dataclass(frozen=True)
class ParsedRequirement:
    identity: str
    line: str
    portable_constraint: bool


def decode_requirements(payload: bytes, *, label: str) -> str:
    if not payload:
        return ""
    if payload.startswith((b"\xff\xfe", b"\xfe\xff")):
        encoding = "utf-16"
    elif b"\x00" in payload[:256]:
        encoding = "utf-16-le"
    else:
        encoding = "utf-8-sig"
    try:
        return payload.decode(encoding)
    except UnicodeDecodeError as exc:
        raise RequirementDeltaError(f"{label} is not valid {encoding}") from exc


def _requirement_text(line: str) -> str:
    return line.split(" #", 1)[0].strip()


def parse_requirements(text: str, *, label: str) -> dict[str, ParsedRequirement]:
    parsed: dict[str, ParsedRequirement] = {}
    for line_number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(("-r ", "--requirement ", "-c ", "--constraint ")):
            raise RequirementDeltaError(
                f"{label}:{line_number} nested requirement/constraint files are not supported by the delta validator"
            )
        if line.startswith("-"):
            identity = f"option:{line.split(maxsplit=1)[0]}"
            item = ParsedRequirement(identity=identity, line=line, portable_constraint=False)
        else:
            try:
                requirement = Requirement(_requirement_text(line))
            except InvalidRequirement as exc:
                raise RequirementDeltaError(f"{label}:{line_number} is not a valid PEP 508 requirement") from exc
            identity = f"distribution:{canonicalize_name(requirement.name)}"
            portable = requirement.url is None or not requirement.url.lower().startswith("file:")
            item = ParsedRequirement(identity=identity, line=line, portable_constraint=portable)
        if identity in parsed:
            raise RequirementDeltaError(f"{label}:{line_number} duplicates requirement identity {identity}")
        parsed[identity] = item
    return parsed


def build_delta(base_text: str, head_text: str, *, label: str) -> dict[str, object]:
    base = parse_requirements(base_text, label=f"{label}@base")
    head = parse_requirements(head_text, label=f"{label}@head")
    changed = [head[key].line for key in sorted(head) if key not in base or head[key].line != base[key].line]
    removed = [key for key in sorted(base) if key not in head]
    constraints = [item.line for key, item in sorted(head.items()) if item.portable_constraint]
    excluded = [
        key for key, item in sorted(head.items()) if key.startswith("distribution:") and not item.portable_constraint
    ]
    return {
        "changed_requirements": changed,
        "removed_identities": removed,
        "portable_constraints": constraints,
        "excluded_nonportable_constraint_identities": excluded,
    }


def _git_blob(repo_root: Path, revision: str, path: str, *, allow_missing: bool) -> bytes:
    revision_check = subprocess.run(
        ["git", "cat-file", "-e", f"{revision}^{{commit}}"],
        cwd=repo_root,
        check=False,
        capture_output=True,
    )
    if revision_check.returncode != 0:
        raise RequirementDeltaError(f"git revision is unavailable: {revision}")
    completed = subprocess.run(
        ["git", "show", f"{revision}:{path}"],
        cwd=repo_root,
        check=False,
        capture_output=True,
    )
    if completed.returncode == 0:
        return completed.stdout
    if allow_missing:
        return b""
    stderr = completed.stderr.decode("utf-8", errors="replace").strip()
    raise RequirementDeltaError(f"cannot read {revision}:{path}: {stderr}")


def _write_lines(path: Path, lines: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "" if not lines else "\n".join(lines) + "\n"
    path.write_text(payload, encoding="utf-8", newline="\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract the portable changed Python requirement surface.")
    parser.add_argument("--base-commit", required=True)
    parser.add_argument("--head-commit", required=True)
    parser.add_argument("--file", required=True)
    parser.add_argument("--requirements-output", type=Path, required=True)
    parser.add_argument("--constraints-output", type=Path, required=True)
    parser.add_argument("--summary-output", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.repo_root.resolve()
    base = decode_requirements(
        _git_blob(root, args.base_commit, args.file, allow_missing=True),
        label=f"{args.base_commit}:{args.file}",
    )
    head = decode_requirements(
        _git_blob(root, args.head_commit, args.file, allow_missing=False),
        label=f"{args.head_commit}:{args.file}",
    )
    result = build_delta(base, head, label=args.file)
    _write_lines(args.requirements_output, result["changed_requirements"])
    _write_lines(args.constraints_output, result["portable_constraints"])
    summary = {
        "schema_version": "aistock_changed_requirements_validation_v1",
        "base_commit": args.base_commit,
        "head_commit": args.head_commit,
        "file": args.file,
        "changed_requirement_count": len(result["changed_requirements"]),
        "removed_requirement_count": len(result["removed_identities"]),
        "portable_constraint_count": len(result["portable_constraints"]),
        "excluded_nonportable_constraint_identities": result["excluded_nonportable_constraint_identities"],
    }
    args.summary_output.parent.mkdir(parents=True, exist_ok=True)
    args.summary_output.write_text(
        json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
