from __future__ import annotations

import fnmatch
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml

from backend.services.validation.module_registry import (
    DEFAULT_MODULE_REGISTRY_PATH,
    ModuleRegistry,
    ModuleRegistryError,
    REPO_ROOT,
    RISK_LEVELS,
)


DEFAULT_FILE_OWNERSHIP_PATH = REPO_ROOT / "tests" / "aistock_validation" / "catalog" / "file_ownership.yaml"
FILE_OWNERSHIP_SCHEMA = "aistock_file_ownership_v1"
OWNERSHIP_SCAN_SCHEMA = "aistock_module_ownership_scan_v1"
OWNERSHIP_STATUSES = {"mapped", "unmapped", "ambiguous"}


class FileOwnershipError(ValueError):
    """Raised when file ownership rules are invalid."""


@dataclass(frozen=True)
class FileOwnershipRule:
    rule_id: str
    include: tuple[str, ...]
    exclude: tuple[str, ...]
    primary_module: str
    impact_modules: tuple[str, ...]
    layer: str
    risk_level: str
    priority: int = 0
    ownership_reason: str = ""

    def matches(self, path_key: str) -> bool:
        return _matches_any(path_key, self.include) and not _matches_any(path_key, self.exclude)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "include": list(self.include),
            "exclude": list(self.exclude),
            "primary_module": self.primary_module,
            "impact_modules": list(self.impact_modules),
            "layer": self.layer,
            "risk_level": self.risk_level,
            "priority": self.priority,
            "ownership_reason": self.ownership_reason,
        }


@dataclass(frozen=True)
class PathOwnership:
    path: str
    ownership_status: str
    primary_module: str | None
    impact_modules: tuple[str, ...]
    layer: str | None
    risk_level: str | None
    matched_rule_ids: tuple[str, ...]
    reason_codes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "ownership_status": self.ownership_status,
            "primary_module": self.primary_module,
            "impact_modules": list(self.impact_modules),
            "layer": self.layer,
            "risk_level": self.risk_level,
            "matched_rule_ids": list(self.matched_rule_ids),
            "reason_codes": list(self.reason_codes),
        }


def _as_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value.strip(),) if value.strip() else ()
    if isinstance(value, list | tuple):
        return tuple(str(item).strip().replace("\\", "/") for item in value if str(item).strip())
    raise FileOwnershipError(f"Expected a string or list, got {type(value).__name__}.")


def _normalize_path(path: str | Path, *, repo_root: Path | None = None) -> str:
    raw = str(path).replace("\\", "/")
    candidate = Path(raw)
    root = Path(repo_root or REPO_ROOT)
    try:
        return candidate.resolve().relative_to(root.resolve()).as_posix()
    except (OSError, ValueError):
        return raw.lstrip("./")


def _repo_path(path: Path) -> str:
    return _normalize_path(path, repo_root=REPO_ROOT)


def _matches(pattern: str, path_key: str) -> bool:
    if fnmatch.fnmatchcase(path_key, pattern):
        return True
    # Let "dir/**" match the directory itself and "**" match zero path segments.
    if pattern.endswith("/**") and path_key == pattern[:-3]:
        return True
    if "/**/" in pattern and fnmatch.fnmatchcase(path_key, pattern.replace("/**/", "/")):
        return True
    return False


def _matches_any(path_key: str, patterns: Iterable[str]) -> bool:
    return any(_matches(pattern, path_key) for pattern in patterns)


def _git_output(repo_root: Path, args: list[str]) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        shell=False,
        check=False,
        timeout=30,
    )
    if completed.returncode != 0:
        args_text = json.dumps(args, ensure_ascii=False)
        raise FileOwnershipError(f"git command {args_text} failed: {(completed.stderr or '').strip()}")
    return completed.stdout or ""


def _git_paths(repo_root: Path, args: list[str]) -> list[str]:
    output = _git_output(repo_root, args)
    return [line.strip().replace("\\", "/") for line in output.splitlines() if line.strip()]


class FileOwnershipCatalog:
    """Read, validate, and apply deterministic file-to-module ownership rules."""

    def __init__(
        self,
        catalog_path: Path | None = None,
        *,
        module_registry: ModuleRegistry | None = None,
    ) -> None:
        self.catalog_path = Path(catalog_path or DEFAULT_FILE_OWNERSHIP_PATH)
        self.module_registry = module_registry or ModuleRegistry(DEFAULT_MODULE_REGISTRY_PATH)
        self._rules_cache: list[FileOwnershipRule] | None = None

    def load(self) -> dict[str, Any]:
        if not self.catalog_path.exists():
            return {
                "schema_version": FILE_OWNERSHIP_SCHEMA,
                "catalog_path": _repo_path(self.catalog_path),
                "missing": True,
                "rules": [],
            }
        try:
            payload = yaml.safe_load(self.catalog_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            raise FileOwnershipError(f"Invalid file ownership YAML: {exc}") from exc
        if not isinstance(payload, dict):
            raise FileOwnershipError("File ownership catalog root must be a mapping.")
        schema_version = str(payload.get("schema_version") or "")
        if schema_version != FILE_OWNERSHIP_SCHEMA:
            raise FileOwnershipError(f"Unsupported file ownership schema: {schema_version!r}.")
        raw_rules = payload.get("rules")
        if not isinstance(raw_rules, list):
            raise FileOwnershipError("File ownership field 'rules' must be a list.")
        try:
            module_ids = self.module_registry.module_ids()
        except ModuleRegistryError as exc:
            raise FileOwnershipError(str(exc)) from exc
        rules = [self._validate_rule(item, module_ids=module_ids) for item in raw_rules]
        self._validate_unique_rules(rules)
        return {
            "schema_version": schema_version,
            "catalog_path": _repo_path(self.catalog_path),
            "missing": False,
            "rules": [rule.to_dict() for rule in rules],
        }

    def list_rules(self) -> list[FileOwnershipRule]:
        if self._rules_cache is not None:
            return list(self._rules_cache)
        loaded = self.load()
        rules = [
            FileOwnershipRule(
                rule_id=item["rule_id"],
                include=tuple(item["include"]),
                exclude=tuple(item["exclude"]),
                primary_module=item["primary_module"],
                impact_modules=tuple(item["impact_modules"]),
                layer=item["layer"],
                risk_level=item["risk_level"],
                priority=int(item["priority"]),
                ownership_reason=item.get("ownership_reason") or "",
            )
            for item in loaded["rules"]
        ]
        self._rules_cache = rules
        return list(rules)

    def match_path(self, path: str | Path) -> PathOwnership:
        path_key = _normalize_path(path)
        matched = [rule for rule in self.list_rules() if rule.matches(path_key)]
        if not matched:
            return PathOwnership(
                path=path_key,
                ownership_status="unmapped",
                primary_module=None,
                impact_modules=(),
                layer=None,
                risk_level=None,
                matched_rule_ids=(),
                reason_codes=("no_matching_file_ownership_rule",),
            )
        max_priority = max(rule.priority for rule in matched)
        top_rules = [rule for rule in matched if rule.priority == max_priority]
        primary_modules = sorted({rule.primary_module for rule in top_rules})
        if len(primary_modules) != 1:
            return PathOwnership(
                path=path_key,
                ownership_status="ambiguous",
                primary_module=None,
                impact_modules=tuple(sorted({module for rule in top_rules for module in rule.impact_modules})),
                layer=None,
                risk_level=max((rule.risk_level for rule in top_rules), key=_risk_rank),
                matched_rule_ids=tuple(rule.rule_id for rule in top_rules),
                reason_codes=("multiple_primary_modules_at_same_priority",),
            )
        primary_rule = top_rules[0]
        impact_modules = sorted({module for rule in top_rules for module in rule.impact_modules})
        return PathOwnership(
            path=path_key,
            ownership_status="mapped",
            primary_module=primary_modules[0],
            impact_modules=tuple(impact_modules),
            layer=primary_rule.layer,
            risk_level=max((rule.risk_level for rule in top_rules), key=_risk_rank),
            matched_rule_ids=tuple(rule.rule_id for rule in top_rules),
        )

    def scan_paths(self, paths: Iterable[str | Path]) -> dict[str, Any]:
        items = [self.match_path(path).to_dict() for path in paths]
        return self._scan_payload(items, source="paths")

    def scan_repository(
        self,
        *,
        repo_root: Path | None = None,
        include_tracked: bool = True,
        include_untracked: bool = False,
    ) -> dict[str, Any]:
        root = Path(repo_root or REPO_ROOT)
        paths: list[str] = []
        if include_tracked:
            paths.extend(_git_paths(root, ["ls-files"]))
        if include_untracked:
            paths.extend(_git_paths(root, ["ls-files", "--others", "--exclude-standard"]))
        unique_paths = list(dict.fromkeys(paths))
        items = [self.match_path(path).to_dict() for path in unique_paths]
        return self._scan_payload(items, source="repository")

    @staticmethod
    def _validate_rule(raw_rule: Any, *, module_ids: set[str]) -> FileOwnershipRule:
        if not isinstance(raw_rule, dict):
            raise FileOwnershipError("Each file ownership rule must be a mapping.")
        rule_id = str(raw_rule.get("rule_id") or "").strip()
        if not rule_id:
            raise FileOwnershipError("File ownership rule is missing rule_id.")
        include = _as_tuple(raw_rule.get("include"))
        if not include:
            raise FileOwnershipError(f"File ownership rule {rule_id} must include at least one glob.")
        primary_module = str(raw_rule.get("primary_module") or "").strip()
        if primary_module not in module_ids:
            raise FileOwnershipError(f"Rule {rule_id} references unknown primary_module={primary_module!r}.")
        impact_modules = _as_tuple(raw_rule.get("impact_modules"))
        unknown_impacts = sorted(module for module in impact_modules if module not in module_ids)
        if unknown_impacts:
            raise FileOwnershipError(f"Rule {rule_id} references unknown impact_modules={unknown_impacts!r}.")
        layer = str(raw_rule.get("layer") or "").strip()
        if not layer:
            raise FileOwnershipError(f"Rule {rule_id} is missing layer.")
        risk_level = str(raw_rule.get("risk_level") or "").strip()
        if risk_level not in RISK_LEVELS:
            raise FileOwnershipError(f"Rule {rule_id} has invalid risk_level={risk_level!r}.")
        return FileOwnershipRule(
            rule_id=rule_id,
            include=include,
            exclude=_as_tuple(raw_rule.get("exclude")),
            primary_module=primary_module,
            impact_modules=impact_modules,
            layer=layer,
            risk_level=risk_level,
            priority=int(raw_rule.get("priority") or 0),
            ownership_reason=str(raw_rule.get("ownership_reason") or "").strip(),
        )

    @staticmethod
    def _validate_unique_rules(rules: list[FileOwnershipRule]) -> None:
        seen: set[str] = set()
        for rule in rules:
            if rule.rule_id in seen:
                raise FileOwnershipError(f"Duplicate file ownership rule_id: {rule.rule_id}.")
            seen.add(rule.rule_id)

    @staticmethod
    def _scan_payload(items: list[dict[str, Any]], *, source: str) -> dict[str, Any]:
        status_counts = {status: 0 for status in sorted(OWNERSHIP_STATUSES)}
        by_module: dict[str, dict[str, Any]] = {}
        for item in items:
            status = str(item.get("ownership_status") or "unmapped")
            status_counts[status] = status_counts.get(status, 0) + 1
            primary_module = item.get("primary_module")
            if primary_module:
                module_bucket = by_module.setdefault(
                    str(primary_module),
                    {"module_id": primary_module, "file_count": 0, "risk_levels": {}},
                )
                module_bucket["file_count"] += 1
                risk = str(item.get("risk_level") or "unknown")
                module_bucket["risk_levels"][risk] = module_bucket["risk_levels"].get(risk, 0) + 1
        return {
            "schema_version": OWNERSHIP_SCAN_SCHEMA,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "totals": {
                "files": len(items),
                "mapped_files": status_counts.get("mapped", 0),
                "unmapped_files": status_counts.get("unmapped", 0),
                "ambiguous_files": status_counts.get("ambiguous", 0),
            },
            "by_status": status_counts,
            "by_module": sorted(by_module.values(), key=lambda item: item["module_id"]),
            "items": sorted(items, key=lambda item: item["path"]),
        }


def _risk_rank(risk_level: str) -> int:
    order = {"low": 1, "medium": 2, "high": 3, "critical": 4}
    return order.get(risk_level, 0)


def write_scan_outputs(payload: dict[str, Any], *, output_json: Path | None, summary_md: Path | None) -> None:
    if output_json:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if summary_md:
        summary_md.parent.mkdir(parents=True, exist_ok=True)
        totals = payload.get("totals") or {}
        lines = [
            "# AIstock Module Ownership Scan",
            "",
            f"- Schema: `{payload.get('schema_version')}`",
            f"- Generated at: `{payload.get('generated_at')}`",
            f"- Source: `{payload.get('source')}`",
            f"- Files: {totals.get('files', 0)}",
            f"- Mapped: {totals.get('mapped_files', 0)}",
            f"- Unmapped: {totals.get('unmapped_files', 0)}",
            f"- Ambiguous: {totals.get('ambiguous_files', 0)}",
            "",
            "## Unmapped Or Ambiguous Files",
            "",
        ]
        problem_items = [
            item
            for item in payload.get("items", [])
            if item.get("ownership_status") in {"unmapped", "ambiguous"}
        ]
        if not problem_items:
            lines.append("No unmapped or ambiguous files were found.")
        else:
            for item in problem_items[:200]:
                reason_text = str(item.get("reason_codes") or [])
                lines.append(
                    f"- `{item.get('path')}`: {item.get('ownership_status')} "
                    f"({reason_text})"
                )
        summary_md.parent.mkdir(parents=True, exist_ok=True)
        with summary_md.open("w", encoding="utf-8") as handle:
            for line in lines:
                handle.write(f"{line}\n")
