from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_GUARDRAIL_ROOT = REPO_ROOT / "tmp" / "validation" / "guardrails"
DEFAULT_LEGACY_ROOT = REPO_ROOT / "tmp" / "validation" / "legacy_inventory"
DEFAULT_BUG_ROOT = REPO_ROOT / "tests" / "aistock_validation" / "bugs"

GUARDRAIL_SCHEMA = "aistock_guardrail_scan_result_v1"
LEGACY_SCHEMA = "aistock_legacy_inventory_v1"
BUG_SCHEMA = "aistock_validation_bug_v1"
MAX_JSON_BYTES = 16 * 1024 * 1024


class ValidationFindingStore:
    """Read quality findings and bug registry records from owned local evidence files."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        guardrail_root: Path | None = None,
        legacy_root: Path | None = None,
        bug_root: Path | None = None,
    ) -> None:
        self.repo_root = Path(repo_root or REPO_ROOT).resolve()
        self.guardrail_root = Path(guardrail_root or DEFAULT_GUARDRAIL_ROOT).resolve()
        self.legacy_root = Path(legacy_root or DEFAULT_LEGACY_ROOT).resolve()
        self.bug_root = Path(bug_root or DEFAULT_BUG_ROOT).resolve()

    def health(self) -> dict[str, Any]:
        findings = self.list_findings(page_size=1)
        bugs = self.list_bugs(page_size=1)
        return {
            "mode": "read_only",
            "guardrail_root": self._repo_path(self.guardrail_root),
            "guardrail_root_exists": self.guardrail_root.exists(),
            "legacy_root": self._repo_path(self.legacy_root),
            "legacy_root_exists": self.legacy_root.exists(),
            "bug_root": self._repo_path(self.bug_root),
            "bug_root_exists": self.bug_root.exists(),
            "finding_count": findings["total"],
            "bug_count": bugs["total"],
            "parse_errors": self._source_parse_errors(),
        }

    def list_findings(
        self,
        *,
        source_type: str | None = None,
        module: str | None = None,
        severity: str | None = None,
        status: str | None = None,
        search: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        items = self._load_findings()
        if source_type:
            source_l = source_type.lower()
            items = [item for item in items if source_l in str(item.get("source_type") or "").lower()]
        if module:
            module_l = module.lower()
            items = [item for item in items if module_l in str(item.get("module") or "").lower()]
        if severity:
            severity_u = severity.upper()
            items = [item for item in items if str(item.get("severity") or "").upper() == severity_u]
        if status:
            status_l = status.lower()
            items = [item for item in items if str(item.get("status") or "").lower() == status_l]
        if search:
            needle = search.lower()
            items = [
                item
                for item in items
                if needle in str(item.get("finding_id") or "").lower()
                or needle in str(item.get("title") or "").lower()
                or needle in str(item.get("file_path") or item.get("path") or "").lower()
                or needle in str(item.get("fingerprint") or "").lower()
            ]
        items.sort(key=lambda item: (self._severity_rank(item.get("severity")), str(item.get("last_seen_at") or ""), item["finding_id"]), reverse=True)
        return self._page(items, page=page, page_size=page_size)

    def get_finding(self, finding_id: str) -> dict[str, Any] | None:
        for item in self._load_findings():
            if item["finding_id"] == finding_id:
                detail = dict(item)
                detail["agent_context"] = self._finding_agent_context(item)
                return detail
        return None

    def finding_summary(self) -> dict[str, Any]:
        findings = self._load_findings()
        return {
            "finding_count": len(findings),
            "by_source_type": self._count_by(findings, "source_type"),
            "by_severity": self._count_by(findings, "severity"),
            "by_status": self._count_by(findings, "status"),
            "by_module": self._count_by(findings, "module"),
            "latest_findings": findings[:10],
            "parse_errors": self._source_parse_errors(),
        }

    def list_bugs(
        self,
        *,
        module: str | None = None,
        severity: str | None = None,
        status: str | None = None,
        agent: str | None = None,
        search: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        items = self._load_bugs()
        if module:
            module_l = module.lower()
            items = [item for item in items if module_l in str(item.get("module") or "").lower()]
        if severity:
            severity_u = severity.upper()
            items = [item for item in items if str(item.get("severity") or "").upper() == severity_u]
        if status:
            status_l = status.lower()
            items = [item for item in items if str(item.get("status") or "").lower() == status_l]
        if agent:
            agent_l = agent.lower()
            items = [item for item in items if agent_l in str(item.get("assigned_agent") or "").lower()]
        if search:
            needle = search.lower()
            items = [
                item
                for item in items
                if needle in str(item.get("bug_id") or "").lower()
                or needle in str(item.get("title") or "").lower()
                or needle in str(item.get("fingerprint") or "").lower()
            ]
        items.sort(key=lambda item: (self._severity_rank(item.get("severity")), str(item.get("last_seen_at") or item.get("created_at") or ""), item["bug_id"]), reverse=True)
        return self._page(items, page=page, page_size=page_size)

    def get_bug(self, bug_id: str) -> dict[str, Any] | None:
        for item in self._load_bugs():
            if item["bug_id"] == bug_id:
                detail = dict(item)
                detail["agent_context"] = self._bug_agent_context(item)
                return detail
        return None

    def bug_agent_context(self, bug_id: str) -> dict[str, Any] | None:
        bug = self.get_bug(bug_id)
        if bug is None:
            return None
        return bug["agent_context"]

    def bug_summary(self) -> dict[str, Any]:
        bugs = self._load_bugs()
        return {
            "bug_count": len(bugs),
            "by_severity": self._count_by(bugs, "severity"),
            "by_status": self._count_by(bugs, "status"),
            "by_module": self._count_by(bugs, "module"),
            "latest_bugs": bugs[:10],
            "parse_errors": self._source_parse_errors(),
        }

    def _load_findings(self) -> list[dict[str, Any]]:
        findings: dict[str, dict[str, Any]] = {}
        for source_path, payload in self._iter_json_payloads(self.guardrail_root, GUARDRAIL_SCHEMA):
            generated_at = payload.get("generated_at")
            for raw in payload.get("findings") or []:
                if not isinstance(raw, dict):
                    continue
                item = self._guardrail_finding(source_path, raw, generated_at)
                findings[item["finding_id"]] = item
        for source_path, payload in self._iter_json_payloads(self.legacy_root, LEGACY_SCHEMA):
            generated_at = payload.get("generated_at")
            for raw in payload.get("items") or []:
                if not isinstance(raw, dict):
                    continue
                item = self._legacy_finding(source_path, raw, generated_at)
                findings[item["finding_id"]] = item
        return list(findings.values())

    def _load_bugs(self) -> list[dict[str, Any]]:
        bugs: dict[str, dict[str, Any]] = {}
        for source_path, payload in self._iter_json_payloads(self.bug_root, BUG_SCHEMA):
            bug = self._normalize_bug(source_path, payload)
            bugs[bug["bug_id"]] = bug
        return list(bugs.values())

    def _guardrail_finding(self, source_path: Path, raw: dict[str, Any], generated_at: Any) -> dict[str, Any]:
        fingerprint = str(raw.get("fingerprint") or self._hash("guardrail", raw.get("rule_id"), raw.get("file"), raw.get("line")))
        finding_id = self._finding_id("guardrail", fingerprint)
        file_path = str(raw.get("file") or "")
        return {
            "finding_id": finding_id,
            "source_type": "guardrail",
            "source_schema": GUARDRAIL_SCHEMA,
            "module": self._module_from_path(file_path),
            "severity": str(raw.get("severity") or "P3").upper(),
            "status": "detected",
            "title": raw.get("title") or raw.get("rule_id") or "Guardrail finding",
            "description": raw.get("message") or raw.get("title"),
            "rule_id": raw.get("rule_id"),
            "category": raw.get("category"),
            "file_path": file_path,
            "line": raw.get("line"),
            "fingerprint": fingerprint,
            "first_seen_at": generated_at,
            "last_seen_at": generated_at,
            "evidence_uri": self._repo_path(source_path),
            "remediation": raw.get("remediation"),
            "baseline_policy": raw.get("baseline_policy"),
            "owner": None,
            "linked_issue": None,
            "allowed_write_scope": [file_path] if file_path else [],
            "required_verification": [
                "python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1",
                "python -m nox -s l0 -- <changed files>",
            ],
        }

    def _legacy_finding(self, source_path: Path, raw: dict[str, Any], generated_at: Any) -> dict[str, Any]:
        path = str(raw.get("path") or "")
        fingerprint = self._hash("legacy_inventory", path, raw.get("category"), raw.get("lifecycle_status"))
        finding_id = self._finding_id("legacy_inventory", fingerprint)
        return {
            "finding_id": finding_id,
            "source_type": "legacy_inventory",
            "source_schema": LEGACY_SCHEMA,
            "module": self._module_from_path(path),
            "severity": self._legacy_severity(raw.get("risk")),
            "status": "baselined",
            "title": f"Legacy lifecycle review: {path or 'unknown'}",
            "description": "Read-only legacy/dead-code inventory candidate. This is not a deletion approval.",
            "category": raw.get("category"),
            "file_path": path,
            "line": None,
            "fingerprint": fingerprint,
            "first_seen_at": generated_at,
            "last_seen_at": generated_at,
            "evidence_uri": self._repo_path(source_path),
            "lifecycle_status": raw.get("lifecycle_status"),
            "risk": raw.get("risk"),
            "confidence": raw.get("confidence"),
            "signals": raw.get("signals") or [],
            "references_found": raw.get("references_found") or 0,
            "reference_examples": raw.get("reference_examples") or [],
            "remediation": raw.get("recommended_action"),
            "owner": None,
            "linked_issue": None,
            "allowed_write_scope": [path] if path else [],
            "required_verification": [
                "python scripts/aistock_legacy_inventory.py <candidate path> --output-json tmp/validation/legacy_inventory/current_candidate.json",
                "python -m nox -s l0 -- <changed files>",
            ],
        }

    def _normalize_bug(self, source_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
        bug_id = str(payload.get("bug_id") or self._hash("bug", self._repo_path(source_path)))
        return {
            "bug_id": bug_id,
            "schema_version": payload.get("schema_version") or BUG_SCHEMA,
            "title": payload.get("title") or bug_id,
            "description": payload.get("description"),
            "module": payload.get("module") or "unknown",
            "severity": str(payload.get("severity") or "P2").upper(),
            "risk_area": payload.get("risk_area"),
            "status": payload.get("status") or "detected",
            "trigger_condition": payload.get("trigger_condition") or {},
            "reproduce_command": payload.get("reproduce_command"),
            "failing_run_id": payload.get("failing_run_id"),
            "evidence_uris": payload.get("evidence_uris") or [],
            "fingerprint": payload.get("fingerprint") or self._hash("bug", bug_id),
            "github_issue_number": payload.get("github_issue_number"),
            "github_issue_url": payload.get("github_issue_url"),
            "assigned_agent": payload.get("assigned_agent"),
            "fix_branch": payload.get("fix_branch"),
            "fix_commit": payload.get("fix_commit"),
            "verification_run_id": payload.get("verification_run_id"),
            "created_at": payload.get("created_at"),
            "first_seen_at": payload.get("first_seen_at") or payload.get("created_at"),
            "last_seen_at": payload.get("last_seen_at") or payload.get("created_at"),
            "fixed_at": payload.get("fixed_at"),
            "submitted_at": payload.get("submitted_at"),
            "closed_at": payload.get("closed_at"),
            "allowed_write_scope": payload.get("allowed_write_scope") or [],
            "suspected_modules": payload.get("suspected_modules") or [],
            "required_verification": payload.get("required_verification") or [],
            "closure_requirements": payload.get("closure_requirements") or [],
            "events": payload.get("events") or [],
            "source_path": self._repo_path(source_path),
        }

    def _finding_agent_context(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": "aistock_validation_agent_context_v1",
            "context_type": "quality_finding",
            "finding_id": item.get("finding_id"),
            "problem_statement": item.get("description") or item.get("title"),
            "finding_source": item.get("source_type"),
            "severity": item.get("severity"),
            "status": item.get("status"),
            "reproduce_command": self._finding_reproduce_command(item),
            "evidence_uris": [item.get("evidence_uri")],
            "allowed_write_scope": item.get("allowed_write_scope") or [],
            "suspected_modules": [item.get("module"), item.get("file_path")],
            "required_verification": item.get("required_verification") or [],
            "closure_requirements": [
                "Do not edit unrelated dirty workspace files.",
                "Add or reuse a regression test before marking verified.",
                "Record a verification run and evidence manifest before closing.",
            ],
        }

    @staticmethod
    def _finding_reproduce_command(item: dict[str, Any]) -> str:
        if item.get("source_type") == "guardrail":
            file_path = item.get("file_path") or "<path>"
            return f"python scripts/aistock_guardrail_scan.py {file_path} --fail-on-severity NONE"
        file_path = item.get("file_path") or "<path>"
        return f"python scripts/aistock_legacy_inventory.py {file_path} --output-json tmp/validation/legacy_inventory/repro.json"

    def _bug_agent_context(self, bug: dict[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": "aistock_validation_agent_context_v1",
            "context_type": "bug",
            "bug_id": bug.get("bug_id"),
            "problem_statement": bug.get("description") or bug.get("title"),
            "finding_source": "validation_failure",
            "severity": bug.get("severity"),
            "status": bug.get("status"),
            "reproduce_command": bug.get("reproduce_command"),
            "evidence_uris": bug.get("evidence_uris") or [],
            "allowed_write_scope": bug.get("allowed_write_scope") or [],
            "suspected_modules": bug.get("suspected_modules") or [],
            "required_verification": bug.get("required_verification") or [],
            "closure_requirements": bug.get("closure_requirements") or [],
            "github_issue_url": bug.get("github_issue_url"),
            "verification_run_id": bug.get("verification_run_id"),
        }

    def _iter_json_payloads(self, root: Path, schema_version: str) -> list[tuple[Path, dict[str, Any]]]:
        payloads: list[tuple[Path, dict[str, Any]]] = []
        if not root.exists():
            return payloads
        for path in sorted(root.rglob("*.json")):
            payload, parse_error = self._read_json(path)
            if parse_error or payload.get("schema_version") != schema_version:
                continue
            payloads.append((path, payload))
        return payloads

    def _source_parse_errors(self) -> list[dict[str, str]]:
        errors: list[dict[str, str]] = []
        for root in (self.guardrail_root, self.legacy_root, self.bug_root):
            if not root.exists():
                continue
            for path in sorted(root.rglob("*.json")):
                _, parse_error = self._read_json(path)
                if parse_error:
                    errors.append({"path": self._repo_path(path), "error": parse_error})
        return errors

    def _read_json(self, path: Path) -> tuple[dict[str, Any], str | None]:
        try:
            if path.stat().st_size > MAX_JSON_BYTES:
                return {}, f"JSON file exceeds max size: {MAX_JSON_BYTES}"
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return {}, f"invalid JSON: {exc}"
        except UnicodeDecodeError as exc:
            return {}, f"invalid UTF-8: {exc}"
        except OSError as exc:
            return {}, f"read error: {exc}"
        if not isinstance(payload, dict):
            return {}, "JSON root is not an object"
        return payload, None

    def _repo_path(self, path: Path) -> str:
        resolved = path.resolve()
        try:
            return resolved.relative_to(self.repo_root).as_posix()
        except ValueError:
            return str(resolved)

    @staticmethod
    def _module_from_path(path: str) -> str:
        if not path:
            return "unknown"
        first = path.replace("\\", "/").split("/", 1)[0]
        if first in {"backend", "frontend", "scripts", "docs", "tests", "monitoring"}:
            return first
        return "repo_root" if "/" not in path else first

    @staticmethod
    def _legacy_severity(risk: Any) -> str:
        risk_l = str(risk or "").lower()
        if risk_l == "high":
            return "P1"
        if risk_l == "medium":
            return "P2"
        return "P3"

    @staticmethod
    def _severity_rank(severity: Any) -> int:
        return {"P0": 4, "P1": 3, "P2": 2, "P3": 1}.get(str(severity or "").upper(), 0)

    @staticmethod
    def _count_by(items: list[dict[str, Any]], key: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in items:
            value = str(item.get(key) or "unknown")
            counts[value] = counts.get(value, 0) + 1
        return dict(sorted(counts.items()))

    @staticmethod
    def _hash(*parts: Any) -> str:
        return hashlib.sha256(":".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[:16]

    def _finding_id(self, source_type: str, fingerprint: str) -> str:
        safe_fingerprint = "".join(ch if ch.isalnum() or ch in "_.-" else "_" for ch in fingerprint)
        return f"{source_type}_{safe_fingerprint}"

    @staticmethod
    def _page(items: list[dict[str, Any]], *, page: int, page_size: int) -> dict[str, Any]:
        safe_page = max(page, 1)
        safe_page_size = max(page_size, 1)
        start = (safe_page - 1) * safe_page_size
        end = start + safe_page_size
        return {
            "items": items[start:end],
            "total": len(items),
            "page": safe_page,
            "page_size": safe_page_size,
            "has_more": end < len(items),
        }
