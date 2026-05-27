from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_ROOT = REPO_ROOT / "tests" / "aistock_validation" / "catalog"
BUGS_ROOT = REPO_ROOT / "tests" / "aistock_validation" / "bugs"
CANDIDATES_ROOT = REPO_ROOT / "tests" / "aistock_validation" / "runs" / "candidates"
FAILURES_ROOT = REPO_ROOT / "tests" / "aistock_validation" / "runs" / "failures"
MODULE_REGISTRY = CATALOG_ROOT / "module_registry.yaml"
FILE_OWNERSHIP = CATALOG_ROOT / "file_ownership.yaml"
TEST_PLANS = CATALOG_ROOT / "test_plans.yaml"
STANDARD_REFS = [
    "docs/standards/aistock_development_standard_v1.5_20260523.md#CONTEXT-BUDGET-001",
    "docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md",
]
VALID_CANDIDATE_STATUSES = {
    "new",
    "deduped",
    "accepted",
    "ignored",
    "promoted",
}
VALID_CANDIDATE_TRANSITIONS = {
    "new": {"deduped", "accepted", "ignored"},
    "deduped": {"accepted", "ignored"},
    "accepted": {"promoted", "ignored"},
    "promoted": set(),
    "ignored": set(),
}
VALID_BUG_STATUSES = {"open", "in_progress", "fixed", "verified", "wontfix"}
ISSUE_FORM_LABEL_ALIASES = {
    "existing bug id": "bug_id",
    "severity": "severity",
    "priority": "severity",
    "aistock module": "module",
    "summary": "summary",
    "regression summary": "summary",
    "problem or opportunity": "summary",
    "context and problem statement": "summary",
    "reproduction steps": "reproduce_command",
    "reproduction command or workflow": "reproduce_command",
    "expected behavior": "expected",
    "expected prior behavior": "expected",
    "actual behavior": "actual",
    "current failing behavior": "actual",
    "evidence": "evidence",
    "required validation": "required_validation",
    "closure validation": "required_validation",
    "validation plan": "required_validation",
    "acceptance criteria": "acceptance",
    "proposed solution": "proposal",
    "proposed architecture": "proposal",
    "design doc or source of truth": "design_doc",
    "last known good": "last_good",
    "first known bad": "first_bad",
}


class IssueFlowError(ValueError):
    """Raised when issue-flow input cannot satisfy AIstock workflow contracts."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _today_compact() -> str:
    return datetime.now().strftime("%Y%m%d")


def _repo_path(path: str | Path) -> str:
    raw = str(path).replace("\\", "/")
    try:
        resolved = Path(path).resolve()
        return resolved.relative_to(REPO_ROOT).as_posix()
    except Exception:
        return raw.lstrip("./")


def _norm_path(path: str | Path) -> str:
    return str(path).replace("\\", "/").lstrip("./")


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise IssueFlowError(f"YAML root must be a mapping: {_repo_path(path)}")
    return data


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise IssueFlowError(f"JSON root must be an object: {_repo_path(path)}")
    return data


def _write_json(path: Path | None, payload: dict[str, Any]) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path is None:
        sys.stdout.write(text)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _stable_hash(*parts: Any, length: int = 12) -> str:
    material = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:length]


def _slug(value: str, max_len: int = 72) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return (slug or "issue")[:max_len].strip("-") or "issue"


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _unique_strings(items: list[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item).strip()
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _severity_code(value: Any, default: str = "P2") -> str:
    text = str(value or default).strip().upper()
    match = re.search(r"\bP[0-3]\b", text)
    return match.group(0) if match else default


def _field_key(label: str) -> str:
    normalized = re.sub(r"\s+", " ", label.strip().lower())
    if normalized in ISSUE_FORM_LABEL_ALIASES:
        return ISSUE_FORM_LABEL_ALIASES[normalized]
    return re.sub(r"[^a-z0-9]+", "_", normalized).strip("_") or "field"


def _clean_form_value(value: str) -> str:
    text = value.strip()
    if text.lower() in {"_no response_", "no response", "n/a", "none"}:
        return ""
    return text


def parse_issue_form_body(body: str) -> dict[str, Any]:
    fields: dict[str, str] = {}
    raw_fields: dict[str, str] = {}
    current_label: str | None = None
    current_lines: list[str] = []

    def flush() -> None:
        nonlocal current_label, current_lines
        if current_label is None:
            return
        value = _clean_form_value("\n".join(current_lines))
        key = _field_key(current_label)
        raw_fields[current_label] = value
        if value:
            fields[key] = value
        current_label = None
        current_lines = []

    for line in body.splitlines():
        match = re.match(r"^#{2,4}\s+(.+?)\s*$", line)
        if match:
            flush()
            current_label = match.group(1).strip()
            continue
        if current_label is not None:
            current_lines.append(line)
    flush()
    return {
        "schema_version": "aistock_issue_form_parse_v1",
        "fields": fields,
        "raw_fields": raw_fields,
    }


def _pattern_matches(pattern: str, path: str) -> bool:
    pattern = _norm_path(pattern)
    path = _norm_path(path)
    if pattern.endswith("/**") and path.startswith(pattern[:-3].rstrip("/") + "/"):
        return True
    return fnmatch.fnmatch(path, pattern)


def _catalog_modules() -> dict[str, dict[str, Any]]:
    payload = _load_yaml(MODULE_REGISTRY)
    modules = payload.get("modules") or []
    return {
        str(item.get("module_id")): item
        for item in modules
        if isinstance(item, dict) and item.get("module_id")
    }


def _plans_by_key() -> dict[str, dict[str, Any]]:
    payload = _load_yaml(TEST_PLANS)
    plans = payload.get("plans") or []
    return {
        str(item.get("plan_key")): item
        for item in plans
        if isinstance(item, dict) and item.get("plan_key")
    }


def _ownership_rules() -> list[dict[str, Any]]:
    payload = _load_yaml(FILE_OWNERSHIP)
    rules = [rule for rule in payload.get("rules") or [] if isinstance(rule, dict)]
    return sorted(rules, key=lambda item: int(item.get("priority") or 0), reverse=True)


def match_changed_files(changed_files: list[str]) -> dict[str, Any]:
    modules = _catalog_modules()
    matched_rules: list[dict[str, Any]] = []
    impacted_modules: list[str] = []
    suggested_scope: list[str] = []
    risk_levels: list[str] = []
    unmatched: list[str] = []

    for file_path in _unique_strings(changed_files):
        matches = []
        for rule in _ownership_rules():
            for pattern in _as_list(rule.get("include")):
                if _pattern_matches(str(pattern), file_path):
                    matches.append(rule)
                    break
        if not matches:
            unmatched.append(file_path)
            continue
        primary = str(matches[0].get("primary_module") or "unknown")
        impacted_modules.append(primary)
        impacted_modules.extend(str(item) for item in _as_list(matches[0].get("impact_modules")))
        risk_levels.append(str(matches[0].get("risk_level") or "medium"))
        suggested_scope.append(file_path)
        matched_rules.append(
            {
                "file": file_path,
                "rule_id": matches[0].get("rule_id"),
                "primary_module": primary,
                "impact_modules": _as_list(matches[0].get("impact_modules")),
                "risk_level": matches[0].get("risk_level") or "medium",
            }
        )

    known_modules = set(modules)
    impacted = [module for module in _unique_strings(impacted_modules) if module in known_modules]
    return {
        "changed_files": _unique_strings(changed_files),
        "impacted_modules": impacted,
        "matched_rules": matched_rules,
        "unmatched_files": unmatched,
        "risk_levels": _unique_strings(risk_levels),
        "suggested_scope": _unique_strings(suggested_scope),
    }


def select_validation(changed_files: list[str], module: str | None = None) -> dict[str, Any]:
    modules = _catalog_modules()
    plans = _plans_by_key()
    ownership = match_changed_files(changed_files)
    impacted = list(ownership["impacted_modules"])
    if module and module not in impacted:
        impacted.insert(0, module)
    if not impacted and module:
        impacted = [module]

    required: list[str] = []
    recommended: list[str] = []
    for module_id in impacted:
        entry = modules.get(module_id) or {}
        module_plans = entry.get("test_plans") or {}
        required.extend(_as_list(module_plans.get("required_on_change")))
        recommended.extend(_as_list(module_plans.get("recommended")))
    if not required:
        required.append("l0")
    required = [plan for plan in _unique_strings(required) if plan in plans]
    recommended = [plan for plan in _unique_strings(recommended) if plan in plans and plan not in required]

    skip_reasons = {
        plan_key: "not selected by changed-file ownership or requested module"
        for plan_key in plans
        if plan_key not in set(required + recommended)
    }
    gates = {
        "ddl": "required" if any(path.endswith(".sql") or "/migrations/" in path for path in changed_files) else "noop",
        "frontend_dependency": "required"
        if any(path in {"frontend/package.json", "frontend/package-lock.json", "frontend/pnpm-lock.yaml"} for path in changed_files)
        else "noop",
        "backend_dependency": "required"
        if any(Path(path).name in {"requirements.txt", "requirements-dev.txt", "requirements.lock.txt", "pyproject.toml"} for path in changed_files)
        else "noop",
    }
    return {
        "schema_version": "aistock_validation_selection_v1",
        "impacted_modules": impacted,
        "ownership": ownership,
        "required_plans": required,
        "recommended_plans": recommended,
        "nightly_plans": ["AIstock Nightly L3 + DR"],
        "skip_reasons": skip_reasons,
        "production_gates": gates,
    }


def build_failure_event(args: argparse.Namespace, source: dict[str, Any] | None = None) -> dict[str, Any]:
    source = source or {}
    title = args.title or source.get("title") or source.get("summary") or "AIstock issue candidate"
    module = args.module or source.get("module") or source.get("module_guess") or "unknown"
    plan_key = args.plan_key or source.get("plan_key")
    nox_session = args.nox_session or source.get("nox_session")
    normalized_error = args.actual or source.get("normalized_error") or source.get("actual") or title
    fingerprint = source.get("fingerprint") or _stable_hash(
        args.source or source.get("source") or "manual",
        module,
        plan_key,
        nox_session,
        normalized_error,
        length=24,
    )
    return {
        "schema_version": "aistock_failure_event_v1",
        "event_id": f"FE-{_today_compact()}-{fingerprint[:12]}",
        "source": args.source or source.get("source") or "manual",
        "timestamp": source.get("timestamp") or _utc_now(),
        "repo": source.get("repo") or "licong01-cloud/AIstock",
        "branch": source.get("branch"),
        "commit": source.get("commit"),
        "workflow": source.get("workflow"),
        "plan_key": plan_key,
        "nox_session": nox_session,
        "module_guess": module,
        "severity_guess": args.severity_guess or source.get("severity_guess") or source.get("severity") or "P2",
        "normalized_error": normalized_error,
        "fingerprint": fingerprint,
        "reproduce_command": args.reproduce_command or source.get("reproduce_command") or "n/a",
        "evidence_refs": _unique_strings(_as_list(source.get("evidence_refs")) + list(args.evidence_ref or [])),
        "changed_files": _unique_strings(_as_list(source.get("changed_files")) + list(args.changed_file or [])),
        "candidate_status": "new",
    }


def candidate_from_event(
    event: dict[str, Any],
    *,
    title: str | None = None,
    candidate_type: str = "bug",
    expected: str | None = None,
    actual: str | None = None,
) -> dict[str, Any]:
    module = event.get("module_guess") or "unknown"
    fingerprint = str(event.get("fingerprint") or _stable_hash(event))
    changed_files = _as_list(event.get("changed_files"))
    validation = select_validation([str(path) for path in changed_files], module=module)
    return {
        "schema_version": "aistock_issue_candidate_v1",
        "candidate_id": f"IC-{_today_compact()}-{fingerprint[:12]}",
        "source_event_id": event.get("event_id"),
        "source": event.get("source"),
        "module": module,
        "risk_level": _risk_from_severity(str(event.get("severity_guess") or "P2")),
        "severity_guess": event.get("severity_guess") or "P2",
        "candidate_type": candidate_type,
        "title": title or event.get("normalized_error") or "AIstock issue candidate",
        "expected": expected or "Expected behavior should be restored.",
        "actual": actual or event.get("normalized_error") or "Current behavior differs from expected.",
        "fingerprint": fingerprint,
        "dedupe_key": f"{module}|{event.get('plan_key') or ''}|{fingerprint[:12]}",
        "suggested_owner": "codex_app",
        "suggested_validation": validation["required_plans"],
        "suggested_scope": validation["ownership"]["suggested_scope"],
        "promotion_target": "bug_registry" if candidate_type in {"bug", "regression"} else "github_issue",
        "evidence_refs": event.get("evidence_refs") or [],
        "reproduce_command": event.get("reproduce_command") or "n/a",
        "status": "new",
        "status_events": [
            {
                "timestamp": _utc_now(),
                "actor": "issue_flow",
                "from_status": None,
                "to_status": "new",
                "reason": "candidate_created",
            }
        ],
        "created_at": _utc_now(),
    }


def _risk_from_severity(severity: str) -> str:
    severity = severity.upper().split()[0]
    if severity in {"P0", "P1"}:
        return "high"
    if severity == "P2":
        return "medium"
    return "low"


def validate_candidate(candidate: dict[str, Any]) -> None:
    required = [
        "schema_version",
        "candidate_id",
        "module",
        "candidate_type",
        "title",
        "fingerprint",
        "dedupe_key",
        "status",
    ]
    missing = [key for key in required if not candidate.get(key)]
    if missing:
        raise IssueFlowError(f"candidate missing required fields: {', '.join(missing)}")
    if candidate["status"] not in VALID_CANDIDATE_STATUSES:
        raise IssueFlowError(f"invalid candidate status: {candidate['status']}")


def transition_candidate(candidate: dict[str, Any], to_status: str, reason: str | None = None) -> dict[str, Any]:
    validate_candidate(candidate)
    from_status = str(candidate["status"])
    if to_status not in VALID_CANDIDATE_STATUSES:
        raise IssueFlowError(f"invalid target candidate status: {to_status}")
    if to_status != from_status and to_status not in VALID_CANDIDATE_TRANSITIONS[from_status]:
        raise IssueFlowError(f"invalid candidate transition: {from_status} -> {to_status}")
    updated = dict(candidate)
    updated["status"] = to_status
    events = list(_as_list(candidate.get("status_events")))
    if to_status != from_status:
        events.append(
            {
                "timestamp": _utc_now(),
                "actor": "issue_flow",
                "from_status": from_status,
                "to_status": to_status,
                "reason": reason or "manual_transition",
            }
        )
    updated["status_events"] = events
    return updated


def extract_candidate(payload: dict[str, Any]) -> dict[str, Any]:
    candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else payload
    if not isinstance(candidate, dict):
        raise IssueFlowError("candidate JSON must be an object or contain a candidate object")
    validate_candidate(candidate)
    return candidate


def candidate_from_issue_form(
    parsed: dict[str, Any],
    *,
    template_type: str,
    issue_number: int | str | None = None,
    issue_url: str | None = None,
) -> dict[str, Any]:
    fields = parsed.get("fields") or {}
    if not isinstance(fields, dict):
        raise IssueFlowError("parsed issue form fields must be an object")

    title = str(fields.get("summary") or fields.get("proposal") or "AIstock issue candidate")
    module = str(fields.get("module") or "unknown").strip() or "unknown"
    severity = _severity_code(fields.get("severity"), default="P2")
    reproduce = str(fields.get("reproduce_command") or "n/a")
    evidence = _unique_strings(
        [line.lstrip("- ").strip() for line in str(fields.get("evidence") or "").splitlines()]
        + ([issue_url] if issue_url else [])
    )
    fingerprint = _stable_hash(
        "github_issue_form",
        template_type,
        issue_number or "",
        module,
        title,
        reproduce,
        str(fields.get("actual") or ""),
        length=24,
    )
    event = {
        "schema_version": "aistock_failure_event_v1",
        "event_id": f"FE-{_today_compact()}-{fingerprint[:12]}",
        "source": "github_issue_form",
        "timestamp": _utc_now(),
        "repo": "licong01-cloud/AIstock",
        "module_guess": module,
        "severity_guess": severity,
        "normalized_error": title,
        "fingerprint": fingerprint,
        "reproduce_command": reproduce,
        "evidence_refs": evidence,
        "github_issue_number": int(issue_number) if issue_number else None,
        "github_issue_url": issue_url,
        "candidate_status": "new",
    }
    candidate_type = {
        "bug": "bug",
        "regression": "regression",
        "feature": "feature",
        "rfc": "rfc",
    }.get(template_type, "bug")
    candidate = candidate_from_event(
        event,
        title=title,
        candidate_type=candidate_type,
        expected=str(fields.get("expected") or fields.get("acceptance") or "Expected behavior should be restored."),
        actual=str(fields.get("actual") or fields.get("summary") or title),
    )
    candidate["source_issue_number"] = int(issue_number) if issue_number else None
    candidate["source_issue_url"] = issue_url
    return {
        "schema_version": "aistock_issue_form_candidate_v1",
        "template_type": template_type,
        "issue_number": int(issue_number) if issue_number else None,
        "issue_url": issue_url,
        "parsed": parsed,
        "event": event,
        "candidate": candidate,
    }


def promote_candidate_to_bug(
    candidate: dict[str, Any],
    *,
    bug_id: str | None = None,
    github_issue_number: str | None = None,
    github_issue_url: str | None = None,
) -> dict[str, Any]:
    bug_id = bug_id or "BUG-DRYRUN"
    severity = str(candidate.get("severity_guess") or "P2").split()[0]
    record = {
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": bug_id,
        "title": candidate.get("title"),
        "module": candidate.get("module"),
        "severity": severity,
        "risk_area": candidate.get("candidate_type") or "bug",
        "status": "open",
        "description": candidate.get("actual"),
        "reproduce_command": candidate.get("reproduce_command") or "n/a",
        "expected": candidate.get("expected"),
        "actual": candidate.get("actual"),
        "evidence_uris": candidate.get("evidence_refs") or [],
        "allowed_write_scope": candidate.get("suggested_scope") or [],
        "suspected_modules": [candidate.get("module")],
        "required_verification": candidate.get("suggested_validation") or [],
        "closure_requirements": [
            "Fix the observed behavior.",
            "Run required verification plans.",
            "Keep BUG JSON and GitHub Issue synchronized.",
        ],
        "non_goals": ["Do not restart production runtime services without explicit approval."],
        "trigger_condition": {
            "source_event_id": candidate.get("source_event_id"),
            "fingerprint": candidate.get("fingerprint"),
        },
        "events": [
            {
                "timestamp": _utc_now(),
                "actor": "issue_flow",
                "action": "promote_candidate_to_bug",
                "note": f"Promoted from {candidate.get('candidate_id')}",
            }
        ],
    }
    if github_issue_number:
        record["github_issue_number"] = int(github_issue_number)
    if github_issue_url:
        record["github_issue_url"] = github_issue_url
    if not record["allowed_write_scope"]:
        record["workflow_gate"] = "triage_only_until_allowed_write_scope_is_set"
    return record


def build_feature_issue(candidate: dict[str, Any]) -> dict[str, Any]:
    body = "\n".join(
        [
            f"<!-- aistock-candidate:{candidate.get('candidate_id')} -->",
            "## Problem or opportunity",
            str(candidate.get("actual") or candidate.get("title")),
            "",
            "## Proposed acceptance",
            f"- Module: `{candidate.get('module')}`",
            f"- Reproduce or discovery command: `{candidate.get('reproduce_command') or 'n/a'}`",
            "- Define validation plan before implementation.",
            "",
            "## Evidence",
            *[f"- {ref}" for ref in candidate.get("evidence_refs") or ["n/a"]],
        ]
    )
    return {
        "title": f"[FEATURE] {candidate.get('title')}",
        "labels": ["enhancement", f"module:{candidate.get('module')}"],
        "body": body,
        "candidate_id": candidate.get("candidate_id"),
    }


def build_fix_ready(record: dict[str, Any], changed_files: list[str]) -> dict[str, Any]:
    module = record.get("module") or record.get("module_guess")
    scope = _unique_strings(_as_list(record.get("allowed_write_scope")) + changed_files)
    validation = select_validation(scope, module=module)
    required = _unique_strings(_as_list(record.get("required_verification")) + validation["required_plans"])
    return {
        "schema_version": "aistock_fix_ready_v1",
        "source_id": record.get("bug_id") or record.get("candidate_id") or record.get("id"),
        "module": module,
        "risk_level": record.get("risk_level") or _risk_from_severity(str(record.get("severity") or record.get("severity_guess") or "P2")),
        "allowed_write_scope": scope,
        "required_verification": required,
        "recommended_verification": validation["recommended_plans"],
        "non_goals": _unique_strings(
            _as_list(record.get("non_goals"))
            + ["Do not restart production runtime services without explicit approval."]
        ),
        "workflow_gate": "allowed" if scope else "triage_only_until_allowed_write_scope_is_set",
        "validation_selection": validation,
    }


def build_context_pack(record: dict[str, Any], changed_files: list[str] | None = None) -> dict[str, Any]:
    changed_files = changed_files or []
    fix_ready = build_fix_ready(record, changed_files)
    issues = [item for item in [record.get("bug_id"), record.get("candidate_id")] if item]
    target_tokens = 12000 if len(issues) <= 1 else 20000
    return {
        "schema_version": "aistock_context_pack_v1",
        "pack_id": f"CP-{_today_compact()}-{_stable_hash(record.get('bug_id'), record.get('candidate_id'), record.get('fingerprint'))}",
        "task_tier": "T1" if len(issues) <= 1 else "T2",
        "phase": "fix_ready",
        "module": fix_ready.get("module"),
        "risk_level": fix_ready.get("risk_level"),
        "issues": issues,
        "problem_statement": record.get("description") or record.get("actual") or record.get("title"),
        "reproduce_command": record.get("reproduce_command") or "n/a",
        "allowed_write_scope": fix_ready["allowed_write_scope"],
        "non_goals": fix_ready["non_goals"],
        "required_verification": fix_ready["required_verification"],
        "evidence_refs": record.get("evidence_uris") or record.get("evidence_refs") or [],
        "standards_refs": STANDARD_REFS,
        "token_budget": {
            "target_tokens": target_tokens,
            "max_tokens": 20000 if target_tokens == 12000 else 30000,
            "full_docs_allowed": False,
        },
    }


def render_context_pack_markdown(pack: dict[str, Any]) -> str:
    lines = [
        f"# AIstock Context Pack {pack.get('pack_id')}",
        "",
        f"- task_tier: `{pack.get('task_tier')}`",
        f"- phase: `{pack.get('phase')}`",
        f"- module: `{pack.get('module')}`",
        f"- risk_level: `{pack.get('risk_level')}`",
        f"- issues: `{', '.join(pack.get('issues') or []) or 'n/a'}`",
        "",
        "## Problem",
        str(pack.get("problem_statement") or "n/a"),
        "",
        "## Reproduce",
        f"`{pack.get('reproduce_command') or 'n/a'}`",
        "",
        "## Allowed Write Scope",
        *[f"- `{item}`" for item in pack.get("allowed_write_scope") or ["triage_only_until_allowed_write_scope_is_set"]],
        "",
        "## Required Verification",
        *[f"- `{item}`" for item in pack.get("required_verification") or ["l0"]],
        "",
        "## Non Goals",
        *[f"- {item}" for item in pack.get("non_goals") or []],
        "",
        "## Evidence",
        *[f"- {item}" for item in pack.get("evidence_refs") or ["n/a"]],
        "",
        "## Code Intelligence",
        *(
            [
                f"- {key}: `{value}`"
                for key, value in (pack.get("code_intelligence") or {}).items()
                if key in {"provider", "status", "context_ref", "affected_tests_ref", "fallback_used"}
            ]
            or ["- n/a"]
        ),
        "",
        "## Standards",
        *[f"- `{item}`" for item in pack.get("standards_refs") or []],
        "",
    ]
    return "\n".join(lines)


def build_batch_plan(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        raise IssueFlowError("batch-plan requires at least one record")
    modules = _unique_strings([str(item.get("module") or "unknown") for item in records])
    if len(modules) != 1:
        raise IssueFlowError(f"batch issues must share one module; got {modules}")
    scope: list[str] = []
    verification: list[str] = []
    issues: list[str] = []
    closure_map: dict[str, list[str]] = {}
    for record in records:
        issue_id = record.get("bug_id") or record.get("candidate_id") or record.get("title")
        issues.append(str(issue_id))
        scope.extend(_as_list(record.get("allowed_write_scope")) or _as_list(record.get("suggested_scope")))
        verification.extend(_as_list(record.get("required_verification")) or _as_list(record.get("suggested_validation")))
        closure_map[str(issue_id)] = _unique_strings(_as_list(record.get("closure_requirements"))) or [
            "Fix issue-specific behavior.",
            "Attach issue-specific evidence.",
        ]
    selected = select_validation([str(path) for path in scope], module=modules[0])
    verification.extend(selected["required_plans"])
    batch_id = f"BATCH-{modules[0].replace('.', '-')}-{_today_compact()}"
    return {
        "schema_version": "aistock_batch_plan_v1",
        "batch_id": batch_id,
        "task_tier": "T2",
        "module": modules[0],
        "issues": issues,
        "shared_files": _unique_strings(scope),
        "shared_validation": _unique_strings(verification),
        "per_issue_commit_map": {},
        "per_issue_closure_map": closure_map,
    }


def _git_output(args: list[str], cwd: Path = REPO_ROOT, check: bool = True) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    if check and proc.returncode != 0:
        raise IssueFlowError(proc.stderr.strip() or proc.stdout.strip() or f"git {' '.join(args)} failed")
    return proc.stdout.strip()


def changed_files_from_git(base: str, head: str) -> list[str]:
    out = _git_output(["diff", "--name-only", f"{base}...{head}"])
    return [line.strip() for line in out.splitlines() if line.strip()]


def scope_check(changed_files: list[str], allowed_scope: list[str]) -> dict[str, Any]:
    allowed = _unique_strings(allowed_scope)
    if not allowed:
        return {
            "status": "missing_scope",
            "violations": changed_files,
            "allowed_write_scope": [],
        }
    violations = [
        path
        for path in changed_files
        if not any(_pattern_matches(pattern, path) or _pattern_matches(pattern.rstrip("/") + "/**", path) for pattern in allowed)
    ]
    return {
        "status": "passed" if not violations else "failed",
        "violations": violations,
        "allowed_write_scope": allowed,
    }


def build_pr_quality(
    *,
    base: str,
    head: str,
    issue_record: dict[str, Any] | None = None,
    changed_files: list[str] | None = None,
) -> dict[str, Any]:
    changed_files = changed_files if changed_files is not None else changed_files_from_git(base, head)
    validation = select_validation(changed_files, module=(issue_record or {}).get("module"))
    scope = _as_list((issue_record or {}).get("allowed_write_scope"))
    scope_result = scope_check(changed_files, [str(item) for item in scope]) if issue_record else {
        "status": "not_provided",
        "violations": [],
        "allowed_write_scope": [],
    }
    linked = _unique_strings(
        _as_list((issue_record or {}).get("bug_id"))
        + _as_list((issue_record or {}).get("github_issue_number"))
        + _as_list((issue_record or {}).get("candidate_id"))
    )
    return {
        "schema_version": "aistock_pr_quality_summary_v1",
        "base": base,
        "head": head,
        "linked_issues": linked,
        "task_tier": "T1" if linked else "T0",
        "changed_files": changed_files,
        "impacted_modules": validation["impacted_modules"],
        "scope_check": scope_result,
        "selected_validation": validation,
        "validation_results": "not_run_by_pr_quality_dry_run",
        "data_acceptance": "not_required",
        "production_ddl_gate": validation["production_gates"]["ddl"],
        "production_frontend_dependency_gate": validation["production_gates"]["frontend_dependency"],
        "production_backend_dependency_gate": validation["production_gates"]["backend_dependency"],
    }


def render_pr_quality_markdown(summary: dict[str, Any]) -> str:
    gates = [
        f"- production_ddl_gate: `{summary.get('production_ddl_gate')}`",
        f"- production_frontend_dependency_gate: `{summary.get('production_frontend_dependency_gate')}`",
        f"- production_backend_dependency_gate: `{summary.get('production_backend_dependency_gate')}`",
    ]
    lines = [
        "## AIstock PR Quality Summary",
        "",
        f"- linked_issues: `{', '.join(summary.get('linked_issues') or []) or 'none'}`",
        f"- task_tier: `{summary.get('task_tier')}`",
        f"- impacted_modules: `{', '.join(summary.get('impacted_modules') or []) or 'none'}`",
        f"- scope_check: `{(summary.get('scope_check') or {}).get('status')}`",
        f"- required_validation: `{', '.join((summary.get('selected_validation') or {}).get('required_plans') or [])}`",
        f"- validation_results: `{summary.get('validation_results')}`",
        f"- data_acceptance: `{summary.get('data_acceptance')}`",
        *gates,
        "",
    ]
    violations = (summary.get("scope_check") or {}).get("violations") or []
    if violations:
        lines.extend(["### Scope Violations", *[f"- `{item}`" for item in violations], ""])
    return "\n".join(lines)


def build_cleanup_plan(branch: str, worktree: str | None = None) -> dict[str, Any]:
    local_branches = set(_git_output(["for-each-ref", "--format=%(refname:short)", "refs/heads"], check=False).splitlines())
    remote_ref = _git_output(["ls-remote", "--heads", "origin", branch], check=False)
    merged = branch in set(_git_output(["branch", "--format=%(refname:short)", "--merged", "origin/main"], check=False).splitlines())
    worktrees = _git_output(["worktree", "list"], check=False).splitlines()
    worktree_match = [line for line in worktrees if worktree and _norm_path(worktree) in _norm_path(line)]
    actions = []
    if remote_ref:
        actions.append({"action": "delete_remote_branch", "branch": branch, "safe": merged})
    if branch in local_branches:
        actions.append({"action": "delete_local_branch", "branch": branch, "safe": merged})
    if worktree_match:
        actions.append({"action": "remove_worktree", "worktree": worktree, "safe": merged})
    return {
        "schema_version": "aistock_cleanup_plan_v1",
        "branch": branch,
        "worktree": worktree,
        "merged_into_origin_main": merged,
        "actions": actions,
        "apply_supported": False,
    }


def _read_record(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    return _load_json(Path(path))


def cmd_issue_form_parse(args: argparse.Namespace) -> int:
    body = Path(args.issue_body_file).read_text(encoding="utf-8-sig")
    parsed = parse_issue_form_body(body)
    payload = candidate_from_issue_form(
        parsed,
        template_type=args.template_type,
        issue_number=args.issue_number,
        issue_url=args.issue_url,
    )
    _write_json(Path(args.output) if args.output else None, payload)
    return 0


def cmd_candidate_create(args: argparse.Namespace) -> int:
    source = _read_record(args.source_json)
    event = build_failure_event(args, source)
    candidate = candidate_from_event(
        event,
        title=args.title or source.get("title"),
        candidate_type=args.candidate_type,
        expected=args.expected or source.get("expected"),
        actual=args.actual or source.get("actual"),
    )
    validate_candidate(candidate)
    payload = {"event": event, "candidate": candidate}
    _write_json(Path(args.output) if args.output else None, payload)
    return 0


def cmd_candidate_dedupe(args: argparse.Namespace) -> int:
    candidate = extract_candidate(_load_json(Path(args.candidate_json)))
    root = Path(args.candidates_dir) if args.candidates_dir else CANDIDATES_ROOT
    matches = []
    if root.exists():
        for path in root.glob("*.json"):
            try:
                other = extract_candidate(_load_json(path))
            except Exception:
                continue
            if other.get("dedupe_key") == candidate.get("dedupe_key") or other.get("fingerprint") == candidate.get("fingerprint"):
                matches.append({"path": _repo_path(path), "candidate_id": other.get("candidate_id")})
    _write_json(None, {"deduplicated": bool(matches), "matches": matches, "candidate_id": candidate.get("candidate_id")})
    return 0


def cmd_promote_bug(args: argparse.Namespace) -> int:
    candidate = extract_candidate(_load_json(Path(args.candidate_json)))
    record = promote_candidate_to_bug(
        candidate,
        bug_id=args.bug_id,
        github_issue_number=args.github_issue_number,
        github_issue_url=args.github_issue_url,
    )
    if args.apply:
        if not record.get("github_issue_number") or not record.get("github_issue_url"):
            raise IssueFlowError("--apply requires --github-issue-number and --github-issue-url")
        bug_id = str(record["bug_id"])
        if not re.fullmatch(r"BUG-\d{3,}", bug_id):
            raise IssueFlowError("--apply requires a canonical BUG-NNN id")
        path = BUGS_ROOT / f"{_today_compact()}_{bug_id}-{_slug(str(record['title']))}.json"
        _write_json(path, record)
        _write_json(None, {"applied": True, "path": _repo_path(path), "record": record})
        return 0
    _write_json(Path(args.output) if args.output else None, {"applied": False, "record": record})
    return 0


def cmd_promote_feature(args: argparse.Namespace) -> int:
    candidate = extract_candidate(_load_json(Path(args.candidate_json)))
    payload = build_feature_issue(candidate)
    _write_json(Path(args.output) if args.output else None, payload)
    return 0


def cmd_candidate_transition(args: argparse.Namespace) -> int:
    candidate = extract_candidate(_load_json(Path(args.candidate_json)))
    payload = transition_candidate(candidate, args.to_status, reason=args.reason)
    _write_json(Path(args.output) if args.output else None, payload)
    return 0


def cmd_fix_ready(args: argparse.Namespace) -> int:
    record = _load_json(Path(args.issue_json))
    payload = build_fix_ready(record, list(args.changed_file or []))
    _write_json(Path(args.output) if args.output else None, payload)
    return 0


def cmd_context_pack(args: argparse.Namespace) -> int:
    if bool(args.issue_json) == bool(args.candidate_json):
        raise IssueFlowError("context-pack requires exactly one of --issue-json or --candidate-json")
    record = _load_json(Path(args.issue_json or args.candidate_json))
    if args.candidate_json:
        record = extract_candidate(record)
    pack = build_context_pack(record, list(args.changed_file or []))
    if args.output_md:
        path = Path(args.output_md)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_context_pack_markdown(pack), encoding="utf-8")
    _write_json(Path(args.output_json) if args.output_json else None, pack)
    return 0


def cmd_batch_plan(args: argparse.Namespace) -> int:
    records = [_load_json(Path(path)) for path in args.issue_json]
    payload = build_batch_plan(records)
    _write_json(Path(args.output) if args.output else None, payload)
    return 0


def cmd_validation_select(args: argparse.Namespace) -> int:
    changed = list(args.changed_file or [])
    if args.from_git:
        changed.extend(changed_files_from_git(args.base, args.head))
    payload = select_validation(changed, module=args.module)
    _write_json(Path(args.output) if args.output else None, payload)
    return 0


def cmd_pr_check(args: argparse.Namespace) -> int:
    issue = _load_json(Path(args.issue_json)) if args.issue_json else None
    changed_files = list(args.changed_file or []) or None
    summary = build_pr_quality(base=args.base, head=args.head, issue_record=issue, changed_files=changed_files)
    if args.output_md:
        path = Path(args.output_md)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_pr_quality_markdown(summary), encoding="utf-8")
    _write_json(Path(args.output_json) if args.output_json else None, summary)
    status = (summary.get("scope_check") or {}).get("status")
    return 2 if args.fail_on_scope and status == "failed" else 0


def cmd_close_sync(args: argparse.Namespace) -> int:
    record = _load_json(Path(args.issue_json))
    if record.get("status") not in VALID_BUG_STATUSES:
        raise IssueFlowError(f"invalid issue status: {record.get('status')}")
    payload = {
        "schema_version": "aistock_close_sync_plan_v1",
        "issue_id": record.get("bug_id") or record.get("candidate_id"),
        "current_status": record.get("status"),
        "merged_pr": args.pr_url,
        "dry_run": not args.apply,
        "actions": [
            "verify closure requirements",
            "sync BUG JSON and GitHub Issue status",
            "record production gates",
        ],
    }
    if args.apply:
        raise IssueFlowError("close-sync --apply is intentionally not implemented in Phase 1; use MCP sync tools")
    _write_json(Path(args.output) if args.output else None, payload)
    return 0


def cmd_cleanup_after_merge(args: argparse.Namespace) -> int:
    plan = build_cleanup_plan(args.branch, args.worktree)
    if args.apply:
        raise IssueFlowError("cleanup-after-merge --apply is intentionally not implemented in Phase 1")
    _write_json(Path(args.output) if args.output else None, plan)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AIstock issue/feature workflow helper.")
    sub = parser.add_subparsers(dest="command", required=True)

    issue_form = sub.add_parser("issue-form-parse", help="Parse a GitHub Issue Form body into a candidate.")
    issue_form.add_argument("--issue-body-file", required=True)
    issue_form.add_argument("--template-type", required=True, choices=["bug", "regression", "feature", "rfc"])
    issue_form.add_argument("--issue-number", type=int)
    issue_form.add_argument("--issue-url")
    issue_form.add_argument("--output")
    issue_form.set_defaults(func=cmd_issue_form_parse)

    candidate = sub.add_parser("candidate-create", help="Create a normalized issue candidate.")
    candidate.add_argument("--source", default="manual")
    candidate.add_argument("--source-json")
    candidate.add_argument("--title")
    candidate.add_argument("--module")
    candidate.add_argument("--severity-guess", default="P2")
    candidate.add_argument("--candidate-type", default="bug", choices=["bug", "regression", "feature", "rfc", "infra_failure", "flaky"])
    candidate.add_argument("--expected")
    candidate.add_argument("--actual")
    candidate.add_argument("--reproduce-command")
    candidate.add_argument("--evidence-ref", action="append")
    candidate.add_argument("--changed-file", action="append")
    candidate.add_argument("--plan-key")
    candidate.add_argument("--nox-session")
    candidate.add_argument("--output")
    candidate.set_defaults(func=cmd_candidate_create)

    dedupe = sub.add_parser("candidate-dedupe", help="Check candidate fingerprint/dedupe key.")
    dedupe.add_argument("--candidate-json", required=True)
    dedupe.add_argument("--candidates-dir")
    dedupe.set_defaults(func=cmd_candidate_dedupe)

    promote_bug = sub.add_parser("promote-bug", help="Promote candidate to BUG JSON draft or apply.")
    promote_bug.add_argument("--candidate-json", required=True)
    promote_bug.add_argument("--bug-id")
    promote_bug.add_argument("--github-issue-number")
    promote_bug.add_argument("--github-issue-url")
    promote_bug.add_argument("--apply", action="store_true")
    promote_bug.add_argument("--output")
    promote_bug.set_defaults(func=cmd_promote_bug)

    promote_feature = sub.add_parser("promote-feature", help="Build a GitHub feature issue payload.")
    promote_feature.add_argument("--candidate-json", required=True)
    promote_feature.add_argument("--output")
    promote_feature.set_defaults(func=cmd_promote_feature)

    transition = sub.add_parser("candidate-transition", help="Dry-run a candidate state transition.")
    transition.add_argument("--candidate-json", required=True)
    transition.add_argument("--to-status", required=True, choices=sorted(VALID_CANDIDATE_STATUSES))
    transition.add_argument("--reason")
    transition.add_argument("--output")
    transition.set_defaults(func=cmd_candidate_transition)

    fix_ready = sub.add_parser("fix-ready", help="Build fix-ready scope and validation.")
    fix_ready.add_argument("--issue-json", required=True)
    fix_ready.add_argument("--changed-file", action="append")
    fix_ready.add_argument("--output")
    fix_ready.set_defaults(func=cmd_fix_ready)

    context = sub.add_parser("context-pack", help="Build an agent-neutral Context Pack.")
    context.add_argument("--issue-json")
    context.add_argument("--candidate-json")
    context.add_argument("--changed-file", action="append")
    context.add_argument("--output-json")
    context.add_argument("--output-md")
    context.set_defaults(func=cmd_context_pack)

    batch = sub.add_parser("batch-plan", help="Build a same-module batch plan.")
    batch.add_argument("--issue-json", action="append", required=True)
    batch.add_argument("--output")
    batch.set_defaults(func=cmd_batch_plan)

    validation = sub.add_parser("validation-select", help="Select validation plans.")
    validation.add_argument("--changed-file", action="append")
    validation.add_argument("--from-git", action="store_true")
    validation.add_argument("--base", default="origin/main")
    validation.add_argument("--head", default="HEAD")
    validation.add_argument("--module")
    validation.add_argument("--output")
    validation.set_defaults(func=cmd_validation_select)

    pr_check = sub.add_parser("pr-check", help="Build a PR quality summary.")
    pr_check.add_argument("--base", default="origin/main")
    pr_check.add_argument("--head", default="HEAD")
    pr_check.add_argument("--issue-json")
    pr_check.add_argument("--changed-file", action="append")
    pr_check.add_argument("--output-json")
    pr_check.add_argument("--output-md")
    pr_check.add_argument("--fail-on-scope", action="store_true")
    pr_check.set_defaults(func=cmd_pr_check)

    close = sub.add_parser("close-sync", help="Build a close/sync dry-run plan.")
    close.add_argument("--issue-json", required=True)
    close.add_argument("--pr-url")
    close.add_argument("--apply", action="store_true")
    close.add_argument("--output")
    close.set_defaults(func=cmd_close_sync)

    cleanup = sub.add_parser("cleanup-after-merge", help="Build a post-merge cleanup plan.")
    cleanup.add_argument("--branch", required=True)
    cleanup.add_argument("--worktree")
    cleanup.add_argument("--apply", action="store_true")
    cleanup.add_argument("--output")
    cleanup.set_defaults(func=cmd_cleanup_after_merge)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except IssueFlowError as exc:
        print(f"issue_flow error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
