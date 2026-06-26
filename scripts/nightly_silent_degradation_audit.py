from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import code_intelligence_adapter as ci_adapter  # noqa: E402
from scripts import llm_provider_adapter as llm_adapter  # noqa: E402

DEFAULT_CONFIG_PATH = ROOT / "configs" / "validation" / "silent_degradation_audit.yaml"
DEFAULT_PROMPT_PACK_PATH = ROOT / "prompt_packs" / "validation_llm" / "silent_degradation_audit.prompt.yml"
SCHEMA_VERSION = "aistock_nightly_silent_degradation_audit_v1"
LLM_MAX_REFERENCE_CHARS = 700
LLM_MAX_CODE_SAMPLE_CHARS = 450
LLM_MAX_CODE_SAMPLE_COUNT = 4
LLM_MAX_MARKER_HITS = 12
LLM_MAX_OUTPUT_TOKENS = 4096
LLM_TIMEOUT_SECONDS = 180
PRODUCTION_GATES = {
    "production_ddl_gate": "noop",
    "production_frontend_dependency_gate": "noop",
    "production_backend_dependency_gate": "noop",
}
FORBIDDEN_OUTPUT_FIELDS = {
    "command",
    "shell_command",
    "run_command",
    "patch",
    "source_patch",
    "diff",
    "bug_json",
    "github_issue_body",
    "fix",
    "fix_patch",
    "write_file",
}
DEFAULT_CODE_EXTENSIONS = {".py", ".ts", ".tsx", ".js", ".jsx", ".yaml", ".yml"}

class SilentDegradationAuditError(RuntimeError):
    """Raised when the readonly silent degradation audit cannot build a safe artifact."""

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def stable_id(*parts: str) -> str:
    return hashlib.sha256("::".join(parts).encode("utf-8", errors="replace")).hexdigest()[:12]

def read_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise SilentDegradationAuditError(f"config must be a mapping: {path}")
    return payload

def read_prompt_pack(path: Path = DEFAULT_PROMPT_PACK_PATH) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}

def _git_value(root: Path, *args: str) -> str | None:
    proc = subprocess.run(["git", "-C", str(root), *args], text=True, encoding="utf-8", errors="replace", capture_output=True, check=False)
    return proc.stdout.strip() if proc.returncode == 0 else None

def _git_tracked_files(root: Path, prefixes: list[str]) -> list[str]:
    if not prefixes:
        return []
    proc = subprocess.run(["git", "-C", str(root), "ls-files", "--", *prefixes], text=True, encoding="utf-8", errors="replace", capture_output=True, check=False)
    if proc.returncode != 0:
        return []
    return [line.strip().replace("\\", "/") for line in proc.stdout.splitlines() if line.strip()]

def _safe_text(path: Path, max_chars: int = 120000) -> str:
    try:
        return path.read_text(encoding="utf-8-sig", errors="replace")[:max_chars]
    except OSError:
        return ""

def _compact_reference_excerpt(text: str, *, keywords: list[str], max_chars: int) -> str:
    picked: list[str] = []
    lower_keywords = [item.lower() for item in keywords if item]
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        lower = stripped.lower()
        if stripped.startswith("#") or any(keyword in lower for keyword in lower_keywords):
            picked.append(stripped[:360])
        if sum(len(item) + 1 for item in picked) >= max_chars:
            break
    if not picked:
        picked = [line.strip()[:360] for line in text.splitlines() if line.strip()][:12]
    return "\n".join(picked)[:max_chars]

def _marker_hits(root: Path, files: list[str], markers: list[str], *, max_hits: int = 40) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    max_per_marker = max(1, max_hits // max(1, len(markers)))
    for marker in [value for value in markers if value]:
        marker_lower = marker.lower()
        marker_count = 0
        for rel in files:
            if marker_count >= max_per_marker:
                break
            path = root / rel
            if path.suffix.lower() not in DEFAULT_CODE_EXTENSIONS:
                continue
            text = _safe_text(path, 80000)
            for line_no, line in enumerate(text.splitlines(), start=1):
                if marker_lower in line.lower():
                    hits.append({"marker": marker, "path": rel, "line": line_no, "excerpt": line.strip()[:220]})
                    marker_count += 1
                    break
                if marker_count >= max_per_marker:
                    break
    return hits

def _sample_code_files(root: Path, files: list[str], *, max_files: int, max_chars_per_file: int) -> list[dict[str, str]]:
    samples: list[dict[str, str]] = []
    priority_ext = {".py", ".ts", ".tsx"}
    for rel in sorted(files, key=lambda item: (Path(item).suffix.lower() not in priority_ext, item))[:max_files]:
        path = root / rel
        if path.suffix.lower() not in DEFAULT_CODE_EXTENSIONS:
            continue
        lines = [line.rstrip()[:220] for line in _safe_text(path, max_chars_per_file).splitlines() if line.strip()]
        if lines:
            samples.append({"path": rel, "excerpt": "\n".join(lines[:40])[:max_chars_per_file]})
    return samples

def _module_targets(config: dict[str, Any], modules: list[str] | None = None) -> list[dict[str, Any]]:
    configured = config.get("modules") if isinstance(config.get("modules"), list) else []
    if not modules:
        return [item for item in configured if isinstance(item, dict)]
    selected = set(modules)
    return [item for item in configured if isinstance(item, dict) and str(item.get("module")) in selected]

def build_review_targets(
    *,
    root: Path = ROOT,
    config_path: Path = DEFAULT_CONFIG_PATH,
    modules: list[str] | None = None,
    code_intelligence_json: Path | None = None,
) -> list[dict[str, Any]]:
    config = read_yaml(config_path)
    defaults = config.get("defaults") if isinstance(config.get("defaults"), dict) else {}
    max_design_chars = int(defaults.get("max_reference_chars_per_doc") or 2400)
    max_code_files = int(defaults.get("max_code_files_per_module") or 24)
    max_code_excerpt_chars = int(defaults.get("max_code_excerpt_chars_per_file") or 1200)
    code_refs = llm_adapter.code_intelligence_refs_from_file(code_intelligence_json)
    targets: list[dict[str, Any]] = []
    for item in _module_targets(config, modules):
        module = str(item.get("module") or "").strip()
        if not module:
            continue
        requirement_keywords = [str(value) for value in item.get("requirement_keywords") or [] if str(value).strip()]
        expected_markers = [str(value) for value in item.get("semantic_expected_markers") or [] if str(value).strip()]
        forbidden_markers = [str(value) for value in item.get("silent_degradation_markers") or [] if str(value).strip()]
        reference_docs: list[dict[str, Any]] = []
        for rel in [str(value).replace("\\", "/") for value in item.get("reference_docs") or [] if str(value).strip()]:
            path = root / rel
            exists = path.exists()
            text = _safe_text(path) if exists else ""
            reference_docs.append({
                "path": rel,
                "exists": exists,
                "excerpt": _compact_reference_excerpt(text, keywords=requirement_keywords + expected_markers, max_chars=max_design_chars) if exists else "",
            })
        code_prefixes = [str(value).replace("\\", "/") for value in item.get("code_paths") or [] if str(value).strip()]
        files = _git_tracked_files(root, code_prefixes)
        expected_hits = _marker_hits(root, files, expected_markers, max_hits=40)
        silent_degradation_hits = _marker_hits(root, files, forbidden_markers, max_hits=40)
        targets.append({
            "module": module,
            "risk": str(item.get("risk") or "P2"),
            "reference_docs": reference_docs,
            "code_paths": code_prefixes,
            "code_file_count": len(files),
            "semantic_expected_markers": expected_markers,
            "semantic_expected_hits": expected_hits,
            "missing_semantic_markers": sorted(set(expected_markers) - {hit["marker"] for hit in expected_hits}),
            "silent_degradation_markers": forbidden_markers,
            "silent_degradation_hits": silent_degradation_hits,
            "code_samples": _sample_code_files(root, files, max_files=max_code_files, max_chars_per_file=max_code_excerpt_chars),
            "code_intelligence_refs": code_refs,
        })
    return targets

def make_finding(
    *,
    module: str,
    severity: str,
    title: str,
    suspected_silent_degradation: str,
    reference_refs: list[str],
    code_refs: list[str],
    confidence: float,
    source: str,
    expected_behavior: str = "",
    observed_code_evidence: str = "",
    why_this_is_not_normal_fallback: str = "",
    manual_validation_suggestion: str = "",
) -> dict[str, Any]:
    finding_id = f"SDA-{stable_id(module, title, '|'.join(reference_refs), '|'.join(code_refs))}"
    return {
        "finding_id": finding_id,
        "module": module[:120],
        "severity": severity if severity in {"P0", "P1", "P2", "P3"} else "P2",
        "title": title[:240],
        "suspected_silent_degradation": suspected_silent_degradation[:1000],
        "expected_behavior": expected_behavior[:800],
        "observed_code_evidence": observed_code_evidence[:800],
        "why_this_is_not_normal_fallback": why_this_is_not_normal_fallback[:800],
        "reference_refs": [str(ref)[:240] for ref in reference_refs[:8]],
        "code_refs": [str(ref)[:240] for ref in code_refs[:12]],
        "confidence": round(float(confidence), 2),
        "source": source,
        "next_action": "manual_analysis_required_before_bug_registration",
        "manual_validation_suggestion": manual_validation_suggestion[:800],
        "official_bug_created": False,
        "github_issue_created": False,
    }

def deterministic_findings(review_targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for target in review_targets:
        module = str(target.get("module") or "unknown")
        missing_docs = [doc["path"] for doc in target.get("reference_docs") or [] if not doc.get("exists")]
        if missing_docs:
            findings.append(make_finding(module=module, severity="P2", title=f"{module} silent degradation audit target references missing reference docs", suspected_silent_degradation="Audit coverage is incomplete because configured reference docs are absent.", reference_refs=missing_docs, code_refs=target.get("code_paths") or [], confidence=0.78, source="deterministic_coverage_check", expected_behavior="Configured reference docs should exist before advisory review.", manual_validation_suggestion="Confirm whether the module should stay in the nightly silent degradation audit target set."))
        if int(target.get("code_file_count") or 0) == 0:
            findings.append(make_finding(module=module, severity="P2", title=f"{module} silent degradation audit target has no tracked code files", suspected_silent_degradation="Audit coverage is incomplete because configured code paths have no tracked files.", reference_refs=[doc["path"] for doc in target.get("reference_docs") or [] if doc.get("exists")], code_refs=target.get("code_paths") or [], confidence=0.76, source="deterministic_coverage_check", expected_behavior="Configured code paths should map to tracked implementation or tests.", manual_validation_suggestion="Check whether the target paths were renamed or whether this audit target should be removed."))
        missing_markers = [str(value) for value in target.get("missing_semantic_markers") or [] if str(value).strip()]
        if missing_markers and target.get("semantic_expected_markers"):
            findings.append(make_finding(module=module, severity=str(target.get("risk") or "P2"), title=f"{module} may miss runtime contract markers", suspected_silent_degradation="Expected runtime/contract markers are absent from configured code paths: " + ", ".join(missing_markers[:8]), reference_refs=[doc["path"] for doc in target.get("reference_docs") or [] if doc.get("exists")][:6], code_refs=target.get("code_paths") or [], confidence=0.64, source="deterministic_marker_check", expected_behavior="Code should expose the configured contract markers or equivalent implementation signals.", why_this_is_not_normal_fallback="Missing contract markers can mean a simplified path replaced durable behavior; this is only a candidate signal.", manual_validation_suggestion="Review the referenced module to confirm whether equivalent semantics exist under different names."))
        risk_hits = target.get("silent_degradation_hits") or []
        if risk_hits:
            findings.append(make_finding(module=module, severity=str(target.get("risk") or "P2"), title=f"{module} contains silent degradation risk markers", suspected_silent_degradation="Potentially silent fallback, fake success, swallowed exception, or fallback-oriented behavior appears in code; needs human review.", reference_refs=[doc["path"] for doc in target.get("reference_docs") or [] if doc.get("exists")][:6], code_refs=[f"{hit.get('path')}:{hit.get('line')}" for hit in risk_hits[:8]], confidence=0.58, source="deterministic_silent_degradation_marker_check", expected_behavior="Fallback paths should be explicit, observable, and fail-safe rather than reporting fake success.", observed_code_evidence=", ".join(f"{hit.get('path')}:{hit.get('line')} {hit.get('marker')}" for hit in risk_hits[:6]), why_this_is_not_normal_fallback="The marker can be legitimate, but it needs review when it hides errors, returns empty success, or bypasses durable behavior.", manual_validation_suggestion="Inspect each referenced fallback path and verify it logs, surfaces, or gates degraded behavior."))
    return findings[:40]

def _payload_contains_forbidden_fields(value: Any) -> bool:
    if isinstance(value, dict):
        for key, child in value.items():
            if str(key) in FORBIDDEN_OUTPUT_FIELDS:
                return True
            if _payload_contains_forbidden_fields(child):
                return True
    if isinstance(value, list):
        return any(_payload_contains_forbidden_fields(item) for item in value)
    return False

def _coerce_llm_findings(raw: dict[str, Any]) -> list[dict[str, Any]]:
    if _payload_contains_forbidden_fields(raw):
        raise SilentDegradationAuditError("LLM silent degradation audit output contained forbidden action fields")
    raw_findings = raw.get("findings") or raw.get("candidate_suggestions") or raw.get("issues") or []
    if not isinstance(raw_findings, list):
        return []
    findings: list[dict[str, Any]] = []
    for item in raw_findings[:40]:
        if not isinstance(item, dict):
            continue
        try:
            confidence_value = float(item.get("confidence", 0.5))
        except (TypeError, ValueError):
            confidence_value = 0.5
        findings.append(make_finding(
            module=str(item.get("module") or "unknown"),
            severity=str(item.get("severity") or item.get("risk") or "P2"),
            title=str(item.get("title") or item.get("summary") or "Possible silent degradation"),
            suspected_silent_degradation=str(item.get("suspected_silent_degradation") or item.get("rationale") or item.get("summary") or ""),
            reference_refs=[str(ref) for ref in item.get("reference_refs") or item.get("design_evidence") or []],
            code_refs=[str(ref) for ref in item.get("code_refs") or item.get("code_evidence") or []],
            confidence=confidence_value,
            source="llm_silent_degradation_audit",
            expected_behavior=str(item.get("expected_behavior") or ""),
            observed_code_evidence=str(item.get("observed_code_evidence") or ""),
            why_this_is_not_normal_fallback=str(item.get("why_this_is_not_normal_fallback") or ""),
            manual_validation_suggestion=str(item.get("manual_validation_suggestion") or ""),
        ))
    return findings

def _parse_ymd(value: Any, *, field: str, index: int) -> date:
    text = str(value or "").strip()
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SilentDegradationAuditError(
            f"suppressions[{index}].{field} must be YYYY-MM-DD"
        ) from exc

def _normalize_code_ref_path(value: Any) -> str:
    text = str(value or "").strip().replace("\\", "/")
    while ":" in text:
        head, sep, tail = text.rpartition(":")
        if sep and tail.isdigit():
            text = head
            continue
        break
    return text.lstrip("./").lower()

def _code_ref_overlaps(suppression_ref: str, finding_ref: str) -> bool:
    suppression_path = _normalize_code_ref_path(suppression_ref)
    finding_path = _normalize_code_ref_path(finding_ref)
    if not suppression_path or not finding_path:
        return False
    if suppression_path == finding_path:
        return True
    suppression_prefix = suppression_path.rstrip("/") + "/"
    finding_prefix = finding_path.rstrip("/") + "/"
    return finding_path.startswith(suppression_prefix) or suppression_path.startswith(finding_prefix)

def _validated_suppressions(config: dict[str, Any], *, audit_date: date) -> list[dict[str, Any]]:
    if "suppressions" not in config:
        return []
    raw_suppressions = config.get("suppressions") or []
    if not isinstance(raw_suppressions, list):
        raise SilentDegradationAuditError("suppressions must be a list when configured")
    suppressions: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_suppressions):
        if not isinstance(raw, dict):
            raise SilentDegradationAuditError(f"suppressions[{index}] must be a mapping")
        module = str(raw.get("module") or "").strip()
        reason = str(raw.get("reason") or "").strip()
        dismissed_by = str(raw.get("dismissed_by") or "").strip()
        dismissed_at = str(raw.get("dismissed_at") or "").strip()
        finding_id = str(raw.get("finding_id") or "").strip()
        title_contains = str(raw.get("title_contains") or "").strip()
        code_refs_raw = raw.get("code_refs_any") or []
        if not module:
            raise SilentDegradationAuditError(f"suppressions[{index}].module is required")
        if not reason:
            raise SilentDegradationAuditError(f"suppressions[{index}].reason is required")
        if not dismissed_by:
            raise SilentDegradationAuditError(f"suppressions[{index}].dismissed_by is required")
        if not dismissed_at:
            raise SilentDegradationAuditError(f"suppressions[{index}].dismissed_at is required")
        if code_refs_raw and not isinstance(code_refs_raw, list):
            raise SilentDegradationAuditError(f"suppressions[{index}].code_refs_any must be a list")
        code_refs_any = [str(item).strip() for item in code_refs_raw if str(item).strip()]
        if not finding_id and not code_refs_any:
            raise SilentDegradationAuditError(
                f"suppressions[{index}] must include finding_id or code_refs_any"
            )
        _parse_ymd(dismissed_at, field="dismissed_at", index=index)
        expires_at = str(raw.get("expires_at") or "").strip()
        expires_date = _parse_ymd(expires_at, field="expires_at", index=index) if expires_at else None
        suppressions.append(
            {
                "index": index,
                "module": module,
                "finding_id": finding_id,
                "code_refs_any": code_refs_any,
                "title_contains": title_contains,
                "reason": reason,
                "dismissed_by": dismissed_by,
                "dismissed_at": dismissed_at,
                "expires_at": expires_at or None,
                "active": expires_date is None or expires_date >= audit_date,
            }
        )
    return suppressions

def _suppression_matches(finding: dict[str, Any], suppression: dict[str, Any]) -> bool:
    if not suppression.get("active"):
        return False
    if str(finding.get("module") or "") != suppression["module"]:
        return False
    title_contains = suppression.get("title_contains")
    if title_contains and title_contains.lower() not in str(finding.get("title") or "").lower():
        return False
    finding_id = str(finding.get("finding_id") or "")
    finding_id_matches = bool(suppression.get("finding_id")) and finding_id == suppression["finding_id"]
    finding_code_refs = [str(item) for item in finding.get("code_refs") or []]
    code_ref_matches = any(
        _code_ref_overlaps(suppression_ref, finding_ref)
        for suppression_ref in suppression.get("code_refs_any") or []
        for finding_ref in finding_code_refs
    )
    return finding_id_matches or code_ref_matches

def apply_suppressions(
    findings: list[dict[str, Any]],
    config: dict[str, Any],
    *,
    audit_date: date | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    suppressions = _validated_suppressions(
        config,
        audit_date=audit_date or datetime.now(timezone.utc).date(),
    )
    if not suppressions:
        return findings, []
    active_findings: list[dict[str, Any]] = []
    suppressed_findings: list[dict[str, Any]] = []
    for finding in findings:
        matched = next((item for item in suppressions if _suppression_matches(finding, item)), None)
        if matched is None:
            active_findings.append(finding)
            continue
        suppressed = dict(finding)
        suppressed["suppressed_by"] = {
            "reason": matched["reason"],
            "dismissed_by": matched["dismissed_by"],
            "dismissed_at": matched["dismissed_at"],
            "expires_at": matched["expires_at"],
            "matched_suppression_index": matched["index"],
        }
        suppressed_findings.append(suppressed)
    return active_findings, suppressed_findings

def _prompt_messages(
    review_targets: list[dict[str, Any]],
    *,
    run_id: str | None,
    commit: str | None,
    prompt_pack: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    compact_targets = [_llm_target_view(target) for target in review_targets]
    prompt_pack = prompt_pack if isinstance(prompt_pack, dict) else {}
    purpose = str(
        prompt_pack.get("purpose")
        or "Find suspected silent degradation in AIstock from compact CodeGraph/Understand Anything refs and code evidence."
    )
    system = (
        "You are an AIstock nightly silent degradation auditor. "
        f"{purpose} "
        "Use your own review judgment; deterministic marker hits are hints, not findings. "
        "Keep the response compact; include at most the highest-signal findings. "
        "Return one strict JSON object only. You may only suggest candidate findings for later human analysis. "
        "Do not create BUGs, GitHub issues, code patches, shell commands, production actions, or closure decisions."
    )
    output_schema = prompt_pack.get("allowed_output") or {
        "summary": "short string",
        "findings": [
            {
                "module": "module key",
                "severity": "P1|P2|P3",
                "title": "short title",
                "suspected_silent_degradation": "failure hidden, fallback masked, fake success, or semantic downgrade risk",
                "expected_behavior": "required behavior from reference docs or runtime contract",
                "observed_code_evidence": "compact code evidence only",
                "reference_refs": ["doc path or section"],
                "code_refs": ["file path or file:line"],
                "why_this_is_not_normal_fallback": "why this is not an acceptable explicit fallback",
                "manual_validation_suggestion": "manual validation only; no commands",
                "confidence": 0.0,
            }
        ],
    }
    user_payload = {
        "schema": output_schema,
        "policy": prompt_pack.get("safety")
        or {
            "warning_only": True,
            "candidate_only": True,
            "manual_analysis_required_before_bug_registration": True,
            "source_modifications_allowed": False,
            "official_bug_creation_allowed": False,
            "github_issue_creation_allowed": False,
        },
        "review_guidance": prompt_pack.get("review_guidance") or {},
        "input": {"run_id": run_id, "commit": commit, "review_targets": compact_targets},
    }
    return [{"role": "system", "content": system}, {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, sort_keys=True)}]

def _llm_evidence(*, provider_summary: dict[str, Any], invoked: bool, reason: str, usage: dict[str, Any] | None = None, error: str | None = None) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "schema_version": llm_adapter.LLM_INVOCATION_EVIDENCE_SCHEMA_VERSION,
        "provider": provider_summary.get("provider"),
        "model": provider_summary.get("model"),
        "invoked": invoked,
        "reason": reason,
        "input_policy": "compact_codegraph_ua_refs_marker_hits_only",
        "redaction_applied": True,
    }
    if usage:
        evidence["usage_summary"] = {"prompt_units": usage.get("prompt_tokens"), "completion_units": usage.get("completion_tokens"), "total_units": usage.get("total_tokens")}
    if error:
        # Provider errors may include request metadata; artifacts keep only a non-secret fingerprint.
        evidence["error_type"] = error.__class__.__name__ if not isinstance(error, str) else "provider_error"
        evidence["error_fingerprint"] = stable_id(llm_adapter.redact_secret_text(str(error))[:500])
    return evidence

def _deterministic_signal_count(review_targets: list[dict[str, Any]]) -> int:
    return len(deterministic_findings(review_targets))

def build_audit(
    *,
    root: Path = ROOT,
    config_path: Path = DEFAULT_CONFIG_PATH,
    llm_config_path: Path = llm_adapter.DEFAULT_CONFIG_PATH,
    prompt_pack_path: Path = DEFAULT_PROMPT_PACK_PATH,
    provider: str = "deterministic",
    modules: list[str] | None = None,
    code_intelligence_json: Path | None = None,
    invoke_llm: bool = False,
    fallback_on_llm_error: bool = True,
    run_id: str | None = None,
) -> dict[str, Any]:
    config = read_yaml(config_path)
    if config.get("schema_version") != "aistock_silent_degradation_audit_config_v1":
        raise SilentDegradationAuditError("unsupported silent degradation audit config schema_version")
    review_targets = build_review_targets(root=root, config_path=config_path, modules=modules, code_intelligence_json=code_intelligence_json)
    llm_config = llm_adapter.load_config(llm_config_path)
    llm_adapter.validate_config(llm_config)
    provider_summary = llm_adapter._provider_model_summary(llm_config, provider)  # noqa: SLF001
    prompt_pack = read_prompt_pack(prompt_pack_path)
    findings = deterministic_findings(review_targets)
    llm_evidence = _llm_evidence(provider_summary=provider_summary, invoked=False, reason="silent_degradation_audit_dry_run_no_network")
    llm_summary: str | None = None
    degraded_reason: str | None = None
    if invoke_llm and provider != "deterministic":
        try:
            result = llm_adapter.invoke_provider_json(
                provider,
                llm_config,
                purpose="silent_degradation_audit",
                messages=_prompt_messages(
                    review_targets,
                    run_id=run_id,
                    commit=_git_value(root, "rev-parse", "HEAD"),
                    prompt_pack=prompt_pack,
                ),
                max_tokens=LLM_MAX_OUTPUT_TOKENS,
                timeout_seconds=LLM_TIMEOUT_SECONDS,
            )
            llm_findings = _coerce_llm_findings(result["payload"])
            findings = llm_findings
            llm_summary = str(result["payload"].get("summary") or "")[:1000]
            provider_summary = {"provider": result.get("provider") or provider_summary.get("provider"), "model": result.get("model") or provider_summary.get("model"), "credential_source": result.get("credential_source") or provider_summary.get("credential_source")}
            llm_evidence = _llm_evidence(provider_summary=provider_summary, invoked=True, reason="silent_degradation_audit_live_provider_json", usage=result.get("usage") if isinstance(result.get("usage"), dict) else None)
        except Exception as exc:
            if not fallback_on_llm_error:
                raise SilentDegradationAuditError(str(exc)) from exc
            llm_evidence = _llm_evidence(provider_summary=provider_summary, invoked=False, reason="silent_degradation_audit_live_provider_failed_fallback", error=exc)
            findings = []
            degraded_reason = "llm_provider_failed_no_marker_findings_emitted"
    findings, suppressed_findings = apply_suppressions(findings, config)
    workflow_gate = "warning" if findings or degraded_reason else "ready"
    no_candidate_reason = None
    if degraded_reason:
        no_candidate_reason = degraded_reason
    elif not findings:
        no_candidate_reason = (
            "all_candidate_findings_suppressed"
            if suppressed_findings
            else "no_silent_degradation_suggestion_crossed_threshold"
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "run_id": run_id,
        "root": str(root),
        "commit": _git_value(root, "rev-parse", "HEAD"),
        "branch": _git_value(root, "branch", "--show-current"),
        "provider": provider_summary.get("provider"),
        "model": provider_summary.get("model"),
        "effective_provider": provider_summary.get("provider"),
        "effective_model": provider_summary.get("model"),
        "llm_gate": "ready" if llm_evidence.get("invoked") else "degraded",
        "workflow_gate": workflow_gate,
        "warning_only": True,
        "candidate_only": True,
        "source_modifications_allowed": False,
        "official_bug_creation_allowed": False,
        "github_issue_creation_allowed": False,
        "manual_analysis_required_before_bug_registration": True,
        "review_targets": review_targets,
        "findings": findings,
        "suppressed_findings": suppressed_findings,
        "summary": {
            "review_target_count": len(review_targets),
            "finding_count": len(findings),
            "suppressed_count": len(suppressed_findings),
            "llm_summary": llm_summary,
            "no_candidate_reason": no_candidate_reason,
            "deterministic_signal_count": _deterministic_signal_count(review_targets)
            if invoke_llm and provider != "deterministic" and degraded_reason
            else None,
            "degraded_reason": degraded_reason,
        },
        "llm_invocation_evidence": llm_evidence,
        "side_effects": {"readonly": True, "writes_source": False, "writes_bug_json": False, "writes_github_issue": False, "writes_database": False, "production_actions_allowed": False},
        "production_gates": PRODUCTION_GATES,
    }

def public_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    llm_summary = llm_adapter.llm_invocation_public_summary(payload.get("llm_invocation_evidence"))
    compact_targets = []
    for target in payload.get("review_targets") or []:
        compact_targets.append({
            "module": target.get("module"),
            "risk": target.get("risk"),
            "reference_refs": [doc.get("path") for doc in target.get("reference_docs") or [] if isinstance(doc, dict)],
            "code_paths": target.get("code_paths") or [],
            "code_file_count": target.get("code_file_count"),
            "missing_semantic_markers": target.get("missing_semantic_markers") or [],
            "expected_marker_hit_count": len(target.get("semantic_expected_hits") or []),
            "silent_degradation_hit_count": len(target.get("silent_degradation_hits") or []),
            "code_intelligence_refs": target.get("code_intelligence_refs") or {},
        })
    return {
        "schema_version": payload.get("schema_version"),
        "generated_at": payload.get("generated_at"),
        "run_id": payload.get("run_id"),
        "commit": payload.get("commit"),
        "branch": payload.get("branch"),
        "provider": payload.get("provider"),
        "model": payload.get("model"),
        "effective_provider": payload.get("effective_provider"),
        "effective_model": payload.get("effective_model"),
        "llm_gate": payload.get("llm_gate"),
        "workflow_gate": payload.get("workflow_gate"),
        "warning_only": True,
        "candidate_only": True,
        "source_modifications_allowed": False,
        "official_bug_creation_allowed": False,
        "github_issue_creation_allowed": False,
        "manual_analysis_required_before_bug_registration": True,
        "review_targets": compact_targets,
        "findings": payload.get("findings") or [],
        "suppressed_findings": payload.get("suppressed_findings") or [],
        "summary": payload.get("summary") or {},
        "llm_invoked": llm_summary.get("invoked"),
        "llm_invocation_evidence": llm_summary,
        "side_effects": payload.get("side_effects") or {},
        "production_gates": payload.get("production_gates") or PRODUCTION_GATES,
    }

def write_json(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    artifact = public_artifact(payload)
    artifact["public_artifact"] = True
    ci_adapter._write_json(path, artifact)  # noqa: SLF001

def write_markdown(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    findings = payload.get("findings") if isinstance(payload.get("findings"), list) else []
    suppressed_findings = payload.get("suppressed_findings") if isinstance(payload.get("suppressed_findings"), list) else []
    llm_evidence = payload.get("llm_invocation_evidence") if isinstance(payload.get("llm_invocation_evidence"), dict) else {}
    llm_invoked = bool(llm_evidence.get("invoked"))
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    lines = [
        "# Nightly LLM Silent Degradation Audit",
        "",
        f"- workflow_gate: `{payload.get('workflow_gate')}`",
        f"- llm_gate: `{payload.get('llm_gate')}`",
        f"- llm_invoked: `{llm_invoked}`",
        f"- llm_reason: `{llm_evidence.get('reason') or 'unknown'}`",
        f"- checked_modules: `{len(payload.get('review_targets') or [])}`",
        f"- finding_count: `{len(findings)}`",
        f"- suppressed_count: `{len(suppressed_findings)}`",
        f"- deterministic_signal_count: `{summary.get('deterministic_signal_count') or 0}`",
        f"- no_candidate_reason: `{summary.get('no_candidate_reason') or 'n/a'}`",
        "- side_effects: `readonly; suggestions only; no source changes; no BUG or GitHub Issue writes`",
        "",
    ]
    if not llm_invoked and summary.get("degraded_reason"):
        lines.extend(
            [
                "## LLM Audit Degraded",
                "",
                "- Live LLM analysis failed; marker-only deterministic signals were not promoted to candidate findings.",
                f"- degraded_reason: `{summary.get('degraded_reason')}`",
                f"- error_type: `{llm_evidence.get('error_type') or 'unknown'}`",
                f"- error_fingerprint: `{llm_evidence.get('error_fingerprint') or 'unknown'}`",
                "",
            ]
        )
    lines.extend(["## Candidate Suggestions", ""])
    if findings:
        for item in findings[:20]:
            lines.append(
                f"- `{item.get('finding_id')}` {item.get('severity')}: {item.get('title')} "
                f"(module={item.get('module')}, confidence={item.get('confidence')})"
            )
            drift = str(item.get("suspected_silent_degradation") or "").strip()
            if drift:
                lines.append(f"  - suspected_silent_degradation: {drift[:280]}")
            refs = item.get("reference_refs") or []
            if refs:
                joined_refs = ", ".join(str(ref) for ref in refs[:3])
                lines.append(f"  - reference_refs: {joined_refs}")
            code_refs = item.get("code_refs") or []
            if code_refs:
                joined_code_refs = ", ".join(str(ref) for ref in code_refs[:5])
                lines.append(f"  - code_refs: {joined_code_refs}")
    else:
        lines.append("- None. No silent degradation suggestion crossed the advisory threshold.")
    lines.extend(["", "## Suppressed Findings", ""])
    if suppressed_findings:
        for item in suppressed_findings[:20]:
            suppressed_by = item.get("suppressed_by") if isinstance(item.get("suppressed_by"), dict) else {}
            lines.append(
                f"- `{item.get('finding_id')}` {item.get('severity')}: {item.get('title')} "
                f"(module={item.get('module')}, matched_suppression_index={suppressed_by.get('matched_suppression_index')})"
            )
            reason = str(suppressed_by.get("reason") or "").strip()
            if reason:
                lines.append(f"  - reason: {reason[:280]}")
            expires_at = suppressed_by.get("expires_at")
            if expires_at:
                lines.append(f"  - expires_at: {expires_at}")
    else:
        lines.append("- None.")
    lines.extend([
        "",
        "## Production Gates",
        "",
        f"- production_ddl_gate: `{PRODUCTION_GATES['production_ddl_gate']}`",
        f"- production_frontend_dependency_gate: `{PRODUCTION_GATES['production_frontend_dependency_gate']}`",
        f"- production_backend_dependency_gate: `{PRODUCTION_GATES['production_backend_dependency_gate']}`",
        "",
    ])
    ci_adapter._write_text(path, "\n".join(lines))  # noqa: SLF001

def _split_csv(values: list[str] | None) -> list[str]:
    result: list[str] = []
    for value in values or []:
        result.extend(item.strip() for item in str(value).split(",") if item.strip())
    return result

def _trim(value: Any, max_chars: int) -> str:
    return str(value or "").strip()[:max_chars]

def _llm_hit_view(hits: Any, *, limit: int = LLM_MAX_MARKER_HITS) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for hit in hits or []:
        if not isinstance(hit, dict):
            continue
        result.append(
            {
                "marker": _trim(hit.get("marker"), 80),
                "path": _trim(hit.get("path"), 220),
                "line": hit.get("line"),
                "excerpt": _trim(hit.get("excerpt"), 120),
            }
        )
        if len(result) >= limit:
            break
    return result

def _llm_target_view(target: dict[str, Any]) -> dict[str, Any]:
    references: list[dict[str, Any]] = []
    for doc in target.get("reference_docs") or []:
        if not isinstance(doc, dict):
            continue
        references.append(
            {
                "path": _trim(doc.get("path"), 220),
                "exists": bool(doc.get("exists")),
                "excerpt": _trim(doc.get("excerpt"), LLM_MAX_REFERENCE_CHARS),
            }
        )
    samples: list[dict[str, str]] = []
    for sample in target.get("code_samples") or []:
        if not isinstance(sample, dict):
            continue
        samples.append(
            {
                "path": _trim(sample.get("path"), 220),
                "excerpt": _trim(sample.get("excerpt"), LLM_MAX_CODE_SAMPLE_CHARS),
            }
        )
        if len(samples) >= LLM_MAX_CODE_SAMPLE_COUNT:
            break
    return {
        "module": target.get("module"),
        "risk": target.get("risk"),
        "reference_docs": references,
        "code_paths": target.get("code_paths") or [],
        "code_file_count": target.get("code_file_count"),
        "semantic_expected_markers": target.get("semantic_expected_markers") or [],
        "semantic_expected_hits": _llm_hit_view(target.get("semantic_expected_hits")),
        "missing_semantic_markers": target.get("missing_semantic_markers") or [],
        "silent_degradation_hits": _llm_hit_view(target.get("silent_degradation_hits")),
        "code_samples": samples,
        "code_intelligence_refs": target.get("code_intelligence_refs") or {},
    }

def _print_success(check: str, payload: dict[str, Any], *, as_json: bool) -> None:
    compact = {"check": check, **payload}
    if as_json:
        print(json.dumps(compact, ensure_ascii=False, sort_keys=True))
        return
    print(" ".join(f"{key}={value}" for key, value in compact.items()))

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build warning-only AIstock nightly silent degradation audit suggestions.")
    parser.add_argument("--json", action="store_true", default=False)
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--llm-config", default=str(llm_adapter.DEFAULT_CONFIG_PATH))
    parser.add_argument("--prompt-pack", default=str(DEFAULT_PROMPT_PACK_PATH))
    parser.add_argument("--provider", choices=["deterministic", "github_models", "deepseek_api"], default="deterministic")
    parser.add_argument("--module", action="append", default=None, help="Module key; may be repeated or comma-separated.")
    parser.add_argument("--code-intelligence-json", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--invoke-llm", action="store_true")
    parser.add_argument("--fail-on-llm-error", action="store_true")
    parser.add_argument("--output", default=None)
    parser.add_argument("--markdown-output", default=None)
    return parser

def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        audit = build_audit(
            root=Path(args.root).resolve(),
            config_path=Path(args.config),
            llm_config_path=Path(args.llm_config),
            prompt_pack_path=Path(args.prompt_pack),
            provider=args.provider,
            modules=_split_csv(args.module),
            code_intelligence_json=Path(args.code_intelligence_json) if args.code_intelligence_json else None,
            invoke_llm=args.invoke_llm,
            fallback_on_llm_error=not args.fail_on_llm_error,
            run_id=args.run_id,
        )
        write_json(Path(args.output) if args.output else None, audit)
        write_markdown(Path(args.markdown_output) if args.markdown_output else None, audit)
        compact = {
            "schema_version": audit["schema_version"],
            "workflow_gate": audit["workflow_gate"],
            "llm_gate": audit["llm_gate"],
            "llm_reason": audit["llm_invocation_evidence"].get("reason"),
            "degraded_reason": audit["summary"].get("degraded_reason"),
            "review_target_count": audit["summary"]["review_target_count"],
            "finding_count": audit["summary"]["finding_count"],
            "suppressed_count": audit["summary"].get("suppressed_count", 0),
            "warning_only": audit["warning_only"],
            "llm_invoked": audit["llm_invocation_evidence"]["invoked"],
            "error_type": audit["llm_invocation_evidence"].get("error_type"),
            "error_fingerprint": audit["llm_invocation_evidence"].get("error_fingerprint"),
            "artifact": args.output,
            "markdown_artifact": args.markdown_output,
        }
        _print_success("nightly-silent-degradation-audit", compact, as_json=args.json)
        return 0
    except (SilentDegradationAuditError, llm_adapter.ProviderAdapterError) as exc:
        message = f"{exc.__class__.__name__}:{stable_id(llm_adapter.redact_secret_text(str(exc))[:500])}"
        if args.json:
            print(json.dumps({"gate": "failed", "error": message}, ensure_ascii=False), flush=True)
        else:
            print(f"gate=failed error={message}")
        return 2

if __name__ == "__main__":
    raise SystemExit(main())

