from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml

from backend.services.validation.module_registry import REPO_ROOT
from backend.services.validation.plan_catalog import (
    ALLOWED_BACKEND_PORTS,
    ALLOWED_COMMAND_KEYS,
    ALLOWED_FRONTEND_PORTS,
    FORBIDDEN_BACKEND_PORTS,
)


CATALOG_INTEGRITY_SCHEMA = "aistock_validation_catalog_integrity_v1"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "tmp" / "validation" / "catalog" / "integrity_report.json"

TEST_PLANS_PATH = Path("tests/aistock_validation/catalog/test_plans.yaml")
MODULE_REGISTRY_PATH = Path("tests/aistock_validation/catalog/module_registry.yaml")
UI_TARGETS_PATH = Path("tests/aistock_validation/catalog/ui_targets.yaml")
FILE_OWNERSHIP_PATH = Path("tests/aistock_validation/catalog/file_ownership.yaml")
RESOURCE_POLICIES_PATH = Path("tests/aistock_validation/catalog/resource_policies.yaml")
NOXFILE_PATH = Path("noxfile.py")
FRONTEND_NAV_PATH = Path("frontend/src/lib/navigation/nav-groups.ts")
WORKFLOWS_DIR = Path(".github/workflows")

P0 = "P0"
P1 = "P1"

RESOURCE_MODES = {
    "none",
    "readonly",
    "isolated_write",
    "candidate_write",
    "prod_readonly",
    "prod_approved_write",
}

LEGACY_MODULE_ALIASES = {
    "development_guardrails": "validation.guardrails",
    "paper_v2_selection_center": "paper_v2",
    "qe_archive": "qe.archive",
    "qe_data_completeness": "qe.data_completeness",
    "validation_center": "validation.center",
}

SAMPLE_LIMITED_RESOURCE_TYPES = {
    "shadow_schema_rows",
    "validation_sync_job",
    "qe_experiment",
    "qe_workspace",
    "qlib_candidate_path",
    "qlib_report",
}


@dataclass(frozen=True)
class CatalogFinding:
    finding_id: str
    severity: str
    file: str
    message: str
    expected: str
    actual: str
    suggested_fix: str
    subject: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "finding_id": self.finding_id,
            "severity": self.severity,
            "file": self.file,
            "message": self.message,
            "expected": self.expected,
            "actual": self.actual,
            "suggested_fix": self.suggested_fix,
        }
        if self.subject:
            payload["subject"] = self.subject
        return payload


class CatalogIntegrityChecker:
    """Read validation catalogs and report cross-catalog consistency findings."""

    def __init__(self, repo_root: Path | str | None = None) -> None:
        self.repo_root = Path(repo_root or REPO_ROOT).resolve()
        self.findings: list[CatalogFinding] = []
        self._yaml_cache: dict[Path, Any] = {}

    def run(self, *, output_path: Path | str | None = None) -> dict[str, Any]:
        self.findings = []
        self._yaml_cache = {}

        test_plans = self._load_mapping(TEST_PLANS_PATH)
        module_registry = self._load_mapping(MODULE_REGISTRY_PATH)
        ui_targets = self._load_mapping(UI_TARGETS_PATH)
        file_ownership = self._load_mapping(FILE_OWNERSHIP_PATH)
        resource_policies = self._load_mapping(RESOURCE_POLICIES_PATH)

        plans = self._list_items(test_plans, TEST_PLANS_PATH, "plans")
        modules = self._list_items(module_registry, MODULE_REGISTRY_PATH, "modules")
        targets = self._list_items(ui_targets, UI_TARGETS_PATH, "targets")
        ownership_rules = self._list_items(file_ownership, FILE_OWNERSHIP_PATH, "rules")
        policies = self._dict_items(resource_policies, RESOURCE_POLICIES_PATH, "policies")

        plan_keys = self._check_plan_keys(plans)
        module_ids = self._check_module_ids(modules)
        nox_sessions = self._collect_nox_sessions()
        workflow_sessions = self._collect_workflow_nox_sessions()

        self._check_plans(plans, module_ids, nox_sessions, policies)
        self._check_module_plan_references(modules, plan_keys)
        self._check_ui_targets(targets, plan_keys, module_ids)
        self._check_file_ownership(ownership_rules, module_ids)
        self._check_workflow_sessions(workflow_sessions, nox_sessions)

        report = self._build_report(
            plans=plans,
            modules=modules,
            targets=targets,
            ownership_rules=ownership_rules,
            policies=policies,
            nox_sessions=nox_sessions,
            workflow_sessions=workflow_sessions,
        )
        if output_path:
            write_integrity_report(report, Path(output_path))
        return report

    def _load_mapping(self, relative_path: Path) -> dict[str, Any]:
        path = self.repo_root / relative_path
        if relative_path in self._yaml_cache:
            cached = self._yaml_cache[relative_path]
            return cached if isinstance(cached, dict) else {}
        if not path.exists():
            self._add_finding(
                "CATALOG-001",
                P0,
                relative_path,
                f"catalog file {relative_path.as_posix()} is missing",
                "catalog YAML file exists and parses as a mapping",
                "file missing",
                "restore the catalog file or update the integrity checker inputs",
            )
            self._yaml_cache[relative_path] = {}
            return {}
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            self._add_finding(
                "CATALOG-001",
                P0,
                relative_path,
                f"catalog file {relative_path.as_posix()} is not valid YAML",
                "valid YAML mapping",
                str(exc),
                "fix YAML syntax before running validation catalogs",
            )
            self._yaml_cache[relative_path] = {}
            return {}
        if not isinstance(payload, dict):
            self._add_finding(
                "CATALOG-001",
                P0,
                relative_path,
                f"catalog file {relative_path.as_posix()} root is not a mapping",
                "YAML root mapping",
                type(payload).__name__,
                "change the catalog root to a mapping with named sections",
            )
            self._yaml_cache[relative_path] = {}
            return {}
        self._yaml_cache[relative_path] = payload
        return payload

    def _list_items(self, payload: dict[str, Any], relative_path: Path, key: str) -> list[dict[str, Any]]:
        raw_items = payload.get(key, [])
        if not isinstance(raw_items, list):
            self._add_finding(
                "CATALOG-001",
                P0,
                relative_path,
                f"catalog field {key!r} must be a list",
                f"{key}: []",
                type(raw_items).__name__,
                f"change {key!r} to a YAML list",
            )
            return []
        items: list[dict[str, Any]] = []
        for index, raw_item in enumerate(raw_items):
            if isinstance(raw_item, dict):
                items.append(raw_item)
                continue
            self._add_finding(
                "CATALOG-001",
                P0,
                relative_path,
                f"catalog field {key!r} item {index} must be a mapping",
                "list item mapping",
                type(raw_item).__name__,
                "replace the malformed item with a mapping",
            )
        return items

    def _dict_items(self, payload: dict[str, Any], relative_path: Path, key: str) -> dict[str, dict[str, Any]]:
        raw_items = payload.get(key, {})
        if not isinstance(raw_items, dict):
            self._add_finding(
                "CATALOG-001",
                P0,
                relative_path,
                f"catalog field {key!r} must be a mapping",
                f"{key}: {{}}",
                type(raw_items).__name__,
                f"change {key!r} to a YAML mapping",
            )
            return {}
        items: dict[str, dict[str, Any]] = {}
        for policy_id, raw_item in raw_items.items():
            if isinstance(raw_item, dict):
                items[str(policy_id)] = raw_item
                continue
            self._add_finding(
                "CATALOG-001",
                P0,
                relative_path,
                f"catalog field {key!r} item {policy_id!r} must be a mapping",
                "policy mapping",
                type(raw_item).__name__,
                "replace the malformed policy item with a mapping",
            )
        return items

    def _check_plan_keys(self, plans: list[dict[str, Any]]) -> set[str]:
        seen: set[str] = set()
        plan_keys: set[str] = set()
        for index, plan in enumerate(plans):
            plan_key = _clean_str(plan.get("plan_key"))
            if not plan_key:
                self._add_finding(
                    "CATALOG-002",
                    P0,
                    TEST_PLANS_PATH,
                    f"plan item {index} is missing plan_key",
                    "every plan has a unique non-empty plan_key",
                    "missing plan_key",
                    "add a stable plan_key to this plan",
                )
                continue
            if plan_key in seen:
                self._add_finding(
                    "CATALOG-002",
                    P0,
                    TEST_PLANS_PATH,
                    f"duplicate plan_key {plan_key!r}",
                    "plan_key values are unique",
                    f"duplicate {plan_key}",
                    "rename or remove the duplicate plan entry",
                    subject=plan_key,
                )
            seen.add(plan_key)
            plan_keys.add(plan_key)
        return plan_keys

    def _check_module_ids(self, modules: list[dict[str, Any]]) -> set[str]:
        module_ids: set[str] = set()
        seen: set[str] = set()
        for module in modules:
            module_id = _clean_str(module.get("module_id"))
            if not module_id:
                self._add_finding(
                    "CATALOG-006",
                    P1,
                    MODULE_REGISTRY_PATH,
                    "module registry item is missing module_id",
                    "every module registry entry has a module_id",
                    "missing module_id",
                    "add a stable module_id",
                )
                continue
            if module_id in seen:
                self._add_finding(
                    "CATALOG-006",
                    P1,
                    MODULE_REGISTRY_PATH,
                    f"duplicate module_id {module_id!r}",
                    "module_id values are unique",
                    f"duplicate {module_id}",
                    "rename or remove the duplicate module entry",
                    subject=module_id,
                )
            seen.add(module_id)
            module_ids.add(module_id)
        return module_ids

    def _collect_nox_sessions(self) -> set[str]:
        path = self.repo_root / NOXFILE_PATH
        if not path.exists():
            self._add_finding(
                "CATALOG-004",
                P0,
                NOXFILE_PATH,
                "noxfile.py is missing",
                "noxfile.py exists and defines allowlisted sessions",
                "file missing",
                "restore noxfile.py before enabling catalog plans",
            )
            return set()
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(NOXFILE_PATH))
        except SyntaxError as exc:
            self._add_finding(
                "CATALOG-004",
                P0,
                NOXFILE_PATH,
                "noxfile.py cannot be parsed",
                "valid Python module with @nox.session functions",
                str(exc),
                "fix noxfile.py syntax",
            )
            return set()
        sessions: set[str] = set()
        for node in tree.body:
            if not isinstance(node, ast.FunctionDef):
                continue
            for decorator in node.decorator_list:
                if not _is_nox_session_decorator(decorator):
                    continue
                sessions.add(_decorator_session_name(decorator) or node.name)
        return sessions

    def _collect_workflow_nox_sessions(self) -> set[str]:
        workflow_root = self.repo_root / WORKFLOWS_DIR
        if not workflow_root.exists():
            return set()
        sessions: set[str] = set()
        for path in sorted(workflow_root.glob("*.yml")) + sorted(workflow_root.glob("*.yaml")):
            relative_path = path.relative_to(self.repo_root)
            text = path.read_text(encoding="utf-8")
            for match in re.finditer(r"(?:python\s+-m\s+)?nox\s+(?:\\\s*)?-s\s+([A-Za-z_][A-Za-z0-9_]*)", text):
                sessions.add(match.group(1))
            try:
                payload = yaml.safe_load(text) or {}
            except yaml.YAMLError as exc:
                self._add_finding(
                    "CATALOG-001",
                    P0,
                    relative_path,
                    f"workflow file {relative_path.as_posix()} is not valid YAML",
                    "valid GitHub Actions YAML",
                    str(exc),
                    "fix workflow YAML syntax",
                )
                continue
            sessions.update(_workflow_matrix_sessions(payload))
        return sessions

    def _check_plans(
        self,
        plans: list[dict[str, Any]],
        module_ids: set[str],
        nox_sessions: set[str],
        resource_policies: dict[str, dict[str, Any]],
    ) -> None:
        for plan in plans:
            plan_key = _clean_str(plan.get("plan_key")) or "<missing-plan-key>"
            command_key = _clean_str(plan.get("command_key"))
            nox_session = _clean_str(plan.get("nox_session"))
            if command_key not in ALLOWED_COMMAND_KEYS:
                self._add_finding(
                    "CATALOG-003",
                    P0,
                    TEST_PLANS_PATH,
                    f"plan {plan_key} uses non-allowlisted command_key",
                    "command_key appears in backend/services/validation/plan_catalog.py",
                    command_key or "missing command_key",
                    "add a mapping to ALLOWED_COMMAND_KEYS or set enabled=false until implemented",
                    subject=plan_key,
                )
            else:
                expected_session = ALLOWED_COMMAND_KEYS[command_key]
                if nox_session and nox_session != expected_session:
                    self._add_finding(
                        "CATALOG-003",
                        P0,
                        TEST_PLANS_PATH,
                        f"plan {plan_key} command_key maps to a different nox_session",
                        f"{command_key} maps to nox_session={expected_session!r}",
                        f"nox_session={nox_session!r}",
                        "align nox_session with ALLOWED_COMMAND_KEYS",
                        subject=plan_key,
                    )
                nox_session = nox_session or expected_session
            if not nox_session:
                self._add_finding(
                    "CATALOG-004",
                    P0,
                    TEST_PLANS_PATH,
                    f"plan {plan_key} is missing nox_session",
                    "nox_session references an existing @nox.session",
                    "missing nox_session",
                    "add nox_session to the plan",
                    subject=plan_key,
                )
            elif nox_session not in nox_sessions:
                self._add_finding(
                    "CATALOG-004",
                    P0,
                    TEST_PLANS_PATH,
                    f"plan {plan_key} references missing nox_session {nox_session!r}",
                    "nox_session exists in noxfile.py",
                    f"{nox_session} not found",
                    "add the nox session or disable/remove the plan until implemented",
                    subject=plan_key,
                )
            module_id = _clean_str(plan.get("module"))
            if module_id and not _resolve_module_id(module_id, module_ids):
                self._add_finding(
                    "CATALOG-006",
                    P1,
                    TEST_PLANS_PATH,
                    f"plan {plan_key} references unknown module {module_id!r}",
                    "plan module exists in module_registry.yaml",
                    module_id,
                    "add the module to module_registry.yaml or correct the plan module",
                    subject=plan_key,
                )
            elif not module_id:
                self._add_finding(
                    "CATALOG-006",
                    P1,
                    TEST_PLANS_PATH,
                    f"plan {plan_key} is missing module",
                    "plan module exists in module_registry.yaml",
                    "missing module",
                    "add a module field to the plan",
                    subject=plan_key,
                )
            self._check_runner_safety(plan, plan_key)
            self._check_resource_policy(plan, plan_key, resource_policies)

    def _check_runner_safety(self, plan: dict[str, Any], plan_key: str) -> None:
        runner_enabled = _as_bool(plan.get("runner_enabled"))
        if not runner_enabled:
            return
        unsafe_reasons: list[str] = []
        if _as_bool(plan.get("writes_business_state")):
            unsafe_reasons.append("writes_business_state=true")
        if _as_bool(plan.get("writes_database")):
            unsafe_reasons.append("writes_database=true")
        backend_ports = _as_int_list(plan.get("allowed_backend_ports"))
        frontend_ports = _as_int_list(plan.get("allowed_frontend_ports"))
        forbidden_backend = sorted(set(backend_ports) & FORBIDDEN_BACKEND_PORTS)
        if forbidden_backend:
            unsafe_reasons.append(f"forbidden_backend_ports={forbidden_backend}")
        forbidden_frontend = sorted(port for port in frontend_ports if port == 3000)
        if forbidden_frontend:
            unsafe_reasons.append(f"forbidden_frontend_ports={forbidden_frontend}")
        non_dev_backend = sorted(set(backend_ports) - ALLOWED_BACKEND_PORTS)
        if non_dev_backend:
            unsafe_reasons.append(f"non_dev_backend_ports={non_dev_backend}")
        non_dev_frontend = sorted(set(frontend_ports) - ALLOWED_FRONTEND_PORTS)
        if non_dev_frontend:
            unsafe_reasons.append(f"non_dev_frontend_ports={non_dev_frontend}")
        if unsafe_reasons:
            self._add_finding(
                "CATALOG-005",
                P0,
                TEST_PLANS_PATH,
                f"runner_enabled plan {plan_key} violates controlled-runner safety",
                "runner_enabled plans are read-only and use only dev ports",
                ", ".join(unsafe_reasons),
                "set runner_enabled=false or make the plan read-only on allowed dev ports",
                subject=plan_key,
            )
        level = _clean_str(plan.get("level")).upper()
        title_key = f"{plan_key} {_clean_str(plan.get('title'))}".lower()
        if level in {"L3", "L4", "L5"} or "live" in title_key:
            self._add_finding(
                "CATALOG-012",
                P1,
                TEST_PLANS_PATH,
                f"L3/live plan {plan_key} must not be runner_enabled by default",
                "L3/live or long-running plans default runner_enabled=false",
                "runner_enabled=true",
                "set runner_enabled=false and run via nightly/manual gate",
                subject=plan_key,
            )

    def _check_resource_policy(
        self,
        plan: dict[str, Any],
        plan_key: str,
        policies: dict[str, dict[str, Any]],
    ) -> None:
        raw_policy = plan.get("resource_policy")
        if _as_bool(plan.get("writes_business_state")) and not raw_policy:
            self._add_finding(
                "RESOURCE-001",
                P0,
                TEST_PLANS_PATH,
                f"plan {plan_key} writes business state but lacks resource_policy",
                "writes_business_state=true plans declare resource_policy",
                "missing resource_policy",
                "add an isolated resource_policy or set writes_business_state=false",
                subject=plan_key,
            )
        resolved_policy, policy_ref = self._resolve_plan_resource_policy(plan, plan_key, policies)
        if raw_policy is not None and not resolved_policy:
            return
        creates_resources = _as_bool(resolved_policy.get("creates_validation_resources"))
        resource_types = _as_list(resolved_policy.get("resource_types"))
        if creates_resources and not resource_types:
            self._add_finding(
                "RESOURCE-002",
                P0,
                TEST_PLANS_PATH,
                f"plan {plan_key} creates validation resources but lacks resource_types",
                "creates_validation_resources=true plans list resource_types",
                "missing resource_types",
                "add resource_types describing the validation resources",
                subject=plan_key,
            )
        if _cleanup_required(resolved_policy.get("cleanup_required")):
            cleanup_policy = resolved_policy.get("cleanup_policy") or plan.get("cleanup_policy")
            cleanup_command = _clean_str(resolved_policy.get("cleanup_command")) or _clean_str(plan.get("cleanup_command"))
            if isinstance(cleanup_policy, dict):
                cleanup_command = cleanup_command or _clean_str(cleanup_policy.get("cleanup_command"))
            if not cleanup_policy and not cleanup_command:
                self._add_finding(
                    "RESOURCE-003",
                    P0,
                    TEST_PLANS_PATH,
                    f"plan {plan_key} requires cleanup but lacks cleanup command or cleanup_policy",
                    "cleanup_required=true plans provide cleanup_command or cleanup_policy",
                    "missing cleanup command and cleanup_policy",
                    "add cleanup_command or cleanup_policy with verification details",
                    subject=plan_key,
                )
        resource_mode = _clean_str(resolved_policy.get("resource_mode"))
        if resource_mode == "prod_approved_write" and not _as_bool(resolved_policy.get("manual_approval_required")):
            self._add_finding(
                "RESOURCE-004",
                P0,
                TEST_PLANS_PATH,
                f"plan {plan_key} allows prod_approved_write without manual approval",
                "prod_approved_write requires manual_approval_required=true",
                "manual_approval_required is false or missing",
                "set manual_approval_required=true and keep runner_enabled=false",
                subject=plan_key,
            )
        if self._plan_has_production_adjacent_write(plan, resolved_policy) and resource_mode != "prod_approved_write":
            forbidden_targets = _as_list(resolved_policy.get("forbidden_db_targets"))
            if resource_mode != "prod_readonly" and "prod_db" not in forbidden_targets:
                self._add_finding(
                    "RESOURCE-005",
                    P0,
                    TEST_PLANS_PATH,
                    f"plan {plan_key} does not explicitly forbid prod_db",
                    "production-adjacent write plans include forbidden_db_targets: [prod_db]",
                    f"forbidden_db_targets={forbidden_targets}",
                    "add prod_db to forbidden_db_targets or mark the plan prod_readonly/prod_approved_write with approvals",
                    subject=plan_key,
                )
        level = _clean_str(plan.get("level")).upper()
        if level in {"L4", "L5"}:
            runtime_policy = plan.get("runtime_policy") if isinstance(plan.get("runtime_policy"), dict) else {}
            evidence_policy = plan.get("evidence_policy") if isinstance(plan.get("evidence_policy"), dict) else {}
            missing_parts = []
            if not runtime_policy.get("timeout_seconds") and not plan.get("max_duration_seconds"):
                missing_parts.append("runtime timeout")
            if not evidence_policy:
                missing_parts.append("evidence_policy")
            if missing_parts:
                self._add_finding(
                    "RESOURCE-006",
                    P1,
                    TEST_PLANS_PATH,
                    f"L4/L5 plan {plan_key} lacks {', '.join(missing_parts)}",
                    "L4/L5 plans declare runtime timeout and evidence_policy",
                    ", ".join(missing_parts),
                    "add runtime_policy.timeout_seconds and evidence_policy",
                    subject=plan_key,
                )
        if resource_mode == "candidate_write" or policy_ref == "qlib_candidate_path":
            forbidden_paths = _as_list(resolved_policy.get("forbidden_paths"))
            if not forbidden_paths:
                self._add_finding(
                    "RESOURCE-007",
                    P1,
                    TEST_PLANS_PATH,
                    f"candidate path plan {plan_key} lacks forbidden production paths",
                    "candidate path policies declare forbidden_paths for production Qlib data",
                    "missing forbidden_paths",
                    "add forbidden_paths covering production Qlib/FI data locations",
                    subject=plan_key,
                )
        if self._requires_sample_bounds(plan, resolved_policy, policy_ref):
            missing_limits = []
            if resolved_policy.get("max_sample_symbols") is None:
                missing_limits.append("max_sample_symbols")
            if resolved_policy.get("max_date_window_days") is None:
                missing_limits.append("max_date_window_days")
            if missing_limits:
                self._add_finding(
                    "RESOURCE-008",
                    P1,
                    TEST_PLANS_PATH,
                    f"small-sample write plan {plan_key} lacks {', '.join(missing_limits)}",
                    "small-sample write plans declare max_sample_symbols and max_date_window_days",
                    ", ".join(missing_limits),
                    "add bounded sample and date-window limits to resource_policy",
                    subject=plan_key,
                )

    def _resolve_plan_resource_policy(
        self,
        plan: dict[str, Any],
        plan_key: str,
        policies: dict[str, dict[str, Any]],
    ) -> tuple[dict[str, Any], str | None]:
        raw_policy = plan.get("resource_policy")
        if raw_policy is None:
            return {}, None
        if isinstance(raw_policy, str):
            policy_ref = raw_policy.strip()
            if policy_ref not in policies:
                self._unknown_resource_policy(plan_key, policy_ref)
                return {}, policy_ref
            return dict(policies[policy_ref]), policy_ref
        if not isinstance(raw_policy, dict):
            self._unknown_resource_policy(plan_key, type(raw_policy).__name__)
            return {}, None

        policy_ref = _clean_str(
            raw_policy.get("policy")
            or raw_policy.get("policy_id")
            or raw_policy.get("policy_ref")
            or raw_policy.get("resource_policy_id")
        )
        raw_mode = _clean_str(raw_policy.get("resource_mode"))
        if not policy_ref and raw_mode in policies:
            policy_ref = raw_mode
        base: dict[str, Any] = {}
        if policy_ref:
            if policy_ref not in policies:
                self._unknown_resource_policy(plan_key, policy_ref)
            else:
                base = dict(policies[policy_ref])
        merged = {**base, **raw_policy}
        if policy_ref and raw_mode == policy_ref and base.get("resource_mode"):
            merged["resource_mode"] = base["resource_mode"]
        merged["resource_policy_id"] = policy_ref or None
        resource_mode = _clean_str(merged.get("resource_mode"))
        if resource_mode and resource_mode not in RESOURCE_MODES:
            self._add_finding(
                "RESOURCE-REF-001",
                P0,
                TEST_PLANS_PATH,
                f"plan {plan_key} references unknown resource_mode {resource_mode!r}",
                f"resource_mode is one of {sorted(RESOURCE_MODES)} or a policy id in resource_policies.yaml",
                resource_mode,
                "fix resource_mode or add the referenced resource policy",
                subject=plan_key,
            )
            return {}, policy_ref or resource_mode
        if not resource_mode:
            self._add_finding(
                "RESOURCE-REF-001",
                P0,
                TEST_PLANS_PATH,
                f"plan {plan_key} resource_policy lacks resource_mode",
                "resource_policy declares resource_mode or references a named policy",
                "missing resource_mode",
                "add resource_mode or policy_ref",
                subject=plan_key,
            )
            return {}, policy_ref
        return merged, policy_ref

    def _unknown_resource_policy(self, plan_key: str, policy_ref: str) -> None:
        self._add_finding(
            "RESOURCE-REF-001",
            P0,
            TEST_PLANS_PATH,
            f"plan {plan_key} references unknown resource_policy {policy_ref!r}",
            "resource_policy references a policy in resource_policies.yaml",
            policy_ref or "missing resource_policy reference",
            "add the policy to resource_policies.yaml or correct the plan reference",
            subject=plan_key,
        )

    @staticmethod
    def _plan_has_production_adjacent_write(plan: dict[str, Any], policy: dict[str, Any]) -> bool:
        resource_mode = _clean_str(policy.get("resource_mode"))
        business_state_write = _clean_str(policy.get("business_state_write"))
        return any(
            [
                _as_bool(plan.get("writes_database")),
                _as_bool(plan.get("writes_business_state")),
                _as_bool(policy.get("creates_validation_resources")),
                resource_mode in {"isolated_write", "candidate_write", "prod_approved_write"},
                business_state_write in {"isolated", "prod_approved"},
            ]
        )

    @staticmethod
    def _requires_sample_bounds(
        plan: dict[str, Any],
        policy: dict[str, Any],
        policy_ref: str | None,
    ) -> bool:
        if not CatalogIntegrityChecker._plan_has_production_adjacent_write(plan, policy):
            return False
        resource_types = set(_as_list(policy.get("resource_types")))
        if resource_types & SAMPLE_LIMITED_RESOURCE_TYPES:
            return True
        return policy_ref in {"shadow_schema_small_sample", "validation_qe_experiment", "qlib_candidate_path"}

    def _check_module_plan_references(self, modules: list[dict[str, Any]], plan_keys: set[str]) -> None:
        for module in modules:
            module_id = _clean_str(module.get("module_id")) or "<missing-module-id>"
            test_plans = module.get("test_plans") or {}
            if not isinstance(test_plans, dict):
                self._add_finding(
                    "CATALOG-007",
                    P1,
                    MODULE_REGISTRY_PATH,
                    f"module {module_id} test_plans must be a mapping",
                    "test_plans has required_on_change/recommended lists",
                    type(test_plans).__name__,
                    "change test_plans to a mapping",
                    subject=module_id,
                )
                continue
            for field_name in ("required_on_change", "recommended"):
                for plan_key in _as_list(test_plans.get(field_name)):
                    if plan_key not in plan_keys:
                        self._add_finding(
                            "CATALOG-007",
                            P1,
                            MODULE_REGISTRY_PATH,
                            f"module {module_id} references unknown test plan {plan_key!r}",
                            "module_registry test plan references exist in test_plans.yaml",
                            f"{field_name}={plan_key}",
                            "add the test plan or remove the stale module_registry reference",
                            subject=module_id,
                        )

    def _check_ui_targets(
        self,
        targets: list[dict[str, Any]],
        plan_keys: set[str],
        module_ids: set[str],
    ) -> None:
        target_hrefs = {_clean_str(target.get("href")) for target in targets if _clean_str(target.get("href"))}
        nav_hrefs = self._collect_frontend_nav_hrefs()
        for href in sorted(nav_hrefs - target_hrefs):
            self._add_finding(
                "CATALOG-008",
                P1,
                UI_TARGETS_PATH,
                f"frontend nav href {href!r} is missing from ui_targets.yaml",
                "frontend navigation hrefs are registered as UI targets",
                f"{href} missing from ui_targets.yaml",
                "add a ui_targets.yaml entry for this route or remove the nav item",
                subject=href,
            )
        for href in sorted(target_hrefs - nav_hrefs):
            self._add_finding(
                "CATALOG-008",
                P1,
                UI_TARGETS_PATH,
                f"ui target href {href!r} is missing from frontend navigation",
                "ui_targets.yaml hrefs match frontend navigation hrefs",
                f"{href} missing from nav-groups.ts",
                "add the nav item or remove the stale ui target",
                subject=href,
            )
        for target in targets:
            route_id = _clean_str(target.get("route_id")) or _clean_str(target.get("href")) or "<missing-route-id>"
            primary_module = _clean_str(target.get("primary_module"))
            if not primary_module or not _resolve_module_id(primary_module, module_ids):
                self._add_finding(
                    "CATALOG-009",
                    P1,
                    UI_TARGETS_PATH,
                    f"ui target {route_id} references unknown primary_module {primary_module!r}",
                    "primary_module exists in module_registry.yaml",
                    primary_module or "missing primary_module",
                    "add the module to module_registry.yaml or correct the ui target",
                    subject=route_id,
                )
            for module_id in _as_list(target.get("impact_modules")):
                if not _resolve_module_id(module_id, module_ids):
                    self._add_finding(
                        "CATALOG-009",
                        P1,
                        UI_TARGETS_PATH,
                        f"ui target {route_id} references unknown impact_module {module_id!r}",
                        "impact_modules exist in module_registry.yaml",
                        module_id,
                        "add the module to module_registry.yaml or correct impact_modules",
                        subject=route_id,
                    )
            for plan_key in [*_as_list(target.get("required_test_plans")), *_as_list(target.get("recommended_test_plans"))]:
                if plan_key not in plan_keys:
                    self._add_finding(
                        "CATALOG-008",
                        P1,
                        UI_TARGETS_PATH,
                        f"ui target {route_id} references unknown test plan {plan_key!r}",
                        "ui target test plan references exist in test_plans.yaml",
                        plan_key,
                        "add the plan or correct the ui target reference",
                        subject=route_id,
                    )

    def _collect_frontend_nav_hrefs(self) -> set[str]:
        path = self.repo_root / FRONTEND_NAV_PATH
        if not path.exists():
            self._add_finding(
                "CATALOG-008",
                P1,
                FRONTEND_NAV_PATH,
                "frontend navigation source is missing",
                "frontend/src/lib/navigation/nav-groups.ts exists",
                "file missing",
                "restore the frontend navigation source or adjust the integrity input",
            )
            return set()
        text = path.read_text(encoding="utf-8")
        return set(re.findall(r'href:\s*"([^"]+)"', text))

    def _check_file_ownership(self, rules: list[dict[str, Any]], module_ids: set[str]) -> None:
        for rule in rules:
            rule_id = _clean_str(rule.get("rule_id")) or "<missing-rule-id>"
            primary_module = _clean_str(rule.get("primary_module"))
            if not primary_module or not _resolve_module_id(primary_module, module_ids):
                self._add_finding(
                    "CATALOG-010",
                    P1,
                    FILE_OWNERSHIP_PATH,
                    f"file ownership rule {rule_id} references unknown primary_module {primary_module!r}",
                    "file ownership primary_module exists in module_registry.yaml",
                    primary_module or "missing primary_module",
                    "add the module to module_registry.yaml or correct the ownership rule",
                    subject=rule_id,
                )
            for module_id in _as_list(rule.get("impact_modules")):
                if not _resolve_module_id(module_id, module_ids):
                    self._add_finding(
                        "CATALOG-010",
                        P1,
                        FILE_OWNERSHIP_PATH,
                        f"file ownership rule {rule_id} references unknown impact_module {module_id!r}",
                        "file ownership impact_modules exist in module_registry.yaml",
                        module_id,
                        "add the module to module_registry.yaml or correct impact_modules",
                        subject=rule_id,
                    )

    def _check_workflow_sessions(self, workflow_sessions: set[str], nox_sessions: set[str]) -> None:
        for session in sorted(workflow_sessions - nox_sessions):
            self._add_finding(
                "CATALOG-011",
                P1,
                WORKFLOWS_DIR,
                f"GitHub workflow references missing nox session {session!r}",
                "workflow nox sessions exist in noxfile.py",
                f"{session} not found",
                "add the nox session or update the workflow matrix/command",
                subject=session,
            )

    def _build_report(
        self,
        *,
        plans: list[dict[str, Any]],
        modules: list[dict[str, Any]],
        targets: list[dict[str, Any]],
        ownership_rules: list[dict[str, Any]],
        policies: dict[str, dict[str, Any]],
        nox_sessions: set[str],
        workflow_sessions: set[str],
    ) -> dict[str, Any]:
        findings = sorted(
            [finding.to_dict() for finding in self.findings],
            key=lambda item: (
                0 if item["severity"] == P0 else 1,
                item["finding_id"],
                item["file"],
                item.get("subject") or "",
            ),
        )
        error_count = sum(1 for finding in findings if finding["severity"] == P0)
        warning_count = len(findings) - error_count
        return {
            "schema_version": CATALOG_INTEGRITY_SCHEMA,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "repo_root": str(self.repo_root),
            "state": "failed" if error_count else "passed",
            "summary": {
                "error_count": error_count,
                "warning_count": warning_count,
                "finding_count": len(findings),
                "plans": len(plans),
                "runner_enabled_plans": sum(1 for plan in plans if _as_bool(plan.get("runner_enabled"))),
                "modules": len(modules),
                "ui_targets": len(targets),
                "file_ownership_rules": len(ownership_rules),
                "resource_policies": len(policies),
                "nox_sessions": len(nox_sessions),
                "workflow_nox_sessions": len(workflow_sessions),
                "production_8001_touched": False,
                "production_db_touched": False,
            },
            "findings": findings,
        }

    def _add_finding(
        self,
        finding_id: str,
        severity: str,
        file: Path,
        message: str,
        expected: str,
        actual: str,
        suggested_fix: str,
        *,
        subject: str | None = None,
    ) -> None:
        self.findings.append(
            CatalogFinding(
                finding_id=finding_id,
                severity=severity,
                file=file.as_posix(),
                message=message,
                expected=expected,
                actual=actual,
                suggested_fix=suggested_fix,
                subject=subject,
            )
        )


def run_catalog_integrity(
    repo_root: Path | str | None = None,
    *,
    output_path: Path | str | None = None,
) -> dict[str, Any]:
    return CatalogIntegrityChecker(repo_root).run(output_path=output_path)


def write_integrity_report(report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _clean_str(value: Any) -> str:
    return str(value or "").strip()


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, Iterable) and not isinstance(value, dict):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _as_int_list(value: Any) -> list[int]:
    ports: list[int] = []
    for raw_port in _as_list(value):
        try:
            ports.append(int(raw_port))
        except ValueError:
            continue
    return ports


def _cleanup_required(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "required", "manual_review", "ttl", "immediate"}
    return bool(value)


def _resolve_module_id(module_id: str, module_ids: set[str]) -> str | None:
    if module_id in module_ids:
        return module_id
    alias = LEGACY_MODULE_ALIASES.get(module_id)
    if alias in module_ids:
        return alias
    dotted = module_id.replace("_", ".")
    if dotted in module_ids:
        return dotted
    return None


def _is_nox_session_decorator(decorator: ast.expr) -> bool:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    return (
        isinstance(target, ast.Attribute)
        and target.attr == "session"
        and isinstance(target.value, ast.Name)
        and target.value.id == "nox"
    )


def _decorator_session_name(decorator: ast.expr) -> str | None:
    if not isinstance(decorator, ast.Call):
        return None
    for keyword in decorator.keywords:
        if keyword.arg == "name" and isinstance(keyword.value, ast.Constant):
            return str(keyword.value.value)
    return None


def _workflow_matrix_sessions(payload: Any) -> set[str]:
    sessions: set[str] = set()
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key == "session":
                sessions.update(_literal_session_values(value))
            else:
                sessions.update(_workflow_matrix_sessions(value))
    elif isinstance(payload, list):
        for item in payload:
            sessions.update(_workflow_matrix_sessions(item))
    return sessions


def _literal_session_values(value: Any) -> set[str]:
    if isinstance(value, str):
        value = value.strip()
        return {value} if value and "${{" not in value else set()
    if isinstance(value, list):
        return {str(item).strip() for item in value if str(item).strip() and "${{" not in str(item)}
    return set()
