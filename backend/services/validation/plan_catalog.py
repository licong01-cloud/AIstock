from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PLAN_CATALOG_PATH = REPO_ROOT / "tests" / "aistock_validation" / "catalog" / "test_plans.yaml"

ALLOWED_COMMAND_KEYS: dict[str, str] = {
    "nox_l0": "l0",
    "nox_guardrail_changed_files": "guardrail_changed_files",
    "nox_validation_coverage_backend": "validation_coverage_backend",
    "nox_validation_module_registry_l0": "validation_module_registry_l0",
    "nox_validation_center_backend": "validation_center_backend",
    "nox_validation_catalog_integrity": "validation_catalog_integrity",
    "nox_validation_center_live_readonly": "validation_center_live_readonly",
    "nox_validation_center_real_port_ui": "validation_center_real_port_ui",
    "nox_validation_center_ui": "validation_center_ui",
    "nox_qe_data_contract_backend": "qe_data_contract_backend",
    "nox_qe_archive_backend": "qe_archive_backend",
    "nox_qe_archive_data_quality": "qe_archive_data_quality",
    "nox_qe_archive_l3": "qe_archive_l3",
    "nox_qe_mcp_backend": "qe_mcp_backend",
    "nox_qe_read_l3": "qe_read_l3",
    "nox_research_pipeline_backend": "research_pipeline_backend",
    "nox_research_mcp_contract": "research_mcp_contract",
    "nox_ra_phase0_baseline": "ra_phase0_baseline",
    "nox_ra_phase1_memory_tree": "ra_phase1_memory_tree",
    "nox_ra_phase2_graph_context": "ra_phase2_graph_context",
    "nox_ra_phase3_react_grounding": "ra_phase3_react_grounding",
    "nox_ra_phase4_external_research": "ra_phase4_external_research",
    "nox_ra_phase5_agent_teams": "ra_phase5_agent_teams",
    "nox_research_assistant_backend": "research_assistant_backend",
    "nox_research_assistant_mcp_contract": "research_assistant_mcp_contract",
    "nox_research_assistant_ui": "research_assistant_ui",
    "nox_paper_v2_backend": "paper_v2_backend",
    "nox_paper_v2_l3": "paper_v2_l3",
    "nox_simulation_core_l2": "simulation_core_l2",
    "nox_localsim_unattended_l3": "localsim_unattended_l3",
    "nox_miniqmt_sim_stub_l3": "miniqmt_sim_stub_l3",
    "nox_simulation_runtime_ops_ui": "simulation_runtime_ops_ui",
    "nox_simulation_dual_backend_l4": "simulation_dual_backend_l4",
    "nox_miniqmt_sim_trading_hours_l5": "miniqmt_sim_trading_hours_l5",
    "nox_model_registry_backend": "model_registry_backend",
    "nox_market_regime_label": "market_regime_label",
    "nox_rl_execution_smoke": "rl_execution_smoke",
    "nox_data_sync_autonomy_backend": "data_sync_autonomy_backend",
    "nox_data_quality_deep": "data_quality_deep",
    "nox_dr_validate": "dr_validate",
    "nox_strategy_package_governance_ui": "strategy_package_governance_ui",
    "nox_market_regime_ui": "market_regime_ui",
    "nox_rl_execution_ui": "rl_execution_ui",
}
ALLOWED_BACKEND_PORTS = {8011, 8012}
ALLOWED_FRONTEND_PORTS = {3011, 3012}
FORBIDDEN_BACKEND_PORTS = {8001}


class ValidationCatalogError(ValueError):
    """Raised when the validation plan catalog violates safety rules."""


def load_allowed_command_keys_from_source(source_path: Path) -> dict[str, str]:
    """Read ALLOWED_COMMAND_KEYS from a plan_catalog.py file without importing it."""
    try:
        tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    except OSError as exc:
        raise ValidationCatalogError(f"Cannot read validation command allowlist: {source_path}") from exc
    except SyntaxError as exc:
        raise ValidationCatalogError(f"Invalid validation command allowlist source: {source_path}: {exc}") from exc

    for node in tree.body:
        value_node: ast.expr | None = None
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == "ALLOWED_COMMAND_KEYS" for target in node.targets):
                value_node = node.value
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == "ALLOWED_COMMAND_KEYS":
                value_node = node.value
        if value_node is None:
            continue
        try:
            raw = ast.literal_eval(value_node)
        except (ValueError, TypeError) as exc:
            raise ValidationCatalogError(
                f"Validation command allowlist must be a literal dict: {source_path}"
            ) from exc
        if not isinstance(raw, dict) or not all(isinstance(key, str) and isinstance(value, str) for key, value in raw.items()):
            raise ValidationCatalogError(
                f"Validation command allowlist must be dict[str, str]: {source_path}"
            )
        return dict(raw)

    raise ValidationCatalogError(f"Validation command allowlist not found: {source_path}")


class ValidationPlanCatalog:
    """Read and validate the Validation Center nox-plan allowlist."""

    def __init__(self, catalog_path: Path | None = None, *, allowed_command_keys: dict[str, str] | None = None) -> None:
        self.catalog_path = Path(catalog_path or DEFAULT_PLAN_CATALOG_PATH)
        self.allowed_command_keys = dict(ALLOWED_COMMAND_KEYS if allowed_command_keys is None else allowed_command_keys)

    def load(self) -> dict[str, Any]:
        if not self.catalog_path.exists():
            return {
                "schema_version": "aistock_validation_plans_v1",
                "plans": [],
                "catalog_path": self._repo_path(self.catalog_path),
                "missing": True,
            }
        try:
            payload = yaml.safe_load(self.catalog_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            raise ValidationCatalogError(f"Invalid validation plan catalog YAML: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValidationCatalogError("Validation plan catalog root must be a mapping.")
        plans = payload.get("plans")
        if plans is None:
            plans = []
        if not isinstance(plans, list):
            raise ValidationCatalogError("Validation plan catalog field 'plans' must be a list.")
        normalized = [self._validate_plan(item) for item in plans]
        return {
            "schema_version": payload.get("schema_version") or "aistock_validation_plans_v1",
            "catalog_path": self._repo_path(self.catalog_path),
            "missing": False,
            "plans": normalized,
        }

    def list_plans(self) -> list[dict[str, Any]]:
        return self.load()["plans"]

    def get_plan(self, plan_key: str) -> dict[str, Any] | None:
        for plan in self.list_plans():
            if plan["plan_key"] == plan_key:
                return plan
        return None

    def _validate_plan(self, raw_plan: Any) -> dict[str, Any]:
        if not isinstance(raw_plan, dict):
            raise ValidationCatalogError("Each validation plan must be a mapping.")
        plan = dict(raw_plan)
        plan_key = str(plan.get("plan_key") or "").strip()
        command_key = str(plan.get("command_key") or "").strip()
        if not plan_key:
            raise ValidationCatalogError("Validation plan is missing plan_key.")
        if command_key not in self.allowed_command_keys:
            raise ValidationCatalogError(
                f"Validation plan {plan_key} uses non-allowlisted command_key={command_key!r}."
            )
        nox_session = str(plan.get("nox_session") or "").strip()
        expected_session = self.allowed_command_keys[command_key]
        if nox_session and nox_session != expected_session:
            raise ValidationCatalogError(
                f"Validation plan {plan_key} maps {command_key} to nox_session={nox_session!r}, "
                f"expected {expected_session!r}."
            )
        backend_ports = self._validate_ports(
            plan_key,
            plan.get("allowed_backend_ports") or [],
            allowed=ALLOWED_BACKEND_PORTS,
            forbidden=FORBIDDEN_BACKEND_PORTS,
            label="backend",
        )
        frontend_ports = self._validate_ports(
            plan_key,
            plan.get("allowed_frontend_ports") or [],
            allowed=ALLOWED_FRONTEND_PORTS,
            forbidden=set(),
            label="frontend",
        )
        plan["plan_key"] = plan_key
        plan["command_key"] = command_key
        plan["nox_session"] = expected_session
        plan["allowed_backend_ports"] = backend_ports
        plan["allowed_frontend_ports"] = frontend_ports
        plan["enabled"] = bool(plan.get("enabled", True))
        plan["writes_database"] = bool(plan.get("writes_database", False))
        plan["writes_artifacts"] = bool(plan.get("writes_artifacts", False))
        plan["writes_business_state"] = bool(plan.get("writes_business_state", False))
        plan["requires_backend"] = bool(plan.get("requires_backend", False))
        plan["requires_frontend"] = bool(plan.get("requires_frontend", False))
        plan["requires_node_api"] = bool(plan.get("requires_node_api", False))
        plan["requires_confirmation"] = bool(plan.get("requires_confirmation", False))
        plan["runner_enabled"] = bool(plan.get("runner_enabled", False))
        plan["mock_api_used"] = bool(plan.get("mock_api_used", False))
        plan["positive_business_success_expected"] = bool(
            plan.get("positive_business_success_expected", False)
        )
        plan["negative_failfast_only"] = bool(plan.get("negative_failfast_only", False))
        if plan["writes_business_state"] and not plan["requires_confirmation"]:
            raise ValidationCatalogError(
                f"Validation plan {plan_key} writes business state but does not require confirmation."
            )
        if plan["runner_enabled"] and plan["writes_business_state"]:
            raise ValidationCatalogError(
                f"Validation plan {plan_key} cannot enable the controlled runner while writing business state."
            )
        return plan

    @staticmethod
    def _validate_ports(
        plan_key: str,
        raw_ports: list[Any],
        *,
        allowed: set[int],
        forbidden: set[int],
        label: str,
    ) -> list[int]:
        ports: list[int] = []
        for raw_port in raw_ports:
            try:
                port = int(raw_port)
            except (TypeError, ValueError) as exc:
                raise ValidationCatalogError(
                    f"Validation plan {plan_key} has invalid {label} port {raw_port!r}."
                ) from exc
            if port in forbidden:
                raise ValidationCatalogError(
                    f"Validation plan {plan_key} uses forbidden production {label} port {port}."
                )
            if allowed and port not in allowed:
                raise ValidationCatalogError(
                    f"Validation plan {plan_key} uses non-dev {label} port {port}; allowed={sorted(allowed)}."
                )
            ports.append(port)
        return ports

    @staticmethod
    def _repo_path(path: Path) -> str:
        try:
            return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
        except ValueError:
            return str(path)
