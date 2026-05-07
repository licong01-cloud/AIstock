from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from backend.services.validation.history_store import ValidationHistoryStore
from backend.services.validation.module_quality import ModuleQualityService
from backend.services.validation.module_registry import MODULE_ID_RE, REPO_ROOT, RISK_LEVELS, ModuleRegistry
from backend.services.validation.plan_catalog import ValidationCatalogError, ValidationPlanCatalog


DEFAULT_UI_TARGET_CATALOG_PATH = REPO_ROOT / "tests" / "aistock_validation" / "catalog" / "ui_targets.yaml"
UI_TARGET_SCHEMA = "aistock_validation_ui_targets_v1"
ROUTE_ID_RE = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*$")
COVERAGE_STATUSES = {"covered", "partial", "planned", "excluded"}


class ValidationUiTargetCatalogError(ValueError):
    """Raised when the UI target catalog violates its schema."""


@dataclass(frozen=True)
class UiTargetDefinition:
    route_id: str
    href: str
    label: str
    nav_group: str
    primary_module: str
    impact_modules: tuple[str, ...]
    risk_level: str
    required_test_plans: tuple[str, ...]
    recommended_test_plans: tuple[str, ...]
    business_operations: tuple[str, ...]
    coverage_status: str
    exclusion_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "route_id": self.route_id,
            "href": self.href,
            "label": self.label,
            "nav_group": self.nav_group,
            "primary_module": self.primary_module,
            "impact_modules": list(self.impact_modules),
            "risk_level": self.risk_level,
            "required_test_plans": list(self.required_test_plans),
            "recommended_test_plans": list(self.recommended_test_plans),
            "business_operations": list(self.business_operations),
            "coverage_status": self.coverage_status,
        }
        if self.exclusion_reason:
            payload["exclusion_reason"] = self.exclusion_reason
        return payload


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def _as_tuple(value: Any, *, field_name: str, required: bool = False) -> tuple[str, ...]:
    if value is None:
        if required:
            raise ValidationUiTargetCatalogError(f"UI target field {field_name!r} is required.")
        return ()
    if isinstance(value, str):
        items = (value.strip(),)
    elif isinstance(value, list | tuple):
        items = tuple(str(item).strip() for item in value if str(item).strip())
    else:
        raise ValidationUiTargetCatalogError(
            f"UI target field {field_name!r} must be a string or list, got {type(value).__name__}."
        )
    if required and not items:
        raise ValidationUiTargetCatalogError(f"UI target field {field_name!r} cannot be empty.")
    return items


def _page(items: list[dict[str, Any]], *, page: int, page_size: int) -> dict[str, Any]:
    start = (page - 1) * page_size
    page_items = items[start : start + page_size]
    return {
        "items": page_items,
        "total": len(items),
        "page": page,
        "page_size": page_size,
        "has_more": start + page_size < len(items),
    }


class ValidationUiTargetCatalog:
    """Read and validate route-level UI coverage targets without executing commands."""

    def __init__(
        self,
        catalog_path: Path | None = None,
        *,
        module_registry: ModuleRegistry | None = None,
        plan_catalog: ValidationPlanCatalog | None = None,
        module_quality_service: ModuleQualityService | None = None,
        history_store: ValidationHistoryStore | None = None,
    ) -> None:
        self.catalog_path = Path(catalog_path or DEFAULT_UI_TARGET_CATALOG_PATH)
        self.module_registry = module_registry or ModuleRegistry()
        self.plan_catalog = plan_catalog or ValidationPlanCatalog()
        self.module_quality_service = module_quality_service
        self.history_store = history_store or ValidationHistoryStore()
        self._module_quality_cache: dict[str, dict[str, Any]] | None = None
        self._module_quality_error: str | None = None
        self._latest_runs_cache: list[dict[str, Any]] | None = None

    def load(self) -> dict[str, Any]:
        if not self.catalog_path.exists():
            return {
                "schema_version": UI_TARGET_SCHEMA,
                "catalog_path": _repo_path(self.catalog_path),
                "missing": True,
                "targets": [],
                "warnings": ["ui_target_catalog_missing"],
            }
        try:
            payload = yaml.safe_load(self.catalog_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            raise ValidationUiTargetCatalogError(f"Invalid UI target YAML: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValidationUiTargetCatalogError("UI target catalog root must be a mapping.")
        schema_version = str(payload.get("schema_version") or "")
        if schema_version != UI_TARGET_SCHEMA:
            raise ValidationUiTargetCatalogError(f"Unsupported UI target schema: {schema_version!r}.")
        raw_targets = payload.get("targets")
        if not isinstance(raw_targets, list):
            raise ValidationUiTargetCatalogError("UI target catalog field 'targets' must be a list.")
        module_ids = self.module_registry.module_ids()
        try:
            plan_keys = {str(plan["plan_key"]) for plan in self.plan_catalog.list_plans()}
        except ValidationCatalogError as exc:
            raise ValidationUiTargetCatalogError(f"Invalid validation plan catalog: {exc}") from exc
        targets = [self._validate_target(item, module_ids=module_ids, plan_keys=plan_keys) for item in raw_targets]
        self._validate_unique_targets(targets)
        return {
            "schema_version": schema_version,
            "catalog_path": _repo_path(self.catalog_path),
            "missing": False,
            "targets": [target.to_dict() for target in targets],
            "warnings": [],
        }

    def list_targets(
        self,
        *,
        nav_group: str | None = None,
        module: str | None = None,
        coverage_status: str | None = None,
        risk_level: str | None = None,
        search: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        loaded = self.load()
        targets = self._filter_targets(
            loaded["targets"],
            nav_group=nav_group,
            module=module,
            coverage_status=coverage_status,
            risk_level=risk_level,
            search=search,
        )
        page_payload = _page(targets, page=page, page_size=page_size)
        page_payload["items"] = [self._enrich_target(item) for item in page_payload["items"]]
        return {
            "schema_version": loaded["schema_version"],
            "catalog_path": loaded["catalog_path"],
            "missing": loaded["missing"],
            **page_payload,
        }

    def get_target(self, route_id: str) -> dict[str, Any] | None:
        loaded = self.load()
        for item in loaded["targets"]:
            if item["route_id"] == route_id:
                return {
                    "schema_version": loaded["schema_version"],
                    "catalog_path": loaded["catalog_path"],
                    "missing": loaded["missing"],
                    "target": self._enrich_target(item),
                }
        return None

    def summary(self) -> dict[str, Any]:
        loaded = self.load()
        targets = loaded["targets"]
        by_nav_group: dict[str, dict[str, Any]] = {}
        by_coverage_status: dict[str, int] = {}
        by_risk_level: dict[str, int] = {}
        warning_count = 0
        for item in targets:
            nav_bucket = by_nav_group.setdefault(
                item["nav_group"],
                {"nav_group": item["nav_group"], "target_count": 0, "warning_count": 0},
            )
            nav_bucket["target_count"] += 1
            warnings = self._target_warnings(item)
            nav_bucket["warning_count"] += len(warnings)
            warning_count += len(warnings)
            by_coverage_status[item["coverage_status"]] = by_coverage_status.get(item["coverage_status"], 0) + 1
            by_risk_level[item["risk_level"]] = by_risk_level.get(item["risk_level"], 0) + 1
        return {
            "schema_version": UI_TARGET_SCHEMA,
            "generated_at": _now_iso(),
            "catalog_path": loaded["catalog_path"],
            "missing": loaded["missing"],
            "target_count": len(targets),
            "nav_group_count": len(by_nav_group),
            "warning_count": warning_count,
            "targets_requiring_action": sum(1 for item in targets if self._target_warnings(item)),
            "by_nav_group": sorted(by_nav_group.values(), key=lambda item: str(item["nav_group"])),
            "by_coverage_status": by_coverage_status,
            "by_risk_level": by_risk_level,
            "production_8001_touched": False,
        }

    def _validate_target(
        self,
        raw_target: Any,
        *,
        module_ids: set[str],
        plan_keys: set[str],
    ) -> UiTargetDefinition:
        if not isinstance(raw_target, dict):
            raise ValidationUiTargetCatalogError("Each UI target item must be a mapping.")
        route_id = str(raw_target.get("route_id") or "").strip()
        if not ROUTE_ID_RE.fullmatch(route_id):
            raise ValidationUiTargetCatalogError(f"Invalid route_id: {route_id!r}.")
        href = str(raw_target.get("href") or "").strip()
        if not href.startswith("/") or " " in href:
            raise ValidationUiTargetCatalogError(f"UI target {route_id} has invalid href={href!r}.")
        label = str(raw_target.get("label") or "").strip()
        nav_group = str(raw_target.get("nav_group") or "").strip()
        if not label:
            raise ValidationUiTargetCatalogError(f"UI target {route_id} is missing label.")
        if not nav_group:
            raise ValidationUiTargetCatalogError(f"UI target {route_id} is missing nav_group.")
        primary_module = str(raw_target.get("primary_module") or "").strip()
        if not MODULE_ID_RE.fullmatch(primary_module) or primary_module not in module_ids:
            raise ValidationUiTargetCatalogError(
                f"UI target {route_id} references unknown primary_module={primary_module!r}."
            )
        impact_modules = _as_tuple(raw_target.get("impact_modules"), field_name="impact_modules")
        for module_id in impact_modules:
            if module_id not in module_ids:
                raise ValidationUiTargetCatalogError(
                    f"UI target {route_id} references unknown impact_module={module_id!r}."
                )
        risk_level = str(raw_target.get("risk_level") or "").strip()
        if risk_level not in RISK_LEVELS:
            raise ValidationUiTargetCatalogError(f"UI target {route_id} has invalid risk_level={risk_level!r}.")
        required_test_plans = _as_tuple(raw_target.get("required_test_plans"), field_name="required_test_plans")
        recommended_test_plans = _as_tuple(raw_target.get("recommended_test_plans"), field_name="recommended_test_plans")
        for plan_key in (*required_test_plans, *recommended_test_plans):
            if plan_key not in plan_keys:
                raise ValidationUiTargetCatalogError(
                    f"UI target {route_id} references unknown test plan={plan_key!r}."
                )
        business_operations = _as_tuple(
            raw_target.get("business_operations"),
            field_name="business_operations",
            required=True,
        )
        coverage_status = str(raw_target.get("coverage_status") or "").strip()
        if coverage_status not in COVERAGE_STATUSES:
            raise ValidationUiTargetCatalogError(
                f"UI target {route_id} has invalid coverage_status={coverage_status!r}."
            )
        exclusion_reason = str(raw_target.get("exclusion_reason") or "").strip()
        if coverage_status == "excluded" and not exclusion_reason:
            raise ValidationUiTargetCatalogError(f"UI target {route_id} is excluded but missing exclusion_reason.")
        if coverage_status != "excluded" and exclusion_reason:
            raise ValidationUiTargetCatalogError(
                f"UI target {route_id} has exclusion_reason but coverage_status is {coverage_status!r}."
            )
        return UiTargetDefinition(
            route_id=route_id,
            href=href,
            label=label,
            nav_group=nav_group,
            primary_module=primary_module,
            impact_modules=impact_modules,
            risk_level=risk_level,
            required_test_plans=required_test_plans,
            recommended_test_plans=recommended_test_plans,
            business_operations=business_operations,
            coverage_status=coverage_status,
            exclusion_reason=exclusion_reason,
        )

    @staticmethod
    def _validate_unique_targets(targets: list[UiTargetDefinition]) -> None:
        seen_route_ids: set[str] = set()
        seen_hrefs: set[str] = set()
        for target in targets:
            if target.route_id in seen_route_ids:
                raise ValidationUiTargetCatalogError(f"Duplicate route_id: {target.route_id}.")
            if target.href in seen_hrefs:
                raise ValidationUiTargetCatalogError(f"Duplicate href: {target.href}.")
            seen_route_ids.add(target.route_id)
            seen_hrefs.add(target.href)

    def _enrich_target(self, target: dict[str, Any]) -> dict[str, Any]:
        warnings = self._target_warnings(target)
        module_quality = self._module_quality(target.get("primary_module"))
        if self._module_quality_error:
            warnings.append("module_quality_unavailable")
        latest_run = self._latest_run_for_module(str(target.get("primary_module") or ""))
        if not latest_run:
            warnings.append("no_latest_validation_run")
        if module_quality and str(module_quality.get("coverage", {}).get("status") or "missing") in {"missing", "failed"}:
            warnings.append("module_coverage_missing_or_failed")
        priority_level = str((module_quality or {}).get("priority", {}).get("level") or "low")
        if priority_level in {"high", "critical"}:
            warnings.append("module_priority_requires_validation")
        return {
            **target,
            "module_quality": module_quality,
            "latest_run": latest_run,
            "warnings": sorted(set(warnings)),
            "proven_by_real_business_evidence": target.get("coverage_status") == "covered" and latest_run is not None,
        }

    @staticmethod
    def _target_warnings(target: dict[str, Any]) -> list[str]:
        if target.get("coverage_status") == "excluded":
            return ["route_excluded_from_validation"]
        if target.get("coverage_status") != "covered":
            return ["route_coverage_not_fully_proven"]
        return []

    def _module_quality(self, module_id: str | None) -> dict[str, Any] | None:
        if not module_id:
            return None
        if self._module_quality_cache is not None:
            return self._module_quality_cache.get(module_id)
        try:
            service = self.module_quality_service or ModuleQualityService()
            self._module_quality_cache = {
                str(item.get("module_id")): item
                for item in service.module_quality_summary(commit_limit=50).get("modules") or []
                if item.get("module_id")
            }
            self._module_quality_error = None
        except Exception as exc:
            self._module_quality_cache = {}
            self._module_quality_error = str(exc) or exc.__class__.__name__
            return None
        return self._module_quality_cache.get(module_id)

    def _latest_run_for_module(self, module_id: str) -> dict[str, Any] | None:
        aliases = {module_id, module_id.replace(".", "_")}
        if self._latest_runs_cache is None:
            self._latest_runs_cache = self.history_store.list_runs(page_size=10000)["items"]
        for run in self._latest_runs_cache:
            run_module = str(run.get("module") or run.get("module_slug") or "")
            if run_module in aliases:
                return run
        return None

    @staticmethod
    def _filter_targets(
        targets: list[dict[str, Any]],
        *,
        nav_group: str | None,
        module: str | None,
        coverage_status: str | None,
        risk_level: str | None,
        search: str | None,
    ) -> list[dict[str, Any]]:
        filtered = targets
        if nav_group:
            nav_group_l = nav_group.lower()
            filtered = [item for item in filtered if nav_group_l in str(item.get("nav_group") or "").lower()]
        if module:
            module_l = module.lower()
            filtered = [
                item
                for item in filtered
                if module_l in str(item.get("primary_module") or "").lower()
                or any(module_l in str(module_id).lower() for module_id in item.get("impact_modules") or [])
            ]
        if coverage_status:
            status_l = coverage_status.lower()
            filtered = [item for item in filtered if str(item.get("coverage_status") or "").lower() == status_l]
        if risk_level:
            risk_l = risk_level.lower()
            filtered = [item for item in filtered if str(item.get("risk_level") or "").lower() == risk_l]
        if search:
            needle = search.lower()
            filtered = [
                item
                for item in filtered
                if needle in str(item.get("route_id") or "").lower()
                or needle in str(item.get("href") or "").lower()
                or needle in str(item.get("label") or "").lower()
                or needle in str(item.get("nav_group") or "").lower()
                or needle in str(item.get("primary_module") or "").lower()
            ]
        return filtered
