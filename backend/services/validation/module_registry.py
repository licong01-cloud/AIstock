from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MODULE_REGISTRY_PATH = REPO_ROOT / "tests" / "aistock_validation" / "catalog" / "module_registry.yaml"
MODULE_REGISTRY_SCHEMA = "aistock_module_registry_v1"
MODULE_ID_RE = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*$")
RISK_LEVELS = {"low", "medium", "high", "critical"}
MODULE_TYPES = {
    "cross_cutting",
    "data_pipeline",
    "diagnostics",
    "docs",
    "product_feature",
    "technical_layer",
    "tests",
    "trading_infra",
}


class ModuleRegistryError(ValueError):
    """Raised when the module registry catalog violates its schema."""


@dataclass(frozen=True)
class ModuleDefinition:
    module_id: str
    display_name: str
    module_type: str
    risk_level: str
    description: str = ""
    description_zh: str = ""
    parent_module: str | None = None
    ui_routes: tuple[str, ...] = ()
    api_routes: tuple[str, ...] = ()
    test_plans_required: tuple[str, ...] = ()
    test_plans_recommended: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "module_id": self.module_id,
            "display_name": self.display_name,
            "parent_module": self.parent_module,
            "module_type": self.module_type,
            "risk_level": self.risk_level,
            "description": self.description,
            "description_zh": self.description_zh,
            "ui_routes": list(self.ui_routes),
            "api_routes": list(self.api_routes),
            "test_plans": {
                "required_on_change": list(self.test_plans_required),
                "recommended": list(self.test_plans_recommended),
            },
        }


def _as_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list | tuple):
        return tuple(str(item).strip() for item in value if str(item).strip())
    raise ModuleRegistryError(f"Expected a string or list, got {type(value).__name__}.")


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


class ModuleRegistry:
    """Read and validate the machine-readable AIstock module registry."""

    def __init__(self, registry_path: Path | None = None) -> None:
        self.registry_path = Path(registry_path or DEFAULT_MODULE_REGISTRY_PATH)

    def load(self) -> dict[str, Any]:
        if not self.registry_path.exists():
            return {
                "schema_version": MODULE_REGISTRY_SCHEMA,
                "registry_path": _repo_path(self.registry_path),
                "missing": True,
                "modules": [],
            }
        try:
            payload = yaml.safe_load(self.registry_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            raise ModuleRegistryError(f"Invalid module registry YAML: {exc}") from exc
        if not isinstance(payload, dict):
            raise ModuleRegistryError("Module registry root must be a mapping.")
        schema_version = str(payload.get("schema_version") or "")
        if schema_version != MODULE_REGISTRY_SCHEMA:
            raise ModuleRegistryError(f"Unsupported module registry schema: {schema_version!r}.")
        raw_modules = payload.get("modules")
        if not isinstance(raw_modules, list):
            raise ModuleRegistryError("Module registry field 'modules' must be a list.")
        modules = [self._validate_module(item) for item in raw_modules]
        self._validate_unique_modules(modules)
        self._validate_parent_modules(modules)
        return {
            "schema_version": schema_version,
            "registry_path": _repo_path(self.registry_path),
            "missing": False,
            "modules": [module.to_dict() for module in modules],
        }

    def list_modules(self) -> list[ModuleDefinition]:
        loaded = self.load()
        modules: list[ModuleDefinition] = []
        for item in loaded["modules"]:
            test_plans = item.get("test_plans") or {}
            modules.append(
                ModuleDefinition(
                    module_id=item["module_id"],
                    display_name=item["display_name"],
                    parent_module=item.get("parent_module"),
                    module_type=item["module_type"],
                    risk_level=item["risk_level"],
                    description=item.get("description") or "",
                    description_zh=item.get("description_zh") or "",
                    ui_routes=tuple(item.get("ui_routes") or []),
                    api_routes=tuple(item.get("api_routes") or []),
                    test_plans_required=tuple(test_plans.get("required_on_change") or []),
                    test_plans_recommended=tuple(test_plans.get("recommended") or []),
                )
            )
        return modules

    def module_ids(self) -> set[str]:
        return {module.module_id for module in self.list_modules()}

    def get_module(self, module_id: str) -> ModuleDefinition | None:
        for module in self.list_modules():
            if module.module_id == module_id:
                return module
        return None

    @staticmethod
    def _validate_module(raw_module: Any) -> ModuleDefinition:
        if not isinstance(raw_module, dict):
            raise ModuleRegistryError("Each module registry item must be a mapping.")
        module_id = str(raw_module.get("module_id") or "").strip()
        if not MODULE_ID_RE.fullmatch(module_id):
            raise ModuleRegistryError(f"Invalid module_id: {module_id!r}.")
        display_name = str(raw_module.get("display_name") or "").strip()
        if not display_name:
            raise ModuleRegistryError(f"Module {module_id} is missing display_name.")
        module_type = str(raw_module.get("module_type") or "").strip()
        if module_type not in MODULE_TYPES:
            raise ModuleRegistryError(f"Module {module_id} has invalid module_type={module_type!r}.")
        risk_level = str(raw_module.get("risk_level") or "").strip()
        if risk_level not in RISK_LEVELS:
            raise ModuleRegistryError(f"Module {module_id} has invalid risk_level={risk_level!r}.")
        parent_module = str(raw_module.get("parent_module") or "").strip() or None
        if parent_module and not MODULE_ID_RE.fullmatch(parent_module):
            raise ModuleRegistryError(f"Module {module_id} has invalid parent_module={parent_module!r}.")
        test_plans = raw_module.get("test_plans") or {}
        if not isinstance(test_plans, dict):
            raise ModuleRegistryError(f"Module {module_id} field test_plans must be a mapping.")
        return ModuleDefinition(
            module_id=module_id,
            display_name=display_name,
            parent_module=parent_module,
            module_type=module_type,
            risk_level=risk_level,
            description=str(raw_module.get("description") or "").strip(),
            description_zh=str(raw_module.get("description_zh") or "").strip(),
            ui_routes=_as_tuple(raw_module.get("ui_routes")),
            api_routes=_as_tuple(raw_module.get("api_routes")),
            test_plans_required=_as_tuple(test_plans.get("required_on_change")),
            test_plans_recommended=_as_tuple(test_plans.get("recommended")),
        )

    @staticmethod
    def _validate_unique_modules(modules: list[ModuleDefinition]) -> None:
        seen: set[str] = set()
        for module in modules:
            if module.module_id in seen:
                raise ModuleRegistryError(f"Duplicate module_id: {module.module_id}.")
            seen.add(module.module_id)

    @staticmethod
    def _validate_parent_modules(modules: list[ModuleDefinition]) -> None:
        module_ids = {module.module_id for module in modules}
        for module in modules:
            if module.parent_module and module.parent_module not in module_ids:
                raise ModuleRegistryError(
                    f"Module {module.module_id} references unknown parent_module={module.parent_module!r}."
                )
