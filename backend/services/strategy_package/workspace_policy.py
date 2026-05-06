"""Path policy helpers for StrategyPackage/QE runtime artifacts.

Worker workspaces live on an isolated Linux node.  Windows-side AIstock code may
only read explicitly materialized local cache/archive files, never the worker
workspace path recorded in QE/RD-Agent metadata.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Iterable

from backend.services.trading_core.errors import StrategyPackageValidationError


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def default_aistock_artifact_roots() -> list[Path]:
    root = project_root()
    return [
        root / "rdagent_assets" / "strategy_package_runtime",
        root / "rdagent_assets" / "strategy_package_runtime_sources",
        root / "rdagent_assets" / "selection_artifacts",
        root / "rdagent_assets" / "rdagent_tasks",
        root / "rdagent_assets" / "production_bundles",
        root / "rdagent_assets" / "model_cache",
        root / "rdagent_assets" / "factor_values",
        root / "rdagent_assets" / "factor_values_realtime",
        root / "rdagent_assets" / "qe_factors",
        root / "rdagent_assets" / "qe_experiments",
        root / "rdagent_assets" / "qe_programs",
        root / "rdagent_assets" / "qe_strategies",
        root / "stock_pools",
        root / "backend" / "data" / "hmm_models",
        root / "qe_archive",
    ]


def _iter_env_roots(var_names: Iterable[str]) -> list[Path]:
    roots: list[Path] = []
    for name in var_names:
        raw = os.getenv(name)
        if not raw:
            continue
        for item in str(raw).split(os.pathsep):
            text = item.strip().strip('"')
            if text:
                roots.append(Path(text))
    return roots


def allowed_aistock_artifact_roots(extra_roots: Iterable[Path | str] | None = None) -> list[Path]:
    roots = list(default_aistock_artifact_roots())
    roots.extend(_iter_env_roots(["AISTOCK_SAFE_ARTIFACT_ROOTS", "AISTOCK_STRATEGY_PACKAGE_RUNTIME_ROOTS"]))
    if extra_roots:
        roots.extend(Path(item) for item in extra_roots)
    return roots


def _resolve_for_policy(path: Path) -> Path:
    return path.expanduser().resolve(strict=False)


def is_relative_to_path(path: Path, root: Path) -> bool:
    resolved = _resolve_for_policy(path)
    resolved_root = _resolve_for_policy(root)
    return resolved == resolved_root or resolved_root in resolved.parents


def is_under_allowed_artifact_root(
    path: Path | str,
    *,
    extra_roots: Iterable[Path | str] | None = None,
) -> bool:
    candidate = Path(path)
    return any(is_relative_to_path(candidate, root) for root in allowed_aistock_artifact_roots(extra_roots))


def is_forbidden_worker_workspace_path(path: Path | str) -> bool:
    raw = str(path or "").strip()
    if not raw:
        return False
    normalized = raw.replace("\\", "/").lower()
    if normalized.startswith("//wsl$") or normalized.startswith("//wsl.localhost"):
        return True
    if "/qe_workspace" in normalized or "/rdagent_workspace" in normalized:
        return True

    candidate = Path(raw)
    for env_name in ("QE_WORKSPACE_WIN", "RDAGENT_WORKSPACE_WIN"):
        env_value = os.getenv(env_name)
        if not env_value:
            continue
        for item in str(env_value).split(os.pathsep):
            root_text = item.strip().strip('"')
            if root_text and is_relative_to_path(candidate, Path(root_text)):
                return True
    if normalized.startswith("/mnt/") or "/mnt/" in normalized:
        return not is_under_allowed_artifact_root(raw)
    return False


def ensure_not_forbidden_worker_workspace_path(path: Path | str, *, purpose: str) -> None:
    if is_forbidden_worker_workspace_path(path):
        raise StrategyPackageValidationError(
            "direct worker workspace path access is forbidden",
            context={"path": str(path), "purpose": purpose},
        )


def ensure_aistock_artifact_path(
    path: Path | str,
    *,
    purpose: str,
    extra_roots: Iterable[Path | str] | None = None,
) -> Path:
    candidate = Path(path)
    ensure_not_forbidden_worker_workspace_path(candidate, purpose=purpose)
    if not is_under_allowed_artifact_root(candidate, extra_roots=extra_roots):
        raise StrategyPackageValidationError(
            "artifact path must be under an AIstock-owned cache or archive root",
            context={
                "path": str(candidate),
                "purpose": purpose,
                "allowed_roots": [str(root) for root in allowed_aistock_artifact_roots(extra_roots)],
            },
        )
    return candidate


def ensure_aistock_cleanup_target(
    path: Path | str,
    *,
    purpose: str,
    allowed_roots: Iterable[Path | str],
) -> Path:
    """Validate that a destructive cleanup target is under explicit local roots."""
    candidate = Path(path)
    ensure_not_forbidden_worker_workspace_path(candidate, purpose=purpose)
    resolved_candidate = _resolve_for_policy(candidate)

    resolved_roots: list[Path] = []
    for root_item in allowed_roots:
        root = Path(root_item)
        ensure_not_forbidden_worker_workspace_path(root, purpose=f"{purpose} root")
        resolved_root = _resolve_for_policy(root)
        resolved_roots.append(resolved_root)
        if resolved_candidate == resolved_root:
            raise StrategyPackageValidationError(
                "cleanup target must not be the artifact root itself",
                context={"path": str(candidate), "purpose": purpose, "root": str(root)},
            )
        if resolved_root in resolved_candidate.parents:
            return candidate

    raise StrategyPackageValidationError(
        "cleanup target must be under an explicit AIstock-owned artifact root",
        context={
            "path": str(candidate),
            "purpose": purpose,
            "allowed_roots": [str(root) for root in resolved_roots],
        },
    )


def remove_aistock_artifact_tree(
    path: Path | str,
    *,
    purpose: str,
    allowed_roots: Iterable[Path | str],
    ignore_errors: bool = False,
) -> bool:
    """Remove a local AIstock-owned directory after explicit path-policy checks."""
    target = ensure_aistock_cleanup_target(path, purpose=purpose, allowed_roots=allowed_roots)
    if not target.exists():
        return False
    if not target.is_dir():
        raise StrategyPackageValidationError(
            "cleanup target must be a directory",
            context={"path": str(target), "purpose": purpose},
        )
    shutil.rmtree(target, ignore_errors=ignore_errors)
    return True


def unlink_aistock_artifact_files(
    root: Path | str,
    pattern: str,
    *,
    purpose: str,
    allowed_roots: Iterable[Path | str],
    missing_ok: bool = True,
) -> int:
    """Delete files matching a pattern under a validated local AIstock-owned root."""
    root_path = ensure_aistock_cleanup_target(root, purpose=f"{purpose} root", allowed_roots=allowed_roots)
    if not root_path.exists():
        return 0
    if not root_path.is_dir():
        raise StrategyPackageValidationError(
            "cleanup file root must be a directory",
            context={"path": str(root_path), "purpose": purpose},
        )

    deleted = 0
    for file_path in root_path.glob(pattern):
        target = ensure_aistock_cleanup_target(file_path, purpose=purpose, allowed_roots=[root_path])
        if target.is_file():
            target.unlink(missing_ok=missing_ok)
            deleted += 1
    return deleted
