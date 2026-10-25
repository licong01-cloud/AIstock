"""Strategy Package Center v1."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .manifest import compute_manifest_sha256, freeze_manifest
from .models import (
    StrategyPackageCanonicalPitBindingV2,
    StrategyPackageComponentInput,
    StrategyPackageComponentRecord,
    StrategyPackageManifest,
)
from .package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from .runtime_variant import (
    RuntimeVariantKind,
    RuntimeVariantValidationStatus,
    StrategyPackageRuntimeVariant,
)
from .validation_run import (
    PackageValidationRetrainMode,
    PackageValidationReproducibility,
    PackageValidationStatus,
    PackageValidationType,
    StrategyPackageValidationRun,
)
from .validation_stability import PackageValidationStabilitySummary, StabilityStatus
from .seed_contract import (
    DerivedSeedContract,
    SeedContractError,
    SeedPolicy,
    build_master_seed_contract,
)
from .validators import StrategyPackageValidator

if TYPE_CHECKING:
    from .components import StrategyPackageComponentService


def __getattr__(name: str) -> Any:
    """Keep the public component export without importing runtime services eagerly.

    ``package_asset_store`` is a low-level dependency of the QE archive publisher.
    Importing the component service while that module is loading pulls the live QE
    runtime back into ``qe_workspace_client`` and creates an order-dependent cycle.
    """

    if name == "StrategyPackageComponentService":
        from .components import StrategyPackageComponentService

        globals()[name] = StrategyPackageComponentService
        return StrategyPackageComponentService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "DerivedSeedContract",
    "SeedContractError",
    "SeedPolicy",
    "RuntimeVariantKind",
    "RuntimeVariantValidationStatus",
    "PackageValidationRetrainMode",
    "PackageValidationReproducibility",
    "PackageValidationStatus",
    "PackageValidationType",
    "PackageValidationStabilitySummary",
    "StrategyPackageAssetRecord",
    "StrategyPackageAssetType",
    "StrategyPackageComponentInput",
    "StrategyPackageComponentRecord",
    "StrategyPackageComponentService",
    "StrategyPackageManifest",
    "StrategyPackageCanonicalPitBindingV2",
    "StrategyPackageRuntimeVariant",
    "StrategyPackageValidationRun",
    "StrategyPackageValidator",
    "StabilityStatus",
    "build_master_seed_contract",
    "compute_manifest_sha256",
    "freeze_manifest",
]
