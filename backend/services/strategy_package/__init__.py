"""Strategy Package Center v1."""

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


def __getattr__(name: str) -> object:
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
