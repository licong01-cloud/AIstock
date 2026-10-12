"""Read successor account state without creating a legacy Paper portfolio."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Any

from backend.services.selection_center.runtime_profile import normalize_selection_runtime_config
from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.ledger import FeeModel

from .localsim_runtime_profile import LocalSimRuntimeProfileConfigV1
from .models import canonical_json_sha256
from .successor_models import SimulationAccountStatus, SimulationLedgerScopeKind


def selection_config_from_localsim_profile(
    config: LocalSimRuntimeProfileConfigV1, *, manifest_sha256: str | None = None,
) -> dict[str, Any]:
    """Project only selection fields from the already frozen profile."""
    daily = config.daily_strategy
    hmm = {"enabled": config.hmm.enabled}
    if config.hmm.enabled:
        hmm.update(
            model_snapshot_id=config.hmm.snapshot_id,
            signal_preset=config.hmm.preset,
            auto_compute=False,
            manual_snapshot_required=True,
        )
    projected = normalize_selection_runtime_config(
        {
            "runtime_profile": {
                "selection": {
                    "top_k": daily.top_k,
                    "daily_strategy_id": daily.strategy_id,
                    "daily_strategy_params": daily.parameters,
                },
                "industry_blacklist": list(dict.fromkeys((*daily.industry_filters, *daily.sector_filters))),
                "hmm": hmm,
                "risk_policy": config.risk_policy,
            },
            "selection_artifact_config": {"auto_generate": True, "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE"},
        }
    )
    if config.runtime_variant_id is not None:
        if not manifest_sha256:
            raise RuntimeConfigInvalidError("frozen LocalSIM variant requires the release manifest identity")
        projected["runtime_variant"] = {
            "variant_id": config.runtime_variant_id,
            "variant_hash": config.runtime_variant_hash,
            "manifest_sha256": manifest_sha256,
            "validation_status": "VALIDATION_PASSED",
            "variant_config": config.runtime_variant_materialized_config,
        }
    return projected


def fee_model_from_localsim_profile(config: LocalSimRuntimeProfileConfigV1) -> FeeModel:
    policy = config.fee_policy
    if set(policy) - {"open_cost", "close_cost", "min_cost"}:
        raise RuntimeConfigInvalidError(
            "LocalSIM fee policy contains unsupported fields",
            context={"reason_code": "LOCALSIM_FEE_POLICY_INVALID"},
        )
    defaults = FeeModel()
    try:
        values = {key: float(policy.get(key, getattr(defaults, key))) for key in ("open_cost", "close_cost", "min_cost")}
    except (TypeError, ValueError) as exc:
        raise RuntimeConfigInvalidError(
            "LocalSIM fee policy values must be numeric",
            context={"reason_code": "LOCALSIM_FEE_POLICY_INVALID"},
        ) from exc
    if any(not isfinite(value) or value < 0 for value in values.values()):
        raise RuntimeConfigInvalidError(
            "LocalSIM fee policy values must be finite and non-negative",
            context={"reason_code": "LOCALSIM_FEE_POLICY_INVALID"},
        )
    return FeeModel(**values)


def frozen_localsim_profile(release: Any) -> LocalSimRuntimeProfileConfigV1 | None:
    metadata = release.release_config_json.get("metadata") or {}
    raw = metadata.get("localsim_runtime_profile_config")
    if raw is None:
        return None
    config = LocalSimRuntimeProfileConfigV1.model_validate(raw)
    if canonical_json_sha256(config.model_dump(mode="json")) != release.runtime_profile_sha256:
        raise RuntimeConfigInvalidError(
            "LocalSIM frozen runtime profile hash does not match release",
            context={"reason_code": "LOCALSIM_FROZEN_PROFILE_HASH_MISMATCH", "release_id": release.release_id},
        )
    daily_hash = canonical_json_sha256(config.daily_strategy.model_dump(mode="json"))
    if release.daily_strategy_profile_version_id != f"lsdaily_{daily_hash[:16]}":
        raise RuntimeConfigInvalidError(
            "LocalSIM daily strategy identity does not match frozen profile",
            context={"reason_code": "LOCALSIM_FROZEN_DAILY_IDENTITY_MISMATCH"},
        )
    selection = selection_config_from_localsim_profile(config, manifest_sha256=release.manifest_sha256)
    if metadata.get("selection_runtime_config") != selection:
        raise RuntimeConfigInvalidError(
            "LocalSIM selection projection does not match frozen profile",
            context={"reason_code": "LOCALSIM_FROZEN_SELECTION_CONFIG_MISMATCH"},
        )
    return config


@dataclass(frozen=True)
class LocalSimNativeAccountContext:
    """Ephemeral runtime view; account and ledger remain the durable authorities."""

    ledger_scope_id: str
    initial_capital: float
    frozen_manifest: StrategyPackageManifest
    profile: LocalSimRuntimeProfileConfigV1
    data_source: MinuteDataSource = MinuteDataSource.DB_HISTORICAL

    @property
    def portfolio_id(self) -> str:
        # Compatibility with the existing economic repository column spelling.
        return self.ledger_scope_id

    @property
    def initial_cash(self) -> float:
        return self.initial_capital


def load_native_account_context(*, binding: Any, release: Any, repository: Any, manifest_loader: Any) -> LocalSimNativeAccountContext:
    metadata = binding.binding_config_json.get("metadata") or {}
    account_id = str(metadata.get("localsim_account_id") or "")
    account = repository.get_account(account_id)
    scope = repository.resolve_ledger_scope_for_account(account_id)
    if (
        binding.strategy_id != account.account_id
        or binding.broker_account_id != account.account_id
        or binding.package_id != account.package_id
        or release.package_id != account.package_id
        or release.manifest_sha256 != account.manifest_sha256
        or binding.manifest_sha256 != release.manifest_sha256
        or binding.release_id != release.release_id
        or binding.release_hash != release.release_hash
        or (scope.scope_kind is SimulationLedgerScopeKind.SUCCESSOR_NATIVE and scope.native_account_id != account.account_id)
    ):
        raise RuntimeConfigInvalidError(
            "LocalSIM native binding does not match account/ledger authority",
            context={"reason_code": "LOCALSIM_NATIVE_ACCOUNT_IDENTITY_MISMATCH", "account_id": account_id},
        )
    if account.status is not SimulationAccountStatus.ACTIVE:
        raise RuntimeConfigInvalidError(
            "LocalSIM account is not active",
            context={"reason_code": "LOCALSIM_NATIVE_ACCOUNT_NOT_ACTIVE", "account_id": account_id},
        )
    profile = frozen_localsim_profile(release)
    if profile is None:
        raise RuntimeConfigInvalidError(
            "LocalSIM native account requires a frozen runtime profile",
            context={"reason_code": "LOCALSIM_NATIVE_PROFILE_MISSING"},
        )
    manifest = manifest_loader(account.package_id)
    if not isinstance(manifest, StrategyPackageManifest):
        manifest = StrategyPackageManifest.model_validate(manifest)
    if (manifest.package_id, manifest.manifest_sha256) != (release.package_id, release.manifest_sha256):
        raise RuntimeConfigInvalidError(
            "LocalSIM native manifest does not match release",
            context={"reason_code": "LOCALSIM_NATIVE_MANIFEST_MISMATCH"},
        )
    fee_model_from_localsim_profile(profile)
    return LocalSimNativeAccountContext(
        ledger_scope_id=scope.ledger_scope_id,
        initial_capital=float(account.initial_capital),
        frozen_manifest=manifest,
        profile=profile,
    )


def is_native_account_binding(binding: Any) -> bool:
    metadata = binding.binding_config_json.get("metadata") or {}
    return bool(metadata.get("localsim_account_id"))
