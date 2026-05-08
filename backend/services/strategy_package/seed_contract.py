"""Master seed contract helpers for StrategyPackage reproducibility."""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping

MAX_MASTER_SEED = (2**63) - 1
MAX_LIBRARY_SEED = (2**32) - 1
_DERIVATION_VERSION = "master_seed_contract_v1"


class SeedContractError(ValueError):
    """Raised when seed contract input is missing or invalid."""


class SeedPolicy(str, Enum):
    FIXED = "fixed"
    MULTI_SEED = "multi_seed"
    RANDOM_LOGGED = "random_logged"
    UNSET_LEGACY = "unset_legacy"


@dataclass(frozen=True)
class DerivedSeedContract:
    """Normalized seed evidence recorded with a manifest, trial, or run."""

    seed_policy: SeedPolicy
    master_seed: int | None
    seed_sequence: tuple[int, ...]
    derivation_version: str = _DERIVATION_VERSION
    python_seed: int | None = None
    python_hash_seed: str | None = None
    numpy_seed: int | None = None
    torch_seed: int | None = None
    torch_cuda_seed: int | None = None
    lightgbm_seed: int | None = None
    xgboost_random_state: int | None = None
    catboost_random_seed: int | None = None
    dataloader_worker_seed_base: int | None = None
    deterministic_algorithms_enabled: bool | None = None
    cudnn_deterministic: bool | None = None
    cudnn_benchmark: bool | None = None
    reproducibility_level: str = "strict_retrain"
    nondeterministic_flags: tuple[str, ...] = ()

    @property
    def is_unset_legacy(self) -> bool:
        return self.seed_policy is SeedPolicy.UNSET_LEGACY

    def to_manifest_dict(self) -> dict[str, Any]:
        return {
            "seed_policy": self.seed_policy.value,
            "master_seed": self.master_seed,
            "seed_sequence": list(self.seed_sequence),
            "derivation_version": self.derivation_version,
            "python_seed": self.python_seed,
            "python_hash_seed": self.python_hash_seed,
            "numpy_seed": self.numpy_seed,
            "torch_seed": self.torch_seed,
            "torch_cuda_seed": self.torch_cuda_seed,
            "lightgbm_seed": self.lightgbm_seed,
            "xgboost_random_state": self.xgboost_random_state,
            "catboost_random_seed": self.catboost_random_seed,
            "dataloader_worker_seed_base": self.dataloader_worker_seed_base,
            "deterministic_algorithms_enabled": self.deterministic_algorithms_enabled,
            "cudnn_deterministic": self.cudnn_deterministic,
            "cudnn_benchmark": self.cudnn_benchmark,
            "reproducibility_level": self.reproducibility_level,
            "nondeterministic_flags": list(self.nondeterministic_flags),
        }

    def to_runtime_seed_kwargs(self) -> dict[str, int]:
        if self.is_unset_legacy:
            raise SeedContractError("unset_legacy seed contract has no runtime seeds")
        return {
            "python_seed": _require_derived(self.python_seed, "python_seed"),
            "numpy_seed": _require_derived(self.numpy_seed, "numpy_seed"),
            "torch_seed": _require_derived(self.torch_seed, "torch_seed"),
            "torch_cuda_seed": _require_derived(self.torch_cuda_seed, "torch_cuda_seed"),
            "lightgbm_seed": _require_derived(self.lightgbm_seed, "lightgbm_seed"),
            "xgboost_random_state": _require_derived(self.xgboost_random_state, "xgboost_random_state"),
            "catboost_random_seed": _require_derived(self.catboost_random_seed, "catboost_random_seed"),
            "dataloader_worker_seed_base": _require_derived(
                self.dataloader_worker_seed_base,
                "dataloader_worker_seed_base",
            ),
        }


def build_master_seed_contract(
    *,
    master_seed: Any,
    seed_policy: str | SeedPolicy = SeedPolicy.FIXED,
    seed_sequence: Iterable[Any] | None = None,
    deterministic_algorithms_enabled: bool | None = None,
    cudnn_deterministic: bool | None = None,
    cudnn_benchmark: bool | None = None,
    nondeterministic_flags: Iterable[str] | None = None,
) -> DerivedSeedContract:
    """Build a deterministic child-seed contract from one explicit master seed.

    Missing or malformed seeds raise ``SeedContractError`` unless the caller
    explicitly declares ``seed_policy=unset_legacy`` for historical metadata.
    """

    policy = _normalize_policy(seed_policy)
    flags = _normalize_flags(nondeterministic_flags)

    if policy is SeedPolicy.UNSET_LEGACY:
        if master_seed is not None:
            raise SeedContractError("unset_legacy seed policy must not provide master_seed")
        if seed_sequence not in (None, [], ()):  # no hidden seeds on legacy evidence
            raise SeedContractError("unset_legacy seed policy must not provide seed_sequence")
        return DerivedSeedContract(
            seed_policy=policy,
            master_seed=None,
            seed_sequence=(),
            reproducibility_level="audit_only",
            nondeterministic_flags=flags,
        )

    normalized_master = _normalize_seed(master_seed, field_name="master_seed", max_value=MAX_MASTER_SEED)
    normalized_sequence = _normalize_seed_sequence(seed_sequence, normalized_master, policy)

    return DerivedSeedContract(
        seed_policy=policy,
        master_seed=normalized_master,
        seed_sequence=normalized_sequence,
        python_seed=_derive_library_seed(normalized_master, "python_random"),
        python_hash_seed=str(_derive_library_seed(normalized_master, "python_hash_seed")),
        numpy_seed=_derive_library_seed(normalized_master, "numpy"),
        torch_seed=_derive_library_seed(normalized_master, "torch_cpu"),
        torch_cuda_seed=_derive_library_seed(normalized_master, "torch_cuda"),
        lightgbm_seed=_derive_library_seed(normalized_master, "lightgbm"),
        xgboost_random_state=_derive_library_seed(normalized_master, "xgboost"),
        catboost_random_seed=_derive_library_seed(normalized_master, "catboost"),
        dataloader_worker_seed_base=_derive_library_seed(normalized_master, "dataloader_worker_base"),
        deterministic_algorithms_enabled=deterministic_algorithms_enabled,
        cudnn_deterministic=cudnn_deterministic,
        cudnn_benchmark=cudnn_benchmark,
        nondeterministic_flags=flags,
    )


def build_master_seed_contract_from_manifest(payload: Mapping[str, Any]) -> DerivedSeedContract:
    """Parse seed fields from a manifest-like mapping and fail fast on omissions."""

    if "seed_policy" not in payload:
        raise SeedContractError("manifest seed_policy is required")
    return build_master_seed_contract(
        master_seed=payload.get("master_seed"),
        seed_policy=payload["seed_policy"],
        seed_sequence=payload.get("seed_sequence"),
        deterministic_algorithms_enabled=payload.get("deterministic_algorithms_enabled"),
        cudnn_deterministic=payload.get("cudnn_deterministic"),
        cudnn_benchmark=payload.get("cudnn_benchmark"),
        nondeterministic_flags=payload.get("nondeterministic_flags"),
    )


def apply_python_random_seed(contract: DerivedSeedContract) -> None:
    """Apply only stdlib random seeding; NumPy/Torch callers wire their own imports."""

    random.seed(_require_derived(contract.python_seed, "python_seed"))


def derive_dataloader_worker_seed(contract: DerivedSeedContract, worker_id: int) -> int:
    if isinstance(worker_id, bool) or not isinstance(worker_id, int) or worker_id < 0:
        raise SeedContractError("worker_id must be a non-negative integer")
    base = _require_derived(contract.dataloader_worker_seed_base, "dataloader_worker_seed_base")
    return (base + worker_id) % (MAX_LIBRARY_SEED + 1)


def _normalize_policy(seed_policy: str | SeedPolicy) -> SeedPolicy:
    if isinstance(seed_policy, SeedPolicy):
        return seed_policy
    try:
        return SeedPolicy(str(seed_policy))
    except ValueError as exc:
        allowed = ", ".join(policy.value for policy in SeedPolicy)
        raise SeedContractError(f"seed_policy must be one of: {allowed}") from exc


def _normalize_seed(value: Any, *, field_name: str, max_value: int) -> int:
    if value is None:
        raise SeedContractError(f"{field_name} is required for non-legacy seed policies")
    if isinstance(value, bool) or not isinstance(value, int):
        raise SeedContractError(f"{field_name} must be an integer")
    if value < 0 or value > max_value:
        raise SeedContractError(f"{field_name} must be between 0 and {max_value}")
    return value


def _normalize_seed_sequence(
    seed_sequence: Iterable[Any] | None,
    master_seed: int,
    policy: SeedPolicy,
) -> tuple[int, ...]:
    if seed_sequence is None:
        return (master_seed,)
    if isinstance(seed_sequence, (str, bytes)):
        raise SeedContractError("seed_sequence must be a sequence of integers")
    normalized = tuple(
        _normalize_seed(seed, field_name="seed_sequence", max_value=MAX_MASTER_SEED)
        for seed in seed_sequence
    )
    if not normalized:
        raise SeedContractError("seed_sequence must not be empty for non-legacy seed policies")
    if policy is SeedPolicy.FIXED and normalized != (master_seed,):
        raise SeedContractError("fixed seed policy requires seed_sequence to equal [master_seed]")
    return normalized


def _normalize_flags(flags: Iterable[str] | None) -> tuple[str, ...]:
    if flags is None:
        return ()
    if isinstance(flags, (str, bytes)):
        raise SeedContractError("nondeterministic_flags must be a sequence of strings")
    normalized: list[str] = []
    for flag in flags:
        if not isinstance(flag, str) or not flag.strip():
            raise SeedContractError("nondeterministic_flags entries must be non-empty strings")
        normalized.append(flag.strip())
    return tuple(normalized)


def _derive_library_seed(master_seed: int, namespace: str) -> int:
    payload = f"{_DERIVATION_VERSION}:{namespace}:{master_seed}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False) % (MAX_LIBRARY_SEED + 1)


def _require_derived(value: int | None, field_name: str) -> int:
    if value is None:
        raise SeedContractError(f"{field_name} is unavailable for this seed contract")
    return value
