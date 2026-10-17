"""Read-only QE consumer probes for one frozen monthly release.

The probes exercise the same active-profile resolver used by task creation.
P10/P11 additionally validate the shared, manifest-bound HMM coefficient file;
P11 must materialise a genuinely filtered PIT instrument sidecar.  No Loop,
training, experiment, database, or runtime action is started here.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .canonical import canonical_json_bytes
from .monthly_consumer_validation import (
    CONSUMER_PROBE_RESULT_SCHEMA,
    ConsumerProbeRequest,
    MonthlyConsumerValidationError,
)
from .monthly_hmm_derive import SHARED_HMM_COEFFICIENT_SCHEMA


QE_CONSUMERS = ("qe_single", "qe_custom", "qe_multi_alpha", "qe_p10", "qe_p11")
_HMM_CONSUMERS = frozenset({"qe_p10", "qe_p11"})
_PROBE_ID = "aistock.monthly.qe.file_only"
_PROBE_VERSION = "1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _content_ref(path: Path, *, root: Path | None = None) -> dict[str, Any]:
    resolved = path.resolve(strict=True)
    if root is not None:
        try:
            ref_id = resolved.relative_to(root.resolve(strict=True)).as_posix()
        except ValueError as exc:
            raise MonthlyConsumerValidationError("QE probe evidence escapes its frozen root") from exc
    else:
        ref_id = path.name
    return {"id": ref_id, "sha256": _sha256(resolved), "size": resolved.stat().st_size}


def _write_canonical_or_identical(path: Path, value: Mapping[str, Any]) -> Path:
    payload = canonical_json_bytes(value) + b"\n"
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
    except FileExistsError as exc:
        if not path.is_file() or path.is_symlink() or path.read_bytes() != payload:
            raise MonthlyConsumerValidationError("QE probe result already exists with different bytes") from exc
    return path


def _default_profile_loader(path: Path) -> Any:
    from backend.services.quantevolver.qe_active_dataset_profile import (
        load_qe_profile,
        validate_controller_snapshot,
    )

    profile = load_qe_profile(path)
    validate_controller_snapshot(profile)
    return profile


def _default_dataset_resolver(**kwargs: Any) -> Any:
    from backend.services.quantevolver.qe_active_dataset_profile import resolve_active_qe_dataset

    return resolve_active_qe_dataset(**kwargs)


def _default_hmm_reader(content: str) -> None:
    from backend.services.quantevolver.config_composer import (
        PRECOMPUTED_HMM_COEFF_JSON_PARAM,
        ConfigComposer,
    )

    resolved = ConfigComposer()._resolve_hmm_coefficients_json(
        {PRECOMPUTED_HMM_COEFF_JSON_PARAM: content},
        {},
    )
    if resolved != content:
        raise MonthlyConsumerValidationError("QE Composer changed the frozen HMM coefficient bytes")


def _profile_raw(profile: Any) -> Mapping[str, Any]:
    raw = getattr(profile, "raw", None)
    if not isinstance(raw, Mapping):
        raise MonthlyConsumerValidationError("QE probe profile has no frozen payload")
    return raw


def _selection(consumer_id: str) -> Mapping[str, Any]:
    if consumer_id in {"qe_single", "qe_p10", "qe_p11"}:
        return {"mode": "stock_universe", "pool_ids": []}
    if consumer_id == "qe_custom":
        return {"mode": "single_index", "pool_ids": ["csi300"]}
    if consumer_id == "qe_multi_alpha":
        return {"mode": "union", "pool_ids": ["csi500", "csi1000"]}
    raise MonthlyConsumerValidationError(f"unsupported QE consumer probe: {consumer_id}")


def _hmm_asset(request: ConsumerProbeRequest) -> tuple[Path, Mapping[str, Any], str]:
    matches: list[tuple[Path, Mapping[str, Any], str]] = []
    for path in sorted(request.derived_asset_paths):
        if path.suffix.lower() != ".json":
            continue
        try:
            content = path.read_text(encoding="utf-8").strip()
            value = json.loads(content)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if (
            isinstance(value, Mapping)
            and value.get("schema_version") == SHARED_HMM_COEFFICIENT_SCHEMA
            and value.get("preset_key") == "preset_A"
        ):
            matches.append((path, value, content))
    if len(matches) != 1:
        raise MonthlyConsumerValidationError(
            "QE P10/P11 require exactly one shared preset_A HMM coefficient asset"
        )
    return matches[0]


def _hmm_binding(
    *,
    profile_raw: Mapping[str, Any],
    manifest_sha256: str,
    asset: Mapping[str, Any],
) -> Mapping[str, Any]:
    components = profile_raw.get("components")
    qe = profile_raw.get("consumers", {}).get("qe") if isinstance(profile_raw.get("consumers"), Mapping) else None
    sector = components.get("sector_context_pins") if isinstance(components, Mapping) else None
    universes = qe.get("universes") if isinstance(qe, Mapping) else None
    stock = universes.get("stock_universe") if isinstance(universes, Mapping) else None
    membership_sha = sector.get("membership_sha256") if isinstance(sector, Mapping) else None
    source_universe_sha = stock.get("sha256") if isinstance(stock, Mapping) else None
    dataset_identity = asset.get("dataset_identity")
    input_hashes = asset.get("input_file_sha256")
    if (
        not isinstance(dataset_identity, Mapping)
        or dataset_identity.get("dataset_manifest_sha256") != manifest_sha256
        or asset.get("dataset_manifest_sha256") != manifest_sha256
        or not isinstance(input_hashes, Mapping)
        or input_hashes.get("sector_membership_spans_parquet") != membership_sha
        or not isinstance(membership_sha, str)
        or not isinstance(source_universe_sha, str)
    ):
        raise MonthlyConsumerValidationError("shared HMM asset identity differs from the QE release")
    return {
        "dataset_binding": {
            "schema_version": "hmm_risk_qe_dataset_binding_v1",
            "dataset_manifest_sha256": manifest_sha256,
            "sector_membership_sha256": membership_sha,
            "source_universe_sha256": source_universe_sha,
        }
    }


def _blacklist_code(profile_raw: Mapping[str, Any], root: Path) -> str:
    components = profile_raw.get("components")
    pins = components.get("sector_context_pins") if isinstance(components, Mapping) else None
    if not isinstance(pins, Mapping):
        raise MonthlyConsumerValidationError("QE P11 sector pins are unavailable")
    code_map_path = root / str(pins.get("component_root") or "") / str(pins.get("code_map_file") or "")
    try:
        value = json.loads(code_map_path.read_text(encoding="utf-8"))
        codes = value.get("member_backed_codes")
        if not isinstance(codes, list):
            entries = value.get("entries")
            codes = (
                [item.get("canonical_l2_code") for item in entries if isinstance(item, Mapping)]
                if isinstance(entries, list)
                else value.get("ordered_codes")
            )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyConsumerValidationError("QE P11 code map is unreadable") from exc
    if not isinstance(codes, list) or not codes:
        raise MonthlyConsumerValidationError("QE P11 code map has no canonical sectors")
    normalized = {str(code).strip().upper() for code in codes}
    # This fixed canonical smoke sector is part of the QE consumer acceptance
    # contract.  The resolver still proves that it filters an executable PIT
    # interval; it is never selected from a model key or current snapshot.
    if "801011.SI" not in normalized:
        raise MonthlyConsumerValidationError("QE P11 canonical smoke sector is unavailable")
    return "801011.SI"


@dataclass(frozen=True, slots=True)
class MonthlyQEConsumerProbe:
    """Exercise one of the five registered QE creation/read paths."""

    profile_loader: Callable[[Path], Any] = _default_profile_loader
    dataset_resolver: Callable[..., Any] = _default_dataset_resolver
    hmm_reader: Callable[[str], None] = _default_hmm_reader
    probe_id: str = _PROBE_ID
    probe_version: str = _PROBE_VERSION

    def run(self, request: ConsumerProbeRequest) -> Path:
        if request.consumer_id not in QE_CONSUMERS:
            raise MonthlyConsumerValidationError("QE probe received another consumer")
        profile = self.profile_loader(request.profile_path)
        raw = _profile_raw(profile)
        custom_params: dict[str, Any] = {}
        evidence = [
            _content_ref(request.binding_path),
            _content_ref(request.profile_path),
        ]
        asset_path: Path | None = None
        asset_content: str | None = None
        if request.consumer_id in _HMM_CONSUMERS:
            asset_path, asset, asset_content = _hmm_asset(request)
            self.hmm_reader(asset_content)
            from backend.services.hmm_risk.qe_assistance_transport import BINDING_PARAM

            custom_params = {
                "enable_sector_hmm": True,
                BINDING_PARAM: _hmm_binding(
                    profile_raw=raw,
                    manifest_sha256=request.dataset_manifest_sha256,
                    asset=asset,
                ),
            }
            evidence.append(_content_ref(asset_path, root=request.controller_candidate_root))

        if request.consumer_id == "qe_p11":
            custom_params["sector_blacklist"] = [
                _blacklist_code(raw, request.controller_candidate_root)
            ]

        resolved = self.dataset_resolver(
            node_id=request.node_id,
            universe_selection=_selection(request.consumer_id),
            custom_params=custom_params,
            profile=profile,
        )
        if resolved is None:
            raise MonthlyConsumerValidationError("QE probe did not resolve the active dataset")
        binding = getattr(resolved, "binding", None)
        split = getattr(resolved, "data_split", None)
        selection_pins = getattr(binding, "selection_pins", None)
        if (
            not isinstance(split, Mapping)
            or not isinstance(selection_pins, Mapping)
            or getattr(binding, "candidate_root", None) != request.node_candidate_root
            or getattr(binding, "release_id", None) != raw.get("release_id")
        ):
            raise MonthlyConsumerValidationError("QE resolver returned another release binding")

        policy = getattr(resolved, "sector_blacklist_policy", None)
        stock_pool_content = getattr(resolved, "stock_pool_content", None)
        if request.consumer_id == "qe_p11":
            filtered = policy.get("blacklist_excluded_count") if isinstance(policy, Mapping) else None
            if type(filtered) is not int or filtered <= 0 or not isinstance(stock_pool_content, str):
                raise MonthlyConsumerValidationError("QE P11 did not materialise a filtered PIT universe")
        elif policy is not None:
            raise MonthlyConsumerValidationError("QE probe unexpectedly applied a sector blacklist")

        coverage = {
            "unresolved_count": 0,
            "resolved_component_file_count": len(request.resolved_component_paths),
            "derived_asset_count": len(request.derived_asset_paths),
            "materialized_interval_count": (
                len([line for line in stock_pool_content.splitlines() if line.strip()])
                if isinstance(stock_pool_content, str)
                else 0
            ),
            "blacklisted_sector_count": (
                len(custom_params.get("sector_blacklist", []))
            ),
        }
        result = {
            "schema_version": CONSUMER_PROBE_RESULT_SCHEMA,
            "status": "PASS",
            "consumer_id": request.consumer_id,
            "node_id": request.node_id,
            "dataset_manifest_sha256": request.dataset_manifest_sha256,
            "binding_sha256": _sha256(request.binding_path),
            "required_window": {
                key: str(split[key])
                for key in ("train_start", "train_end", "valid_start", "valid_end", "test_start", "backtest_end", "test_end")
                if key in split
            },
            "coverage_counts": coverage,
            "adapter": {"id": self.probe_id, "version": self.probe_version},
            "evidence_refs": evidence,
            "side_effect_flags": {
                "outcomes_read": False,
                "training_started": False,
                "experiment_started": False,
                "runtime_action_performed": False,
            },
        }
        return _write_canonical_or_identical(
            request.binding_path.parent / f"{request.consumer_id}-qe-probe-result.json",
            result,
        )


def monthly_qe_consumer_probes() -> Mapping[str, MonthlyQEConsumerProbe]:
    """Return the exact code-owned QE subset for the monthly registry."""

    return {consumer_id: MonthlyQEConsumerProbe() for consumer_id in QE_CONSUMERS}


__all__: Sequence[str] = (
    "MonthlyQEConsumerProbe",
    "QE_CONSUMERS",
    "monthly_qe_consumer_probes",
)
