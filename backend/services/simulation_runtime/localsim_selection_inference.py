"""LocalSIM transport and immutable-asset addressing for the shared node code.

There is no scoring, feature, sequence, fusion or selection algorithm here.
Native package preparation owns those contracts; the node runs the existing CLI.
"""

from __future__ import annotations

from collections import OrderedDict
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import subprocess
from threading import Lock
import time
from typing import Any
from uuid import uuid4

from backend.infra.wsl_qlib_runner import win_to_wsl_path
from backend.services.canonical_equity_pit import PitAuthorityStatus
from backend.services.canonical_pit_inference_boundary import (
    InferencePitMode, manifest_inference_pit_identity, resolve_inference_pit_identity,
)
from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
from backend.services.selection_center.canonical_pit_runtime import (
    SelectionPitRuntimeLease, has_canonical_pit_runtime_profile, require_canonical_pit_runtime_binding,
)
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
    LiveInferenceResult,
    PreparedInferenceWorkspace,
    QEExperimentRuntimeAssetResolver,
    QEExperimentRuntimeSource,
    WslStrategyPackageInferenceProvider,
    _manifest_runtime_custom_params,
    _manifest_runtime_data_split,
    _manifest_runtime_experiment_id,
    _runtime_factor_name,
    _single_model_asset_for_runtime,
    _write_inside_runtime_source,
)
from backend.services.strategy_package.models import FactorAsset
from backend.services.trading_core.errors import DataUnavailableError, PackageAssetInvalidError
from .models import canonical_json_sha256
from scripts.strategy_package_live_inference import _atomic_json


@dataclass
class LocalSimInferenceScope:
    run_id: str
    root: Path
    clocks: SelectionSignalDayV1
    artifacts: dict[str, Any]
    result: Any = None
    signal_result: Any = None
    clocks_bound: bool = False
    source_universes: list[dict[str, Any]] = field(default_factory=list)
    completed_prefix: list[dict[str, Any]] = field(default_factory=list)
    stage: str = "native_policy_preparation"
    source_plan: dict | None = None
    source_plan_inputs: list[dict] = field(default_factory=list)
    file_prefetch_owner: str | None = None


@dataclass(frozen=True)
class _PrefetchedFileDay:
    index_path: Path
    index_sha256: str
    producer_workspaces: dict[str | None, str]


LOCALSIM_INFERENCE_SCOPE: ContextVar[LocalSimInferenceScope | None] = ContextVar("localsim_inference_scope", default=None)


def own_scope() -> LocalSimInferenceScope:
    scope = LOCALSIM_INFERENCE_SCOPE.get()
    if scope is None:
        raise ValueError("LocalSIM inference requires its own explicit run scope")
    return scope


def _require_windows_x_root(value: Path | str) -> Path:
    root = Path(value).resolve()
    if root.drive.upper() != "X:" or not root.is_relative_to(Path("X:/AIstock_temp").resolve()):
        raise ValueError("LocalSIM temporary storage must resolve beneath X:/AIstock_temp")
    return root


def windows_x_root(value: Path | str) -> Path:
    root = _require_windows_x_root(value)
    root.mkdir(parents=True, exist_ok=True)
    return root


def probe_localsim_wsl_witness(*, distro: str, witness: Path, run_id: str) -> dict[str, str]:
    """Read the consumer-owned witness through both native filesystem entrances."""
    resolved = witness.resolve()
    try:
        _require_windows_x_root(resolved.parent)
    except ValueError:
        raise DataUnavailableError("LocalSIM witness is outside its owned X directory",
                                   context={"reason_code": "LOCALSIM_WSL_X_PATH_UNAVAILABLE"}) from None
    try:
        content = resolved.read_bytes()
        if content != run_id.encode("utf-8"):
            raise ValueError("witness identity differs")
        probe = subprocess.run(["wsl", "-d", distro, "--", "cat", win_to_wsl_path(str(resolved))],
                               capture_output=True, text=True, timeout=20, check=False,
                               creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    except subprocess.TimeoutExpired:
        raise DataUnavailableError("LocalSIM WSL path probe timed out", context={
            "reason_code": "LOCALSIM_WSL_X_PATH_TIMEOUT", "failure_stage": "WSL_PATH_PROBE",
            "run_id": run_id, "timeout_seconds": 20,
        }) from None
    except (OSError, ValueError):
        raise DataUnavailableError("LocalSIM owned WSL witness is unavailable",
                                   context={"reason_code": "LOCALSIM_WSL_X_PATH_UNAVAILABLE"}) from None
    if probe.returncode or probe.stdout != run_id:
        raise DataUnavailableError("WSL cannot read this LocalSIM X directory",
                                   context={"reason_code": "LOCALSIM_WSL_X_PATH_UNAVAILABLE"})
    return {"distro": distro, "witness_path": str(resolved), "run_id": run_id,
            "witness_sha256": hashlib.sha256(content).hexdigest()}


class LocalSimReferencedAssetResolver(QEExperimentRuntimeAssetResolver):
    """Address native frozen assets in place; materialize only small code/config.

    Windows cross-volume hard links and privileged symlinks are unavailable.
    This consumer's thin workspace address adapter delegates schema/order/code
    construction to the original resolver, and references weight/processor blobs
    through absolute node paths. Native multi-alpha still dispatches every seed.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._models: dict[str, Any] = {}
        self._package_context: dict[str, Any] = {}

    def _source_from_package_assets(self, manifest, *, package_id, model_asset_override=None,
                                    factor_set_override=None, runtime_assets_override=None, cache_namespace=None,
                                    observation_panel: dict[str, Any] | None = None,
                                    reference_factor_set: list[FactorAsset] | None = None):
        scope = own_scope()
        scope.stage = "native_asset_addressing"
        model = model_asset_override or _single_model_asset_for_runtime(manifest)
        model_factors = manifest.factor_set if factor_set_override is None else factor_set_override
        factors_by_name = {_runtime_factor_name(factor, package_id=package_id): factor for factor in model_factors}
        factor_names = list(factors_by_name)
        for factor in reference_factor_set or []:
            name = _runtime_factor_name(factor, package_id=package_id)
            if name in factors_by_name and factors_by_name[name].sha256 != factor.sha256:
                raise PackageAssetInvalidError("reference factor conflicts with a frozen model factor")
            factors_by_name[name] = factor

        root = scope.root / "workspaces" / uuid4().hex
        root.mkdir(parents=True)
        for name, factor in factors_by_name.items():
            payload = self._read_package_asset_bytes(asset_ref=factor.asset_ref, expected_sha256=factor.sha256,
                                                    package_id=package_id, asset_kind="factor_code", logical_name=name)
            target = root / "factors" / (name + ".py")
            _write_inside_runtime_source(target, payload, source_dir=root, package_id=package_id)
        self._materialize_alpha158_conf(manifest, source_dir=root, package_id=package_id,
                                        runtime_assets_override=runtime_assets_override)
        self._materialize_model_code_assets(model, model_dir=root / "model", source_dir=root, package_id=package_id)
        self._models[str(root)] = model
        evidence = manifest.source_evidence if isinstance(manifest.source_evidence, dict) else {}
        backtest = manifest.backtest_context if isinstance(manifest.backtest_context, dict) else {}
        # An inference declaration is distinct from a training compatibility
        # binding. Keep native stock-pool metadata without promoting the latter
        # into a frozen inference identity or constructing a member list.
        self._package_context[str(root)] = {
            "pit_identity": manifest_inference_pit_identity(evidence),
            "universe_policy": manifest.universe_policy.model_dump(mode="json") if manifest.universe_policy else None,
            "canonical_pit_binding": manifest.canonical_pit_binding.model_dump(mode="json") if manifest.canonical_pit_binding else None,
        }
        custom_params = _manifest_runtime_custom_params(manifest, runtime_assets_override=runtime_assets_override)
        if observation_panel is not None:
            custom_params["observation_panel"] = dict(observation_panel)
        return QEExperimentRuntimeSource(
            experiment_id=_manifest_runtime_experiment_id(manifest), db_workspace_path=Path(), asset_workspace_path=root,
            factor_names=factor_names, custom_params=custom_params,
            data_split=_manifest_runtime_data_split(evidence, backtest), model_params_origin="package_asset",
            source_workspace_type="strategy_package_asset_store", package_id=package_id, manifest_sha256=manifest.manifest_sha256,
        )

    def load_source_for_strategy_package(self, *, manifest, package_id, **_kwargs):
        return self.load_frozen_source_for_strategy_package(manifest=manifest, package_id=package_id)

    def _blob(self, descriptor) -> Path:
        self.asset_store.verify(descriptor.asset_ref, sha256=descriptor.sha256, size_bytes=descriptor.size_bytes)
        # Existing LocalPackageAssetStore's immutable documented layout.
        root = getattr(self.asset_store, "root", None)
        if root is None:
            raise DataUnavailableError("LocalSIM asset store has no read-only local addressing contract",
                                       context={"reason_code": "LOCALSIM_ASSET_REFERENCE_UNAVAILABLE"})
        path = (Path(root) / "blobs" / descriptor.sha256[:2] / descriptor.sha256).resolve(strict=True)
        if not path.is_relative_to(Path(root).resolve()):
            raise ValueError("immutable package reference escaped its asset root")
        return path

    def prepare_workspace(self, *, package_id, manifest_sha256, source, runtime_config=None,
                          path_converter=None, cache_namespace=None, verify_model_code_contract=True):
        if path_converter is None:
            raise ValueError("LocalSIM shared inference requires its WSL node")
        root = source.asset_workspace_path
        model = self._models.pop(str(root))
        context = self._package_context.pop(str(root))
        config = runtime_config or {}
        artifact_config = config.get("selection_artifact_config") or config.get("selection_artifact") or {}
        if not isinstance(artifact_config, dict):
            raise ValueError("native LocalSIM selection artifact config must be an object")
        pit_identity = artifact_config.get("pit_identity", context["pit_identity"])
        lease = None
        if has_canonical_pit_runtime_profile(config):
            clocks = own_scope().clocks
            lease = require_canonical_pit_runtime_binding(config, trade_date=clocks.decision_date)
            lease.require_trade_date(clocks.data_cutoff)
            lease.require_trade_date(clocks.target_trade_date)
            if lease.authority_status is PitAuthorityStatus.ACTIVE_CANONICAL and own_scope().source_plan is None:
                if pit_identity is not None:
                    declared = resolve_inference_pit_identity(
                        pit_identity, version_tag="localsim", allow_active_canonical_pointer=False,
                    )
                    if declared.mode is not InferencePitMode.ROLLING_RUNTIME or SelectionPitRuntimeLease.from_binding(declared.binding) != lease:
                        raise ValueError("LocalSIM inference declaration differs from its frozen PIT lease")
                pit_identity = {"mode": InferencePitMode.ROLLING_RUNTIME.value, **lease.as_dict()}
        if own_scope().source_plan is not None:
            from backend.services.advisory_historical_range import input_source_plan as source_plans
            plan = own_scope().source_plan
            route = source_plans.resolve_day_source(plan, own_scope().clocks)
            # The public plan owns the physical native envelope. An explicit
            # per-request declaration is still checked by that same helper.
            session = source_plans.create_input_session(plan, own_scope().clocks,
                consumer_scope="LOCALSIM", run_id=plan["run_id"], max_bytes=1024 * 1024)
            try:
                pit_identity = source_plans.session_pit_identity(session,
                    request_identity=artifact_config.get("pit_identity"), manifest_identity=context["pit_identity"])
            finally:
                session.close()
            if route["source"] == "DATABASE" and SelectionPitRuntimeLease.from_binding(
                    resolve_inference_pit_identity(pit_identity, version_tag="localsim",
                        allow_active_canonical_pointer=False).binding) != lease:
                raise ValueError("LocalSIM source plan PIT identity differs from its release")
        order = self._build_factor_order(source=source, conf_path=self._resolve_conf_path(source))
        factors_dir = root / "factors"
        factor_files = self._resolve_factor_files(factors_dir, order.dynamic_factors)
        observation_panel = (source.custom_params or {}).get("observation_panel")
        if observation_panel is not None:
            factor_files.update(self._resolve_factor_files(factors_dir, observation_panel["reference_factor_names"]))
        weight = self._blob(model)
        processor = self._blob(model.preprocessor_asset) if model.preprocessor_asset else None
        factor_order_path, entry_path, manifest_path = root / "factor_order.json", root / "strategy_package_factor_entry.py", root / "manifest.json"
        _atomic_json(factor_order_path, {**vars(order), "package_id": package_id, "source_experiment_id": source.experiment_id,
                                       "total_factors": len(order.factor_order), "alpha158_count": len(order.alpha158_factors),
                                       "dynamic_count": len(order.dynamic_factors), "source": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                                       "is_aligned": True})
        entry_path.write_text(self._build_factor_entry_source(factor_files=factor_files, path_converter=path_converter), encoding="utf-8")
        _atomic_json(manifest_path, {
            "schema_version": 1, "task_id": package_id, "source": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
            "primary_assets": {"model_weight_relpath": path_converter(str(weight)),
                               "factor_entry_relpath": entry_path.name,
                               "dataset_processor_relpath": path_converter(str(processor)) if processor else None},
            "assets": {"model_weight": path_converter(str(weight)), "factor_entry": entry_path.name,
                       "factor_order": factor_order_path.name, "factors_count": len(order.factor_order),
                       "dataset_processor": path_converter(str(processor)) if processor else None},
            "diagnostics": {"package_id": package_id, "package_manifest_sha256": manifest_sha256,
                            "source_workspace_type": source.source_workspace_type, "model_params_origin": "package_asset",
                            "model_source_path": str(weight), "asset_addressing": "READ_ONLY_REFERENCE"},
            "native_package_identity": context,
            "canonical_pit_runtime_lease": lease.as_dict() if lease else None,
            **({"prefetch_uses_plan_pit": True}
               if own_scope().file_prefetch_owner and lease is not None and pit_identity is not None
               and artifact_config.get("pit_identity") is None
               and context["pit_identity"] is None else {}),
            **({"pit_identity": pit_identity} if pit_identity is not None else {}),
            **({"observation_panel": observation_panel} if observation_panel is not None else {}),
        })
        return PreparedInferenceWorkspace(
            workspace_path=root, manifest_path=manifest_path, factor_order_path=factor_order_path, factor_entry_path=entry_path,
            model_params_path=weight, source_workspace_path=root, factor_source_dir=root / "factors",
            factor_order=order.factor_order, alpha158_factors=order.alpha158_factors, dynamic_factors=order.dynamic_factors,
            model_source_path=weight, model_candidate_count=1, dataset_processor_path=processor, model_params_origin="package_asset",
        )


def _require_native_pit_receipt(payload, lease_payload, clocks, *, file_binding_hash=None, planned_identity=None):
    """Bind the shared engine's authoritative input receipt to this daily lease."""
    lease = SelectionPitRuntimeLease.from_mapping(lease_payload)
    context = payload.get("input_context", {})
    # Native receipts deliberately omit universe_codes. Compare their compact
    # projection against the full request; never resolve a receipt as a request
    # or fetch online members to complete it.
    identity = resolve_inference_pit_identity(
        planned_identity if planned_identity is not None else context.get("pit_identity"),
        version_tag="localsim", allow_active_canonical_pointer=False)
    if planned_identity is not None and context.get("pit_identity") != identity.as_dict():
        raise ValueError("LocalSIM planned PIT receipt differs from its complete native request")
    if identity.mode is InferencePitMode.FROZEN_CANDIDATE and planned_identity is not None and file_binding_hash:
        members = payload.get("metadata", {}).get("universe_members")
        if (not isinstance(members, list) or tuple(sorted(members)) != identity.universe_codes
                or len(members) != payload["universe_count"] or identity.universe_as_of != clocks.data_cutoff
                or any(getattr(identity.binding, key) != getattr(lease, key) for key in
                       ("authority_id", "rule_version", "rule_parameters_digest"))):
            raise ValueError("LocalSIM frozen native population differs from its planned file input")
    elif identity.mode is not InferencePitMode.ROLLING_RUNTIME or SelectionPitRuntimeLease.from_binding(identity.binding) != lease:
        raise ValueError("LocalSIM node population differs from the native daily PIT lease")
    if context.get("pit_mode") != identity.receipt_mode:
        raise ValueError("LocalSIM node population differs from the native daily PIT lease")
    if context.get("frozen_file_binding_hash") != file_binding_hash:
        raise ValueError("LocalSIM PIT receipt differs from its explicit daily input source")
    # The CLI keeps engine input_context/source_read_receipts intact and
    # projects skinny metadata. Never infer identity from absent metadata or
    # manufacture it from the expected lease.
    receipts = [row for row in payload.get("source_read_receipts", []) if row.get("source_role") == "pit_universe"]
    expected = {
        "dataset_id": f"market.stock_universe_pit:{lease.universe_key}",
        "query_template_id": "StockUniversePitService.get_eligible_codes",
        "query_template_version": "canonical_pit_v2",
        "partition_ref": clocks.data_cutoff.isoformat(),
        "parameter_hash": canonical_json_sha256({
            "trade_date": clocks.data_cutoff.isoformat(), "ensure": False, "pit_identity": identity.as_dict(),
        }),
        "row_count": payload["universe_count"],
        "content_hash": context.get("universe_input_hash"),
    }
    if file_binding_hash is not None:
        # Native file receipts wrap the same PIT parameters with the frozen
        # dataset identity. Select this format only from our own input scope,
        # never from a node's self-declared source or the current active data.
        expected.update(
            dataset_id="dataset_files.day_selection_universe",
            query_template_id="FrozenDataset.day_selection_universe",
            query_template_version="frozen_market_dataset_files_v1",
            parameter_hash=canonical_json_sha256({
                "native_parameters": expected["parameter_hash"], "file_binding_hash": file_binding_hash,
            }),
        )
    if len(receipts) != 1 or any(receipts[0].get(key) != value for key, value in expected.items()):
        raise ValueError("LocalSIM PIT source receipt differs from its daily input identity")


class LocalSimSharedInferenceProvider(WslStrategyPackageInferenceProvider):
    """One owned node process per historical seed group, with independent workspaces."""

    def __init__(self, *, performance_reference: bool = False, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.performance_reference = performance_reference
        self._prefetched_file_days: OrderedDict[tuple[str, date], _PrefetchedFileDay] = OrderedDict()
        self._prefetched_file_days_lock = Lock()

    def require_frozen_file_input_identity(self):
        from .localsim_file_inputs import current_file_dataset
        return current_file_dataset(required=True).identity

    @property
    def can_batch_seeds(self) -> bool:
        scope = LOCALSIM_INFERENCE_SCOPE.get()
        return scope is not None and scope.source_plan is not None and not self.performance_reference

    def _build_env_exports(self, *, historical_read_only: bool = False) -> str:
        # The production factory supplies no optional safe_artifact_roots.
        # Bind the validated owned run, without mutating this shared provider
        # or process-wide environment while other days may run concurrently.
        root = windows_x_root(own_scope().root)
        roots = tuple(dict.fromkeys((root, *self.safe_artifact_roots)))
        translated = ":".join(win_to_wsl_path(str(item)) for item in roots)
        native = super()._build_env_exports(historical_read_only=historical_read_only)
        return f"{native} AISTOCK_STRATEGY_PACKAGE_RUNTIME_ROOTS={self._quote(translated)}"

    def run(self, *, workspace, trade_date: date, cutoff_date: date | None = None, historical_read_only=False):
        return self._run_workspaces({None: workspace}, trade_date=trade_date, cutoff_date=cutoff_date)[None]

    def run_seed_batch(self, *, workspaces: dict[str, PreparedInferenceWorkspace], trade_date: date,
                       cutoff_date: date | None = None) -> dict[str, LiveInferenceResult]:
        if not self.can_batch_seeds or len(workspaces) < 2 or any(not seed.strip() for seed in workspaces):
            raise ValueError("LocalSIM seed batching requires an owned historical plan and distinct seed IDs")
        return self._run_workspaces(workspaces, trade_date=trade_date, cutoff_date=cutoff_date)

    def _file_prefetch_key(self, scope, dataset, workspaces) -> str | None:
        if self.performance_reference or scope.file_prefetch_owner is None or dataset is None:
            return None
        manifests = {seed: json.loads(ws.manifest_path.read_text(encoding="utf-8")) for seed, ws in workspaces.items()}
        if any(manifest.get("prefetch_uses_plan_pit") is not True for manifest in manifests.values()):
            return None
        identities = []
        for seed, ws in workspaces.items():
            manifest = manifests[seed]
            manifest.pop("pit_identity")  # Rendered separately from the frozen plan for each cutoff.
            workspace = ws.workspace_path
            path_prefix = win_to_wsl_path(str(workspace)).encode("utf-8")
            member_hashes = {}
            for path in [ws.factor_order_path, *workspace.rglob("*.py")]:
                contents = path.read_bytes()
                if path == ws.factor_entry_path:
                    contents = contents.replace(path_prefix, b"<workspace>")
                member_hashes[path.relative_to(workspace).as_posix()] = hashlib.sha256(contents).hexdigest()
            blobs = [p.stat() for p in (ws.model_params_path, ws.dataset_processor_path) if p is not None]
            identities.append([seed, manifest, member_hashes,
                               [[s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns] for s in blobs]])
        return canonical_json_sha256({"plan": scope.source_plan["plan_hash"], "file": dataset.identity,
                                     "owner": scope.file_prefetch_owner, "workspaces": identities})

    @staticmethod
    def _next_file_request(scope, dataset, workspaces):
        from backend.services.advisory_historical_range import input_source_plan as source_plans

        days = scope.source_plan["days"]
        ordinal = next(i for i, row in enumerate(days) if row["decision_date"] == str(scope.clocks.decision_date))
        if ordinal + 1 == len(days):
            return None
        row = days[ordinal + 1]
        clocks = SelectionSignalDayV1(*(date.fromisoformat(row[k]) for k in ("decision_date", "data_cutoff", "target_trade_date")))
        route = source_plans.resolve_day_source(scope.source_plan, clocks)
        if (route["source"] != "DATASET_FILES" or route["file_binding_hash"] != dataset.identity
                or route.get("pit_policy") is None):
            return None
        for ws in workspaces.values():
            manifest = json.loads(ws.manifest_path.read_text(encoding="utf-8"))
            lease = SelectionPitRuntimeLease.from_mapping(manifest["canonical_pit_runtime_lease"])
            if not all(lease.coverage_start <= day <= lease.coverage_end
                       for day in (clocks.decision_date, clocks.data_cutoff, clocks.target_trade_date)):
                return None
        session = source_plans.create_input_session(scope.source_plan, clocks, consumer_scope="LOCALSIM",
            run_id=scope.source_plan["run_id"], max_bytes=0, root_override=dataset.root)
        try:
            pit = source_plans.session_pit_identity(session, request_identity=None, manifest_identity=None)
        finally:
            session.close()
        if pit is None:
            raise ValueError("LocalSIM file prefetch requires daily frozen PIT membership")
        return clocks, pit

    def _run_workspaces(self, workspaces, *, trade_date: date, cutoff_date: date | None):
        from backend.data_service.frozen_dataset import FrozenDatasetError
        from backend.services.advisory_historical_range.input_source_plan import SourcePlanError
        from .localsim_file_inputs import current_file_dataset

        scope = own_scope()
        if any(not Path(workspace.workspace_path).resolve().is_relative_to(scope.root.resolve())
               for workspace in workspaces.values()):
            raise ValueError("LocalSIM workspace must belong to its own run")
        first = next(iter(workspaces.values()))
        scope.stage = "shared_raw_inference"
        clocks = SelectionSignalDayV1(trade_date, cutoff_date or trade_date, scope.clocks.target_trade_date)
        if clocks.decision_date != scope.clocks.decision_date or (scope.clocks_bound and clocks != scope.clocks):
            raise ValueError("native LocalSIM inference requested different C/D/T")
        # The native selection service resolves PREVIOUS_TRADING_DAY_CLOSE
        # after entering this scope; do not reconstruct that calendar policy.
        scope.clocks, scope.clocks_bound = clocks, True
        dataset = current_file_dataset()
        prefetch_key = self._file_prefetch_key(scope, dataset, workspaces)
        if prefetch_key is not None:
            with self._prefetched_file_days_lock:
                cached = self._prefetched_file_days.get((prefetch_key, trade_date))
            if cached is not None:
                # Match the shared CLI's frozen checkpoint verification. The
                # new daily object's pins include files opened by the node on
                # the prior day but not yet opened in this Windows scope.
                for component in dataset.binding["files"]:
                    dataset.file(component)
                dataset.assert_unchanged()
                data = cached.index_path.read_bytes()
                if hashlib.sha256(data).hexdigest() != cached.index_sha256:
                    raise ValueError("LocalSIM prefetched node index content changed")
                return self._consume_node_day(scope, json.loads(data), cached.index_path, workspaces, dataset,
                                              producer_workspaces=cached.producer_workspaces)
        following, preflight_error = None, None
        if prefetch_key is not None:
            try:
                following = self._next_file_request(scope, dataset, workspaces)
            except (FrozenDatasetError, SourcePlanError) as exc:
                # A future day's missing source must not prevent inference for
                # the current day. The future day will prepare its own input.
                preflight_error = {"stage": "prefetch_following_input", "reason_code": exc.reason_code}
        root = scope.root / "node" / uuid4().hex
        root.mkdir(parents=True)
        plan, output = root / "plan.json", root / "batch.json"
        requests = [
            {**clocks.to_dict(), **({"request_id": seed, "runtime_workspace": win_to_wsl_path(str(workspace.workspace_path))}
                                   if seed is not None else {})}
            for seed, workspace in workspaces.items()
        ]
        if following is not None:
            future_clocks, future_pit = following
            requests.extend({**future_clocks.to_dict(), "pit_identity": future_pit,
                             "runtime_workspace": win_to_wsl_path(str(workspace.workspace_path)),
                             **({"request_id": seed} if seed is not None else {})}
                            for seed, workspace in workspaces.items())
        _atomic_json(plan, requests)
        # Actual bidirectional path verification, rather than assuming /mnt/x.
        witness = root / "path-witness.txt"
        witness.write_text(scope.run_id, encoding="utf-8")
        probe_localsim_wsl_witness(distro=self.distro, witness=witness, run_id=scope.run_id)
        arguments = ["scripts/strategy_package_live_inference.py", "--runtime-workspace", win_to_wsl_path(str(first.workspace_path)),
                     "--historical-plan", win_to_wsl_path(str(plan)), "--historical-read-only", "--consumer-scope", "LOCALSIM",
                     "--run-id", scope.run_id, "--temp-root", win_to_wsl_path(str(root)), "--output-path", win_to_wsl_path(str(output))]
        node_run_id = scope.run_id
        if scope.source_plan is not None:
            node_run_id = scope.source_plan["run_id"]
            arguments[arguments.index("--run-id") + 1] = node_run_id
            source_path = root / "source-plan.json"
            _atomic_json(source_path, scope.source_plan)
            arguments.extend(["--source-plan-file", win_to_wsl_path(str(source_path)),
                              "--input-revision", scope.source_plan["plan_hash"]])
            if dataset is not None:
                arguments.extend(["--dataset-root", win_to_wsl_path(str(dataset.root))])
        elif dataset is not None:
            binding_path = root / "dataset-binding.json"
            _atomic_json(binding_path, dataset.binding)
            # The shared CLI opens this exact locator and verifies pinned content
            # through WSL before reading any market values. No dataset/model copy.
            arguments.extend(["--dataset-binding", win_to_wsl_path(str(binding_path)),
                              "--dataset-root", win_to_wsl_path(str(dataset.root)),
                              "--input-revision", dataset.identity])
        if self.performance_reference:
            arguments.append("--performance-reference")
        command = (f"source {self.conda_sh} && conda activate {self.conda_env} && cd {self._quote(win_to_wsl_path(str(self.repo_root)))} && "
                   + self._build_env_exports(historical_read_only=True)
                   + " PGOPTIONS='-c default_transaction_read_only=on' PYTHONDONTWRITEBYTECODE=1 python "
                   + " ".join(self._quote(arg) for arg in arguments))
        started = time.perf_counter()
        try:
            completed = subprocess.run(["wsl", "-d", self.distro, "bash", "-lc", command], capture_output=True,
                                       text=True, encoding="utf-8", errors="replace", check=False, timeout=self.timeout_seconds,
                                       creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        except subprocess.TimeoutExpired:
            # TimeoutExpired includes the credential-bearing command in its
            # default text. Never allow that object into native runtime logs.
            raise DataUnavailableError("LocalSIM owned node exceeded its inference timeout", context={
                "reason_code": "LOCALSIM_NODE_TIMEOUT", "run_id": scope.run_id,
                "timeout_seconds": self.timeout_seconds}) from None
        log_text = completed.stdout + "\n" + completed.stderr
        for key, value in os.environ.items():
            if value and any(marker in key.upper() for marker in ("PASSWORD", "TOKEN", "SECRET", "API_KEY")):
                log_text = log_text.replace(value, "[REDACTED]")
        (root / "execution.log").write_text(log_text, encoding="utf-8")
        # Never include subprocess args or raw stdout/stderr (may contain credentials).
        if not output.is_file():
            raise DataUnavailableError("LocalSIM node did not publish its own index", context={
                "reason_code": "LOCALSIM_NODE_INDEX_MISSING", "returncode": completed.returncode, "run_id": scope.run_id})
        index_bytes = output.read_bytes()
        index = json.loads(index_bytes)
        index_results = index.get("results", [])
        if following is not None:
            requested = [(request["decision_date"], request.get("request_id")) for request in requests]
            indexed = [(item.get("trade_date"), item.get("request_id")) for item in index_results]
            if indexed != requested[:len(indexed)]:
                raise ValueError("LocalSIM file batch index is not an ordered prefix of its plan")
        complete = (index.get("status") == "COMPLETE" and index.get("ok") is True
                    and completed.returncode == 0 and len(index_results) == len(requests))
        complete_prefix = (following is not None and index.get("status") == "FAILED"
                           and index.get("ok") is False and completed.returncode != 0
                           and index.get("failed_stage") == "raw_inference"
                           and index.get("failed_clocks") == following[0].to_dict()
                           and len(workspaces) <= len(index_results) < len(requests))
        if (index.get("consumer_scope") != "LOCALSIM" or index.get("run_id") != node_run_id
                or not (complete or complete_prefix)):
            raise DataUnavailableError("LocalSIM node computation failed", context={
                "reason_code": index.get("reason_code", "LOCALSIM_NODE_FAILED"), "failed_stage": index.get("failed_stage"),
                "failed_clocks": index.get("failed_clocks"), "failed_request_id": index.get("failed_request_id"),
                "completed_prefix": index.get("results", []), "run_id": scope.run_id})
        if following is not None:
            dataset.assert_unchanged()
        results = self._consume_node_day(scope, index, output, workspaces, dataset,
                                         wall_seconds=time.perf_counter() - started, preflight_error=preflight_error)
        if following is not None and complete:
            addresses = {seed: win_to_wsl_path(str(ws.workspace_path)) for seed, ws in workspaces.items()}
            with self._prefetched_file_days_lock:
                self._prefetched_file_days[(prefetch_key, following[0].decision_date)] = _PrefetchedFileDay(
                    output, hashlib.sha256(index_bytes).hexdigest(), addresses)
                if len(self._prefetched_file_days) > 32:
                    self._prefetched_file_days.popitem(last=False)
        return results

    def _consume_node_day(self, scope, index, output, workspaces, dataset, *, producer_workspaces=None,
                          wall_seconds=None, preflight_error=None):
        clocks = scope.clocks
        node_run_id = scope.source_plan["run_id"] if scope.source_plan is not None else scope.run_id
        if index.get("consumer_scope") != "LOCALSIM" or index.get("run_id") != node_run_id:
            raise ValueError("LocalSIM node index consumer/run identity mismatch")
        items = [item for item in index["results"] if item.get("trade_date", str(clocks.decision_date)) == str(clocks.decision_date)]
        if len(items) != len(workspaces):
            raise ValueError("LocalSIM node did not complete this day's seed roster")
        return {
            seed: self._read_node_result(scope=scope, index=index, item=item, output=output, node_run_id=node_run_id,
                seed=seed, workspace=workspace, clocks=clocks, dataset=dataset, wall_seconds=wall_seconds,
                producer_workspace=producer_workspaces[seed] if producer_workspaces is not None else None,
                preflight_error=preflight_error)
            for (seed, workspace), item in zip(workspaces.items(), items, strict=True)
        }

    def _read_node_result(self, *, scope, index, item, output, node_run_id, seed, workspace, clocks, dataset, wall_seconds,
                          producer_workspace, preflight_error):
        directory = output.parent / "batch_dates" / clocks.decision_date.isoformat()
        if seed is not None:
            directory /= hashlib.sha256(seed.encode("utf-8")).hexdigest()
        path = directory / "scores.json"
        if (item.get("request_id") != seed or item.get("output_path") != win_to_wsl_path(str(path))
                or item.get("sha256") != hashlib.sha256(path.read_bytes()).hexdigest()):
            raise ValueError("LocalSIM node output path/content identity mismatch")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if dataset is not None and payload.get("input_context", {}).get("frozen_file_binding_hash") != dataset.identity:
            raise ValueError("LocalSIM shared node returned a different frozen file input identity")
        metadata = payload.get("metadata", {})
        if (payload.get("ok") is not True or metadata.get("consumer_scope") != "LOCALSIM"
                or metadata.get("run_id") != node_run_id or metadata.get("clocks") != clocks.to_dict()
                or metadata.get("binding") != index.get("binding") or metadata.get("request_id") != seed):
            raise ValueError("LocalSIM node consumer/run/clock binding mismatch")
        if (seed is not None or producer_workspace is not None) and metadata.get("runtime_workspace") != (
                producer_workspace or win_to_wsl_path(str(workspace.workspace_path))):
            raise ValueError("LocalSIM node output belongs to a different seed workspace")
        native_manifest = json.loads(workspace.manifest_path.read_text(encoding="utf-8"))
        if scope.source_plan is not None:
            from .localsim_input_source_plan import validate_payload
            receipt = validate_payload(scope.source_plan, clocks, payload)
            _require_native_pit_receipt(payload, native_manifest["canonical_pit_runtime_lease"], clocks,
                file_binding_hash=dataset.identity if dataset is not None else None,
                planned_identity=native_manifest["pit_identity"])
            from backend.services.advisory_historical_range import input_source_plan as source_plans
            leg_key = canonical_json_sha256({"factor_order": workspace.factor_order,
                "weight": str(workspace.model_params_path), "processor": str(workspace.dataset_processor_path)})
            pending = scope.root.parent / "source-inputs" / (leg_key + ".json")
            if pending.exists():
                source_plans.verify_retry_inputs(json.loads(pending.read_text(encoding="utf-8")), payload)
            else:
                _atomic_json(pending, payload)
            scope.source_plan_inputs.append({"leg_key": leg_key, "source_plan_receipt": receipt,
                "input_context": payload["input_context"], "source_read_receipts": payload["source_read_receipts"]})
            metadata["selection_native_input_context"] = payload["input_context"]
            metadata["localsim_source_plan"] = {"run_id": node_run_id, "plan_hash": scope.source_plan["plan_hash"]}
        elif native_manifest.get("canonical_pit_runtime_lease") is not None:
            _require_native_pit_receipt(payload, native_manifest["canonical_pit_runtime_lease"], clocks,
                                        file_binding_hash=dataset.identity if dataset is not None else None)
        duration = ({"inference_file_prefetch_hit": True} if producer_workspace is not None
                    else {"inference_wall_seconds" if seed is None else "inference_batch_wall_seconds": wall_seconds})
        metadata.update(inference_backend="wsl", **duration,
                        inference_batch_index=str(output), wsl_distro=self.distro, wsl_conda_env=self.conda_env)
        if preflight_error is not None:
            metadata["prefetch_following_failure"] = preflight_error
        elif index.get("status") == "FAILED":
            metadata["prefetch_following_failure"] = {
                "reason_code": index.get("reason_code"), "clocks": index.get("failed_clocks"),
            }
        scope.source_universes.append({"members": metadata.get("universe_members"), "count": payload["universe_count"],
                                       "input_hash": payload["input_context"].get("universe_input_hash")})
        scope.completed_prefix.append({"stage": "shared_raw_inference", "clocks": clocks.to_dict(), "output_sha256": item["sha256"],
                                       **({"request_id": seed} if seed is not None else {}),
                                       **({"file_prefetch_hit": True} if producer_workspace is not None else {}),
                                       **({"prefetch_following_failure": metadata["prefetch_following_failure"]}
                                          if "prefetch_following_failure" in metadata else {})})
        return LiveInferenceResult(scores=payload["scores"], metadata=metadata, universe_count=payload["universe_count"],
                                   source_read_receipts=payload["source_read_receipts"], input_context=payload["input_context"])
