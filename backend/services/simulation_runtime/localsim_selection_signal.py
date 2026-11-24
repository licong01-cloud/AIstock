"""LocalSIM's thin, independent consumer of the common selection signal kernel.

Planned historical evidence binds a full raw artifact stored by the owning
selection attempt before committing a compact daily-evidence row.
Account/order/ledger commits remain entirely with the original lifecycle.
"""

from __future__ import annotations

from copy import copy, deepcopy
from dataclasses import replace
from datetime import date
import hashlib
import json
import re
from pathlib import Path
from uuid import uuid4

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.advisory_historical_range.selection_signal_kernel import (
    SelectionSignalBatchError, SelectionSignalDayV1, SelectionSignalKernel, SelectionSignalRequestV1,
)
from backend.services.advisory_historical_range.inference_session import inference_output_lease
from backend.services.selection_center.hmm_runtime import SectorHMMRuntime
from backend.services.selection_center.canonical_pit_runtime import has_canonical_pit_runtime_profile, require_canonical_pit_generation_current
from backend.services.selection_center.models import SelectionCandidate, SelectionExclusion
from backend.services.selection_center.prospective_evidence import SelectionStageTrace
from backend.services.selection_center.prospective_evidence_assembler import ProspectiveSelectionEvidenceAssembler
from backend.services.simulation_signal.contracts import DailySelectionEvidence, canonical_json_sha256
from backend.services.simulation_execution.localsim.suspension import localsim_suspension_policy_identity
from backend.services.simulation_signal.strategy_package_selection import (
    StrategyPackageSelectionResult, StrategyPackageSelectionService,
)
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    SelectionScoreArtifact, StrategyPackageSelectionArtifactRepository, StrategyPackageSelectionArtifactService,
)
from backend.services.strategy_package.selection_computation import StrategyPackageSelectionComputation
from backend.services.strategy_package.selection_signal_preparation import (
    PreparedRawSelectionArtifactV2, StrategyPackageSignalPreparationResultV1,
)
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError
from scripts.strategy_package_live_inference import _atomic_json

from .localsim_selection_inference import (
    LOCALSIM_INFERENCE_SCOPE, LocalSimInferenceScope, LocalSimReferencedAssetResolver,
    LocalSimSharedInferenceProvider, own_scope, windows_x_root,
)
from .localsim_file_inputs import current_file_dataset


def _selection_code_identity():
    base = Path(__file__).resolve().parents[1]
    paths = (
        Path(__file__), Path(__file__).with_name("localsim_selection_inference.py"),
        base.parents[1] / "scripts/strategy_package_live_inference.py", base.parent / "inference_engine.py",
        base / "advisory_historical_range/selection_signal_kernel.py",
        base / "simulation_signal/strategy_package_selection.py",
        base / "selection_center/tradability.py", base / "selection_center/suspension_provider.py",
        base.parent / "data_service/frozen_dataset.py", base / "advisory_historical_range/inference_inputs.py",
        Path(__file__).with_name("localsim_file_inputs.py"), base / "simulation_data/frozen_file_adapters.py",
        Path(__file__).with_name("localsim_input_source_plan.py"), base / "advisory_historical_range/input_source_plan.py",
    )
    paths += tuple(base / "advisory_historical_range" / (name + ".py") for name in (
        "alpha158_batch", "inference_session", "inference_features", "factor_input_materialization"))
    paths += tuple(base / "strategy_package" / (name + ".py") for name in (
        "selection_computation", "selection_artifact", "selection_signal_preparation", "live_inference", "runtime",
        "multi_alpha_live", "reference_prices"))
    return {str(p.name): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}


# Fingerprint the sources actually loaded by this process, not a later main
# checkout. Shared reference/filter changes must invalidate old projections.
_LOADED_SELECTION_CODE_IDENTITY = _selection_code_identity()


class LocalSimRawRepository:
    """Only the current run's outputs; NEVER delegates reads to a global store."""

    def save(self, artifact):
        scope = own_scope()
        metadata = deepcopy(artifact.metadata)
        # Native multi-alpha preserves a common population hash/count but does
        # not expose the legs' full member lists at its parent metadata level.
        # Project that independently observed identical population, not a union
        # of scored symbols or an invented membership list.
        sources = scope.source_universes
        if sources and all(isinstance(v["members"], list) for v in sources):
            baseline = sources[0]
            if (baseline["count"] != artifact.universe_count
                    or any(v["count"] != baseline["count"] or v["input_hash"] != baseline["input_hash"]
                           or v["members"] != baseline["members"] for v in sources)):
                raise ValueError("native LocalSIM leg populations conflict with the parent input population")
            metadata["universe_members"] = list(baseline["members"])
        metadata["localsim_signal_run"] = {"consumer_scope": "LOCALSIM", "run_id": scope.run_id, "clocks": scope.clocks.to_dict()}
        if scope.source_plan is not None:
            if not scope.source_plan_inputs:
                raise ValueError("LocalSIM planned raw artifact has no actual native input receipts")
            metadata["localsim_signal_run"].update(source_plan_hash=scope.source_plan["plan_hash"],
                source_run_id=scope.source_plan["run_id"], native_inputs=deepcopy(scope.source_plan_inputs))
            provider = metadata.get("provider_metadata", {})
            native_context = provider.get("selection_native_input_context")
            normalized = metadata.get("artifact_input_context")
            if native_context is not None:
                common = ("requested_trade_date", "effective_trade_date", "score_trade_date", "pit_mode",
                    "calendar_version", "calendar_hash", "calendar_source", "universe_input_hash",
                    "window_start_date", "required_window", "window_resolution", "window_lineage_hash")
                if not isinstance(normalized, dict) or any(native_context.get(k) != normalized.get(k) for k in common):
                    raise ValueError("LocalSIM normalized context differs from its complete native context")
            scope.source_plan_inputs.clear()
        dataset = current_file_dataset()
        if dataset is not None:
            metadata["localsim_signal_run"]["file_binding_hash"] = dataset.identity
        artifact = SelectionScoreArtifact.model_validate({**artifact.model_dump(), "metadata": metadata})
        artifact = StrategyPackageSelectionArtifactRepository._with_digest(artifact)
        scope.stage = "native_raw_publication"
        scope.artifacts[artifact.package_id] = artifact
        _atomic_json(scope.root / "raw" / (hashlib.sha256(artifact.package_id.encode()).hexdigest() + ".json"), artifact.model_dump(mode="json"))
        scope.source_universes.clear()  # A later package can have its own native population.
        return artifact

    def get(self, *, package_id, manifest_sha256, trade_date, data_source, runtime_config_hash):
        artifact = own_scope().artifacts.get(package_id)
        if artifact is None:
            raise DataUnavailableError("this LocalSIM run has not computed the requested raw artifact",
                                       context={"reason_code": "LOCALSIM_OWN_RAW_NOT_COMPUTED"})
        if (artifact.manifest_sha256, artifact.trade_date, artifact.data_source, artifact.runtime_config_hash) != (
            manifest_sha256, trade_date, data_source, runtime_config_hash
        ):
            # Native health/preparation may probe both v2 and legacy runtime
            # hashes. A miss is not permission to regenerate frozen scores.
            raise DataUnavailableError("LocalSIM raw artifact does not match this native lookup",
                                       context={"reason_code": "LOCALSIM_OWN_RAW_NOT_COMPUTED"})
        return artifact.model_copy(deep=True)


class LocalSimSelectionArtifactService(StrategyPackageSelectionArtifactService):
    def _load_reference_prices(self, symbols, trade_date):
        dataset = current_file_dataset()
        if dataset is None:
            return super()._load_reference_prices(symbols, trade_date)
        from backend.services.simulation_data.frozen_file_adapters import file_reference_prices
        return file_reference_prices(dataset, symbols=symbols, as_of_date=trade_date)

    def generate_from_live_inference(self, **kwargs):
        if kwargs["package_id"] in own_scope().artifacts:
            raise InvalidStateTransitionError(
                "LocalSIM frozen raw cannot be regenerated after a native identity miss",
                context={"reason_code": "LOCALSIM_FROZEN_RAW_IDENTITY_CONFLICT"},
            )
        return super().generate_from_live_inference(**kwargs)


class LocalSimReadOnlyHmmSnapshots:
    """Native snapshot SELECTs only; deliberately has no generator/train methods."""

    def __init__(self, conn_factory=get_conn):
        self._conn_factory = conn_factory

    def _rows(self, field, value):
        # field is selected exclusively by the two methods below.
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT s.*, c.display_name AS config_display_name FROM model_train_snapshots s "
                            "JOIN model_train_configs c ON c.config_id = s.config_id WHERE s." + field + " = %s "
                            "ORDER BY s.trained_at DESC", (value,))
                return [dict(row) for row in cur.fetchall()]

    def get_snapshot(self, snapshot_id):
        rows = self._rows("snapshot_id", snapshot_id)
        return rows[0] if rows else None

    def list_snapshots(self, config_id):
        return self._rows("config_id", config_id)


class LocalSimReadOnlyHmmRuntime(SectorHMMRuntime):
    def adjust_candidates_with_receipt(self, **kwargs):
        clocks = own_scope().clocks
        if kwargs.get("trade_date") != clocks.decision_date:
            raise ValueError("LocalSIM HMM request day differs from its own signal clocks")
        # A native coefficient labelled D may legally have input cutoff C.
        # Preserve that contract rather than shifting to the C-labelled row.
        # The injected provider has no train/generate API.
        return super().adjust_candidates_with_receipt(**kwargs)

    def _load_coefficients(self, **kwargs):
        return super()._load_coefficients(**{**kwargs, "allow_generate": False})


class LocalSimKernelComputation:
    """Compose native prepared values into the public kernel; no policy copy."""

    def __init__(self, computation=None):
        self.native = computation or StrategyPackageSelectionComputation()

    def compute(self, *, request, prepared_signals, providers):
        scope = own_scope()
        scope.stage = "shared_selection_computation"
        prepared = {}
        for package_id, signal in prepared_signals.items():
            artifact = scope.artifacts[package_id]
            prepared[package_id] = StrategyPackageSignalPreparationResultV1(
                raw=PreparedRawSelectionArtifactV2(artifact, artifact.canonical_v2_header(),
                                                  {"status": artifact.status.value, "consumer_scope": "LOCALSIM", "run_id": scope.run_id,
                                                   "artifact_id": artifact.artifact_id, "artifact_payload_sha256": artifact.artifact_payload_sha256,
                                                   "completed_prefix": deepcopy(scope.completed_prefix)},
                                                  tuple(artifact.metadata.get("source_read_receipts", []))),
                prepared_signal=signal,
            )
        source = canonical_json_sha256({p: {"input": a.artifact_input_context_hash,
                                           "revision": a.source_revision_set_hash, "assets": a.asset_closure_hash}
                                       for p, a in scope.artifacts.items()})
        if scope.source_plan is not None:
            source = scope.source_plan["plan_hash"]
        from backend.services.selection_center.canonical_pit_runtime import CANONICAL_PIT_RUNTIME_LEASE_KEY
        runtime_configs = {}
        for package_id, profile in request.package_runtime_profiles.items():
            config = {"runtime_profile": profile.model_dump(mode="json")}
            # Admission attaches this excluded field to the parsed profile.
            # Preserve it at the native root boundary when serializing for the
            # kernel; otherwise ST risk silently falls back to its old default.
            lease = profile.risk_policy.canonical_pit_runtime_lease
            if lease is not None:
                config[CANONICAL_PIT_RUNTIME_LEASE_KEY] = deepcopy(lease)
            runtime_configs[package_id] = config
        common_request = SelectionSignalRequestV1(
            consumer_scope="LOCALSIM", run_id=scope.run_id, date_plan=(scope.clocks,),
            ordered_package_ids=request.ordered_package_ids,
            manifest_hashes={p: prepared_signals[p].manifest_sha256 for p in request.ordered_package_ids},
            runtime_configs=runtime_configs,
            package_top_k=request.package_top_k, source_identity=source,
            selection_mode=request.selection_mode.value, package_weights=request.package_weights,
        )
        try:
            result = SelectionSignalKernel(computation=self.native, providers=providers).run(
                request=common_request, prepare_day=lambda *, package_id, **_kwargs: prepared[package_id],
            )[0]
        except SelectionSignalBatchError as exc:
            # Keep native reason_code/retryability; do not turn a model error
            # into VALID_NO_CANDIDATE or invent an account retry policy.
            raise exc.cause from exc
        scope.signal_result = result
        return result.computation


_ARCHIVE_OWNER = r"[0-9a-f]{64}/lsselection_[0-9a-f]{32}"


def _owned_evidence_path(root: Path, identity: str, run_id: str, directory: str, package_id: str) -> Path:
    root = root.resolve()
    if re.fullmatch(_ARCHIVE_OWNER, f"{identity}/{run_id}") is None:
        raise ValueError("invalid LocalSIM evidence owner")
    package_key = hashlib.sha256(package_id.encode()).hexdigest()
    expected = root / identity / run_id / directory / (package_key + ".json")
    if expected.resolve() != expected or not expected.is_relative_to(root):
        raise ValueError("LocalSIM evidence path escaped its owning attempt")
    return expected


def read_localsim_evidence_raw(owner, *, root: Path | None, package_id: str) -> SelectionScoreArtifact:
    ref = owner.get("raw_artifact_ref")
    if ref is None:
        return SelectionScoreArtifact.model_validate(owner.get("raw_artifact", {}))
    if root is None or owner.get("consumer_scope") != "LOCALSIM" or "raw_artifact" in owner:
        raise ValueError("LocalSIM raw evidence has conflicting storage ownership")
    path = _owned_evidence_path(root, owner["request_identity"], owner["run_id"], "raw_artifacts", package_id)
    try:
        data = path.read_bytes()
    except FileNotFoundError as exc:
        raise DataUnavailableError("committed LocalSIM raw artifact is unavailable", context={
            "reason_code": "LOCALSIM_RAW_ARTIFACT_UNAVAILABLE", "run_id": owner["run_id"], "package_id": package_id,
        }) from exc
    if (ref["path"] != path.relative_to(root.resolve()).as_posix() or ref["bytes"] != len(data)
            or hashlib.sha256(data).hexdigest() != ref["sha256"]):
        raise ValueError("LocalSIM raw evidence does not match its committed content reference")
    artifact = SelectionScoreArtifact.model_validate_json(data)
    if artifact.package_id != package_id or artifact.artifact_payload_sha256 != ref["artifact_payload_sha256"]:
        raise ValueError("LocalSIM raw artifact identity differs from committed evidence")
    return artifact


def _write_owned_raw(scope, owner, artifact):
    root = scope.root.parent.parent.resolve()
    path = _owned_evidence_path(root, scope.root.parent.name, scope.run_id, "raw_artifacts", artifact.package_id)
    if path.exists():
        raise FileExistsError("LocalSIM raw artifact already exists in this new attempt")
    _atomic_json(path, artifact.model_dump(mode="json"))
    with path.open("rb") as stream:
        sha = hashlib.file_digest(stream, "sha256").hexdigest()
    owner["raw_artifact_ref"] = {
        "path": path.relative_to(root).as_posix(), "bytes": path.stat().st_size,
        "sha256": sha, "artifact_payload_sha256": artifact.artifact_payload_sha256,
    }


def read_failed_daily_evidence_archive(root, ref, *, release, trade_date, source_plan, evidence_hash_suffix):
    """Admit the original journaled evidence for the exact failed native insert."""
    from .localsim_input_source_plan import day_route, validate_artifact_source
    from .localsim_replay_recovery import FAILED_EVIDENCE_ARCHIVE_PATTERN

    if re.fullmatch(FAILED_EVIDENCE_ARCHIVE_PATTERN, ref) is None:
        raise ValueError("invalid LocalSIM failed evidence reference")
    identity, run_id, expected_sha = ref.split("/")
    intent_path = _owned_evidence_path(root, identity, run_id, "evidence", release.package_id)
    failure_path = intent_path.parent.parent / "failure.json"
    if failure_path.resolve() != failure_path or (intent_path.parent.parent / "completion.json").exists():
        raise ValueError("LocalSIM failed archive is not an uncommitted attempt")
    failure = json.loads(failure_path.read_bytes())
    route = day_route(source_plan, trade_date)
    clocks = SelectionSignalDayV1(*(date.fromisoformat(route[key]) for key in
        ("decision_date", "data_cutoff", "target_trade_date")))
    if (failure.get("status") != "FAILED" or failure.get("consumer_scope") != "LOCALSIM"
            or failure.get("run_id") != run_id or failure.get("error_type") != "ProgramLimitExceeded"
            or failure.get("failed_stage") != "native_daily_evidence_transaction"
            or failure.get("clocks") != clocks.to_dict()
            or any(entry.get("stage") == "native_daily_evidence_transaction" for entry in failure["completed_prefix"])):
        raise ValueError("LocalSIM failure journal does not prove the failed native write")
    data = intent_path.read_bytes()
    if hashlib.sha256(data).hexdigest() != expected_sha:
        raise ValueError("LocalSIM failed evidence bytes changed")
    document = json.loads(data)
    del data
    payload = document["evidence_payload_json"]
    owner = payload["localsim_signal_run"]
    artifact = SelectionScoreArtifact.model_validate(owner["raw_artifact"])
    projection = owner["projection"]
    if (canonical_json_sha256(payload) != document["artifact_hash"]
            or not document["artifact_hash"].endswith(evidence_hash_suffix)
            or document["evidence_id"] != "dse_" + document["artifact_hash"][:16]
            or document["package_id"] != release.package_id or document["manifest_sha256"] != release.manifest_sha256
            or document["release_id"] != release.release_id or document["release_hash"] != release.release_hash
            or document["runtime_profile_version_id"] != release.runtime_profile_version_id
            or document["runtime_profile_hash"] != release.runtime_profile_sha256
            or document["target_trade_date"] != str(trade_date) or document["cutoff_date"] != str(clocks.data_cutoff)
            or owner.get("consumer_scope") != "LOCALSIM" or owner.get("run_id") != run_id
            or owner.get("request_identity") != identity or owner.get("clocks") != clocks.to_dict()
            or owner.get("suspension_policy_identity") != localsim_suspension_policy_identity()
            or owner.get("projection_sha256") != canonical_json_sha256(projection)
            or set(projection["package_results"]) != {release.package_id}
            or projection["manifest_sha256_by_package"] != {release.package_id: release.manifest_sha256}
            or artifact.package_id != release.package_id or artifact.manifest_sha256 != release.manifest_sha256
            or artifact.trade_date != trade_date or artifact.data_source != "DB_HISTORICAL"
            or StrategyPackageSelectionArtifactRepository._with_digest(artifact).artifact_payload_sha256
                != artifact.artifact_payload_sha256):
        raise ValueError("LocalSIM failed evidence does not match the original release, day or insert identity")
    raw_owner = artifact.metadata.get("localsim_signal_run") or {}
    if (raw_owner.get("consumer_scope") != "LOCALSIM" or raw_owner.get("run_id") != run_id
            or raw_owner.get("clocks") != clocks.to_dict()):
        raise ValueError("LocalSIM failed raw evidence has different ownership")
    validate_artifact_source(source_plan, clocks, artifact)
    del owner["raw_artifact"]
    return document, artifact


def _persist_owned_evidence(repository, evidence, payload, scope):
    digest = canonical_json_sha256(payload)
    prepared = DailySelectionEvidence.model_validate({**evidence,
        "evidence_id": f"dse_{digest[:16]}", "artifact_hash": digest, "evidence_payload_json": payload})
    # Publish the intent before the database write. Recovery reads the exact
    # native row, including after a crash between commit and checkpoint.
    _atomic_json(scope.root / "evidence" / (hashlib.sha256(prepared.package_id.encode()).hexdigest() + ".json"),
                 prepared.model_dump(mode="json"))
    stored = repository.save_daily_selection_evidence(prepared)
    _require_owned_evidence(stored, prepared, scope.run_id)
    scope.completed_prefix.append({"stage": "native_daily_evidence_transaction", "package_id": stored.package_id,
                                   "evidence_id": stored.evidence_id, "artifact_hash": stored.artifact_hash})
    return stored


class LocalSimDailyEvidenceWriter:
    def __init__(self, repository):
        self.repository = repository

    def save_daily_selection_evidence(self, evidence):
        scope = own_scope()
        scope.stage = "native_daily_evidence_transaction"
        artifact = scope.artifacts[evidence.package_id]
        common = scope.signal_result
        payload = deepcopy(evidence.model_dump(mode="json")["evidence_payload_json"])
        projection = json.loads((scope.root / "projection.json").read_text(encoding="utf-8"))
        payload["localsim_signal_run"] = {
            "schema_version": "localsim_selection_signal_run_v1", "consumer_scope": "LOCALSIM", "run_id": scope.run_id,
            "suspension_policy_identity": localsim_suspension_policy_identity(),
            "clocks": scope.clocks.to_dict(), "semantic_identity": common.semantic_identity,
            "storage_identity": common.storage_identity, "membership_status": common.membership_status[evidence.package_id],
            "unscored_members": list(common.unscored_members[evidence.package_id]),
            "projection": projection, "projection_sha256": canonical_json_sha256(projection),
            "request_identity": scope.root.parent.name,
        }
        owner = payload["localsim_signal_run"]
        if scope.source_plan is None:
            owner["raw_artifact"] = artifact.model_dump(mode="json")
        else:
            _write_owned_raw(scope, owner, artifact)
        raw_owner = artifact.metadata.get("localsim_signal_run", {})
        if raw_owner.get("run_id") != scope.run_id:
            payload["localsim_signal_run"]["reused_raw_run_id"] = raw_owner.get("run_id")
        return _persist_owned_evidence(self.repository, evidence.model_dump(mode="json", exclude={"evidence_payload_json"}),
                                       payload, scope)


def _require_owned_evidence(stored, expected, run_id):
    envelope = stored.evidence_payload_json.get("localsim_signal_run", {})
    if (stored.evidence_id != expected.evidence_id or stored.artifact_hash != expected.artifact_hash
            or stored.evidence_payload_json != expected.evidence_payload_json
            or envelope.get("consumer_scope") != "LOCALSIM" or envelope.get("run_id") != run_id):
        raise ValueError("native evidence readback does not belong to this LocalSIM run/content")


def _serialize_projection(kwargs):
    return {
        "runtime_config": kwargs["runtime_config"],
        "package_results": {p: [v.model_dump(mode="json") for v in rows] for p, rows in kwargs["package_results"].items()},
        "excluded_results": {p: [v.model_dump(mode="json") for v in rows] for p, rows in kwargs["excluded_results"].items()},
        "manifest_sha256_by_package": kwargs["manifest_sha256_by_package"],
        "stage_trace_by_package": {p: v.model_dump(mode="json") for p, v in kwargs["stage_trace_by_package"].items()},
    }


class LocalSimProspectiveCaptureBoundary:
    """Fail closed if a future caller attempts an Advisory capture path."""

    def capture(self, **_kwargs):
        raise ValueError("LocalSIM may publish only its native daily selection evidence")


class LocalSimSelectionSignalService(StrategyPackageSelectionService):
    def __init__(self, *, repository, temporary_root="X:/AIstock_temp/LOCALSIM", provider=None, asset_resolver=None,
                 package_repository=None, native_computation=None, hmm_runtime=None, **kwargs):
        self.temporary_root = windows_x_root(temporary_root)
        self.native_repository = repository
        raw_repository = LocalSimRawRepository()
        resolver = asset_resolver or LocalSimReferencedAssetResolver(cache_root=self.temporary_root / "assets")
        node = provider or LocalSimSharedInferenceProvider(repo_root=Path(__file__).resolve().parents[3])
        artifact_service = LocalSimSelectionArtifactService(
            package_repository=package_repository, artifact_repository=raw_repository,
            runtime_asset_resolver=resolver, live_inference_provider=node,
        )
        runtime = StrategyPackageRuntime(artifact_repository=raw_repository,
                                         hmm_runtime=hmm_runtime)
        super().__init__(package_repository=package_repository, repository=LocalSimDailyEvidenceWriter(repository), runtime=runtime,
                         selection_artifact_service=artifact_service, selection_computation=LocalSimKernelComputation(native_computation),
                         phase1_trace_capture_service=LocalSimProspectiveCaptureBoundary(), **kwargs)

    def _build_operational_evidence_by_package(self, **kwargs):
        scope = own_scope()
        # This is a projection of ALREADY computed native candidates/stages, not
        # another ranking algorithm. It allows commit-before-checkpoint recovery.
        computation = scope.signal_result.computation
        projection = _serialize_projection(kwargs)
        projection.update(aggregate_results=[v.model_dump(mode="json") for v in computation.aggregate_results],
                          valid_no_candidate=computation.valid_no_candidate, no_candidate_reason=computation.no_candidate_reason)
        _atomic_json(scope.root / "projection.json", projection)
        return super()._build_operational_evidence_by_package(**kwargs)

    def run_selection(self, **kwargs):
        dataset = current_file_dataset()
        if kwargs.get("data_source") == "DATASET_FILES" and dataset is None:
            current_file_dataset(required=True)
        if dataset is not None and not getattr(self, "_file_bound", False):
            if kwargs.get("data_source") != "DATASET_FILES":
                raise ValueError("frozen file scope cannot consume a different market source")
            from backend.services.simulation_data.frozen_file_adapters import (
                FileTradingCalendar, FileTradabilityFilter,
            )
            from backend.services.selection_center.risk_policy import StPitRiskDecisionProvider, StockRiskPolicyService
            bound = copy(self)
            bound.calendar_provider = FileTradingCalendar(dataset)
            bound.tradability_filter = FileTradabilityFilter(dataset)
            st = StPitRiskDecisionProvider(span_loader=dataset.spans)
            st.source_name = "dataset_files.stock_universe_pit"
            bound.risk_policy_service = StockRiskPolicyService(providers={"st_pit": st})
            bound._file_bound = True
            return bound.run_selection(**kwargs)
        recovery_evidence = kwargs.pop("recovery_evidence", None)
        from .localsim_replay_recovery import current_localsim_evidence_resume
        archive_resume = current_localsim_evidence_resume()
        if kwargs.get("prospective_context") is not None:
            raise ValueError("LocalSIM consumer uses its own daily evidence, not an Advisory prospective writer")
        trade_date = kwargs["trade_date"]
        from .localsim_input_source_plan import current_source_plan
        source_plan = current_source_plan()
        config = deepcopy(kwargs.get("runtime_config") or {})
        if dataset is not None:
            profile = config.get("runtime_profile", {})
            if (profile.get("hmm") or {}).get("enabled"):
                raise DataUnavailableError("file mode has no native HMM input adapter", context={
                    "reason_code": "FROZEN_DATASET_HMM_INPUT_UNCOVERED"})
            if (profile.get("risk_policy") or {}).get("enabled") and any(
                name != "st_pit" for name in (profile["risk_policy"].get("providers") or ["st_pit"])
            ):
                raise DataUnavailableError("file mode does not cover a requested native risk provider", context={
                    "reason_code": "FROZEN_DATASET_RISK_POLICY_UNCOVERED"})
        release = kwargs.get("runtime_release")
        if archive_resume is not None and (
            source_plan is None or recovery_evidence is not None or release is None
            or archive_resume["trade_date"] != str(trade_date) or archive_resume["release_id"] != release.release_id
            or archive_resume["native_input"]["source_plan_hash"] != source_plan["plan_hash"]
            or kwargs["package_ids"] != [release.package_id]
        ):
            raise InvalidStateTransitionError("LocalSIM archived selection recovery scope changed")
        identities = {p: self.package_repository.get(p).manifest_sha256 for p in kwargs["package_ids"]}
        identity = canonical_json_sha256({"packages": identities, "date": trade_date.isoformat(), "source": kwargs["data_source"],
                                          "localsim_suspension_policy": localsim_suspension_policy_identity(),
                                         "ordered_package_ids": kwargs["package_ids"],
                                         "config": config, "mode": kwargs["mode"].value,
                                         "file_binding_hash": dataset.identity if dataset else None,
                                         **({"source_plan_hash": source_plan["plan_hash"]} if source_plan else {}),
                                         "release": getattr(release, "release_hash", None),
                                          "recovery_evidence": getattr(recovery_evidence, "artifact_hash", None),
                                          **({"failed_evidence_archive": archive_resume["native_input"]["archive_ref"]}
                                             if archive_resume is not None else {}),
                                          "code": dict(_LOADED_SELECTION_CODE_IDENTITY)})
        logical_root = self.temporary_root / identity
        logical_root.mkdir(parents=True, exist_ok=True)
        with inference_output_lease(logical_root / "checkpoint.json"):
            recovered = self._recover(logical_root, identity, expected_package_ids=kwargs["package_ids"])
            if recovered is not None:
                return recovered
            # Ordinary uncommitted DB work is still recomputed. A failed insert
            # can carry the exact archived result admitted by replay recovery.
            run_id = "lsselection_" + uuid4().hex
            root = logical_root / run_id
            root.mkdir()
            # Fresh inference resolves symbolic PIT dates in its native provider.
            # Reusing frozen scores skips that provider, so resolve the same
            # native policy before validating the frozen C/D/T identity.
            clock_config = self._apply_point_in_time_selection_config(config, trade_date=trade_date) if recovery_evidence is not None else config
            cutoff = self.parse_selection_cutoff_date(self.selection_artifact_config(clock_config), trade_date=trade_date, strict_before=True)
            from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
            scope = LocalSimInferenceScope(run_id, root, SelectionSignalDayV1(trade_date, cutoff or trade_date, trade_date), {})
            if source_plan is not None:
                from .localsim_input_source_plan import day_route
                route = day_route(source_plan, trade_date)
                planned_clocks = SelectionSignalDayV1(*(date.fromisoformat(route[key]) for key in
                    ("decision_date", "data_cutoff", "target_trade_date")))
                if cutoff is not None and cutoff != planned_clocks.data_cutoff:
                    raise ValueError("LocalSIM native C differs from the frozen source plan")
                scope.clocks, scope.clocks_bound, scope.source_plan = planned_clocks, True, source_plan
                if route["source"] == "DATASET_FILES" and release is not None and recovery_evidence is None:
                    scope.file_prefetch_owner = canonical_json_sha256({
                        "release_hash": release.release_hash,
                        "producer_code": _selection_code_identity(),
                    })
            native_kwargs = kwargs
            if source_plan is not None:
                # The public kernel uses DB_HISTORICAL as its historical signal
                # request role. Physical FILE/DB identity belongs to the frozen
                # source plan and actual native input receipts. Create artifacts
                # with that role from the outset; never retag a sealed artifact.
                expected_source = "DATASET_FILES" if route["source"] == "DATASET_FILES" else "DB_HISTORICAL"
                if kwargs["data_source"] != expected_source:
                    raise ValueError("LocalSIM market source differs from its frozen daily route")
                native_kwargs = {**kwargs, "data_source": "DB_HISTORICAL"}
            if recovery_evidence is not None:
                artifact = self._recovery_raw(recovery_evidence, kwargs=native_kwargs, clocks=scope.clocks)
                scope.artifacts[artifact.package_id] = artifact
            token = LOCALSIM_INFERENCE_SCOPE.set(scope)
            _atomic_json(logical_root / "attempt.json", {"identity": identity, "consumer_scope": "LOCALSIM", "run_id": run_id,
                                                        "package_ids": kwargs["package_ids"]})
            try:
                if archive_resume is None:
                    result = super().run_selection(**native_kwargs)
                else:
                    result = self._commit_failed_archive(scope, release=release, recovery=archive_resume)
                scope.result = result
                _atomic_json(root / "completion.json", {"consumer_scope": "LOCALSIM", "run_id": run_id, "identity": identity,
                                                        "status": "SIGNAL_COMMITTED", "clocks": scope.clocks.to_dict()})
                _atomic_json(logical_root / "checkpoint.json", {"consumer_scope": "LOCALSIM", "run_id": run_id, "identity": identity,
                                                               "status": "SIGNAL_COMMITTED"})
                return result
            except Exception as exc:
                context = getattr(exc, "context", {})
                source_failure = {}
                if scope.source_plan is not None:
                    from backend.services.advisory_historical_range.input_source_plan import gap_receipt
                    source_failure["source_gap_receipt"] = gap_receipt(exc, stage=scope.stage,
                        clocks=scope.clocks, plan_hash=scope.source_plan["plan_hash"])
                _atomic_json(root / "failure.json", {"status": "FAILED", "consumer_scope": "LOCALSIM", "run_id": run_id,
                                                     "error_type": type(exc).__name__, "reason_code": getattr(exc, "reason_code", None)
                                                         or context.get("reason_code", type(exc).__name__),
                                                     "failed_stage": context.get("failed_stage") or scope.stage,
                                                     "clocks": scope.clocks.to_dict(), "completed_prefix": scope.completed_prefix,
                                                     "node_completed_prefix": context.get("completed_prefix", []), **source_failure})
                raise
            finally:
                LOCALSIM_INFERENCE_SCOPE.reset(token)

    def _commit_failed_archive(self, scope, *, release, recovery):
        native = recovery["native_input"]
        evidence, artifact = read_failed_daily_evidence_archive(
            self.temporary_root, native["archive_ref"], release=release, trade_date=scope.clocks.decision_date,
            source_plan=scope.source_plan, evidence_hash_suffix=native["original_evidence_hash"],
        )
        payload = evidence.pop("evidence_payload_json")
        owner = payload["localsim_signal_run"]
        projection = owner["projection"]
        owner.update(run_id=scope.run_id, request_identity=scope.root.parent.name,
                     reused_raw_run_id=artifact.metadata["localsim_signal_run"]["run_id"],
                     original_evidence_hash=evidence["artifact_hash"])
        result = self._projected_result(projection, {}, {evidence["package_id"]: artifact})
        scope.stage = "native_daily_evidence_transaction"
        _atomic_json(scope.root / "projection.json", projection)
        _write_owned_raw(scope, owner, artifact)
        stored = _persist_owned_evidence(self.native_repository, evidence, payload, scope)
        stored_by_package = {stored.package_id: stored}
        return replace(result, evidence_by_package=stored_by_package, runtime_config=self._attach_evidence_refs(
            projection["runtime_config"], stored_by_package, capture_receipt=result.evidence_capture_receipt,
        ))

    def _recovery_raw(self, evidence, *, kwargs, clocks):
        stored = self.native_repository.get_daily_selection_evidence(evidence.evidence_id)
        # Evidence freezes arrays as tuples. Restore its canonical JSON shape
        # before handing native membership/score inputs back to the kernel.
        payload = stored.model_dump(mode="json")["evidence_payload_json"]
        owner = payload.get("localsim_signal_run", {})
        if (stored.artifact_hash != evidence.artifact_hash
                or canonical_json_sha256(payload) != evidence.artifact_hash
                or kwargs["package_ids"] != [stored.package_id]
                or stored.manifest_sha256 != self.package_repository.get(stored.package_id).manifest_sha256
                or stored.target_trade_date != kwargs["trade_date"]
                or owner.get("consumer_scope") != "LOCALSIM" or not owner.get("run_id")
                or owner.get("clocks") != clocks.to_dict()
                or self.selection_artifact_config(kwargs.get("runtime_config") or {}).get("force_regenerate")):
            raise InvalidStateTransitionError("LocalSIM recovery raw evidence is not the exact native frozen input",
                                              context={"reason_code": "LOCALSIM_FROZEN_RAW_EVIDENCE_CONFLICT"})
        artifact = read_localsim_evidence_raw(owner, root=getattr(self, "temporary_root", None), package_id=stored.package_id)
        dataset = current_file_dataset()
        if dataset is not None and artifact.metadata.get("localsim_signal_run", {}).get("file_binding_hash") != dataset.identity:
            raise InvalidStateTransitionError("LocalSIM frozen raw file identity differs from this replay")
        from .localsim_input_source_plan import current_source_plan, validate_artifact_source
        plan = current_source_plan()
        if plan is not None:
            validate_artifact_source(plan, clocks, artifact)
        sealed = StrategyPackageSelectionArtifactRepository._with_digest(artifact)
        if (sealed.artifact_payload_sha256 != artifact.artifact_payload_sha256
                or artifact.package_id != stored.package_id or artifact.manifest_sha256 != stored.manifest_sha256
                or artifact.trade_date != stored.target_trade_date or artifact.data_source != kwargs["data_source"]
                or artifact.metadata.get("localsim_signal_run", {}).get("consumer_scope") != "LOCALSIM"
                or artifact.metadata.get("localsim_signal_run", {}).get("run_id")
                   != owner.get("reused_raw_run_id", owner["run_id"])
                or artifact.metadata.get("localsim_signal_run", {}).get("clocks") != clocks.to_dict()):
            raise InvalidStateTransitionError("LocalSIM recovery raw artifact identity is invalid",
                                              context={"reason_code": "LOCALSIM_FROZEN_RAW_IDENTITY_CONFLICT"})
        return artifact

    def _recover(self, logical_root, identity, *, expected_package_ids=None):
        attempt = logical_root / "attempt.json"
        if not attempt.exists():
            return None
        marker = json.loads(attempt.read_text(encoding="utf-8"))
        if marker.get("identity") != identity or marker.get("consumer_scope") != "LOCALSIM":
            raise ValueError("LocalSIM checkpoint identity conflicts with this request")
        run_id = marker["run_id"]
        root = (logical_root / run_id).resolve()
        if not root.is_relative_to(logical_root.resolve()) or root.parent != logical_root.resolve():
            raise ValueError("LocalSIM checkpoint path escaped its own run")
        projection_path = root / "projection.json"
        if not projection_path.exists():
            return None
        projection = json.loads(projection_path.read_text(encoding="utf-8"))
        packages = marker.get("package_ids")
        if (not isinstance(packages, list) or not packages or len(set(packages)) != len(packages)
                or (expected_package_ids is not None and packages != list(expected_package_ids))
                or set(projection["package_results"]) != set(packages)
                or any(set(projection[field]) != set(packages) for field in (
                    "excluded_results", "manifest_sha256_by_package", "stage_trace_by_package"))):
            raise ValueError("LocalSIM recovery package population differs from its complete request")
        evidence = {}
        for package_id in projection["package_results"]:
            path = root / "evidence" / (hashlib.sha256(package_id.encode()).hexdigest() + ".json")
            if not path.exists():
                return None
            expected = DailySelectionEvidence.model_validate_json(path.read_text(encoding="utf-8"))
            getter = getattr(self.native_repository, "get_daily_selection_evidence", None)
            if getter is None:
                raise ValueError("native LocalSIM evidence writer does not expose committed evidence readback")
            try:
                stored = getter(expected.evidence_id)
            except DataUnavailableError:
                return None  # No native commit: the next attempt must recompute.
            _require_owned_evidence(stored, expected, run_id)
            if stored.evidence_payload_json["localsim_signal_run"].get("projection_sha256") != canonical_json_sha256(projection):
                raise ValueError("LocalSIM projection content differs from its native committed evidence")
            evidence[package_id] = stored
        artifacts = {p: read_localsim_evidence_raw(
            v.model_dump(mode="json")["evidence_payload_json"]["localsim_signal_run"], root=logical_root.parent, package_id=p,
        ) for p, v in evidence.items()}
        from .localsim_input_source_plan import current_source_plan, day_route, validate_artifact_source
        plan = current_source_plan()
        if plan is not None:
            route = day_route(plan, next(iter(evidence.values())).target_trade_date)
            clocks = SelectionSignalDayV1(*(date.fromisoformat(route[key]) for key in
                ("decision_date", "data_cutoff", "target_trade_date")))
            for artifact in artifacts.values():
                validate_artifact_source(plan, clocks, artifact)
        result = self._projected_result(projection, evidence, artifacts)
        _atomic_json(logical_root / "checkpoint.json", {**marker, "status": "SIGNAL_COMMITTED", "recovery": "NATIVE_EVIDENCE_READBACK"})
        return result

    def _projected_result(self, projection, evidence, artifacts):
        if has_canonical_pit_runtime_profile(projection["runtime_config"]):
            require_canonical_pit_generation_current(projection["runtime_config"], authority_resolver=self.pit_authority_resolver)
        package_results = {p: [SelectionCandidate.model_validate(v) for v in rows] for p, rows in projection["package_results"].items()}
        traces = {p: SelectionStageTrace.model_validate(v) for p, v in projection["stage_trace_by_package"].items()}
        # Scheduler binds one package. Aggregate projection is stored by the
        # computation hook below, preserving all native fusion modes as well.
        aggregate = [SelectionCandidate.model_validate(v) for v in projection["aggregate_results"]]
        capture = ProspectiveSelectionEvidenceAssembler.not_requested_receipt()
        config = self._attach_evidence_refs(projection["runtime_config"], evidence, capture_receipt=capture)
        return StrategyPackageSelectionResult(
            runtime_config=config, package_results=package_results, aggregate_results=aggregate,
            excluded_results={p: [SelectionExclusion.model_validate(v) for v in rows] for p, rows in projection["excluded_results"].items()},
            manifest_sha256_by_package=projection["manifest_sha256_by_package"], evidence_by_package=evidence,
            valid_no_candidate=projection["valid_no_candidate"], no_candidate_reason=projection["no_candidate_reason"],
            stage_trace_by_package=traces, evidence_capture_receipt=capture, score_artifacts_by_package=artifacts,
        )
