"""LocalSIM's thin, independent consumer of the common selection signal kernel.

Full native raw artifacts are published inside the existing daily-evidence
transaction. The global raw-artifact repository is never a discovery/cache path.
Account/order/ledger commits remain entirely with the original lifecycle.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
from uuid import uuid4

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.advisory_historical_range.selection_signal_kernel import (
    SelectionSignalBatchError, SelectionSignalKernel, SelectionSignalRequestV1,
)
from backend.services.advisory_historical_range.inference_session import inference_output_lease
from backend.services.selection_center.hmm_runtime import SectorHMMRuntime
from backend.services.selection_center.canonical_pit_runtime import has_canonical_pit_runtime_profile, require_canonical_pit_generation_current
from backend.services.selection_center.models import SelectionCandidate, SelectionExclusion
from backend.services.selection_center.prospective_evidence import SelectionStageTrace
from backend.services.selection_center.prospective_evidence_assembler import ProspectiveSelectionEvidenceAssembler
from backend.services.simulation_signal.contracts import DailySelectionEvidence, canonical_json_sha256
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
        common_request = SelectionSignalRequestV1(
            consumer_scope="LOCALSIM", run_id=scope.run_id, date_plan=(scope.clocks,),
            ordered_package_ids=request.ordered_package_ids,
            manifest_hashes={p: prepared_signals[p].manifest_sha256 for p in request.ordered_package_ids},
            runtime_configs={p: {"runtime_profile": profile.model_dump(mode="json")}
                             for p, profile in request.package_runtime_profiles.items()},
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
            "clocks": scope.clocks.to_dict(), "semantic_identity": common.semantic_identity,
            "storage_identity": common.storage_identity, "membership_status": common.membership_status[evidence.package_id],
            "unscored_members": list(common.unscored_members[evidence.package_id]),
            "raw_artifact": artifact.model_dump(mode="json"),
            "projection": projection, "projection_sha256": canonical_json_sha256(projection),
            "request_identity": scope.root.parent.name,
        }
        raw_owner = artifact.metadata.get("localsim_signal_run", {})
        if raw_owner.get("run_id") != scope.run_id:
            payload["localsim_signal_run"]["reused_raw_run_id"] = raw_owner.get("run_id")
        digest = canonical_json_sha256(payload)
        prepared = DailySelectionEvidence.model_validate({**evidence.model_dump(mode="json"),
                                                         "evidence_id": f"dse_{digest[:16]}", "artifact_hash": digest,
                                                         "evidence_payload_json": payload})
        # Intent is durable BEFORE the native transaction. A crash after commit
        # is recovered only by reading this exact owned evidence ID from writer.
        _atomic_json(scope.root / "evidence" / (hashlib.sha256(evidence.package_id.encode()).hexdigest() + ".json"),
                     prepared.model_dump(mode="json"))
        stored = self.repository.save_daily_selection_evidence(prepared)
        _require_owned_evidence(stored, prepared, scope.run_id)
        scope.completed_prefix.append({"stage": "native_daily_evidence_transaction", "package_id": stored.package_id,
                                       "evidence_id": stored.evidence_id, "artifact_hash": stored.artifact_hash})
        return stored


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
        recovery_evidence = kwargs.pop("recovery_evidence", None)
        if kwargs.get("prospective_context") is not None:
            raise ValueError("LocalSIM consumer uses its own daily evidence, not an Advisory prospective writer")
        trade_date = kwargs["trade_date"]
        config = deepcopy(kwargs.get("runtime_config") or {})
        release = kwargs.get("runtime_release")
        identities = {p: self.package_repository.get(p).manifest_sha256 for p in kwargs["package_ids"]}
        code_paths = (
            Path(__file__), Path(__file__).with_name("localsim_selection_inference.py"),
            Path(__file__).resolve().parents[3] / "scripts/strategy_package_live_inference.py",
            Path(__file__).resolve().parents[3] / "backend/inference_engine.py",
            Path(__file__).resolve().parents[1] / "advisory_historical_range/selection_signal_kernel.py",
        )
        code_paths += tuple(Path(__file__).resolve().parents[1] / "strategy_package" / (name + ".py") for name in (
            "selection_computation", "selection_artifact", "selection_signal_preparation", "live_inference", "runtime", "multi_alpha_live"))
        code_paths += (Path(__file__).resolve().parents[1] / "simulation_signal/strategy_package_selection.py",)
        identity = canonical_json_sha256({"packages": identities, "date": trade_date.isoformat(), "source": kwargs["data_source"],
                                         "ordered_package_ids": kwargs["package_ids"],
                                         "config": config, "mode": kwargs["mode"].value,
                                         "release": getattr(release, "release_hash", None),
                                          "recovery_evidence": getattr(recovery_evidence, "artifact_hash", None),
                                          "code": {str(p.name): hashlib.sha256(p.read_bytes()).hexdigest() for p in code_paths}})
        logical_root = self.temporary_root / identity
        logical_root.mkdir(parents=True, exist_ok=True)
        with inference_output_lease(logical_root / "checkpoint.json"):
            recovered = self._recover(logical_root, identity, expected_package_ids=kwargs["package_ids"])
            if recovered is not None:
                return recovered
            # Uncommitted DB input has no immutable revision guarantee. A retry
            # gets its own run and recomputes, retaining the original failure.
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
            if recovery_evidence is not None:
                artifact = self._recovery_raw(recovery_evidence, kwargs=kwargs, clocks=scope.clocks)
                scope.artifacts[artifact.package_id] = artifact
            token = LOCALSIM_INFERENCE_SCOPE.set(scope)
            _atomic_json(logical_root / "attempt.json", {"identity": identity, "consumer_scope": "LOCALSIM", "run_id": run_id,
                                                        "package_ids": kwargs["package_ids"]})
            try:
                result = super().run_selection(**kwargs)
                scope.result = result
                _atomic_json(root / "completion.json", {"consumer_scope": "LOCALSIM", "run_id": run_id, "identity": identity,
                                                        "status": "SIGNAL_COMMITTED", "clocks": scope.clocks.to_dict()})
                _atomic_json(logical_root / "checkpoint.json", {"consumer_scope": "LOCALSIM", "run_id": run_id, "identity": identity,
                                                               "status": "SIGNAL_COMMITTED"})
                return result
            except Exception as exc:
                context = getattr(exc, "context", {})
                _atomic_json(root / "failure.json", {"status": "FAILED", "consumer_scope": "LOCALSIM", "run_id": run_id,
                                                     "error_type": type(exc).__name__, "reason_code": context.get("reason_code", type(exc).__name__),
                                                     "failed_stage": context.get("failed_stage") or scope.stage,
                                                     "clocks": scope.clocks.to_dict(), "completed_prefix": scope.completed_prefix,
                                                     "node_completed_prefix": context.get("completed_prefix", [])})
                raise
            finally:
                LOCALSIM_INFERENCE_SCOPE.reset(token)

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
        artifact = SelectionScoreArtifact.model_validate(owner.get("raw_artifact", {}))
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
        if has_canonical_pit_runtime_profile(projection["runtime_config"]):
            # Original admission rejects a PIT generation change; recovery must
            # not bypass that native guard merely because evidence committed.
            require_canonical_pit_generation_current(projection["runtime_config"], authority_resolver=self.pit_authority_resolver)
        artifacts = {p: SelectionScoreArtifact.model_validate(dict(v.evidence_payload_json["localsim_signal_run"]["raw_artifact"]))
                     for p, v in evidence.items()}
        package_results = {p: [SelectionCandidate.model_validate(v) for v in rows] for p, rows in projection["package_results"].items()}
        traces = {p: SelectionStageTrace.model_validate(v) for p, v in projection["stage_trace_by_package"].items()}
        # Scheduler binds one package. Aggregate projection is stored by the
        # computation hook below, preserving all native fusion modes as well.
        aggregate = [SelectionCandidate.model_validate(v) for v in projection["aggregate_results"]]
        capture = ProspectiveSelectionEvidenceAssembler.not_requested_receipt()
        config = self._attach_evidence_refs(projection["runtime_config"], evidence, capture_receipt=capture)
        result = StrategyPackageSelectionResult(
            runtime_config=config, package_results=package_results, aggregate_results=aggregate,
            excluded_results={p: [SelectionExclusion.model_validate(v) for v in rows] for p, rows in projection["excluded_results"].items()},
            manifest_sha256_by_package=projection["manifest_sha256_by_package"], evidence_by_package=evidence,
            valid_no_candidate=projection["valid_no_candidate"], no_candidate_reason=projection["no_candidate_reason"],
            stage_trace_by_package=traces, evidence_capture_receipt=capture, score_artifacts_by_package=artifacts,
        )
        _atomic_json(logical_root / "checkpoint.json", {**marker, "status": "SIGNAL_COMMITTED", "recovery": "NATIVE_EVIDENCE_READBACK"})
        return result
