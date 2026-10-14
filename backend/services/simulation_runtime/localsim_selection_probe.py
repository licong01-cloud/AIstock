"""Read-only, isolated actual-package acceptance for LocalSIM selection.

Uses native releases as read-only policy inputs, an in-memory native evidence
writer, and separate shared-engine computations. It never executes an account.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from contextlib import contextmanager
from datetime import date
import hashlib
import json
import os
from pathlib import Path
from threading import Event, Thread
import time

from dotenv import load_dotenv
import psycopg2
import psutil


@contextmanager
def read_only_connection():
    conn = psycopg2.connect(host=os.environ["TDX_DB_HOST"], port=os.getenv("TDX_DB_PORT", "5432"),
                            dbname=os.environ["TDX_DB_NAME"], user=os.environ["TDX_DB_USER"],
                            password=os.environ["TDX_DB_PASSWORD"], options="-c default_transaction_read_only=on -c statement_timeout=30000")
    try:
        conn.set_session(readonly=True)
        yield conn
    finally:
        conn.rollback()
        conn.close()


def _disk_bytes(root):
    # Our own run tree only. Referenced immutable blobs are not counted as new
    # disk materialization; no recursive deletion or asset modification occurs.
    total = 0
    identities = set()
    for path in root.rglob("*"):
        try:
            if path.is_file() and not path.is_symlink():
                stat = path.stat()
                identity = (stat.st_dev, stat.st_ino)
                if identity not in identities:
                    identities.add(identity)
                    total += stat.st_size
        except FileNotFoundError:
            continue  # Common owned materializations can be released meanwhile.
    return total


def _comparison(a, b):
    x, y = a["raw_artifact"], b["raw_artifact"]
    native_stages = []
    lineage_verified = []
    for payload in (a, b):
        projection = payload["evidence"]["evidence_payload_json"]["localsim_signal_run"]["projection"]
        stages = deepcopy(projection["stage_trace_by_package"])
        raw = payload["raw_artifact"]
        alpha = stages[raw["package_id"]]["alpha_raw"]["semantic_payload"]
        lineage_verified.append(alpha.get("artifact_id") == raw["artifact_id"])
        # Native raw receipts intentionally carry their own generated artifact
        # IDs. Verify those exact links before comparing ALL remaining values.
        # Never rewrite the original stored receipt or its native hash.
        alpha.pop("artifact_id", None)
        native_stages.append(stages)
    checks = {"full_scores": x["scores_json"] == y["scores_json"], "input_context": x["artifact_input_context_hash"] == y["artifact_input_context_hash"],
              "source_revision": x["source_revision_set_hash"] == y["source_revision_set_hash"], "manifest": x["manifest_sha256"] == y["manifest_sha256"],
              "asset_closure": x["asset_closure_hash"] == y["asset_closure_hash"], "canonical_raw_payload": x["artifact_payload_sha256"] == y["artifact_payload_sha256"],
              "policy": x["runtime_config_hash"] == y["runtime_config_hash"], "topk": a["selected"] == b["selected"],
              "exclusions": a["excluded"] == b["excluded"], "native_stage_values": native_stages[0] == native_stages[1],
              "native_alpha_lineage_verified": all(lineage_verified), "status": a["status"] == b["status"]}
    envelopes = [p["evidence"]["evidence_payload_json"]["localsim_signal_run"] for p in (a, b)]
    checks.update(clocks=envelopes[0]["clocks"] == envelopes[1]["clocks"],
                  population=x["universe_count"] == y["universe_count"] and x["metadata"].get("universe_members") == y["metadata"].get("universe_members"),
                  unknown=envelopes[0]["unscored_members"] == envelopes[1]["unscored_members"],
                  membership_status=envelopes[0]["membership_status"] == envelopes[1]["membership_status"])
    return {"checks": checks, "native_stage_hashes_equal": a["stage_semantic_hashes"] == b["stage_semantic_hashes"],
            "native_alpha_storage_ids_differ": x["artifact_id"] != y["artifact_id"],
            "native_kernel_semantic_id_equal": envelopes[0]["semantic_identity"] == envelopes[1]["semantic_identity"]}


def _baseline_day(root, mode, day):
    payload = json.loads((root / mode / (day.isoformat() + ".json")).read_text(encoding="utf-8"))
    from backend.services.simulation_signal.contracts import DailySelectionEvidence, canonical_json_sha256
    DailySelectionEvidence.model_validate(payload["evidence"])
    envelope = payload["evidence"]["evidence_payload_json"]["localsim_signal_run"]
    if envelope["raw_artifact"] != payload["raw_artifact"]:
        raise ValueError("baseline raw scores differ from their own native committed evidence")
    if "projection" not in envelope:
        # Earlier accepted-format probes sealed the full projection hash in
        # native evidence and retained the full file. This is comparison only;
        # the production consumer never accepts another run's result/cache.
        paths = list((root / mode).glob("*/" + envelope["run_id"] + "/projection.json"))
        if len(paths) != 1:
            raise ValueError("baseline has no unique content-bound native projection")
        projection = json.loads(paths[0].read_text(encoding="utf-8"))
        if canonical_json_sha256(projection) != envelope["projection_sha256"]:
            raise ValueError("baseline native projection hash differs from its own committed evidence")
        envelope["projection"] = projection
    if canonical_json_sha256(envelope["projection"]) != envelope["projection_sha256"]:
        raise ValueError("baseline complete projection differs from its native evidence hash")
    return payload


class _OwnedDiskSampler:
    def __init__(self, root):
        self.root, self.peak_bytes = root, 0
        self.stop = Event()
        self.thread = Thread(target=self._sample, daemon=True)

    def _sample(self):
        while not self.stop.is_set():
            self.peak_bytes = max(self.peak_bytes, _disk_bytes(self.root))
            self.stop.wait(1)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.stop.set()
        self.thread.join()
        self.peak_bytes = max(self.peak_bytes, _disk_bytes(self.root))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding-id", required=True)
    parser.add_argument("--dates", nargs="+", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--env-file", default="F:/Dev/AIstock/.env")
    parser.add_argument("--asset-root", default="F:/Dev/AIstock/rdagent_assets/package_assets")
    parser.add_argument("--modes", nargs="+", choices=("optimized", "reference"), default=["optimized", "reference"])
    parser.add_argument("--baseline-root", help="Explicit read-only reference probe for comparison; never a production input")
    args = parser.parse_args()
    load_dotenv(args.env_file, override=False)
    os.environ["PGOPTIONS"] = "-c default_transaction_read_only=on"
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    root = Path(args.output_root).resolve()
    if root.exists():
        raise FileExistsError("choose a new LocalSIM probe root; no prior run is overwritten")
    from scripts.strategy_package_live_inference import _atomic_json, _configure_temporary_root
    _configure_temporary_root(str(root))
    from backend.services.selection_center.models import SelectionMode
    from backend.services.simulation_runtime.localsim_selection_inference import LocalSimReferencedAssetResolver, LocalSimSharedInferenceProvider
    from backend.services.simulation_runtime.localsim_selection_signal import (
        LocalSimSelectionSignalService, LocalSimReadOnlyHmmRuntime, LocalSimReadOnlyHmmSnapshots,
    )
    from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository, SimulationRuntimeRepository
    from backend.services.simulation_signal.strategy_package_selection import StrategyPackageSelectionService
    from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
    from backend.services.strategy_package.repository import StrategyPackageRepository

    class ProbeCommitCrash(RuntimeError):
        pass

    class CrashOnceWriter(InMemorySimulationRuntimeRepository):
        """Real computed evidence; the probe's in-memory writer only."""
        crash_pending = True

        def save_daily_selection_evidence(self, evidence):
            stored = super().save_daily_selection_evidence(evidence)
            if self.crash_pending:
                self.crash_pending = False
                raise ProbeCommitCrash("read-only probe: native commit before checkpoint")
            return stored

    source_root = Path(__file__).resolve().parents[3]
    source_paths = [Path(__file__), Path(__file__).with_name("localsim_selection_signal.py"),
                    Path(__file__).with_name("localsim_selection_inference.py"), source_root / "scripts/strategy_package_live_inference.py",
                    source_root / "backend/inference_engine.py"]
    source_paths += [source_root / "backend/services/advisory_historical_range" / (name + ".py") for name in (
        "selection_signal_kernel", "alpha158_batch", "inference_session", "inference_inputs", "inference_features", "factor_input_materialization")]
    source_paths += [source_root / "backend/services/strategy_package" / (name + ".py") for name in (
        "selection_computation", "selection_artifact", "runtime", "live_inference", "multi_alpha_live")]
    source_paths += [source_root / "backend/services/simulation_signal/strategy_package_selection.py"]
    before = {str(p.relative_to(source_root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_paths}
    reader = SimulationRuntimeRepository(conn_factory=read_only_connection)
    binding = reader.get_simulation_release_binding(args.binding_id)
    release = reader.get_strategy_runtime_release(binding.release_id)
    config = StrategyPackageSelectionService.release_selection_runtime_config(release)
    dates = [date.fromisoformat(v) for v in args.dates]
    if not dates or any(a >= b for a, b in zip(dates, dates[1:])):
        raise ValueError("probe requires an explicit increasing date plan")
    package_reader = StrategyPackageRepository(conn_factory=read_only_connection)
    progress = {"status": "RUNNING", "package_id": binding.package_id, "release_hash": release.release_hash,
                "consumer_scope": "LOCALSIM", "source_hashes": before, "modes": {}, "equivalence": "PENDING"}
    baseline_root = None
    if args.baseline_root:
        baseline_root = Path(args.baseline_root).resolve(strict=True)
        if baseline_root.drive.upper() != "X:" or not baseline_root.is_relative_to(Path("X:/AIstock_temp").resolve()):
            raise ValueError("reference baseline must be an existing X:/AIstock_temp directory")
        baseline = json.loads((baseline_root / "progress.json").read_text(encoding="utf-8"))
        def common(hashes):
            return {k: v for k, v in hashes.items() if "simulation_runtime" not in k}
        if (args.modes != ["optimized"] or baseline["status"] != "COMPLETE" or baseline["source_binding"] != "EXACT_PASS"
                or baseline["package_id"] != binding.package_id or baseline["release_hash"] != release.release_hash
                or common(baseline["source_hashes"]) != common(before)):
            raise ValueError("reference baseline package/release/shared-code identity is incompatible")
        progress["baseline_root"] = str(baseline_root)
    _atomic_json(root / "progress.json", progress)
    try:
        for mode in args.modes:
            repository = CrashOnceWriter()
            provider = LocalSimSharedInferenceProvider(repo_root=source_root, performance_reference=mode == "reference",
                                                       safe_artifact_roots=(root, Path(args.asset_root)), timeout_seconds=3600)
            resolver = LocalSimReferencedAssetResolver(cache_root=root / mode / "assets",
                                                       asset_store=LocalPackageAssetStore(args.asset_root), conn_factory=read_only_connection)
            service = LocalSimSelectionSignalService(repository=repository, temporary_root=root / mode,
                                                    package_repository=package_reader, provider=provider, asset_resolver=resolver,
                                                    hmm_runtime=LocalSimReadOnlyHmmRuntime(LocalSimReadOnlyHmmSnapshots(read_only_connection)))
            measurements = []
            progress["modes"][mode] = measurements
            for ordinal, day in enumerate(dates):
                progress.update(active_mode=mode, active_day=day.isoformat())
                _atomic_json(root / "progress.json", progress)
                started = time.perf_counter()
                kwargs = dict(package_ids=[binding.package_id], mode=SelectionMode.SINGLE_PACKAGE, trade_date=day,
                              data_source="DB_HISTORICAL", runtime_config=config, runtime_release=release,
                              created_by="localsim_shared_selection_readonly_probe")
                injected_recovery = False
                with _OwnedDiskSampler(root / mode) as sampler:
                    try:
                        result = service.run_selection(**kwargs)
                    except ProbeCommitCrash:
                        injected_recovery = True
                        # A fresh consumer instance simulates worker restart;
                        # the original native writer contains the committed ID.
                        service = LocalSimSelectionSignalService(repository=repository, temporary_root=root / mode,
                                                                 package_repository=package_reader, provider=provider, asset_resolver=resolver,
                                                                 hmm_runtime=LocalSimReadOnlyHmmRuntime(LocalSimReadOnlyHmmSnapshots(read_only_connection)))
                        result = service.run_selection(**kwargs)
                seconds = time.perf_counter() - started
                artifact = result.score_artifacts_by_package[binding.package_id]
                native = artifact.model_dump(mode="json")
                payload = {"raw_artifact": native, "selected": [v.model_dump(mode="json") for v in result.aggregate_results],
                           "excluded": {p: [v.model_dump(mode="json") for v in rows] for p, rows in result.excluded_results.items()},
                           "stage_semantic_hashes": {p: {name: getattr(v, name).semantic_hash for name in (
                               "alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective")}
                                                     for p, v in result.stage_trace_by_package.items()},
                           "status": {"valid_no_candidate": result.valid_no_candidate, "no_candidate_reason": result.no_candidate_reason},
                           "evidence": result.evidence_by_package[binding.package_id].model_dump(mode="json")}
                _atomic_json(root / mode / (day.isoformat() + ".json"), payload)
                performance = native["metadata"]["provider_metadata"].get("inference_performance", {})
                measurements.append({"day": day.isoformat(), "wall_seconds": seconds, "ordinal": ordinal,
                                     "score_count": artifact.score_count, "universe_count": artifact.universe_count,
                                     "process_measurements": performance, "retained_disk_bytes": _disk_bytes(root / mode),
                                     "observed_peak_disk_bytes": sampler.peak_bytes,
                                     "windows_worker_lifetime_peak_bytes": getattr(psutil.Process().memory_info(), "peak_wset", None),
                                     "commit_before_checkpoint_recovered": injected_recovery})
                if before != {str(p.relative_to(source_root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_paths}:
                    progress["source_binding"] = "CHANGED_DURING_PROBE"
                    raise ValueError("source changed during the read-only probe")
                # Exact retry exercises the real consumer's native readback,
                # never another consumer's output or a mocked model invocation.
                retry_started = time.perf_counter()
                retry = service.run_selection(**kwargs)
                if retry.aggregate_results != result.aggregate_results or retry.evidence_by_package != result.evidence_by_package:
                    raise ValueError("LocalSIM exact retry changed its own committed selection")
                measurements[-1].update(recovery_seconds=time.perf_counter() - retry_started,
                                        native_evidence_count=len(repository.daily_selection_evidences),
                                        owned_node_run_count=len(list((root / mode).rglob("batch.json"))))
                _atomic_json(root / "progress.json", progress)
        if set(args.modes) == {"optimized", "reference"} or baseline_root is not None:
            comparisons = []
            for day in dates:
                a = _baseline_day(root, "optimized", day)
                b = _baseline_day(baseline_root or root, "reference", day)
                comparisons.append({"day": day.isoformat(), **_comparison(a, b)})
            progress["comparisons"] = comparisons
            progress["equivalence"] = "EXACT_PASS" if all(all(row["checks"].values()) for row in comparisons) else "NOT_PROVED"
        after = {str(p.relative_to(source_root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_paths}
        progress.update(status="COMPLETE", source_binding="EXACT_PASS" if before == after else "CHANGED_DURING_PROBE")
        _atomic_json(root / "progress.json", progress)
        print(json.dumps({"status": progress["status"], "equivalence": progress["equivalence"], "source_binding": progress["source_binding"],
                          "output_root": str(root)}, ensure_ascii=False))
        return 0 if progress["source_binding"] == "EXACT_PASS" and progress["equivalence"] != "NOT_PROVED" else 1
    except Exception as exc:
        progress.update(status="FAILED", error_type=type(exc).__name__, reason_code=getattr(exc, "context", {}).get("reason_code", type(exc).__name__))
        _atomic_json(root / "progress.json", progress)
        print(json.dumps({"status": "FAILED", "error_type": type(exc).__name__, "reason_code": progress["reason_code"], "output_root": str(root)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
