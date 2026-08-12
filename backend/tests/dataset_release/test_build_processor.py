from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PureWindowsPath

import pytest

from backend.services.dataset_release.build_processor import (
    BUILD_RESOURCE_RECEIPT_SCHEMA,
    BUILD_STAGE_RESULT_SCHEMA,
    BuildStageLayout,
    BuildStageFailed,
    BuildResourceEvidenceInvalid,
    ProductionBuildProcessor,
    _portable_child_receipt,
    _qlib_dump_operations,
    _validate_supervised_resource_receipt,
)
from backend.services.dataset_release.artifact_ready_source import (
    ARTIFACT_READY_RECHECK_SCHEMA,
)
from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.candidate_consumer_smoke import (
    CANDIDATE_CONSUMER_SMOKE_SCHEMA,
    HMM_INDEX_H5_READER_CONTRACT,
    QE_DAILY_FIELDS,
    QE_INDEX_FIELDS,
    QE_MINUTE_FIELDS,
    QE_QLIB_READER_CONTRACT,
)
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.component_artifact_manifest import (
    seal_component_artifact_manifest,
)
from backend.services.dataset_release.component_manifest_producer import (
    snapshot_candidate_artifacts,
)
from backend.services.dataset_release.contracts import (
    CandidateIdentity,
    Component,
    OperationKind,
    PitProvenanceState,
    ProducerProvenanceState,
    ReleaseIdentity,
    RunGenerationIdentity,
    Scope,
    SourceProbeIdentity,
    SourceProbeSubjectKind,
    build_operation_target,
    new_build_probe_subject,
)
from backend.services.dataset_release.control_store import (
    ControlStore,
    build_candidate_registration_id,
    volume_identity,
)
from backend.services.dataset_release.daily_minute_materializer import QlibDumpToolchain
from backend.services.dataset_release.lease import LeaseManager
from backend.services.dataset_release.publisher import artifact_tree_digest
from backend.services.dataset_release.resolution import (
    BUILD_INPUTS_SCHEMA_VERSION,
    RESOLUTION_PLAN_SCHEMA_VERSION,
    SOURCE_PROBE_SCHEMA_VERSION,
)
from backend.services.dataset_release.state_machine import (
    DatasetReleaseStateMachine,
    IntentSpec,
)
from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS
from backend.services.dataset_release.worker import WORKER_ERROR_RECEIPT_SCHEMA


ZERO_STAGE_SAFETY = {
    "database_writes": 0,
    "provider_database_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
}
ZERO_PROBE_SAFETY = {
    "database_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
}


@dataclass
class FakeStageCommands:
    def windows_command(
        self,
        *,
        stage: str,
        run_id: str,
        attempt_id: str,
        attempt_fence: int,
        pressure_rung: int,
        stage_timeout_seconds: int,
        plan_ref: str,
        layout: BuildStageLayout,
        result_path: Path,
        prerequisite_refs,
    ):
        return [
            "fixture-stage",
            stage,
            str(result_path),
            str(pressure_rung),
            str(stage_timeout_seconds),
        ]


def _qlib_toolchain() -> QlibDumpToolchain:
    fixture_file = Path(__file__).resolve()
    digest = hashlib.sha256(fixture_file.read_bytes()).hexdigest()
    return QlibDumpToolchain(
        distro="Ubuntu",
        conda_sh="/opt/conda/etc/profile.d/conda.sh",
        conda_env="qlib",
        dump_script_wsl="/opt/qlib/dump_bin.py",
        dump_script_windows=fixture_file,
        dump_script_sha256=digest,
        guardian_python="python3",
        guardian_script_wsl="/opt/aistock/wsl_resource_guardian.py",
        guardian_script_windows=fixture_file,
        guardian_script_sha256=digest,
        heartbeat_path_wsl="/dynamic/attempt-heartbeat.json",
        runner_python_wsl="python3",
        runner_script_wsl="/opt/aistock/subprocess_runner.py",
        runner_script_windows=fixture_file,
        runner_script_sha256=digest,
    )


def test_qlib_external_writer_cannot_target_final_or_cow_tree(tmp_path) -> None:
    staging = tmp_path / ".staging" / "release"
    final_qlib = staging / "daily_bin" / "qlib"
    final_qlib.mkdir(parents=True)
    baseline = final_qlib / "baseline.bin"
    baseline.write_bytes(b"sealed-baseline")
    private_csv = staging / "daily_bin" / ".writer-private" / "daily" / "csv"
    private_csv.mkdir(parents=True)
    batch_body = {
        "schema_version": "dataset_release_qlib_batched_dump_manifest_v1",
        "dataset": "daily_bin",
        "freq": "day",
        "fields": list(QLIB_STOCK_FIELDS),
        "max_codes_per_batch": 20,
        "per_batch_timeout_seconds": 1800,
        "resource_checkpoint_identity": {
            "attempt_id": "dsa_fixture",
            "fence": 1,
            "execution_id": "build-dump-daily",
        },
        "phases": [{"fixture": True}],
        "expected_total_code_writes": 1,
        "expected_total_rows": 1,
    }
    batch_identity = hashlib.sha256(
        json.dumps(
            batch_body,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()
    batch_path = private_csv / "batch_manifest.json"
    batch_path.write_text(
        json.dumps(
            {**batch_body, "manifest_identity": batch_identity},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    batch_sha = hashlib.sha256(batch_path.read_bytes()).hexdigest()
    layout = BuildStageLayout(
        release_id="release",
        release_digest="1" * 64,
        staging_relative_path=".staging/release",
        final_relative_path="release",
        staging_path=staging,
        final_path=tmp_path / "release",
    )
    unsafe = {
        "operation_id": "daily",
        "dataset": "daily_bin",
        "mode": "batched_patch",
        "component_action": "INCREMENTAL",
        "csv_relative_path": "daily_bin/.writer-private/daily/csv",
        "qlib_relative_path": "daily_bin/qlib",
        "writer_targets_digest": "2" * 64,
        "batch_manifest_identity": batch_identity,
        "batch_manifest_sha256": batch_sha,
    }
    with pytest.raises(BuildStageFailed, match="not isolated"):
        _qlib_dump_operations(
            {"qlib_dump_operations": [unsafe]},
            layout=layout,
            attempt_id="dsa_fixture",
            attempt_fence=1,
            expected_max_codes_per_batch=20,
        )

    private_qlib = staging / "daily_bin" / ".writer-private" / "daily" / "qlib"
    private_qlib.mkdir()
    (private_qlib / "baseline.bin").write_bytes(b"private-copy")
    safe = {
        **unsafe,
        "qlib_relative_path": "daily_bin/.writer-private/daily/qlib",
        "writer_targets_digest": digest_named_fields(
            "dataset_release_qlib_dump_writer_targets_v1",
            {
                "dataset": "daily_bin",
                "mode": "batched_patch",
                "target": "daily_bin/.writer-private/daily/qlib",
            },
        ),
    }
    assert _qlib_dump_operations(
        {"qlib_dump_operations": [safe]},
        layout=layout,
        attempt_id="dsa_fixture",
        attempt_fence=1,
        expected_max_codes_per_batch=20,
    ) == (safe,)
    with pytest.raises(BuildStageFailed, match="manifest identity differs"):
        _qlib_dump_operations(
            {"qlib_dump_operations": [safe]},
            layout=layout,
            attempt_id="dsa_fixture",
            attempt_fence=1,
            expected_max_codes_per_batch=10,
        )
    selective = {**safe, "component_action": "SELECTIVE_REBUILD"}
    assert _qlib_dump_operations(
        {"qlib_dump_operations": [selective]},
        layout=layout,
        attempt_id="dsa_fixture",
        attempt_fence=1,
        expected_max_codes_per_batch=20,
    ) == (selective,)
    full = {
        **safe,
        "mode": "batched_full",
        "component_action": "FULL_REBUILD",
        "writer_targets_digest": digest_named_fields(
            "dataset_release_qlib_dump_writer_targets_v1",
            {
                "dataset": "daily_bin",
                "mode": "batched_full",
                "target": "daily_bin/.writer-private/daily/qlib",
            },
        ),
    }
    assert _qlib_dump_operations(
        {"qlib_dump_operations": [full]},
        layout=layout,
        attempt_id="dsa_fixture",
        attempt_fence=1,
        expected_max_codes_per_batch=20,
    ) == (full,)
    with pytest.raises(BuildStageFailed, match="action/mode differs"):
        _qlib_dump_operations(
            {"qlib_dump_operations": [{**full, "component_action": "INCREMENTAL"}]},
            layout=layout,
            attempt_id="dsa_fixture",
            attempt_fence=1,
            expected_max_codes_per_batch=20,
        )
    with pytest.raises(BuildStageFailed, match="manifest identity differs"):
        _qlib_dump_operations(
            {"qlib_dump_operations": [{**selective, "batch_manifest_sha256": "f" * 64}]},
            layout=layout,
            attempt_id="dsa_fixture",
            attempt_fence=1,
            expected_max_codes_per_batch=20,
        )
    (private_qlib / "baseline.bin").write_bytes(b"malicious-writer-change")
    (private_qlib / "extra.bin").write_bytes(b"scratch")
    assert baseline.read_bytes() == b"sealed-baseline"


class FakeContext:
    def __init__(self, *, store, cas, profile, claim, run_id, layout_release):
        self.store = store
        self.cas = cas
        self.profile = profile
        self.claim = claim
        self.target_id = run_id
        self.layout_release = layout_release
        self.calls = []
        self.pressure_rung = 0
        self.supervised_commands = []
        self.timeout_calls = []
        self.dump_operations = [
            {
                "operation_id": dataset.removesuffix("_bin"),
                "dataset": dataset,
                "mode": "batched_full",
                "component_action": "FULL_REBUILD",
                "csv_relative_path": (f"{dataset}/.writer-private/{dataset.removesuffix('_bin')}/csv"),
                "qlib_relative_path": (f"{dataset}/.writer-private/{dataset.removesuffix('_bin')}/qlib"),
                "writer_targets_digest": digest_named_fields(
                    "dataset_release_qlib_dump_writer_targets_v1",
                    {
                        "dataset": dataset,
                        "mode": "batched_full",
                        "target": (f"{dataset}/.writer-private/{dataset.removesuffix('_bin')}/qlib"),
                    },
                ),
                "batch_manifest_identity": "pending",
                "batch_manifest_sha256": "pending",
            }
            for dataset in ("daily_bin", "minute_bin")
        ]

    @property
    def tokens(self):
        return (self.claim.host, self.claim.release)

    @property
    def supervised_heartbeat_path(self):
        root = self.store.root / "heartbeats"
        root.mkdir(exist_ok=True)
        return root / f"{self.claim.attempt_id}-{self.claim.attempt_fence}.json"

    def checkpoint(self):
        return None

    def run_supervised(
        self,
        command,
        *,
        execution_id,
        cwd,
        env=None,
        runtime="windows",
        timeout_seconds=None,
        cooperative_grace_seconds=30.0,
        wsl=None,
    ):
        self.calls.append((execution_id, runtime))
        self.supervised_commands.append(tuple(command))
        self.timeout_calls.append((execution_id, timeout_seconds))
        if runtime == "wsl":
            if execution_id == "build-consumer-smoke":
                result_path = (
                    self.store.root
                    / "attempt_runs"
                    / f"{self.claim.attempt_id}-{self.claim.attempt_fence}"
                    / execution_id
                    / "semantic_result.json"
                )
                result_path.parent.mkdir(parents=True, exist_ok=True)
                self.consumer_smoke = self._consumer_smoke()
                result_path.write_text(
                    json.dumps(self.consumer_smoke, sort_keys=True, separators=(",", ":")),
                    encoding="utf-8",
                )
                return self._child(execution_id, runtime="wsl")
            dataset = "daily_bin" if execution_id.endswith("daily") else "minute_bin"
            staging = self._staging()
            operation = next(item for item in self.dump_operations if item["dataset"] == dataset)
            working = staging / operation["qlib_relative_path"]
            working.mkdir(parents=True, exist_ok=True)
            (working / "fixture.bin").write_bytes(dataset.encode("ascii"))
            return self._child(execution_id, runtime="wsl")

        stage = command[1]
        result_path = Path(command[2])
        result_path.parent.mkdir(parents=True, exist_ok=True)
        staging = self._staging()
        if stage == "prepare":
            staging.mkdir(parents=True)
            for dataset in ("daily_bin", "minute_bin"):
                operation = next(
                    (item for item in self.dump_operations if item["dataset"] == dataset),
                    None,
                )
                csv_root = (
                    staging / operation["csv_relative_path"] if operation is not None else staging / dataset / "csv"
                )
                csv_root.mkdir(parents=True)
                if operation is None:
                    continue
                batch_root = csv_root / "full" / "batch-0000-stock"
                batch_root.mkdir(parents=True)
                csv_path = batch_root / "000001.sz.csv"
                csv_path.write_text(
                    "date,symbol,open,high,low,close,volume,amount,factor,"
                    "up_limit_price,down_limit_price,prev_close,limit_up,limit_down\n"
                    "2026-07-31,000001.SZ,10,10,10,10,1,10,1,11,9,10,0,0\n",
                    encoding="utf-8",
                )
                body = {
                    "schema_version": "dataset_release_qlib_batched_dump_manifest_v1",
                    "dataset": dataset,
                    "freq": "day" if dataset == "daily_bin" else "1min",
                    "fields": list(QLIB_STOCK_FIELDS),
                    "max_codes_per_batch": self.profile.pressure_ladder["minute_batch"][self.pressure_rung],
                    "per_batch_timeout_seconds": 1800,
                    "resource_checkpoint_identity": {
                        "attempt_id": self.claim.attempt_id,
                        "fence": self.claim.attempt_fence,
                        "execution_id": f"build-dump-{dataset.removesuffix('_bin')}",
                    },
                    "phases": [
                        {
                            "phase_id": "full",
                            "kind": "full",
                            "batches": [
                                {
                                    "ordinal": 0,
                                    "role": "stock",
                                    "mode": "dump_fix",
                                    "relative_path": "full/batch-0000-stock",
                                    "files": [
                                        {
                                            "code": "000001.SZ",
                                            "role": "stock",
                                            "relative_path": csv_path.name,
                                            "rows": 1,
                                            "sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
                                            "start": "2026-07-31",
                                            "end": "2026-07-31",
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "expected_total_code_writes": 1,
                    "expected_total_rows": 1,
                }
                identity = hashlib.sha256(
                    json.dumps(
                        body,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode("utf-8")
                ).hexdigest()
                manifest_path = csv_root / "batch_manifest.json"
                manifest_path.write_text(
                    json.dumps(
                        {**body, "manifest_identity": identity},
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n",
                    encoding="utf-8",
                )
                operation["batch_manifest_identity"] = identity
                operation["batch_manifest_sha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
                qlib_root = staging / operation["qlib_relative_path"]
                qlib_root.mkdir(parents=True)
            (staging / "factor_bundle").mkdir()
            (staging / "factor_bundle" / "fixture.h5").write_bytes(b"factor")
            (staging / "index_context").mkdir()
            (staging / "index_context" / "index_daily.h5").write_bytes(b"index")
        elif stage == "finalize-bins":
            for dataset in ("daily_bin", "minute_bin"):
                operation = next(
                    (item for item in self.dump_operations if item["dataset"] == dataset),
                    None,
                )
                working = (
                    staging / operation["qlib_relative_path"]
                    if operation is not None
                    else staging / dataset / ".writer-private" / "none" / "qlib"
                )
                if working.exists():
                    working.rename(staging / dataset / "qlib")
        elif stage == "validate":
            (staging / "validation_complete.json").write_text('{"status":"PASS"}\n', encoding="utf-8")
        payload = self._stage_payload(stage)
        result_path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        return self._child(execution_id, runtime="windows")

    def run_source_recheck_supervised(
        self,
        command,
        *,
        execution_id,
        cwd,
        timeout_seconds,
        cooperative_grace_seconds=30.0,
    ):
        del cwd, cooperative_grace_seconds
        self.calls.append((execution_id, "windows"))
        self.supervised_commands.append(tuple(command))
        self.timeout_calls.append((execution_id, timeout_seconds))
        values = {command[index]: command[index + 1] for index in range(2, len(command), 2)}
        contract_ref = self.cas.verify(values["--artifact-ready-contract-ref"])
        now = datetime.now(UTC) - timedelta(seconds=1)
        roots = {
            "daily_bin": "1" * 64,
            "minute_bin": "2" * 64,
            "factor_h5_static": "3" * 64,
            "domestic_index_context": "4" * 64,
        }
        key = digest_named_fields(
            ARTIFACT_READY_RECHECK_SCHEMA,
            {
                "artifact_ready_contract_ref": contract_ref.sha256,
                "artifact_ready_content_root": self.source_content_root,
                "initial_source_content_root": "5" * 64,
                "fresh_source_content_root": "6" * 64,
                "pit_snapshot_digest": self.pit_snapshot_digest,
                "effective_component_roots": roots,
                "execution_id": execution_id,
                "run_id": self.target_id,
                "attempt_id": self.claim.attempt_id,
                "attempt_fence": self.claim.attempt_fence,
                "observed_at": now,
            },
        )
        probe_ref = self.cas.put_json(
            {
                "schema_version": ARTIFACT_READY_RECHECK_SCHEMA,
                "profile": self.profile.profile,
                "artifact_ready_contract_ref": contract_ref.as_dict(),
                "artifact_ready_content_root": self.source_content_root,
                "initial_source_content_root": "5" * 64,
                "fresh_source_content_root": "6" * 64,
                "raw_source_changed": True,
                "pit_snapshot_digest": self.pit_snapshot_digest,
                "effective_component_roots": roots,
                "initial_component_provenance_roots": roots,
                "fresh_component_provenance_roots": roots,
                "fresh_artifact_ready_contract_ref": contract_ref.as_dict(),
                "observed_at": now.isoformat().replace("+00:00", "Z"),
                "valid_until": (now + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
                "execution_id": execution_id,
                "run_id": self.target_id,
                "attempt_id": self.claim.attempt_id,
                "attempt_fence": self.claim.attempt_fence,
                "source_probe_key": key,
                "status": "PASS",
                "freshness_authority": ("fresh_db_readback_plus_immutable_provider_overlay_v1"),
                "provider_recheck_policy": "no_provider_refetch_v1",
                "safety": {
                    **ZERO_STAGE_SAFETY,
                    "candidate_writes": 0,
                },
            }
        )
        result_path = Path(values["--result-path"])
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(
            json.dumps(
                {
                    "schema_version": "dataset_release_source_recheck_result_v1",
                    "status": "PASS",
                    "run_id": self.target_id,
                    "attempt_id": self.claim.attempt_id,
                    "attempt_fence": self.claim.attempt_fence,
                    "execution_id": execution_id,
                    "artifact_ready_contract_ref": contract_ref.as_dict(),
                    "artifact_ready_content_root": self.source_content_root,
                    "fresh_raw_source_content_root": "6" * 64,
                    "source_probe_key": key,
                    "source_probe_ref": probe_ref.as_dict(),
                    "stage_timeout_seconds": int(timeout_seconds),
                    "safety": {
                        **ZERO_STAGE_SAFETY,
                        "candidate_writes": 0,
                    },
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        return self._child(execution_id, runtime="windows")

    def _consumer_smoke(self):
        cutoff = "2026-07-31"

        def feature(fields, instruments, timestamps):
            keys = sorted(f"{instrument}|{timestamp}" for instrument in instruments for timestamp in timestamps)
            return {
                "rows": len(keys),
                "fields": list(fields),
                "start": cutoff,
                "end": cutoff,
                "finite_values": len(keys) * len(fields),
                "max_abs_value": 1.0,
                "unique_keys": len(keys),
                "instruments": sorted(instruments),
                "first_timestamp": min(timestamps),
                "last_timestamp": max(timestamps),
                "key_digest": hashlib.sha256(json.dumps(keys, separators=(",", ":")).encode("utf-8")).hexdigest(),
            }

        morning = [f"{cutoff} 09:{minute:02d}:00" for minute in range(31, 60)]
        morning += [f"{cutoff} 10:{minute:02d}:00" for minute in range(60)]
        morning += [f"{cutoff} 11:{minute:02d}:00" for minute in range(31)]
        afternoon = [f"{cutoff} 13:{minute:02d}:00" for minute in range(1, 60)]
        afternoon += [f"{cutoff} 14:{minute:02d}:00" for minute in range(60)]
        afternoon += [f"{cutoff} 15:00:00"]
        minute_timestamps = tuple(morning + afternoon)

        return {
            "schema_version": CANDIDATE_CONSUMER_SMOKE_SCHEMA,
            "status": "PASS",
            "execution_kind": "production_supervised_wsl",
            "profile": self.profile.profile,
            "cutoff": cutoff,
            "stage_timeout_seconds": self.profile.stage_timeouts_seconds["consumer"],
            "identity": {
                "run_id": self.target_id,
                "attempt_id": self.claim.attempt_id,
                "attempt_fence": self.claim.attempt_fence,
                "release_id": self.layout_release.name,
                "release_digest": self.release_digest,
                "staging_relative_path": self._staging().relative_to(self.layout_release.parent).as_posix(),
            },
            "qe": {
                "status": "PASS",
                "reader_contract": QE_QLIB_READER_CONTRACT,
                "qlib_init_provider_frequencies": ["1min", "day"],
                "stock_instrument": "000001.SZ",
                "daily": feature(QE_DAILY_FIELDS, ("000001.SZ",), (cutoff,)),
                "minute": feature(
                    QE_MINUTE_FIELDS,
                    ("000001.SZ",),
                    minute_timestamps,
                ),
                "indices": {
                    **feature(QE_INDEX_FIELDS, self.profile.index_codes, (cutoff,)),
                    "codes": list(self.profile.index_codes),
                },
                "benchmark": {
                    **feature(
                        ("$close/Ref($close,1)-1",),
                        ("000300.SH",),
                        (cutoff,),
                    ),
                    "code": "000300.SH",
                },
            },
            "hmm_index_contract": {
                "status": "PASS",
                "reader_contract": HMM_INDEX_H5_READER_CONTRACT,
                "schema_version": "qe_index_context_v1",
                "universe_version": "qe_hmm_domestic_core_v1",
                "benchmark": "000300.SH",
                "fields": ["idx_close_point", "idx_return_1d"],
                "rows": 2,
                "cutoff_rows": 1,
                "cutoff": cutoff,
                "existing_hmm_consumer_activation": "not_activated_not_switched",
            },
            "consumer_activation": {
                "qe_candidate": "validated_not_activated",
                "existing_hmm": "not_activated_not_switched",
            },
            "safety": ZERO_STAGE_SAFETY,
        }

    def _staging(self):
        return self.layout_release.parent / ".staging" / self.claim.attempt_id / str(self.claim.attempt_fence)

    def _stage_payload(self, stage):
        run = self.store.get_run(self.target_id)
        assert run is not None
        payload = {
            "schema_version": BUILD_STAGE_RESULT_SCHEMA,
            "stage": stage,
            "status": "PASS",
            "run_id": self.target_id,
            "attempt_id": self.claim.attempt_id,
            "attempt_fence": self.claim.attempt_fence,
            "release_id": self.layout_release.name,
            "release_digest": self.release_digest,
            "staging_relative_path": self._staging().relative_to(self.layout_release.parent).as_posix(),
            "stage_timeout_seconds": self.profile.stage_timeouts_seconds["full_build"],
            "resource_receipt": self._resource(stage),
            "safety": ZERO_STAGE_SAFETY,
        }
        if stage == "prepare":
            payload["consumer_smoke_instrument"] = "000001.SZ"
            payload["qlib_dump_operations"] = list(self.dump_operations)
        if stage == "validate":
            root = self._staging()
            artifact_root = artifact_tree_digest(root)
            artifact_snapshot = snapshot_candidate_artifacts(root)
            assert artifact_snapshot.artifact_root == artifact_root
            candidate_identity = CandidateIdentity(
                registration_uuid=build_candidate_registration_id(self.release_digest),
                allowlisted_root_id=self.profile.candidate_root_id,
                volume_serial=volume_identity(self.layout_release.parent),
                root_relative_path=self.layout_release.name,
                profile=self.profile.profile,
                scope=Scope.FULL,
                cutoff=date(2026, 7, 31),
                lineage_anchor=f"BUILD_RELEASE_DIGEST:{self.release_digest}",
                pit_provenance_state=PitProvenanceState.KNOWN,
                pit_provenance_digest_or_sentinel=self.pit_snapshot_digest,
                artifact_root=artifact_root,
                producer_provenance_state=ProducerProvenanceState.KNOWN,
                producer_provenance_digest_or_sentinel=self.producer_fingerprint,
            ).key
            component_ref = seal_component_artifact_manifest(
                self.cas,
                {
                    "profile": self.profile.profile,
                    "scope": "full",
                    "cutoff": "2026-07-31",
                    "candidate_identity": candidate_identity,
                    "artifact_root": artifact_root,
                    "semantic_profile_digest": self.profile.semantic_profile_digest,
                    "producer_fingerprint": self.producer_fingerprint,
                    "artifact_fingerprint": self.artifact_fingerprint,
                    "validation_fingerprint": self.validation_fingerprint,
                    "source_content_root": self.source_content_root,
                    "artifact_ready_content_root": self.source_content_root,
                    "pit_snapshot_digest": self.pit_snapshot_digest,
                    "components": {
                        component.value: {
                            "status": "UNAVAILABLE",
                            "reason_code": "FIXTURE_COMPONENT_MANIFEST_UNAVAILABLE",
                        }
                        for component in Component
                    },
                },
            )
            manifest_root = self.cas.get_json(component_ref)["manifest_root"]
            validation_ref = self.cas.put_json(
                {
                    "status": "PASS",
                    "validation_fingerprint": self.validation_fingerprint,
                    "evidence": {
                        "qe_hmm_consumer_smoke": self.consumer_smoke,
                    },
                }
            )
            manifest_ref = self.cas.put_json(
                {
                    "artifact_root": artifact_root,
                    "manifest_root": manifest_root,
                    "component_artifact_manifest_ref": component_ref.as_dict(),
                }
            )
            probe_ref = self._probe()
            payload.update(
                {
                    "validation_status": "PASS",
                    "required_validation_failures": 0,
                    "validation_ref": validation_ref.as_dict(),
                    "manifest_ref": manifest_ref.as_dict(),
                    "artifact_root": artifact_root,
                    "artifact_snapshot": artifact_snapshot.receipt(),
                    "manifest_root": manifest_root,
                    "component_artifact_manifest_ref": component_ref.as_dict(),
                    "artifact_ready_content_root": self.source_content_root,
                    "producer_provenance_digest": self.producer_fingerprint,
                    "source_probe_ref": probe_ref.as_dict(),
                    "runtime_real_data_evidence": "not_run_not_authorized",
                }
            )
        return payload

    def _probe(self):
        now = datetime.now(UTC) - timedelta(seconds=1)
        valid = now + timedelta(hours=1)
        subject = new_build_probe_subject(self.logical_request_key)
        body = {
            "schema_version": SOURCE_PROBE_SCHEMA_VERSION,
            "probe_policy_version": "fixture-fresh-probe-v1",
            "subject_kind": SourceProbeSubjectKind.NEW_BUILD.value,
            "subject_identity": subject,
            "candidate_identity": None,
            "artifact_root": None,
            "logical_request_key": self.logical_request_key,
            "source_content_root": self.source_content_root,
            "source_provenance_root": "7" * 64,
            "pit_snapshot_digest": self.pit_snapshot_digest,
            "snapshot_tokens": ["fixture:repeatable-read:2"],
            "probe_ordinal": 2,
            "observed_at": now.isoformat().replace("+00:00", "Z"),
            "valid_until": valid.isoformat().replace("+00:00", "Z"),
        }
        receipt_digest = digest_named_fields(SOURCE_PROBE_SCHEMA_VERSION, body)
        key = SourceProbeIdentity(
            logical_request_key=self.logical_request_key,
            candidate_identity=None,
            artifact_root=None,
            source_content_root=self.source_content_root,
            source_provenance_root="7" * 64,
            pit_digest=self.pit_snapshot_digest,
            probe_policy_version="fixture-fresh-probe-v1",
            probe_receipt_digest=receipt_digest,
            subject_kind=SourceProbeSubjectKind.NEW_BUILD,
            subject_identity=subject,
        ).key
        return self.cas.put_json(
            {
                **body,
                "receipt_digest": receipt_digest,
                "source_probe_key": key,
                "safety": ZERO_PROBE_SAFETY,
            }
        )

    def _resource(self, stage):
        ladder = self.profile.pressure_ladder
        rung = self.pressure_rung
        return {
            "schema_version": BUILD_RESOURCE_RECEIPT_SCHEMA,
            "policy_digest": self.profile.resource_policy_digest,
            "stage": stage,
            "admission_checked": True,
            "all_chunks_checked": True,
            "chunks_completed": 1,
            "peak_owned_private_commit_bytes": 1024,
            "checkpoints": [
                {
                    "sequence": 0,
                    "kind": "admission",
                    "decision": "READY",
                    "pressure_rung": rung,
                    "host_available_bytes": 32 * 1024**3,
                    "owned_private_commit_bytes": 0,
                },
                {
                    "sequence": 1,
                    "kind": "chunk",
                    "chunk_id": f"{stage}:fixture-1",
                    "decision": "READY",
                    "pressure_rung": rung,
                    "host_available_bytes": 32 * 1024**3,
                    "owned_private_commit_bytes": 1024,
                },
                {
                    "sequence": 2,
                    "kind": "final",
                    "decision": "READY",
                    "pressure_rung": rung,
                    "host_available_bytes": 32 * 1024**3,
                    "owned_private_commit_bytes": 1024,
                },
            ],
            "effective_rung": {
                "index": rung,
                "h5_batch": ladder["h5_batch"][rung],
                "minute_batch": ladder["minute_batch"][rung],
                "chunk_months": ladder["date_chunk_months"][rung],
                "row_group_rows": ladder["row_group_rows"][rung],
                "dump_workers": ladder["dump_workers"][rung],
            },
            "memory_control_semantics": {
                "factor_h5": "bounded_date_slice_plus_row_group_rows_v1",
                "h5_batch": "reserved_profile_telemetry_not_consumed_v1",
                "minute_batch": "child_manifest_plus_parent_bound_v1",
            },
        }

    def _child(self, execution_id, *, runtime):
        rung = self.pressure_rung
        ladder = self.profile.pressure_ladder
        timeout = self.profile.stage_timeouts_seconds[
            "consumer"
            if execution_id == "build-consumer-smoke"
            else "source_freeze"
            if execution_id == "prepublish-source-recheck"
            else "qlib_dump"
            if runtime == "wsl"
            else "full_build"
        ]
        return {
            "schema_version": "dataset_supervised_execution_receipt_v1",
            "execution_id": execution_id,
            "runtime": runtime,
            "command_sha256": "6" * 64,
            "wrapper_pid": 10,
            "child_pid": 11,
            "returncode": 0,
            "elapsed_seconds": 0.1,
            "cooperative_reason": None,
            "log_segments": [],
            "log_total_bytes": 0,
            "segment_limit_bytes": 16 * 1024**2,
            "cancellation_requested": False,
            "timeout_seconds": float(timeout),
            "job_current_commit_bytes": 1024,
            "job_peak_commit_bytes": 2048,
            "active_processes": 0,
            "wsl_readback": ({"active_state": "inactive"} if runtime == "wsl" else None),
            "resource_gate_receipt": {
                "schema_version": "dataset_release_resource_gate_receipt_v1",
                "sample_count": 3,
                "retained_sample_count": 3,
                "final_status": "READY",
                "checkpoint_requested": False,
                "pressure_rung": rung,
                "next_pressure_rung": rung,
                "pressure_settings": {
                    "h5_batch": ladder["h5_batch"][rung],
                    "minute_batch": ladder["minute_batch"][rung],
                    "chunk_months": ladder["date_chunk_months"][rung],
                    "row_group_rows": ladder["row_group_rows"][rung],
                    "dump_workers": ladder["dump_workers"][rung],
                },
                "wsl_required": runtime == "wsl",
                "aggregate_owned_peak_commit_bytes": 2048,
                "data_scope_changed": False,
            },
            "result_path": "attempt/result.json",
            "log_root": "attempt/logs",
        }


def _setup(tmp_path, dataset_profile):
    candidate_root = tmp_path / "candidates"
    candidate_root.mkdir()
    store = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(store.root)
    profile = replace(
        dataset_profile,
        candidate_root=PureWindowsPath(str(candidate_root)),
        control_root=PureWindowsPath(str(store.root)),
    )
    logical = "1" * 64
    source = "2" * 64
    provenance = "3" * 64
    pit = "4" * 64
    resolved = digest_named_fields(
        "dataset_release_resolved_intent_v1",
        {
            "logical_request_key": logical,
            "source_content_root": source,
            "frozen_pit_spans_digest": pit,
        },
    )
    producer = "a" * 64
    artifact = "b" * 64
    validation = "c" * 64
    action = "d" * 64
    source_ref = cas.put_json({"fixture": "source"})
    pit_ref = cas.put_json({"fixture": "pit"})
    artifact_ready_ref = cas.put_json({"fixture": "artifact-ready-contract"})
    build_inputs = {
        "schema_version": BUILD_INPUTS_SCHEMA_VERSION,
        "profile": profile.profile,
        "scope": "full",
        "cutoff": "2026-07-31",
        "logical_request_key": logical,
        "resolved_intent_key": resolved,
        "semantic_profile_digest": profile.semantic_profile_digest,
        "predicted_new_bytes": 128 * 1024**3,
        "source_manifest_ref": source_ref.as_dict(),
        "pit_snapshot_ref": pit_ref.as_dict(),
        "artifact_ready_contract_ref": artifact_ready_ref.as_dict(),
        "artifact_ready_content_root": source,
        "artifact_ready_provenance_root": "e" * 64,
        "provider_receipt_refs": [],
        "artifact_ready_derived_source_receipt_refs": [],
        "source_snapshot": {
            "source_content_root": source,
            "pit_snapshot_digest": pit,
        },
        "source_probe": {
            "subject_kind": "NEW_BUILD",
            "subject_identity": new_build_probe_subject(logical),
            "candidate_identity": None,
            "artifact_root": None,
        },
        "partitions": [{"fixture": True}],
        "baseline": {},
        "fingerprints": {
            "producer_fingerprint": producer,
            "artifact_fingerprint": artifact,
            "validation_fingerprint": validation,
            "sample_policy": "full_required_rows_no_sampling",
            "decision_schema": "dataset_release_decision_v1",
        },
        "safety": {
            **ZERO_STAGE_SAFETY,
            "candidate_writes": 0,
        },
    }
    target = build_operation_target(resolved, action)
    generation = RunGenerationIdentity(
        operation_kind=OperationKind.BUILD,
        decision_schema="dataset_release_decision_v1",
        producer_fingerprint=producer,
        artifact_fingerprint=artifact,
        validation_identity=validation,
        sample_policy="full_required_rows_no_sampling",
        operation_target=target,
    ).digest
    plan_ref = cas.put_json(
        {
            "schema_version": RESOLUTION_PLAN_SCHEMA_VERSION,
            "resolved_intent_key": resolved,
            "source_content_root": source,
            "source_provenance_root": None,
            "pit_snapshot_digest": pit,
            "action_plan_digest": action,
            "operation_kind": "BUILD",
            "attestation_target_key": None,
            "attestation_observation_key": None,
            "source_probe_subject_kind": "NEW_BUILD",
            "source_probe_subject_identity": new_build_probe_subject(logical),
            "source_probe_key": None,
            "source_probe_ref": None,
            "source_probe_cas_ref": None,
            "build_inputs": build_inputs,
            "actions": [],
            "safety": ZERO_PROBE_SAFETY,
        }
    )
    run = DatasetReleaseStateMachine(store).create_queued_run(
        intent=IntentSpec(
            logical_request_key=logical,
            resolved_intent_key=resolved,
            source_content_root=source,
            source_provenance_root=provenance,
            pit_snapshot_digest=pit,
        ),
        run_generation_digest=generation,
        operation_kind="BUILD",
        plan_ref=plan_ref.sha256,
    )
    release = ReleaseIdentity(
        resolved_intent_key=resolved,
        frozen_pit_spans_digest=pit,
        scope=Scope.FULL,
        producer_fingerprint=producer,
        artifact_fingerprint=artifact,
        cutoff=date(2026, 7, 31),
        profile=profile.profile,
    )
    claim = LeaseManager(store).claim_build(
        run_id=run["run_id"],
        release_id=release.release_id,
        owner_identity="fixture-worker",
        ttl_seconds=300,
        hybrid_wsl=True,
        staging_ref=None,
    )
    final = candidate_root / release.release_id
    context = FakeContext(
        store=store,
        cas=cas,
        profile=profile,
        claim=claim,
        run_id=run["run_id"],
        layout_release=final,
    )
    context.release_digest = release.digest
    context.logical_request_key = logical
    context.source_content_root = source
    context.pit_snapshot_digest = pit
    context.producer_fingerprint = producer
    context.artifact_fingerprint = artifact
    context.validation_fingerprint = validation
    return profile, store, cas, run, release, context, candidate_root


def test_production_build_processor_supervises_every_data_stage_and_publishes_atomically(
    tmp_path, dataset_profile
) -> None:
    profile, store, cas, run, release, context, candidate_root = _setup(tmp_path, dataset_profile)
    processor = ProductionBuildProcessor(
        profile=profile,
        profile_path=Path("configs/datasets/qe_backtest_monthly_v1.yaml"),
        project_root=Path.cwd(),
        store=store,
        cas=cas,
        candidate_root=candidate_root,
        qlib_toolchain=_qlib_toolchain(),
        stage_commands=FakeStageCommands(),
    )

    resource = processor.resource_spec(run)
    assert resource.predicted_new_bytes == 128 * 1024**3
    result = processor.process(context)

    assert resource.release_id == release.release_id
    assert resource.staging_ref is None
    assert result.disposition.value == "DURABLE_SUCCESS"
    assert context.calls == [
        ("build-prepare", "windows"),
        ("build-dump-daily", "wsl"),
        ("build-dump-minute", "wsl"),
        ("build-finalize-bins", "windows"),
        ("build-consumer-smoke", "wsl"),
        ("build-validate", "windows"),
        ("prepublish-source-recheck", "windows"),
    ]
    durable = store.get_run(run["run_id"])
    assert durable is not None and durable["state"] == "SUCCEEDED"
    registration = store.latest_candidate_registration(profile=profile.profile, scope="full")
    assert registration is not None
    assert registration["state"] == "RELEASED"
    assert registration["lineage_anchor"] == f"BUILD_RELEASE_DIGEST:{release.digest}"
    publish_record = store.get_publish_record(release.release_id)
    assert publish_record is not None
    build_receipt = cas.get_json(str(publish_record["build_receipt_ref"]))
    assert build_receipt["artifact_snapshot"]["schema_version"] == ("dataset_release_candidate_artifact_snapshot_v1")
    assert build_receipt["artifact_snapshot"]["artifact_root"] == registration["artifact_root"]
    assert build_receipt["artifact_snapshot"]["content_read_passes"] == 1
    assert (candidate_root / release.release_id / ".dataset_release_committed.json").is_file()


@pytest.mark.parametrize(
    "crash_window,relative",
    [
        (
            "qlib_child_success_before_parent_receipt",
            "daily_bin/.writer-private/daily/qlib/child-success.bin",
        ),
        ("prepare_result_before_parent_cas", "prepare-result.json"),
        ("finalize_result_before_parent_cas", "finalize-result.json"),
    ],
)
def test_successor_attempt_uses_new_fenced_staging_and_never_adopts_crash_bytes(
    tmp_path,
    dataset_profile,
    crash_window,
    relative,
) -> None:
    profile, store, cas, run, release, first, candidate_root = _setup(tmp_path, dataset_profile)
    processor = ProductionBuildProcessor(
        profile=profile,
        profile_path=Path("configs/datasets/qe_backtest_monthly_v1.yaml"),
        project_root=Path.cwd(),
        store=store,
        cas=cas,
        candidate_root=candidate_root,
        qlib_toolchain=_qlib_toolchain(),
        stage_commands=FakeStageCommands(),
    )
    first_layout = processor._layout(release, first)
    processor._bind_attempt_staging(first, first_layout)
    old_file = first_layout.staging_path / relative
    old_file.parent.mkdir(parents=True, exist_ok=True)
    old_bytes = f"old-attempt:{crash_window}".encode("utf-8")
    old_file.write_bytes(old_bytes)
    old_result = (
        store.root
        / "attempt_runs"
        / f"{first.claim.attempt_id}-{first.claim.attempt_fence}"
        / crash_window
        / "semantic_result.json"
    )
    old_result.parent.mkdir(parents=True, exist_ok=True)
    old_result.write_text('{"status":"child-finished-parent-not-durable"}\n', encoding="utf-8")

    active = store.get_run(run["run_id"])
    assert active is not None
    DatasetReleaseStateMachine(store).transition_owned_and_release(
        run_id=run["run_id"],
        attempt_id=first.claim.attempt_id,
        expected_state="EXECUTING",
        expected_row_version=active["row_version"],
        attempt_fence=first.claim.attempt_fence,
        tokens=first.tokens,
        next_state="WAITING_RESOURCE",
        attempt_terminal_state="RELEASED_WAITING",
    )
    with store.transaction() as connection:
        updated = connection.execute(
            """
            UPDATE runs SET state='QUEUED',row_version=row_version+1
            WHERE run_id=? AND state='WAITING_RESOURCE'
              AND active_attempt_id IS NULL
            """,
            (run["run_id"],),
        )
        assert updated.rowcount == 1
    second_claim = LeaseManager(store).claim_build(
        run_id=run["run_id"],
        release_id=release.release_id,
        owner_identity="fixture-worker-successor",
        ttl_seconds=300,
        hybrid_wsl=True,
        staging_ref=None,
    )
    second = FakeContext(
        store=store,
        cas=cas,
        profile=profile,
        claim=second_claim,
        run_id=run["run_id"],
        layout_release=candidate_root / release.release_id,
    )
    for name in (
        "release_digest",
        "logical_request_key",
        "source_content_root",
        "pit_snapshot_digest",
        "producer_fingerprint",
        "artifact_fingerprint",
        "validation_fingerprint",
    ):
        setattr(second, name, getattr(first, name))

    second_layout = processor._layout(release, second)
    assert second_layout.staging_path != first_layout.staging_path
    result = processor.process(second)

    assert result.disposition.value == "DURABLE_SUCCESS"
    assert old_file.read_bytes() == old_bytes
    assert old_result.is_file()
    first_attempt = store.get_attempt(first.claim.attempt_id)
    second_attempt = store.get_attempt(second.claim.attempt_id)
    assert first_attempt["staging_ref"] == str(first_layout.staging_path)
    assert second_attempt["staging_ref"] == str(second_layout.staging_path)
    publish = store.get_publish_record(release.release_id)
    assert publish is not None and publish["attempt_id"] == second.claim.attempt_id
    final = candidate_root / release.release_id
    assert final.is_dir() and not (final / relative).exists()


def test_build_processor_restores_monotonic_pressure_rung_from_waiting_attempt(tmp_path, dataset_profile) -> None:
    profile, store, cas, run, release, context, candidate_root = _setup(tmp_path, dataset_profile)
    error_ref = cas.put_json(
        {
            "schema_version": WORKER_ERROR_RECEIPT_SCHEMA,
            "kind": "build",
            "target_id": run["run_id"],
            "worker_instance_id": "fixture-worker",
            "capability_digest": "5" * 64,
            "disposition": "WAITING",
            "error_code": "RESOURCE_CHECKPOINT_REQUESTED",
            "retry_after_seconds": 30,
            "terminal_state": None,
            "context": {
                "reason_code": "RESOURCE_PRESSURE_LADDER",
                "pressure_rung": 1,
                "data_scope_changed": False,
            },
            "observed_at": datetime.now(UTC).isoformat(),
            "safety": ZERO_PROBE_SAFETY,
        }
    )
    active = store.get_run(run["run_id"])
    DatasetReleaseStateMachine(store).transition_owned_and_release(
        run_id=run["run_id"],
        attempt_id=context.claim.attempt_id,
        expected_state="EXECUTING",
        expected_row_version=active["row_version"],
        attempt_fence=context.claim.attempt_fence,
        tokens=context.tokens,
        next_state="WAITING_RESOURCE",
        attempt_terminal_state="RELEASED_WAITING",
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET error_ref=? WHERE attempt_id=?",
            (error_ref.sha256, context.claim.attempt_id),
        )
    processor = ProductionBuildProcessor(
        profile=profile,
        profile_path=Path("configs/datasets/qe_backtest_monthly_v1.yaml"),
        project_root=Path.cwd(),
        store=store,
        cas=cas,
        candidate_root=candidate_root,
        qlib_toolchain=_qlib_toolchain(),
        stage_commands=FakeStageCommands(),
    )

    assert processor.resource_spec(store.get_run(run["run_id"])).pressure_rung == 1


def test_build_processor_starts_no_qlib_writer_for_reused_bin_components(tmp_path, dataset_profile) -> None:
    profile, store, cas, _run, _release, context, candidate_root = _setup(tmp_path, dataset_profile)
    context.dump_operations = []
    processor = ProductionBuildProcessor(
        profile=profile,
        profile_path=Path("configs/datasets/qe_backtest_monthly_v1.yaml"),
        project_root=Path.cwd(),
        store=store,
        cas=cas,
        candidate_root=candidate_root,
        qlib_toolchain=_qlib_toolchain(),
        stage_commands=FakeStageCommands(),
    )

    processor.process(context)

    assert context.calls == [
        ("build-prepare", "windows"),
        ("build-finalize-bins", "windows"),
        ("build-consumer-smoke", "wsl"),
        ("build-validate", "windows"),
        ("prepublish-source-recheck", "windows"),
    ]
    assert all("dump_bin.py" not in " ".join(command) for command in context.supervised_commands)


def test_build_portable_child_receipt_preserves_durable_log_cas_reference() -> None:
    value = {
        "returncode": 0,
        "log_segments": [
            {
                "stream": "stdout",
                "generation": 1,
                "path": "attempt-local/stdout.log",
                "size_bytes": 3,
                "sha256": "a" * 64,
                "cas_ref": {
                    "sha256": "a" * 64,
                    "size": 3,
                    "relative_path": "cas/aa/" + "a" * 64,
                },
            }
        ],
        "result_path": "attempt-local/result.json",
        "log_root": "attempt-local",
    }

    portable = _portable_child_receipt(value)

    assert "path" not in portable["log_segments"][0]
    assert portable["log_segments"][0]["cas_ref"]["sha256"] == "a" * 64


def test_build_pressure_rung_one_reaches_stage_cli_and_reduces_dump_workers(tmp_path, dataset_profile) -> None:
    profile, store, cas, _run, _release, context, candidate_root = _setup(tmp_path, dataset_profile)
    context.pressure_rung = 1
    processor = ProductionBuildProcessor(
        profile=profile,
        profile_path=Path("configs/datasets/qe_backtest_monthly_v1.yaml"),
        project_root=Path.cwd(),
        store=store,
        cas=cas,
        candidate_root=candidate_root,
        qlib_toolchain=_qlib_toolchain(),
        stage_commands=FakeStageCommands(),
    )

    processor.process(context)

    windows_commands = [command for command in context.supervised_commands if command[0] == "fixture-stage"]
    assert windows_commands and all(command[-2] == "1" for command in windows_commands)
    assert all(int(command[-1]) == profile.stage_timeouts_seconds["full_build"] for command in windows_commands)
    dump_workers = profile.pressure_ladder["dump_workers"][1]
    dump_commands = context.supervised_commands[1:3]
    assert all(f"--max-workers {dump_workers}" in command[2] for command in dump_commands)
    assert dump_workers < profile.pressure_ladder["dump_workers"][0]
    assert dict(context.timeout_calls) == {
        "build-prepare": float(profile.stage_timeouts_seconds["full_build"]),
        "build-dump-daily": float(profile.stage_timeouts_seconds["qlib_dump"]),
        "build-dump-minute": float(profile.stage_timeouts_seconds["qlib_dump"]),
        "build-finalize-bins": float(profile.stage_timeouts_seconds["full_build"]),
        "build-consumer-smoke": float(profile.stage_timeouts_seconds["consumer"]),
        "build-validate": float(profile.stage_timeouts_seconds["full_build"]),
        "prepublish-source-recheck": float(profile.stage_timeouts_seconds["source_freeze"]),
    }


def test_build_rejects_stage_without_matching_authoritative_resource_gate(tmp_path, dataset_profile) -> None:
    profile, _store, _cas, _run, _release, context, _candidate_root = _setup(tmp_path, dataset_profile)
    receipt = context._child("build-prepare", runtime="windows")
    receipt["resource_gate_receipt"]["pressure_rung"] = 1

    with pytest.raises(BuildResourceEvidenceInvalid, match="authoritative"):
        _validate_supervised_resource_receipt(
            receipt,
            profile=profile,
            execution_id="build-prepare",
            runtime="windows",
            pressure_rung=0,
            timeout_seconds=profile.stage_timeouts_seconds["full_build"],
        )
