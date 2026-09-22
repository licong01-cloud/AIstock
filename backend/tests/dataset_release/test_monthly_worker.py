from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
from typing import Any, Mapping

import pytest

from backend.services.dataset_release.monthly_unified import (
    COMPONENTS,
    CONSUMER_READBACK_SCHEMA,
    NODE_REGISTRATION_SCHEMA,
    RELEASE_CLOSURE_SCHEMA,
    REQUIRED_CONSUMERS,
    REQUIRED_NODES,
    SOURCE_GATES,
    TELEMETRY_COUNT_FIELDS,
    classify_component_actions,
)
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_worker import (
    PRODUCER_EVIDENCE_SCHEMA,
    MonthlyProducerError,
    ProducerContext,
    RegisteredMonthlyPipeline,
    SubprocessStageProducer,
)
from backend.services.dataset_release.profile_contract import (
    ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS,
)


SHA = "a" * 64
MANIFEST = "b" * 64


@dataclass
class Producer:
    stage: str
    artifact: Path
    mutate: Any = None
    producer_id: str = "test.producer"
    producer_version: str = "1"

    def produce(self, context: ProducerContext) -> Mapping[str, Any]:
        output_artifacts: list[dict[str, str]] = []

        def write(name: str, value: Mapping[str, Any]) -> dict[str, Any]:
            path = self.artifact.parent / name
            path.write_bytes(canonical_json_bytes(value) + b"\n")
            output_artifacts.append({"id": path.name, "path": str(path)})
            return {
                "id": path.name,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size": path.stat().st_size,
            }

        ref = write(f"{self.stage.lower()}-evidence.json", {"stage": self.stage})
        change = {
            "dataset": "kline_daily_raw",
            "fields": ["open", "close"],
            "instruments": ["000001.SZ"],
            "start": "2026-09-01",
            "end": "2026-09-30",
            "kind": "TAIL_APPEND",
            "source_receipt_sha256": SHA,
        }
        from backend.services.dataset_release.monthly_unified import SourceChange
        from datetime import date

        parsed = SourceChange(
            dataset=change["dataset"],
            fields=tuple(change["fields"]),
            instruments=tuple(change["instruments"]),
            start=date.fromisoformat(change["start"]),
            end=date.fromisoformat(change["end"]),
            kind=change["kind"],
            source_receipt_sha256=SHA,
        )
        scope: dict[str, Any] = {"dataset_manifest_sha256": MANIFEST}
        if self.stage == "SOURCE":
            actions = classify_component_actions([parsed])
            snapshot_ref = write(
                "source-snapshot.json",
                {
                    "schema_version": "aistock_monthly_source_snapshot_identity_v1",
                    "snapshot_group_id": "pg-exported-snapshot-test",
                    "snapshot_id": "1-ABC-1",
                    "source_as_of": "2026-10-01T00:00:00+00:00",
                    "repair_watermark": "repair-10",
                },
            )
            repair_ref = write(
                "source-repair-overlap.json",
                {
                    "schema_version": "aistock_monthly_repair_overlap_check_v1",
                    "snapshot_group_id": "pg-exported-snapshot-test",
                    "initial_repair_watermark": "repair-10",
                    "overlapping_repair_ids": [],
                    "status": "PASS",
                },
            )
            change_ref = write(
                "source-changes.json",
                {
                    "schema_version": "aistock_monthly_source_change_scope_v1",
                    "changes": [change],
                    "component_actions": actions,
                },
            )
            gate_refs = [
                (
                    lambda expectation_ref, readback_ref: write(
                        f"source-gate-{gate}.json",
                        {
                            "schema_version": "aistock_monthly_source_gate_v2",
                            "gate_id": gate,
                            "snapshot_group_id": "pg-exported-snapshot-test",
                            "expectation_contract_ref": expectation_ref["id"],
                            "readback_ref": readback_ref["id"],
                            "expected_count": 1,
                            "observed_count": 1,
                            "explained_missing_count": 0,
                            "unexplained_missing_count": 0,
                            "duplicate_count": 0,
                            "invalid_value_count": 0,
                            "status": "PASS",
                            "exception_refs": [],
                        },
                    )
                )(
                    write(f"source-contract-{gate}.json", {"gate": gate, "kind": "expectation"}),
                    write(f"source-readback-{gate}.json", {"gate": gate, "kind": "readback"}),
                )
                for gate in SOURCE_GATES
            ]
            contract_ref = write(
                "source-producer-contract.json",
                {
                    "schema_version": "aistock_monthly_source_producer_contract_v1",
                    "producer_id": self.producer_id,
                    "producer_version": self.producer_version,
                    "contract_sha256": SHA,
                },
            )
            scope = {
                "gates": list(SOURCE_GATES),
                "changes": [change],
                "component_actions": actions,
                "database_read_performed": True,
                "database_write_performed": False,
                "repair_receipts": [],
                "source_as_of": "2026-10-01T00:00:00+00:00",
                "snapshot_group_id": "pg-exported-snapshot-test",
                "consistent_input_set_complete": True,
                "snapshot_identity_refs": [snapshot_ref],
                "repair_overlap_check_refs": [repair_ref],
                "change_scope_refs": [change_ref],
                "source_gate_refs": gate_refs,
                "producer_contract_refs": [contract_ref],
            }
        elif self.stage == "BUILD":
            ref = write("qe_dataset_manifest.json", {"dataset_manifest_sha256": MANIFEST})
            scope.update(
                {
                    "predecessor_manifest_sha256": SHA,
                    "component_actions": {component: "REUSE" for component in COMPONENTS},
                    "dataset_manifest_ref": ref,
                }
            )
        elif self.stage == "DERIVE":
            asset_ref = write("hmm-coefficients.json", {"schema_version": "hmm-test-v1"})
            registry_ref = write(
                "derived-assets.json",
                {
                    "schema_version": "aistock_dataset_derived_asset_registry_v1",
                    "source_dataset_manifest_sha256": MANIFEST,
                    "assets": [
                        {
                            "asset_id": "hmm",
                            "path": asset_ref["id"],
                            "sha256": asset_ref["sha256"],
                            "size": asset_ref["size"],
                            "schema_version": "hmm-test-v1",
                        }
                    ],
                },
            )
            scope.update(
                {
                    "hmm_fit_count": 0,
                    "training_started": False,
                    "derived_assets": [asset_ref],
                    "source_dataset_manifest_sha256": MANIFEST,
                    "derived_asset_registry_ref": registry_ref,
                    "derived_asset_registry_sha256": registry_ref["sha256"],
                }
            )
        elif self.stage == "LOCAL_VALIDATE":
            closure = {
                "schema_version": RELEASE_CLOSURE_SCHEMA,
                "dataset_manifest_ref": ref,
                "derived_asset_refs": [ref],
                "consumer_contract_refs": [ref],
                "source_readiness_refs": [ref],
                "component_validation_refs": [ref],
                "lineage_ref": ref,
            }
            closure["canonical_sha256"] = hashlib.sha256(canonical_json_bytes(closure)).hexdigest()
            ref = write("release-closure.json", closure)
            scope.update(
                {
                    "pool_gap_counts": {
                        name: 0 for name in ("stock_universe", "csi300", "csi500", "csi1000", "star50", "star100")
                    },
                    "dataset_identity_complete": True,
                    "release_closure": closure,
                    "release_closure_ref": ref,
                    "release_closure_file_sha256": ref["sha256"],
                }
            )
        elif self.stage == "DEPLOY":
            registrations = {
                node: {
                    "schema_version": NODE_REGISTRATION_SCHEMA,
                    "node_id": node,
                    "release_id": "qe_hmm_full_v2_20260930",
                    "dataset_manifest_ref": ref,
                    "closure_ref": ref,
                    "candidate_root": f"/releases/{node}",
                    "relative_file_refs": [ref],
                    "deployment_receipt_ref": ref,
                    "runtime_registration": {
                        "relative_path": f".aistock-release-registry/{MANIFEST}.json",
                        "sha256": MANIFEST,
                        "size": 1,
                        "registration_sha256": MANIFEST,
                    },
                }
                for node in REQUIRED_NODES
            }
            registration_refs = {
                node: write(f"node-{node}.json", registration) for node, registration in registrations.items()
            }
            scope.update(
                {
                    "nodes": list(REQUIRED_NODES),
                    "node_manifest_sha256": {node: MANIFEST for node in REQUIRED_NODES},
                    "node_registrations": registrations,
                    "node_registration_refs": registration_refs,
                }
            )
        elif self.stage == "CONSUMER_VALIDATE":
            readbacks = {
                name: {
                    "schema_version": CONSUMER_READBACK_SCHEMA,
                    "consumer_id": name,
                    "node_id": "controller",
                    "binding_ref": ref,
                    "required_window": {"start": "2018-08-01", "end": "2026-09-30"},
                    "resolved_component_refs": [ref],
                    "derived_asset_refs": (
                        [ref]
                        if "derived_assets"
                        in ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS[name]
                        else []
                    ),
                    "coverage_counts": {"unresolved": 0},
                    "command_or_adapter_version": "test-v1",
                    "result_ref": ref,
                    "side_effect_flags": {
                        "outcomes_read": False,
                        "training_started": False,
                        "experiment_started": False,
                        "runtime_action_performed": False,
                    },
                    "dataset_manifest_sha256": MANIFEST,
                }
                for name in REQUIRED_CONSUMERS
            }
            readback_refs = {name: write(f"consumer-{name}.json", readback) for name, readback in readbacks.items()}
            scope.update(
                {
                    "consumers": list(REQUIRED_CONSUMERS),
                    "consumer_readbacks": readbacks,
                    "consumer_readback_refs": readback_refs,
                    "outcomes_read": False,
                    "training_started": False,
                    "experiment_started": False,
                    "runtime_action_performed": False,
                }
            )
        if self.mutate is not None:
            self.mutate(scope)
        return {
            "schema_version": PRODUCER_EVIDENCE_SCHEMA,
            "scope": scope,
            "input_artifacts": [],
            "output_artifacts": output_artifacts,
            "counts": {field: 0 for field in TELEMETRY_COUNT_FIELDS},
            "errors": [],
        }


def _pipeline(tmp_path: Path, *, stage: str = "SOURCE", mutate: Any = None) -> RegisteredMonthlyPipeline:
    artifact = tmp_path / "artifact.json"
    artifact.write_text("{}\n", encoding="utf-8")
    producers = {
        name: Producer(name, artifact, mutate if name == stage else None)
        for name in ("SOURCE", "BUILD", "DERIVE", "LOCAL_VALIDATE", "DEPLOY", "CONSUMER_VALIDATE")
    }
    return RegisteredMonthlyPipeline(producers, artifact_roots=(tmp_path,))


def _run(pipeline: RegisteredMonthlyPipeline, stage: str) -> Mapping[str, Any]:
    return pipeline.run_stage(
        stage=stage,
        operation_id="dmr_" + "1" * 32,
        attempt=1,
        request={},
        plan={
            "release_id": "qe_hmm_full_v2_20260930",
            "predecessor": {"dataset_manifest_sha256": SHA},
        },
        prior_receipts={},
    )


def test_pipeline_computes_hashes_from_registered_artifacts(tmp_path: Path) -> None:
    receipt = _run(_pipeline(tmp_path), "SOURCE")
    assert receipt["status"] == "PASS"
    artifact = tmp_path / receipt["output_refs"][0]["id"]
    assert receipt["output_refs"][0]["size"] == artifact.stat().st_size
    assert receipt["output_refs"][0]["sha256"] != SHA


def test_pipeline_rejects_caller_style_pass_without_full_scope(tmp_path: Path) -> None:
    pipeline = _pipeline(tmp_path, mutate=lambda scope: scope.clear())
    with pytest.raises(MonthlyProducerError, match="gates and changes"):
        _run(pipeline, "SOURCE")


def test_pipeline_rejects_uncomputed_component_action(tmp_path: Path) -> None:
    pipeline = _pipeline(
        tmp_path,
        mutate=lambda scope: scope["component_actions"].update({"day": "REUSE"}),
    )
    with pytest.raises(MonthlyProducerError, match="change scope artifact differs"):
        _run(pipeline, "SOURCE")


def test_pipeline_rejects_scope_reference_not_pinned_by_output_bytes(tmp_path: Path) -> None:
    def mutate(scope: dict[str, Any]) -> None:
        scope["change_scope_refs"][0]["sha256"] = SHA

    with pytest.raises(MonthlyProducerError, match="not pinned by the producer output bytes"):
        _run(_pipeline(tmp_path, mutate=mutate), "SOURCE")


def test_consumer_requires_exact_manifest_readback_for_every_consumer(tmp_path: Path) -> None:
    def mutate(scope: dict[str, Any]) -> None:
        scope["consumer_readbacks"]["qe_p11"]["dataset_manifest_sha256"] = SHA

    with pytest.raises(MonthlyProducerError, match="consumer identity differs"):
        _run(_pipeline(tmp_path, stage="CONSUMER_VALIDATE", mutate=mutate), "CONSUMER_VALIDATE")


def test_consumer_rejects_readback_not_equal_to_pinned_artifact(tmp_path: Path) -> None:
    def mutate(scope: dict[str, Any]) -> None:
        scope["consumer_readbacks"]["qe_p11"]["coverage_counts"] = {"unresolved": 1}

    with pytest.raises(MonthlyProducerError, match="artifact content differs"):
        _run(_pipeline(tmp_path, stage="CONSUMER_VALIDATE", mutate=mutate), "CONSUMER_VALIDATE")


def test_deployment_requires_identical_three_node_manifest(tmp_path: Path) -> None:
    def mutate(scope: dict[str, Any]) -> None:
        scope["node_manifest_sha256"]["rdagent-node1"] = SHA

    with pytest.raises(MonthlyProducerError, match="node manifest"):
        _run(_pipeline(tmp_path, stage="DEPLOY", mutate=mutate), "DEPLOY")


def test_subprocess_producer_digest_changes_when_registered_script_bytes_change(
    tmp_path: Path,
) -> None:
    executable = tmp_path / "python.exe"
    executable.write_bytes(b"interpreter")
    script = tmp_path / "producer.py"
    script.write_text("print('one')\n", encoding="utf-8")

    def pipeline() -> RegisteredMonthlyPipeline:
        producer = SubprocessStageProducer(
            producer_id="official.monthly",
            producer_version="1",
            command=(str(executable), str(script), "--context", "{context}"),
            operation_state_root=tmp_path,
            project_root=tmp_path,
        )
        return RegisteredMonthlyPipeline(
            {
                stage: producer
                for stage in ("SOURCE", "BUILD", "DERIVE", "LOCAL_VALIDATE", "DEPLOY", "CONSUMER_VALIDATE")
            },
            artifact_roots=(tmp_path,),
        )

    before = pipeline().stage_identity("SOURCE")
    script.write_text("print('two')\n", encoding="utf-8")
    after = pipeline().stage_identity("SOURCE")
    assert before != after
