from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import csv
import hashlib
from pathlib import Path
import struct
from types import SimpleNamespace

import pytest

import backend.services.dataset_release.monthly_mature_build_runner as runner_module
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.canonical import canonical_json_bytes, digest_named_fields
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.monthly_build_bridge import CompiledMonthlyBuild, MonthlyBuildBridgeError
from backend.services.dataset_release.monthly_build_executor import PhysicalBuildResult
from backend.services.dataset_release.monthly_mature_build_runner import (
    MatureMonthlyPhysicalBuildRunner,
    MonthlyBuildExecutionTools,
    MonthlyConsumerSmokeResult,
    MonthlyMatureBuildError,
)
from backend.services.dataset_release.monthly_supervised_build import (
    SupervisedMonthlyConsumerSmoke,
    SupervisedMonthlyQlibWriter,
)
from backend.services.dataset_release.monthly_supervised_scope import (
    ResourceSupervisedMonthlyBuildScopeFactory,
)
from backend.services.dataset_release.monthly_worker import ProducerContext


def _context() -> ProducerContext:
    return ProducerContext(
        stage="BUILD",
        operation_id="dmr_" + "1" * 32,
        attempt=2,
        request={},
        plan={"release_id": "qe_hmm_full_v2_20260930", "target_cutoff": "2026-09-30"},
        prior_receipts={},
    )


def _compiled() -> CompiledMonthlyBuild:
    release_digest = digest_named_fields(
        "aistock_monthly_physical_release_v1",
        {
            "release_id": "qe_hmm_full_v2_20260930",
            "target_cutoff": "2026-09-30",
            "source_bundle_sha256": "a" * 64,
            "action_plan_digest": "b" * 64,
        },
    )
    return CompiledMonthlyBuild(
        source_bundle_path=Path("bundle.json"),
        source_bundle_sha256="a" * 64,
        source_stage_receipt_ref=SimpleNamespace(),
        monthly_actions={},
        physical_plan={
            "action_plan_digest": "b" * 64,
            "release_digest": release_digest,
            "build_inputs": {},
        },
    )


def _cas(tmp_path: Path) -> CASStore:
    store = ControlStore.initialize(tmp_path / "control")
    return CASStore(store.root)


def _native_month_fixture(tmp_path: Path):
    """Actual immutable parent, native bins and sealed September inputs."""
    from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS

    parent = tmp_path / "august"
    provider = parent / "components/day"
    (provider / "features/000001.sz").mkdir(parents=True)
    (provider / "calendars").mkdir()
    (provider / "instruments").mkdir()
    (provider / "meta_export.json").write_bytes(b"{}\n")
    (provider / "calendars/day.txt").write_text("2026-08-31\n", encoding="utf-8")
    (provider / "instruments/all.txt").write_text("000001.SZ\t2026-08-31\t2026-08-31\n", encoding="utf-8")
    for field in QLIB_STOCK_FIELDS:
        (provider / f"features/000001.sz/{field}.day.bin").write_bytes(struct.pack("<ff", 0, 10))
    manifest = {
        "release_id": "qe_hmm_full_v2_20260831", "cutoff_trade_date": "2026-08-31",
        "components": {"day_meta_export": {
            "path": "components/day/meta_export.json", "size": 3,
            "sha256": hashlib.sha256(b"{}\n").hexdigest(),
        }},
    }
    manifest["dataset_manifest_sha256"] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    raw = canonical_json_bytes(manifest) + b"\n"
    (parent / "qe_dataset_manifest.json").write_bytes(raw)
    context = replace(_context(), plan={**_context().plan, "target_cutoff": "2026-09-01", "predecessor": {
        "candidate_root": str(parent), "cutoff": "2026-08-31",
        "release_id": manifest["release_id"], "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
    }, "predecessor_manifest_ref": {
        "id": "qe_dataset_manifest.json", "sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw),
        "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
    }})
    staging = tmp_path / ".staging/september.building"
    inputs = staging / ".month-inputs/daily"
    inputs.mkdir(parents=True)
    csv_path = inputs / "000001.SZ.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=("date", "symbol", *QLIB_STOCK_FIELDS))
        writer.writeheader()
        writer.writerow({"date": "2026-09-01", "symbol": "000001.SZ", **dict.fromkeys(QLIB_STOCK_FIELDS, 12)})
    calendar = inputs.parent / "calendar.txt"
    calendar.write_text("2026-08-31\n2026-09-01\n", encoding="utf-8")
    population = inputs.parent / "all.txt"
    population.write_text("000001.SZ\t2026-08-31\t2026-09-01\n", encoding="utf-8")
    def file_ref(path):
        return {"id": path.relative_to(staging).as_posix(), "size": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    prepared = {
        "schema_version": "aistock_monthly_legacy_qlib_operation_v1", "dataset": "daily_bin",
        "cutoff": "2026-09-01", "predecessor_manifest_sha256": manifest["dataset_manifest_sha256"],
        "source_bundle_sha256": "a" * 64, "csv_relative_path": ".month-inputs/daily",
        "calendar_ref": file_ref(calendar), "instruments_ref": file_ref(population),
        "csv_refs": [file_ref(csv_path)], "qfq_basis_changes": {},
    }
    cas = _cas(tmp_path)
    operation = {"operation_id": "daily", "dataset": "daily_bin", "mode": "inherited_month",
                 "preparation_ref": cas.put_json(prepared).as_dict()}
    compiled = _compiled()
    physical = dict(compiled.physical_plan)
    physical["release_digest"] = digest_named_fields("aistock_monthly_physical_release_v1", {
        "release_id": context.plan["release_id"], "target_cutoff": "2026-09-01",
        "source_bundle_sha256": compiled.source_bundle_sha256, "action_plan_digest": physical["action_plan_digest"],
    })
    runner = MatureMonthlyPhysicalBuildRunner(
        profile=SimpleNamespace(candidate_root=tmp_path, stage_timeouts_seconds={"full_build": 3600}),
        cas=cas, project_root=tmp_path, finalizer=_Finalizer(), qlib_writer=_Writer(), consumer_smoke=_Smoke(),
    )
    return runner, context, staging, replace(compiled, physical_plan=physical), operation, prepared, provider


def test_formal_build_runner_dispatches_to_real_native_month_append(tmp_path, monkeypatch):
    runner, context, staging, compiled, operation, _, provider = _native_month_fixture(tmp_path)
    received = []
    def run(invocation):
        value = {"schema_version": "dataset_release_build_stage_result_v1", "stage": invocation.stage, "status": "PASS"}
        if invocation.stage == "prepare":
            value["qlib_dump_operations"] = [operation]
        if invocation.stage == "finalize-bins":
            received.append(runner.cas.get_json(invocation.prerequisites["qlib_dump_daily"]))
        return value
    monkeypatch.setattr(runner_module, "run_build_stage", run)
    runner.execute(context=context, staging_root=staging, compiled=compiled)
    assert runner.qlib_writer.operations == []
    assert struct.unpack("<fff", (staging / "daily_bin/qlib/features/000001.sz/close.day.bin").read_bytes()) == (0, 10, 12)
    assert struct.unpack("<ff", (provider / "features/000001.sz/close.day.bin").read_bytes()) == (0, 10)
    assert received[0]["receipt"]["feature_file_count"] == 12
    assert received[0]["publication_allowed"] is False
    assert received[0]["historical_source_rows_read"] == 0
    assert "runtime" not in received[0]  # Never forge a WSL child receipt.


@pytest.mark.parametrize("fault", ["parent", "source", "cutoff", "csv_drift", "extra_csv", "escape", "duplicate", "operation", "catalog", "reference", "ads"])
def test_native_month_dispatch_rejects_unbound_inputs_before_writing(tmp_path, fault):
    runner, context, staging, compiled, operation, prepared, _ = _native_month_fixture(tmp_path)
    if fault == "parent":
        prepared["predecessor_manifest_sha256"] = "c" * 64
    elif fault == "source":
        prepared["source_bundle_sha256"] = "c" * 64
    elif fault == "cutoff":
        prepared["cutoff"] = "2026-09-02"
    elif fault == "csv_drift":
        (staging / prepared["csv_refs"][0]["id"]).write_text("changed", encoding="utf-8")
    elif fault == "extra_csv":
        (staging / prepared["csv_relative_path"] / "000002.SZ.csv").write_text("unexpected", encoding="utf-8")
    elif fault == "escape":
        prepared["csv_refs"][0]["id"] = "../escape.csv"
    elif fault == "duplicate":
        prepared["csv_refs"].append(dict(prepared["csv_refs"][0]))
    elif fault == "catalog":
        context = replace(context, plan={**context.plan, "predecessor": {**context.plan["predecessor"], "candidate_root": str(tmp_path.parent / "other")}})
    elif fault == "reference":
        context = replace(context, plan={**context.plan, "predecessor_manifest_ref": {**context.plan["predecessor_manifest_ref"], "dataset_manifest_sha256": "c" * 64}})
    elif fault == "ads":
        prepared["csv_refs"][0]["id"] += ":alternate"
    else:
        operation["unexpected"] = True
    operation["preparation_ref"] = runner.cas.put_json(prepared).as_dict()
    with pytest.raises((MonthlyMatureBuildError, MonthlyBuildBridgeError, ValueError)):
        runner._append_inherited_month(context=context, staging_root=staging, compiled=compiled, operation=operation)
    assert not (staging / "daily_bin").exists()


def test_native_month_dispatch_reports_actual_write_progress(tmp_path):
    from backend.tests.dataset_release.test_monthly_unified_v2 import _request, _service, Pipeline

    runner, context, staging, compiled, operation, _, _ = _native_month_fixture(tmp_path)
    (tmp_path / "durable").mkdir()
    service = _service(tmp_path / "durable", Pipeline())
    operation_id = service.submit(_request())["operation_id"]
    service.store.update_state(operation_id, attempt=1, current_stage="BUILD", status="BUILDING")
    checkpoint, durable_progress = service._stage_control(operation_id, attempt=1, stage="BUILD")
    events = []

    def progress(event):
        durable_progress(event)
        events.append(event)

    context = replace(context, progress=progress, checkpoint=checkpoint)
    runner._append_inherited_month(context=context, staging_root=staging, compiled=compiled, operation=operation)
    actual = [event for event in events if "completed_feature_files" in event]
    assert actual[-1]["completed_instruments"] == actual[-1]["total_instruments"] == 1
    assert actual[-1]["completed_feature_files"] == 12
    assert actual[-1]["instruments_per_second"] > 0
    assert service.status(operation_id)["stage_progress"]["completed_feature_files"] == 12
    assert not any(service.status(operation_id)["checkpoints"].values())


def test_native_month_dispatch_does_not_seal_success_when_input_changes(tmp_path, monkeypatch):
    runner, context, staging, compiled, operation, prepared, _ = _native_month_fixture(tmp_path)
    append = runner_module.append_legacy_qlib_month
    def racing_append(*args, **kwargs):
        receipt = append(*args, **kwargs)
        (staging / prepared["csv_refs"][0]["id"]).write_text("changed after write", encoding="utf-8")
        return receipt
    monkeypatch.setattr(runner_module, "append_legacy_qlib_month", racing_append)
    with pytest.raises(MonthlyMatureBuildError, match="changed during append"):
        runner._append_inherited_month(context=context, staging_root=staging, compiled=compiled, operation=operation)


class _Writer:
    def __init__(self) -> None:
        self.operations: list[str] = []

    def execute(self, *, context, staging_root, operation):  # type: ignore[no-untyped-def]
        del context, staging_root
        self.operations.append(operation["operation_id"])
        return {"kind": "supervised", "operation_id": operation["operation_id"]}


class _Smoke:
    def execute(self, *, context, staging_root, prepare_result, release_digest):  # type: ignore[no-untyped-def]
        del context, staging_root, prepare_result
        return MonthlyConsumerSmokeResult(
            semantic_receipt={"status": "PASS", "release_digest": release_digest},
            resource_receipt={"schema_version": "supervised"},
        )


class _Finalizer:
    def __init__(self) -> None:
        self.refs: set[str] = set()

    def execute(self, *, context, staging_root, compiled, validation_result, stage_refs):  # type: ignore[no-untyped-def]
        del context, compiled, validation_result
        self.refs = set(stage_refs)
        manifest = staging_root / "qe_dataset_manifest.json"
        component = staging_root / "component.bin"
        manifest.write_bytes(b"manifest")
        component.write_bytes(b"component")
        return PhysicalBuildResult(manifest_path=manifest, component_artifacts=(component,))


class _Supervisor:
    def __init__(self, *, attempt_id: str, fence: int, control_root: Path) -> None:
        self.attempt_id = attempt_id
        self.fence = fence
        self.control_root = control_root
        self.heartbeat_path = control_root / "heartbeat.json"
        self.events: list[str] = []

    def __enter__(self):  # type: ignore[no-untyped-def]
        self.events.append("enter")
        return self

    def __exit__(self, exc_type, exc, traceback):  # type: ignore[no-untyped-def]
        del exc_type, exc, traceback
        self.events.append("exit")

    def run_supervised(self, command, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError((command, kwargs))


def test_runner_executes_mature_stages_in_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stages: list[tuple[str, set[str]]] = []

    def run(invocation):  # type: ignore[no-untyped-def]
        stages.append((invocation.stage, set(invocation.prerequisites)))
        value = {
            "schema_version": "dataset_release_build_stage_result_v1",
            "stage": invocation.stage,
            "status": "PASS",
        }
        if invocation.stage == "prepare":
            value["qlib_dump_operations"] = [
                {"operation_id": "daily"},
                {"operation_id": "minute"},
            ]
        return value

    monkeypatch.setattr(runner_module, "run_build_stage", run)
    writer = _Writer()
    finalizer = _Finalizer()
    staging = tmp_path / ".staging" / "candidate.building"
    staging.parent.mkdir()
    staging.mkdir()
    runner = MatureMonthlyPhysicalBuildRunner(
        profile=SimpleNamespace(
            stage_timeouts_seconds={"full_build": 3600},
            candidate_root=tmp_path,
        ),
        cas=_cas(tmp_path),
        project_root=tmp_path,
        qlib_writer=writer,
        consumer_smoke=_Smoke(),
        finalizer=finalizer,
    )

    result = runner.execute(context=_context(), staging_root=staging, compiled=_compiled())

    assert writer.operations == ["daily", "minute"]
    assert [stage for stage, _ in stages] == ["prepare", "finalize-bins", "validate"]
    assert stages[1][1] == {"prepare", "qlib_dump_daily", "qlib_dump_minute"}
    assert stages[2][1] == {
        "prepare",
        "qlib_dump_daily",
        "qlib_dump_minute",
        "finalize_bins",
        "consumer_smoke",
    }
    assert finalizer.refs == stages[2][1] | {"consumer_smoke_resource", "validate"}
    assert result.manifest_path == staging / "qe_dataset_manifest.json"


def test_runner_creates_attempt_bound_execution_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(invocation):  # type: ignore[no-untyped-def]
        value = {
            "schema_version": "dataset_release_build_stage_result_v1",
            "stage": invocation.stage,
            "status": "PASS",
        }
        if invocation.stage == "prepare":
            value["qlib_dump_operations"] = [{"operation_id": "daily"}]
        return value

    monkeypatch.setattr(runner_module, "run_build_stage", run)
    scoped_writer = _Writer()
    scoped_smoke = _Smoke()
    lifecycle: list[tuple[str, int]] = []

    @contextmanager
    def scope(context):  # type: ignore[no-untyped-def]
        lifecycle.append(("enter", context.attempt))
        try:
            yield MonthlyBuildExecutionTools(scoped_writer, scoped_smoke)
        finally:
            lifecycle.append(("exit", context.attempt))

    finalizer = _Finalizer()
    staging = tmp_path / ".staging" / "candidate.building"
    staging.parent.mkdir()
    staging.mkdir()
    runner = MatureMonthlyPhysicalBuildRunner(
        profile=SimpleNamespace(
            stage_timeouts_seconds={"full_build": 3600},
            candidate_root=tmp_path,
        ),
        cas=_cas(tmp_path),
        project_root=tmp_path,
        finalizer=finalizer,
        execution_scope_factory=scope,
    )

    runner.execute(context=_context(), staging_root=staging, compiled=_compiled())

    assert lifecycle == [("enter", 2), ("exit", 2)]
    assert scoped_writer.operations == ["daily"]


def test_runner_rejects_process_scoped_and_attempt_scoped_tools(tmp_path: Path) -> None:
    with pytest.raises(MonthlyMatureBuildError, match="exactly one"):
        MatureMonthlyPhysicalBuildRunner(
            profile=SimpleNamespace(
                stage_timeouts_seconds={"full_build": 3600},
                candidate_root=tmp_path,
            ),
            cas=_cas(tmp_path),
            project_root=tmp_path,
            qlib_writer=_Writer(),
            consumer_smoke=_Smoke(),
            finalizer=_Finalizer(),
            execution_scope_factory=lambda _context: pytest.fail("not entered"),
        )


def test_supervised_scope_binds_one_supervisor_to_writer_and_smoke(
    tmp_path: Path,
) -> None:
    context = _context()
    supervisor = _Supervisor(
        attempt_id=f"{context.operation_id}-build",
        fence=context.attempt,
        control_root=tmp_path,
    )
    observed: list[ProducerContext] = []

    def create(value: ProducerContext) -> _Supervisor:
        observed.append(value)
        return supervisor

    factory = ResourceSupervisedMonthlyBuildScopeFactory(
        profile=SimpleNamespace(),
        project_root=tmp_path,
        toolchain=SimpleNamespace(),
        supervisor_factory=create,
    )

    with factory(context) as tools:
        assert isinstance(tools.qlib_writer, SupervisedMonthlyQlibWriter)
        assert isinstance(tools.consumer_smoke, SupervisedMonthlyConsumerSmoke)
        assert tools.qlib_writer.supervisor is supervisor
        assert tools.consumer_smoke.supervisor is supervisor
        assert supervisor.events == ["enter"]

    assert observed == [context]
    assert supervisor.events == ["enter", "exit"]


def test_supervised_scope_rejects_stale_attempt(tmp_path: Path) -> None:
    context = _context()
    supervisor = _Supervisor(
        attempt_id=f"{context.operation_id}-build",
        fence=context.attempt - 1,
        control_root=tmp_path,
    )
    factory = ResourceSupervisedMonthlyBuildScopeFactory(
        profile=SimpleNamespace(),
        project_root=tmp_path,
        toolchain=SimpleNamespace(),
        supervisor_factory=lambda _context: supervisor,
    )

    with pytest.raises(ValueError, match="identity differs"):
        with factory(context):
            pass

    assert supervisor.events == []


def test_runner_rejects_non_pass_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner_module,
        "run_build_stage",
        lambda invocation: {
            "schema_version": "dataset_release_build_stage_result_v1",
            "stage": invocation.stage,
            "status": "FAILED",
        },
    )
    runner = MatureMonthlyPhysicalBuildRunner(
        profile=SimpleNamespace(
            stage_timeouts_seconds={"full_build": 3600},
            candidate_root=tmp_path,
        ),
        cas=_cas(tmp_path),
        project_root=tmp_path,
        qlib_writer=_Writer(),
        consumer_smoke=_Smoke(),
        finalizer=_Finalizer(),
    )
    (tmp_path / ".staging").mkdir()

    with pytest.raises(MonthlyMatureBuildError, match="prepare"):
        runner.execute(
            context=_context(),
            staging_root=tmp_path / ".staging" / "candidate.building",
            compiled=_compiled(),
        )


def test_runner_rejects_bridge_release_digest_drift(tmp_path: Path) -> None:
    runner = MatureMonthlyPhysicalBuildRunner(
        profile=SimpleNamespace(
            stage_timeouts_seconds={"full_build": 3600},
            candidate_root=tmp_path,
        ),
        cas=_cas(tmp_path),
        project_root=tmp_path,
        qlib_writer=_Writer(),
        consumer_smoke=_Smoke(),
        finalizer=_Finalizer(),
    )
    staging = tmp_path / ".staging" / "candidate.building"
    staging.parent.mkdir()
    compiled = _compiled()
    drifted = CompiledMonthlyBuild(
        source_bundle_path=compiled.source_bundle_path,
        source_bundle_sha256=compiled.source_bundle_sha256,
        source_stage_receipt_ref=compiled.source_stage_receipt_ref,
        monthly_actions=compiled.monthly_actions,
        physical_plan={**compiled.physical_plan, "release_digest": "f" * 64},
    )

    with pytest.raises(MonthlyMatureBuildError, match="release digest differs"):
        runner.execute(context=_context(), staging_root=staging, compiled=drifted)
