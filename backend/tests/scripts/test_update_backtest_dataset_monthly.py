from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_service import (
    DatasetReleaseControlService,
    DatasetReleaseProfileBinding,
)
from backend.services.dataset_release.control_store import (
    CandidateRegistrationSpec,
    ControlStore,
)
from scripts import update_backtest_dataset_monthly as cli
from backend.services.dataset_release.errors import ProfileValidationError
from backend.services.dataset_release.profile import (
    CANONICAL_INITIAL_MIGRATION_PLAN_ID,
    load_dataset_profile,
    load_initial_migration_plan,
)
from backend.services.dataset_release.direct_monthly import (
    DirectMonthlyLayout,
    initial_state,
    write_state,
)


ROOT = Path(__file__).resolve().parents[3]
V2_PROFILE_PATH = ROOT / "configs" / "datasets" / "qe_backtest_monthly_v2.yaml"
INITIAL_PLAN_PATH = ROOT / "configs" / "datasets" / "migrations" / "pit_v2_initial_20260731_v1.yaml"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _service(tmp_path) -> tuple[DatasetReleaseControlService, ControlStore]:
    store = ControlStore.initialize(tmp_path / "control")
    service = DatasetReleaseControlService(
        (
            DatasetReleaseProfileBinding(
                profile_id="qe_hmm_full_v1",
                semantic_profile_digest=_digest("semantic-profile"),
                cutoff_policy="previous_month_last_completed_trading_day",
                store=store,
                cas=CASStore(store.root),
                cutoff_resolver=lambda _: date(2026, 7, 31),
                candidate_root_id="aistock-x-candidate-v1",
            ),
        )
    )
    return service, store


def _dual_profile_service(tmp_path) -> tuple[DatasetReleaseControlService, ControlStore]:
    store = ControlStore.initialize(tmp_path / "control")
    plan = load_initial_migration_plan(INITIAL_PLAN_PATH)
    bindings = tuple(
        DatasetReleaseProfileBinding(
            profile_id=profile_id,
            semantic_profile_digest=_digest(profile_id),
            cutoff_policy="previous_month_last_completed_trading_day",
            store=store,
            cas=CASStore(store.root),
            cutoff_resolver=lambda _: date(2026, 7, 31),
            candidate_root_id="aistock-x-candidate-v1",
            initial_migration_plans=({plan.plan_id: plan} if profile_id == "qe_hmm_full_v2" else {}),
        )
        for profile_id in ("qe_hmm_full_v1", "qe_hmm_full_v2")
    )
    return DatasetReleaseControlService(bindings), store


def _output(capsys) -> dict:
    captured = capsys.readouterr()
    assert captured.err == ""
    return json.loads(captured.out)


def test_monthly_is_one_durable_submission_and_never_creates_run_or_data(tmp_path, capsys) -> None:
    service, store = _service(tmp_path)
    observed = datetime(2026, 8, 11, tzinfo=UTC)

    assert (
        cli.main(
            ["monthly", "--candidate-only"],
            service=service,
            observed_at=observed,
        )
        == 0
    )
    first = _output(capsys)
    assert first["state"] == "QUEUED_RESOLUTION"
    assert first["run_id"] is None
    assert first["idempotency_key"].startswith("dsi_")
    assert first["execution_started_by_cli"] is False
    assert first["production_activation"] == "not_requested"

    assert (
        cli.main(
            ["monthly", "--candidate-only"],
            service=service,
            observed_at=observed,
        )
        == 0
    )
    second = _output(capsys)
    assert second["submission_id"] != first["submission_id"]
    assert second["idempotency_key"] != first["idempotency_key"]
    assert second["logical_request_key"] == first["logical_request_key"]
    assert second["replayed"] is False
    assert len(store._many("SELECT * FROM submissions", ())) == 2
    assert store._many("SELECT * FROM runs", ()) == []
    assert list((store.root / "staging").iterdir()) == []


def test_monthly_explicit_idempotency_key_replays_exact_submission(tmp_path, capsys) -> None:
    service, store = _service(tmp_path)
    observed = datetime(2026, 8, 11, tzinfo=UTC)
    arguments = [
        "monthly",
        "--candidate-only",
        "--idempotency-key",
        "operator-retry-20260811",
    ]

    assert cli.main(arguments, service=service, observed_at=observed) == 0
    first = _output(capsys)
    assert cli.main(arguments, service=service, observed_at=observed) == 0
    replay = _output(capsys)

    assert replay["submission_id"] == first["submission_id"]
    assert replay["idempotency_key"] == "operator-retry-20260811"
    assert replay["replayed"] is True
    assert len(store._many("SELECT * FROM submissions", ())) == 1
    assert store._many("SELECT * FROM runs", ()) == []


def test_monthly_routes_v2_to_direct_builder_without_creating_legacy_submission(
    tmp_path, capsys, monkeypatch
) -> None:
    service, store = _dual_profile_service(tmp_path)
    observed = datetime(2026, 8, 11, tzinfo=UTC)
    calls = []
    monkeypatch.setattr(
        cli,
        "_direct_monthly",
        lambda _service, args, *, observed_at: calls.append((args.scope, observed_at))
        or {
            "ok": True,
            "action": "monthly-direct",
            "status": "CANDIDATE_READY",
            "execution_started_by_cli": True,
            "production_activation": "not_requested",
        },
    )

    assert (
        cli.main(["--profile", "qe_hmm_full_v2", "monthly", "--candidate-only"], service=service, observed_at=observed)
        == 0
    )
    canonical = _output(capsys)
    assert canonical["action"] == "monthly-direct"
    assert canonical["execution_started_by_cli"] is True
    assert calls == [("full", observed)]
    assert store._many("SELECT * FROM submissions", ()) == []
    assert cli.main(["monthly", "--candidate-only"], service=service, observed_at=observed) == 0
    legacy = _output(capsys)
    assert legacy["submission_id"]
    assert len(store._many("SELECT * FROM submissions", ())) == 1


def test_direct_status_reads_latest_candidate_local_state_without_control_submission(
    tmp_path, monkeypatch
) -> None:
    parent = tmp_path / "candidates"
    parent.mkdir()
    baseline = parent / "baseline-candidate"
    baseline.mkdir()
    older = DirectMonthlyLayout.create(
        candidate_parent=parent,
        candidate_root=parent / "20260731-qe_hmm_full_v2-direct-20260801-candidate",
        baseline_root=baseline,
        cutoff=date(2026, 7, 31),
    )
    latest = DirectMonthlyLayout.create(
        candidate_parent=parent,
        candidate_root=parent / "20260831-qe_hmm_full_v2-direct-20260902-candidate",
        baseline_root=baseline,
        cutoff=date(2026, 8, 31),
    )
    for layout in (older, latest):
        write_state(layout, initial_state(layout))
    monkeypatch.setattr(
        cli,
        "load_dataset_profile",
        lambda _path: SimpleNamespace(candidate_root=parent),
    )

    result = cli._direct_status()

    assert result["action"] == "status-direct"
    assert result["cutoff"] == "2026-08-31"
    assert result["candidate_root"] == str(latest.candidate_root)
    assert result["bounded_read"] is True


def test_initial_migration_cli_submits_fixed_plan_once_without_execution(tmp_path, capsys) -> None:
    service, store = _dual_profile_service(tmp_path)
    observed = datetime(2027, 3, 15, tzinfo=UTC)
    arguments = [
        "--profile",
        "qe_hmm_full_v2",
        "initial-migration",
        "--plan",
        "pit_v2_initial_20260731_v1",
        "--scope",
        "sample",
        "--candidate-only",
    ]

    assert cli.main(arguments, service=service, observed_at=observed) == 0
    result = _output(capsys)
    request_row = store.get_submission(result["submission_id"])
    request = CASStore(store.root).get_json_bounded(request_row["request_ref"], max_bytes=2 * 1024**2)["request"]

    assert result["action"] == "initial-migration"
    assert result["fixed_cutoff"] == "2026-07-31"
    assert result["execution_started_by_cli"] is False
    assert result["production_activation"] == "not_requested"
    assert request["plan_id"] == "pit_v2_initial_20260731_v1"
    assert request["plan_digest"] == result["plan_digest"]
    assert store._many("SELECT * FROM runs", ()) == []


def test_initial_migration_plan_is_fixed_allowlisted_and_digest_bound() -> None:
    profile = load_dataset_profile(V2_PROFILE_PATH)
    first = load_initial_migration_plan(INITIAL_PLAN_PATH)
    second = load_initial_migration_plan(INITIAL_PLAN_PATH)

    assert profile.initial_migration_plan_ids == (CANONICAL_INITIAL_MIGRATION_PLAN_ID,)
    assert first.plan_id in profile.initial_migration_plan_ids
    assert first.cutoff.isoformat() == "2026-07-31"
    assert first.allowed_scopes == ("sample", "full")
    assert first.sample_instruments == (
        "000001.SZ",
        "300379.SZ",
        "300741.SZ",
        "600462.SH",
        "600930.SH",
        "688981.SH",
    )
    assert len(first.event_windows) == 11
    assert len(first.index_windows) == 2
    assert first.plan_digest == second.plan_digest
    assert len(first.plan_digest) == 64


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("cutoff", "2026-08-31", "cutoff must remain"),
        ("allowed_scopes", ["sample"], "scopes differ"),
        ("source_identity_policy", "trust_previous_receipt", "source identity policy differs"),
    ],
)
def test_initial_migration_plan_drift_fails_closed(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    payload = yaml.safe_load(INITIAL_PLAN_PATH.read_text(encoding="utf-8"))
    payload[field] = value
    target = tmp_path / INITIAL_PLAN_PATH.name
    target.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ProfileValidationError, match=message):
        load_initial_migration_plan(target)


def test_initial_migration_event_window_cannot_change_under_same_plan_id(tmp_path: Path) -> None:
    payload = yaml.safe_load(INITIAL_PLAN_PATH.read_text(encoding="utf-8"))
    payload["event_windows"][0]["start"] = "2026-07-28"
    target = tmp_path / INITIAL_PLAN_PATH.name
    target.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ProfileValidationError, match="event windows differ"):
        load_initial_migration_plan(target)


def test_status_latest_is_bounded_and_does_not_execute(tmp_path, capsys) -> None:
    service, _ = _service(tmp_path)
    observed = datetime(2026, 8, 11, tzinfo=UTC)
    cli.main(
        ["monthly", "--candidate-only"],
        service=service,
        observed_at=observed,
    )
    submitted = _output(capsys)

    assert cli.main(["status", "--latest"], service=service) == 0
    result = _output(capsys)
    assert result == {
        "action": "status",
        "bounded_read": True,
        "execution_started_by_cli": False,
        "ok": True,
        "outcome": None,
        "production_activation": "not_requested",
        "profile": "qe_hmm_full_v1",
        "retention": {
            "automatic_deletion_allowed": False,
            "reason_codes": ["reference_state_unsettled"],
            "retain_all_txt": True,
            "retain_complete_dataset": True,
            "retain_manifests_and_receipts": True,
            "retain_pit_snapshot": True,
            "retention_class": "FULL_IMMUTABLE",
        },
        "run_id": None,
        "run_state": None,
        "submission_id": submitted["submission_id"],
        "submission_state": "QUEUED_RESOLUTION",
        "updated_at": service.get_submission("qe_hmm_full_v1", submitted["submission_id"])["updated_at"],
        "worker_health": {
            "age_seconds": None,
            "capability_digest": None,
            "files_scanned": 0,
            "instance_id": None,
            "last_poll_at": None,
            "reason": "profile_config_digest_not_bound",
            "state": "unavailable",
            "worker_status": None,
        },
    }


def test_events_cli_is_bounded_and_never_executes(tmp_path, capsys) -> None:
    service, store = _service(tmp_path)
    cli.main(
        ["monthly", "--candidate-only"],
        service=service,
        observed_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    submitted = _output(capsys)

    assert (
        cli.main(
            [
                "events",
                "--submission-id",
                submitted["submission_id"],
                "--limit",
                "1",
            ],
            service=service,
        )
        == 0
    )
    result = _output(capsys)
    assert result["bounded_read"] is True
    assert result["execution_started_by_cli"] is False
    assert [item["type"] for item in result["items"]] == ["SUBMISSION_QUEUED"]
    assert store._many("SELECT * FROM runs", ()) == []


def test_submission_receipt_cli_reads_terminal_cas_without_execution(tmp_path, capsys) -> None:
    service, store = _service(tmp_path)
    cli.main(
        ["monthly", "--candidate-only"],
        service=service,
        observed_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    submitted = _output(capsys)
    reference = CASStore(store.root).put_json(
        {
            "schema_version": "dataset_release_worker_error_v1",
            "kind": "resolution",
            "target_id": submitted["submission_id"],
            "error_code": "TRANSIENT_SOURCE",
        }
    )
    with store.transaction() as connection:
        connection.execute(
            "UPDATE submissions SET state='BLOCKED_RETRY_EXHAUSTED',terminal_receipt_ref=? WHERE submission_id=?",
            (reference.sha256, submitted["submission_id"]),
        )

    assert (
        cli.main(
            ["receipt", "--submission-id", submitted["submission_id"]],
            service=service,
        )
        == 0
    )
    result = _output(capsys)
    assert result["submission_id"] == submitted["submission_id"]
    assert result["receipt"]["error_code"] == "TRANSIENT_SOURCE"
    assert result["execution_started_by_cli"] is False


def test_reattest_latest_freezes_catalog_identity_without_candidate_write(tmp_path, capsys) -> None:
    service, store = _service(tmp_path)
    candidate = service.catalog_existing(
        profile_id="qe_hmm_full_v1",
        registration=CandidateRegistrationSpec(
            allowlisted_root_id="aistock-x-candidate-v1",
            volume_serial="volume-x",
            root_relative_path="legacy/candidate-20260731",
            profile="qe_hmm_full_v1",
            scope="full",
            cutoff=date(2026, 7, 31),
            lineage_anchor=f"LEGACY_RECEIPT:legacy-1:{_digest('receipt')}",
            artifact_root=_digest("artifact"),
            producer_provenance_state="KNOWN",
            producer_provenance_digest_or_sentinel=_digest("producer"),
            pit_provenance_state="KNOWN",
            pit_provenance_digest_or_sentinel=_digest("pit"),
            legacy_receipt_ref=_digest("receipt"),
        ),
    )
    staging_before = list((store.root / "staging").iterdir())

    assert cli.main(["reattest-existing", "--latest"], service=service) == 0
    result = _output(capsys)
    assert result["candidate_registration_id"] == candidate["registration_id"]
    assert result["candidate_identity"] == candidate["candidate_identity"]
    assert result["candidate_write"] == "forbidden"
    assert result["execution_started_by_cli"] is False
    request = store.get_submission(result["submission_id"])
    outer = CASStore(store.root).get_json_bounded(request["request_ref"], max_bytes=2**20)
    assert outer["request"]["schema_version"] == "dataset_release_reattest_request_v1"
    assert outer["request"]["root_relative_path"] == "legacy/candidate-20260731"
    assert "candidate_path" not in outer["request"]
    assert list((store.root / "staging").iterdir()) == staging_before
    assert store._many("SELECT * FROM runs", ()) == []


def test_cancel_is_a_durable_command_not_process_control(tmp_path, capsys) -> None:
    service, store = _service(tmp_path)
    cli.main(
        ["monthly", "--candidate-only"],
        service=service,
        observed_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    submitted = _output(capsys)

    assert (
        cli.main(
            ["cancel-request", "--submission-id", submitted["submission_id"]],
            service=service,
        )
        == 0
    )
    result = _output(capsys)
    assert result["command_state"] == "QUEUED"
    assert result["idempotency_key"].startswith("dsc_")
    assert result["process_control"] == "not_requested"
    assert store.get_command(result["command_id"])["type"] == "CANCEL_REQUESTED"


def test_required_safety_selectors_fail_before_any_write(tmp_path, capsys) -> None:
    service, store = _service(tmp_path)
    assert cli.main(["monthly"], service=service) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["execution_started_by_cli"] is False
    assert store.latest_submission() is None

    assert cli.main(["status"], service=service) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["production_activation"] == "not_requested"
    assert store.latest_submission() is None
