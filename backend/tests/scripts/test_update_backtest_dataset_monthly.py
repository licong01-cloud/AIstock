from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime

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
