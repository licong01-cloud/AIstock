from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from backend.services.dataset_release.control_store import (
    CandidateRegistrationSpec,
    ControlStore,
    ControlStoreNotInitialized,
    ControlStoreSchemaMismatch,
    IdempotencyConflict,
    StateConflict,
    build_candidate_registration_id,
    utc_now,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _candidate_spec(
    suffix: str,
    *,
    path: str | None = None,
    cutoff: date = date(2026, 7, 31),
    artifact_root: str | None = None,
    last_attested_at: datetime | None = None,
) -> CandidateRegistrationSpec:
    return CandidateRegistrationSpec(
        allowlisted_root_id="candidate-root",
        volume_serial="volume-001",
        root_relative_path=path or f"legacy/{suffix}",
        profile="qe_hmm_full_v1",
        scope="full",
        cutoff=cutoff,
        lineage_anchor=f"LEGACY_RECEIPT:legacy-{suffix}:{_digest(f'receipt-{suffix}')}",
        artifact_root=artifact_root or _digest(f"artifact-{suffix}"),
        producer_provenance_state="KNOWN",
        producer_provenance_digest_or_sentinel=_digest(f"producer-{suffix}"),
        pit_provenance_state="KNOWN",
        pit_provenance_digest_or_sentinel=_digest(f"pit-{suffix}"),
        legacy_receipt_ref=f"cas:legacy-receipt-{suffix}",
        last_attested_at=last_attested_at,
    )


def test_runtime_open_never_initializes_store(tmp_path) -> None:
    root = tmp_path / "missing-control"

    with pytest.raises(ControlStoreNotInitialized):
        ControlStore(root)

    assert not root.exists()


def test_explicit_init_sets_wal_full_foreign_keys_and_identity(tmp_path) -> None:
    root = tmp_path / "control"
    store = ControlStore.initialize(root)

    assert store.db_path.is_file()
    assert (root / "control_store_identity.json").is_file()
    assert (root / "cas" / "sha256").is_dir()
    assert (root / "staging").is_dir()
    assert (root / "quarantine").is_dir()
    assert (root / "logs").is_dir()
    assert (root / "heartbeats").is_dir()
    assert (root / "worker_heartbeats").is_dir()
    assert store.integrity_check() == {
        "ok": True,
        "quick_check": "ok",
        "journal_mode": "wal",
        "synchronous": 2,
        "foreign_keys": 1,
    }
    with store.transaction(immediate=False) as connection:
        metadata = connection.execute("SELECT * FROM schema_metadata WHERE singleton=1").fetchone()
        resolution_columns = {row["name"] for row in connection.execute("PRAGMA table_info(resolution_attempts)")}
        attestation_columns = {row["name"] for row in connection.execute("PRAGMA table_info(attestations)")}
        target_index = connection.execute(
            """
            SELECT * FROM pragma_index_list('attestations')
            WHERE name='attestations_target_observed'
            """
        ).fetchone()
    assert metadata["ddl_sha256"] == store.identity["ddl_sha256"]
    assert "source_probe_ordinal" in resolution_columns
    assert "attestation_target_key" in attestation_columns
    assert target_index is not None and int(target_index["unique"]) == 0
    assert ControlStore.initialize(root).identity == store.identity


def test_init_recovers_exact_database_commit_before_identity_crash(tmp_path) -> None:
    root = tmp_path / "control"

    def crash(point: str) -> None:
        assert point == "after_database_commit_before_identity"
        raise RuntimeError("injected init crash")

    with pytest.raises(RuntimeError, match="injected init crash"):
        ControlStore.initialize(root, fault_injector=crash)

    assert (root / "control.sqlite3").is_file()
    assert not (root / "control_store_identity.json").exists()
    recovered = ControlStore.initialize(root)
    assert recovered.integrity_check()["ok"] is True
    assert recovered.identity["control_schema_version"] == 1


def test_init_recovery_rejects_schema_drift_without_identity(tmp_path) -> None:
    root = tmp_path / "control"

    def crash(_point: str) -> None:
        raise RuntimeError("injected init crash")

    with pytest.raises(RuntimeError):
        ControlStore.initialize(root, fault_injector=crash)
    connection = sqlite3.connect(root / "control.sqlite3")
    try:
        connection.execute("CREATE TABLE foreign_state(value TEXT)")
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(ControlStoreSchemaMismatch, match="exactly match"):
        ControlStore.initialize(root)
    assert not (root / "control_store_identity.json").exists()


def test_candidate_exact_path_registration_is_idempotent_and_identity_bound(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    spec = _candidate_spec("one")

    first = store.register_candidate(spec)
    replay = store.register_candidate(spec)
    explicit_replay = store.register_candidate(
        replace(
            spec,
            registration_id=first["registration_id"],
            candidate_identity=first["candidate_identity"],
        )
    )

    assert replay == first == explicit_replay
    assert len(store._many("SELECT * FROM candidate_registrations", ())) == 1
    assert first["root_relative_path"] == "legacy/one"
    assert len(first["candidate_identity"]) == 64


def test_build_candidate_registration_uuid_is_release_digest_deterministic() -> None:
    first = build_candidate_registration_id(_digest("release-a"))
    replay = build_candidate_registration_id(_digest("release-a"))
    changed = build_candidate_registration_id(_digest("release-b"))
    assert first == replay
    assert first != changed
    with pytest.raises(StateConflict, match="SHA-256"):
        build_candidate_registration_id("not-a-release-digest")


@pytest.mark.parametrize(
    "field",
    [
        "artifact_root",
        "producer_provenance_digest_or_sentinel",
        "pit_provenance_digest_or_sentinel",
        "lineage_anchor",
    ],
)
def test_candidate_exact_path_rejects_immutable_drift(tmp_path, field: str) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    spec = _candidate_spec("drift")
    original = store.register_candidate(spec)
    replacement = (
        {"lineage_anchor": f"LEGACY_RECEIPT:other:{_digest('other-receipt')}"}
        if field == "lineage_anchor"
        else {field: _digest(f"different-{field}")}
    )

    with pytest.raises(StateConflict, match="different immutable"):
        store.register_candidate(replace(spec, **replacement))

    assert store.latest_candidate_registration(profile="qe_hmm_full_v1", scope="full") == original


def test_candidate_attested_time_is_monotonic_and_idempotent(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    registered = store.register_candidate(_candidate_spec("attested"))
    observed = datetime.now(UTC)

    attested = store.mark_candidate_attested(
        registration_id=registered["registration_id"],
        candidate_identity=registered["candidate_identity"],
        attested_at=observed,
    )
    replay = store.mark_candidate_attested(
        registration_id=registered["registration_id"],
        candidate_identity=registered["candidate_identity"],
        attested_at=observed,
    )

    assert replay == attested
    assert attested["state"] == "ATTESTED"
    with pytest.raises(StateConflict, match="cannot regress"):
        store.mark_candidate_attested(
            registration_id=registered["registration_id"],
            candidate_identity=registered["candidate_identity"],
            attested_at=observed - timedelta(seconds=1),
        )


def test_candidate_catalog_latest_and_list_are_bounded_without_filesystem_scan(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    older = store.register_candidate(_candidate_spec("older", cutoff=date(2026, 6, 30)))
    latest = store.register_candidate(_candidate_spec("latest", cutoff=date(2026, 7, 31)))

    rows = store.list_candidate_registrations(profile="qe_hmm_full_v1", scope="full", limit=1)
    assert rows == [latest]
    assert store.latest_candidate_registration(profile="qe_hmm_full_v1", scope="full") == latest
    assert older not in rows
    with pytest.raises(ValueError, match="1..100"):
        store.list_candidate_registrations(limit=101)


def test_candidate_latest_refuses_complete_rank_tie(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    observed = datetime(2026, 8, 1, tzinfo=UTC)
    artifact = _digest("same-artifact")
    store.register_candidate(
        _candidate_spec(
            "tie-a",
            artifact_root=artifact,
            last_attested_at=observed,
        )
    )
    store.register_candidate(
        _candidate_spec(
            "tie-b",
            artifact_root=artifact,
            last_attested_at=observed,
        )
    )

    with pytest.raises(StateConflict, match="ambiguous"):
        store.latest_candidate_registration(profile="qe_hmm_full_v1", scope="full")


def test_submission_idempotency_replays_same_hash_and_rejects_conflict(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    request = {
        "principal": "operator-a",
        "route": "POST:/api/v1/dataset-releases/runs",
        "idempotency_key": "monthly-2026-07",
        "request_hash": "request-hash-v1",
        "logical_request_key": "qe_hmm_full_v1:2026-07-31:full",
        "request_ref": "cas:request",
    }

    first = store.submit(**request)
    replay = store.submit(**request)

    assert first["replayed"] is False
    assert replay["replayed"] is True
    assert replay["submission_id"] == first["submission_id"]
    assert replay["state"] == "QUEUED_RESOLUTION"
    events = store.list_events(submission_id=first["submission_id"])
    assert [event["type"] for event in events] == ["SUBMISSION_QUEUED"]

    with pytest.raises(IdempotencyConflict, match="IDEMPOTENCY_CONFLICT"):
        store.submit(**{**request, "request_hash": "different-hash"})


def test_begin_immediate_rolls_back_entity_and_event_together(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")

    with pytest.raises(RuntimeError, match="injected"):
        with store.transaction() as connection:
            connection.execute(
                """
                INSERT INTO submissions(
                    submission_id,logical_request_key,request_ref,actor,state,created_at,updated_at
                ) VALUES ('s','logical','ref','actor','QUEUED_RESOLUTION','t','t')
                """
            )
            raise RuntimeError("injected")

    assert store.get_submission("s") is None


def test_schema_or_root_identity_drift_fails_closed_without_migration(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    connection = sqlite3.connect(store.db_path)
    try:
        connection.execute("PRAGMA user_version=99")
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(ControlStoreSchemaMismatch, match="SCHEMA_MISMATCH"):
        ControlStore(store.root)

    # Restore only so the second assertion tests identity drift independently.
    connection = sqlite3.connect(store.db_path)
    try:
        connection.execute("PRAGMA user_version=1")
        connection.commit()
    finally:
        connection.close()
    identity_path = store.root / "control_store_identity.json"
    identity = json.loads(identity_path.read_text(encoding="utf-8"))
    identity["normalized_root"] = "x:/moved-control-root"
    identity_path.write_text(json.dumps(identity), encoding="utf-8")

    with pytest.raises(ControlStoreSchemaMismatch, match="identity drifted"):
        ControlStore(store.root)


def test_ddl_digest_drift_fails_closed_without_implicit_migration(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    connection = sqlite3.connect(store.db_path)
    try:
        connection.execute(
            "UPDATE schema_metadata SET ddl_sha256=? WHERE singleton=1",
            ("0" * 64,),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(ControlStoreSchemaMismatch, match="SCHEMA_MISMATCH"):
        ControlStore(store.root)


def test_actual_sqlite_catalog_drift_fails_closed_even_if_metadata_is_unchanged(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    connection = sqlite3.connect(store.db_path)
    try:
        connection.execute("CREATE TABLE foreign_state(value TEXT)")
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(ControlStoreSchemaMismatch, match="SCHEMA_MISMATCH"):
        ControlStore(store.root)


def test_bounded_run_catalog_and_durable_commands(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    submission = store.submit(
        principal="operator",
        route="/runs",
        idempotency_key="page-and-command",
        request_hash="request-hash",
        logical_request_key="logical-key",
        request_ref="request-ref",
    )
    command = store.enqueue_command(
        target_type="submission",
        target_id=submission["submission_id"],
        command_type="CANCEL_REQUESTED",
        principal="operator",
        route="/submissions/cancel-request",
        idempotency_key="cancel-once",
        request_hash="cancel-request-hash",
        actor="operator",
    )
    active_alias = store.enqueue_command(
        target_type="submission",
        target_id=submission["submission_id"],
        command_type="CANCEL_REQUESTED",
        principal="operator",
        route="/submissions/cancel-request",
        idempotency_key="cancel-concurrent-alias",
        request_hash="cancel-request-hash",
        actor="operator",
    )
    assert active_alias["command_id"] == command["command_id"]
    assert active_alias["replayed"] is True
    with store.transaction() as connection:
        connection.execute(
            "UPDATE commands SET state='APPLIED',applied_at=? WHERE command_id=?",
            (utc_now(), command["command_id"]),
        )
    replay = store.enqueue_command(
        target_type="submission",
        target_id=submission["submission_id"],
        command_type="CANCEL_REQUESTED",
        principal="operator",
        route="/submissions/cancel-request",
        idempotency_key="cancel-once",
        request_hash="cancel-request-hash",
        actor="operator",
    )
    assert command["state"] == "QUEUED"
    assert replay["state"] == "QUEUED" and replay["applied_at"] is None
    assert replay["command_id"] == command["command_id"]
    assert replay["replayed"] is True
    next_invocation = store.enqueue_command(
        target_type="submission",
        target_id=submission["submission_id"],
        command_type="CANCEL_REQUESTED",
        principal="operator",
        route="/submissions/cancel-request",
        idempotency_key="cancel-after-terminal-command",
        request_hash="cancel-request-hash",
        actor="operator",
    )
    assert next_invocation["command_id"] != command["command_id"]
    assert next_invocation["replayed"] is False
    events = store.list_events(submission_id=submission["submission_id"], limit=201)
    assert [event["type"] for event in events] == [
        "SUBMISSION_QUEUED",
        "CANCEL_REQUESTED",
        "CANCEL_REQUESTED",
    ]
    assert store.latest_run() is None

    with pytest.raises(IdempotencyConflict, match="command key"):
        store.enqueue_command(
            target_type="submission",
            target_id=submission["submission_id"],
            command_type="CANCEL_REQUESTED",
            principal="operator",
            route="/submissions/cancel-request",
            idempotency_key="cancel-once",
            request_hash="different-cancel-request-hash",
            actor="operator",
        )


def test_list_runs_rejects_partial_cursor(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    with pytest.raises(ValueError, match="requires both"):
        store.list_runs(before_created_at="2026-08-01T00:00:00Z")
