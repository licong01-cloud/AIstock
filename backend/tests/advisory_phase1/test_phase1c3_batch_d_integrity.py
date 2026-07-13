"""Negative, recovery, and source-view evidence for Advisory Batch D."""

from __future__ import annotations

from datetime import timedelta
from copy import deepcopy
import hashlib
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.dataset_build import (
    AttemptOperation,
    BaseSnapshotIdentity,
    BuildCheckpoint,
    DatasetBuildError,
    InMemoryDatasetBuildRepository,
)
from backend.services.advisory_phase1.dataset_store import (
    LocalContentAddressedStore,
    LocalContentAddressedStoreError,
    StoredCasObject,
)
from backend.services.advisory_phase1.calculation_evidence import LocalCalculationEvidenceStore
from backend.services.advisory_phase1.outcome_engine import CalculationEvidenceBundle
from backend.services.advisory_phase1.snapshot_writer import (
    DatasetSnapshotMaterializer,
    DatasetSnapshotPipeline,
    DeterministicParquetWriter,
    DescriptorCalculationEvidenceReader,
    DiskBackedRows,
    FullParquetVerifier,
    FullParquetVerificationReceipt,
    MaterializationReceipt,
    PromotionReceipt,
    DatasetManifest,
    DatasetManifestCore,
    DatasetCapabilityManifest,
    DatasetCapabilityRow,
    LogicalRowPartitionSpool,
    LogicalDatasetRow,
    SnapshotFileIdentity,
    VerifiedDatasetFile,
    PostgresSnapshotSourceReader,
    SnapshotWriterError,
    _AttemptHeartbeat,
    _OUTCOME_LABEL_FIELDS,
    SNAPSHOT_ARROW_SCHEMAS_V1,
    _partition_key_from_logical_path,
    _sha256,
    _utc,
    _validate_capability_against_request,
    _validate_relational_rows,
)
from backend.tests.advisory_phase1.test_phase1c3_batch_d_writer import (
    UTC_TS,
    _capability_manifest,
    _fixture_rows,
    _identity,
    _materialized_build,
    _request,
    _write_full_fixture,
)


class _ControlCursor:
    def __enter__(self):  # type: ignore[no-untyped-def]
        return self

    def __exit__(self, *args):  # type: ignore[no-untyped-def]
        return None

    def execute(self, *args):  # type: ignore[no-untyped-def]
        return None


class _ReadOnlyConnection:
    def __init__(self) -> None:
        self.rolled_back = 0
        self.session: dict[str, object] = {}

    def __enter__(self):
        return self

    def __exit__(self, *args):  # type: ignore[no-untyped-def]
        return None

    def set_session(self, **kwargs):  # type: ignore[no-untyped-def]
        self.session = kwargs

    def cursor(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return _ControlCursor()

    def rollback(self) -> None:
        self.rolled_back += 1


def test_postgres_source_reader_materializes_every_role_from_one_read_only_view(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    fixture = _fixture_rows()
    connection = _ReadOnlyConnection()
    evidence = {
        row.values["calculation_evidence_sha256"]: CalculationEvidenceBundle.model_validate(
            row.values["calculation_evidence_json"]
        )
        for row in fixture["outcome_source_evidence"]
    }

    class EvidenceReader:
        @staticmethod
        def get(**descriptor):  # type: ignore[no-untyped-def]
            return evidence[descriptor["sha256"]]

    reader = PostgresSnapshotSourceReader(conn_factory=lambda: connection, evidence_reader=EvidenceReader())
    monkeypatch.setattr(reader, "_validate_authority_columns", lambda cursor: None)
    monkeypatch.setattr(reader, "_authority_summary_hash", lambda **kwargs: "frozen")
    monkeypatch.setattr(reader, "_validate_stream_counts", lambda **kwargs: None)
    monkeypatch.setattr(
        reader,
        "_selected_observations",
        lambda **kwargs: [dict(row.values) for row in fixture["selected_observations"]],
    )
    monkeypatch.setattr(
        reader,
        "_selected_labels",
        lambda **kwargs: [dict(row.values) for row in fixture["selected_labels"]],
    )
    by_query = {
        "batchd_signals": "canonical_signals",
        "batchd_observations": "observation_versions",
        "batchd_lineage": "lineage",
        "batchd_stages": "stage_summaries",
        "batchd_candidates": "stage_candidates",
        "batchd_outcomes": "outcome_labels",
        "batchd_gaps": "gaps",
        "batchd_source_revisions": "source_revisions",
    }

    def query(conn, sql, params, *, name):  # type: ignore[no-untyped-def]
        assert conn is connection and sql and isinstance(params, tuple)
        return DiskBackedRows(dict(row.values) for row in fixture[by_query[name]])

    monkeypatch.setattr(reader, "_query", query)
    rows = reader.read(_materialized_build())
    try:
        assert connection.session == {
            "isolation_level": "REPEATABLE READ",
            "readonly": True,
            "autocommit": False,
        }
        assert connection.rolled_back == 1
        assert {role: len(values) for role, values in rows.items()} == {
            role: len(values) for role, values in fixture.items()
        }
    finally:
        for values in rows.values():
            values.close()
    failing = PostgresSnapshotSourceReader(conn_factory=lambda: connection, evidence_reader=EvidenceReader())
    monkeypatch.setattr(failing, "_validate_authority_columns", lambda cursor: None)
    monkeypatch.setattr(
        failing,
        "_authority_summary_hash",
        lambda **kwargs: (_ for _ in ()).throw(SnapshotWriterError("SOURCE_FAILED", "authority failed")),
    )
    with pytest.raises(SnapshotWriterError, match="SOURCE_FAILED"):
        failing.read(_materialized_build())
    assert connection.rolled_back == 2


def test_authority_summary_rejects_capture_or_source_drift(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    request = _request()
    capture_rows = []
    for member in request.captures:
        payload = member.model_dump(mode="python", exclude={"date_start", "date_end"})
        payload.update(
            capture_status="COMPLETE",
            request_payload_jsonb={
                "capture_request_hash": member.capture_request_hash,
                "source_revision_set_id": member.source_revision_set_id,
                "source_revision_set_hash": member.source_revision_set_hash,
            },
        )
        capture_rows.append(payload)
    source = {
        "source_revision_set_id": request.snapshot_source_revision_set_id,
        "source_revision_set_hash": request.snapshot_source_revision_set_hash,
        "query_registry_hash": request.query_registry_hash,
        "requested_source_cutoff": request.label_as_of_ts,
        "label_as_of_ts": request.label_as_of_ts,
        "research_only": True,
        "member_count": 1,
        "schema_version": "advisory_phase1_source_revision_set_v2",
    }
    source_holder = [source]
    member_count = [1]

    class Cursor(_ControlCursor):
        def __init__(self) -> None:
            self.query = ""

        def execute(self, sql, params):  # type: ignore[no-untyped-def]
            self.query = sql

        def fetchall(self):  # type: ignore[no-untyped-def]
            return capture_rows

        def fetchone(self):  # type: ignore[no-untyped-def]
            return {"member_count": member_count[0]} if "count(*)" in self.query else source_holder[0]

    class Connection:
        def cursor(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            return Cursor()

    import backend.services.advisory_phase1.snapshot_writer as snapshot_writer

    monkeypatch.setattr(
        snapshot_writer,
        "_load_persisted_capture_request_read_only",
        lambda cur, row: SimpleNamespace(**row["request_payload_jsonb"]),
    )
    monkeypatch.setattr(
        snapshot_writer,
        "_capture_source_revision_identity",
        lambda parsed: (parsed.source_revision_set_id, parsed.source_revision_set_hash),
    )
    build = _materialized_build()
    assert len(PostgresSnapshotSourceReader._authority_summary_hash(conn=Connection(), build=build)) == 64
    saved_captures = list(capture_rows)
    capture_rows.clear()
    with pytest.raises(SnapshotWriterError, match="captures are missing"):
        PostgresSnapshotSourceReader._authority_summary_hash(conn=Connection(), build=build)
    capture_rows.extend(saved_captures)
    capture_rows[0]["capture_status"] = "RUNNING"
    with pytest.raises(SnapshotWriterError, match="capture authority differs"):
        PostgresSnapshotSourceReader._authority_summary_hash(conn=Connection(), build=build)
    capture_rows[0]["capture_status"] = "COMPLETE"
    original_payload = capture_rows[0]["request_payload_jsonb"]
    capture_rows[0]["request_payload_jsonb"] = {**original_payload, "capture_request_hash": "0" * 64}
    with pytest.raises(SnapshotWriterError, match="capture authority differs"):
        PostgresSnapshotSourceReader._authority_summary_hash(conn=Connection(), build=build)
    capture_rows[0]["request_payload_jsonb"] = original_payload
    source_holder[0] = None
    with pytest.raises(SnapshotWriterError, match="source revision authority"):
        PostgresSnapshotSourceReader._authority_summary_hash(conn=Connection(), build=build)
    source_holder[0] = source
    member_count[0] = 0
    with pytest.raises(SnapshotWriterError, match="membership count"):
        PostgresSnapshotSourceReader._authority_summary_hash(conn=Connection(), build=build)
    member_count[0] = 1
    capture_rows[0]["membership_hash"] = "0" * 64
    with pytest.raises(SnapshotWriterError, match="capture authority differs"):
        PostgresSnapshotSourceReader._authority_summary_hash(conn=Connection(), build=build)


def test_selected_observation_payload_and_stream_count_contracts(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from backend.tests.advisory_phase1.test_label_capture import _request as label_capture_request

    label_request = label_capture_request(batch_id="capture-2")
    payload = _request().model_dump(mode="python", exclude={"build_request_hash"})
    payload["selected_observation_mappings"] = (
        {
            "identity_id": label_request.selected_observation_mappings[0].selected_mapping_id,
            "identity_hash": label_request.selected_observation_mappings[0].selected_mapping_hash,
        },
    )
    payload.pop("selected_observation_mapping_set_hash")
    request = type(_request()).model_validate(payload)
    repository = InMemoryDatasetBuildRepository(now_provider=lambda: UTC_TS)
    build = repository.create_or_get(request, actor="test")
    reader = PostgresSnapshotSourceReader(conn_factory=lambda: None, evidence_reader=None)
    monkeypatch.setattr(
        reader,
        "_load_persisted_capture_requests",
        lambda *args, **kwargs: [
            (
                {
                    "capture_batch_id": "capture-2",
                    "capture_request_hash": label_request.capture_request_hash,
                },
                label_request,
            )
        ],
    )
    selected = reader._selected_observations(conn=object(), build=build)
    assert selected[0]["selected_mapping_id"] == label_request.selected_observation_mappings[0].selected_mapping_id
    conflicting_reference = label_request.selected_observation_mappings[0].model_copy(
        update={"terminal_revision_no": 2}
    )
    conflicting_request = label_request.model_copy(
        update={"selected_observation_mappings": (conflicting_reference,)}
    )
    monkeypatch.setattr(
        reader,
        "_load_persisted_capture_requests",
        lambda *args, **kwargs: [
            ({"capture_request_hash": label_request.capture_request_hash}, label_request),
            ({"capture_request_hash": conflicting_request.capture_request_hash}, conflicting_request),
        ],
    )
    with pytest.raises(SnapshotWriterError, match="conflicts across captures"):
        reader._selected_observations(conn=object(), build=build)
    monkeypatch.setattr(
        reader,
        "_load_persisted_capture_requests",
        lambda *args, **kwargs: [({"capture_request_hash": label_request.capture_request_hash}, object())],
    )
    with pytest.raises(SnapshotWriterError, match="payload is invalid"):
        reader._selected_observations(conn=object(), build=build)

    fixture = _fixture_rows()
    spooled = {role: DiskBackedRows(values) for role, values in fixture.items()}

    class CountCursor(_ControlCursor):
        def __init__(self, counts):  # type: ignore[no-untyped-def]
            self.counts = iter(counts)

        def fetchone(self):  # type: ignore[no-untyped-def]
            return (next(self.counts),)

    class CountConnection:
        def __init__(self, counts):  # type: ignore[no-untyped-def]
            self.counts = counts

        def cursor(self):  # type: ignore[no-untyped-def]
            return CountCursor(self.counts)

    authority_counts = [1, 1, 1, 1, 1, 2, 1, 0, 1]
    try:
        PostgresSnapshotSourceReader._validate_stream_counts(
            conn=CountConnection(authority_counts),
            build=_materialized_build(),
            rows=spooled,
            signal_ids=["signal-1"],
            observation_ids=["observation-1"],
            stage_ids=["stage-1"],
        )
        with pytest.raises(SnapshotWriterError, match="canonical_signals"):
            PostgresSnapshotSourceReader._validate_stream_counts(
                conn=CountConnection([0]),
                build=_materialized_build(),
                rows=spooled,
                signal_ids=["signal-1"],
                observation_ids=["observation-1"],
                stage_ids=["stage-1"],
            )
    finally:
        for values in spooled.values():
            values.close()


def test_selected_label_reconstruction_exhaustively_fails_closed(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from backend.services.advisory_phase1.label_builder import (
        InMemoryOutcomeLabelRepository,
        enumerate_candidate_labels,
    )
    from backend.services.advisory_phase1.label_builder_postgres import PostgresOutcomeLabelRepository
    from backend.services.advisory_phase1.outcome_engine import OutcomeEngine
    from backend.tests.advisory_phase1.test_label_builder import (
        AS_OF,
        _append_request,
        _candidate_request,
        _context,
    )

    context, policies, source_set = _context()
    descriptor = enumerate_candidate_labels(context=context, label_policy_bundle=policies.bundle).descriptors[0]
    result = OutcomeEngine().calculate(
        _candidate_request(descriptor=descriptor, policies=policies, source_set=source_set)
    )
    append = _append_request(
        descriptor=descriptor,
        policies=policies,
        source_set=source_set,
        result=result,
        uri="file:///evidence",
    )
    version = InMemoryOutcomeLabelRepository(now_provider=lambda: AS_OF).append(
        request=append,
        created_by_capture_batch_id="capture-2",
    )
    monkeypatch.setattr(PostgresOutcomeLabelRepository, "_from_row", lambda self, row: version)
    reader = PostgresSnapshotSourceReader(conn_factory=lambda: None, evidence_reader=object())
    with pytest.raises(SnapshotWriterError, match="cannot be reconstructed"):
        reader._selected_labels(
            build=_materialized_build(),
            outcome_rows=({"label_key_hash": version.label_key_hash},),
        )
    future = version.model_copy(update={"computed_at": AS_OF + timedelta(days=1)})
    monkeypatch.setattr(PostgresOutcomeLabelRepository, "_from_row", lambda self, row: future)
    with pytest.raises(SnapshotWriterError, match="cannot be reconstructed"):
        reader._selected_labels(
            build=_materialized_build(),
            outcome_rows=({"label_key_hash": future.label_key_hash},),
        )
    universe_owner = version.owner.model_copy(
        update={"observation_version_id": None, "candidate_stage_evidence_id": None}
    )
    universe = version.model_copy(update={"owner": universe_owner})
    monkeypatch.setattr(PostgresOutcomeLabelRepository, "_from_row", lambda self, row: universe)
    with pytest.raises(SnapshotWriterError, match="cannot be reconstructed"):
        reader._selected_labels(
            build=_materialized_build(),
            outcome_rows=({"label_key_hash": universe.label_key_hash},),
        )
    with pytest.raises(SnapshotWriterError, match="cannot be reconstructed"):
        reader._selected_labels(build=_materialized_build(), outcome_rows=())


def test_authority_column_schema_parity_accepts_exact_and_rejects_drift() -> None:
    table_fields = {
        "advisory_signal_observation": {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["canonical_signals"]},
        "advisory_signal_observation_version": {
            field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["observation_versions"]
        },
        "advisory_signal_observation_lineage": {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["lineage"]},
        "advisory_signal_stage_evidence": {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["stage_summaries"]},
        "advisory_signal_stage_candidate": {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["stage_candidates"]},
        "advisory_dataset_build_gap": {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["gaps"]} - {"source_kind"},
    }
    first = [(table, column) for table, columns in table_fields.items() for column in columns | {"created_at"}]
    outcomes = [(column,) for column in set(_OUTCOME_LABEL_FIELDS) | {"created_at"}]
    sources = [(column,) for column in {field.name for field in SNAPSHOT_ARROW_SCHEMAS_V1["source_revisions"]} | {"created_at"}]

    class Cursor(_ControlCursor):
        def __init__(self, responses):  # type: ignore[no-untyped-def]
            self.responses = iter(responses)
            self.current = []

        def execute(self, *args):  # type: ignore[no-untyped-def]
            self.current = next(self.responses)

        def fetchall(self):  # type: ignore[no-untyped-def]
            return self.current

    PostgresSnapshotSourceReader._validate_authority_columns(Cursor([first, outcomes, sources]))
    drift = list(first) + [("advisory_signal_observation", "unexpected")]
    with pytest.raises(SnapshotWriterError, match="authority columns differ"):
        PostgresSnapshotSourceReader._validate_authority_columns(Cursor([drift]))
    bad_outcomes = list(outcomes) + [("unexpected",)]
    with pytest.raises(SnapshotWriterError, match="outcome_label"):
        PostgresSnapshotSourceReader._validate_authority_columns(Cursor([first, bad_outcomes]))


def test_server_cursor_query_and_source_failure_paths_close_spools(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    class Cursor(_ControlCursor):
        def __init__(self, *, fail: bool = False) -> None:
            self.fail = fail
            self.itersize = 0
            self.batches = iter(([{"value": 1}], [{"value": 2}], []))

        def execute(self, sql, params):  # type: ignore[no-untyped-def]
            if self.fail:
                raise RuntimeError("query failed")

        def fetchmany(self, size):  # type: ignore[no-untyped-def]
            return next(self.batches)

    class Connection:
        def __init__(self, *, fail: bool = False) -> None:
            self.fail = fail

        def cursor(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            return Cursor(fail=self.fail)

    monkeypatch.setattr(PostgresSnapshotSourceReader, "_check_rss", lambda: None)
    rows = PostgresSnapshotSourceReader._query(Connection(), "SELECT 1", (), name="stream")
    try:
        assert list(rows) == [{"value": 1}, {"value": 2}]
    finally:
        rows.close()
    with pytest.raises(RuntimeError, match="query failed"):
        PostgresSnapshotSourceReader._query(Connection(fail=True), "SELECT 1", (), name="stream")


def test_source_mapping_count_and_materializer_configuration_drift_fail_closed(monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
    from backend.tests.advisory_phase1.test_label_capture import _request as label_capture_request

    label_request = label_capture_request(batch_id="capture-2")
    reader = PostgresSnapshotSourceReader(conn_factory=lambda: None, evidence_reader=None)
    monkeypatch.setattr(
        reader,
        "_load_persisted_capture_requests",
        lambda *args, **kwargs: [
            (
                {
                    "capture_batch_id": "capture-2",
                    "capture_request_hash": label_request.capture_request_hash,
                },
                label_request,
            )
        ],
    )
    with pytest.raises(SnapshotWriterError, match="mapping set differs"):
        reader._selected_observations(conn=object(), build=_materialized_build())

    fixture = _fixture_rows()
    spooled = {role: DiskBackedRows(values) for role, values in fixture.items()}
    spooled["selected_labels"].close()
    spooled["selected_labels"] = DiskBackedRows()

    class CountCursor(_ControlCursor):
        def __init__(self) -> None:
            self.counts = iter((1, 1, 1, 1, 1, 2, 1, 0, 1))

        def fetchone(self):  # type: ignore[no-untyped-def]
            return (next(self.counts),)

    class CountConnection:
        def cursor(self):  # type: ignore[no-untyped-def]
            return CountCursor()

    try:
        with pytest.raises(SnapshotWriterError, match="derived stream counts"):
            PostgresSnapshotSourceReader._validate_stream_counts(
                conn=CountConnection(),
                build=_materialized_build(),
                rows=spooled,
                signal_ids=["signal-1"],
                observation_ids=["observation-1"],
                stage_ids=["stage-1"],
            )
    finally:
        for values in spooled.values():
            values.close()

    class NeverRead:
        @staticmethod
        def read(build):  # type: ignore[no-untyped-def]
            raise AssertionError("invalid configuration must fail before source access")

    store = LocalContentAddressedStore(
        root=(tmp_path / "store").resolve(),
        repository_root=(tmp_path / "repo").resolve(),
        store_identity=_identity(),
    )
    materializer = DatasetSnapshotMaterializer(
        source_reader=NeverRead(), writer=DeterministicParquetWriter()
    )
    for update, message in (
        ({"writer_version": "wrong"}, "writer/builder"),
        ({"compression_config": {"codec": "snappy"}}, "compression config"),
    ):
        request_payload = _request().model_dump(
            mode="python", exclude={"build_request_hash", "compression_config_hash"}
        )
        request_payload.update(update)
        request = type(_request()).model_validate(request_payload)
        build = InMemoryDatasetBuildRepository(now_provider=lambda: UTC_TS).create_or_get(request, actor="test")
        with pytest.raises(SnapshotWriterError, match=message):
            materializer.materialize(build=build, attempt_id="attempt-1", store=store)


def test_descriptor_evidence_reader_reopens_typed_canonical_blob(tmp_path) -> None:  # type: ignore[no-untyped-def]
    repository_root = (tmp_path / "repo").resolve()
    repository_root.mkdir()
    identity = {
        "backend": "LOCAL_FILESYSTEM_V1",
        "durability_mode": LocalContentAddressedStore.expected_durability_mode(),
        "atomic_publish_mode": "HARDLINK_CREATE_IF_ABSENT_V1",
    }
    store = LocalCalculationEvidenceStore(
        root=(tmp_path / "evidence").resolve(),
        repository_root=repository_root,
        store_identity=identity,
    )
    bundle = CalculationEvidenceBundle(evidence_payload={"typed": True})
    stored = store.put(bundle)
    reader = DescriptorCalculationEvidenceReader(repository_root=repository_root)
    descriptor = {
        "uri": stored.uri,
        "sha256": stored.sha256,
        "size_bytes": stored.size_bytes,
        "store_backend_hash": stored.store_backend_hash,
    }
    assert reader.get(**descriptor) == bundle
    assert reader.get(**descriptor) == bundle
    wrong = tmp_path / "evidence" / "wrong"
    wrong.write_bytes(b"wrong")
    with pytest.raises(SnapshotWriterError, match="URI is not canonical"):
        reader.get(**dict(descriptor, uri=wrong.as_uri()))
    with pytest.raises(SnapshotWriterError, match="identity differs"):
        reader.get(**dict(descriptor, store_backend_hash="0" * 64))


def test_attempt_heartbeat_propagates_repository_failure() -> None:
    class Repository:
        @staticmethod
        def heartbeat_attempt(**kwargs):  # type: ignore[no-untyped-def]
            raise DatasetBuildError("HEARTBEAT_FAILED", "lost fencing")

    attempt = SimpleNamespace(attempt_id="attempt-1", fencing_token=1)
    with pytest.raises(DatasetBuildError, match="HEARTBEAT_FAILED"):
        with _AttemptHeartbeat(repository=Repository(), attempt=attempt, interval_seconds=0.001):
            time.sleep(0.02)
    with pytest.raises(RuntimeError, match="primary failure"):
        with _AttemptHeartbeat(repository=Repository(), attempt=attempt, interval_seconds=0.001):
            time.sleep(0.02)
            raise RuntimeError("primary failure")

    heartbeat = _AttemptHeartbeat(repository=Repository(), attempt=attempt)
    heartbeat._thread = SimpleNamespace(join=lambda timeout: None, is_alive=lambda: True)
    with pytest.raises(SnapshotWriterError, match="did not stop"):
        heartbeat.__exit__(None, None, None)


def test_low_level_identity_spool_rss_and_materializer_fail_closed(monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(ValueError, match="sha256"):
        _sha256("bad", field_name="hash")
    with pytest.raises(ValueError, match="timezone"):
        _utc(UTC_TS.replace(tzinfo=None), field_name="timestamp")
    assert _utc(UTC_TS, field_name="timestamp") == UTC_TS
    with pytest.raises(ValueError, match="unknown dataset role"):
        LogicalDatasetRow(logical_role="unknown", values={})

    identity = SnapshotFileIdentity(
        logical_path="a",
        logical_role="role",
        partition_key_hash="1" * 64,
        ordinal=0,
        sha256="2" * 64,
        size_bytes=1,
        row_count=0,
        schema_fingerprint="3" * 64,
        partition_content_hash="4" * 64,
        compression="none",
        writer_version="writer",
    )
    second = identity.model_copy(update={"logical_path": "b"})
    files_hash = canonical_json_sha256([identity.canonical_identity(), second.canonical_identity()])
    with pytest.raises(ValueError, match="ordinals are not unique"):
        MaterializationReceipt(
            build_id="build",
            attempt_id="attempt",
            source_identity_hash="5" * 64,
            capture_set_hash="6" * 64,
            source_revision_set_hash="7" * 64,
            files=(identity, second),
            file_set_hash=files_hash,
        )

    spool = LogicalRowPartitionSpool()
    try:
        row = _fixture_rows()["canonical_signals"][0]
        spool.append(row)
        with pytest.raises(SnapshotWriterError, match="duplicate frozen sort keys"):
            spool.append(row)
    finally:
        spool.close()

    monkeypatch.setattr(
        "psutil.Process",
        lambda pid: SimpleNamespace(memory_info=lambda: SimpleNamespace(rss=3 * 1024**3)),
    )
    with pytest.raises(SnapshotWriterError, match="RSS exceeds"):
        PostgresSnapshotSourceReader._check_rss()

    class WrongRoleSource:
        @staticmethod
        def read(build):  # type: ignore[no-untyped-def]
            rows = {role: [] for role in SNAPSHOT_ARROW_SCHEMAS_V1}
            rows["gaps"] = [_fixture_rows()["canonical_signals"][0]]
            return rows

    store = LocalContentAddressedStore(
        root=(tmp_path / "store").resolve(),
        repository_root=(tmp_path / "repo").resolve(),
        store_identity=_identity(),
    )
    materializer = DatasetSnapshotMaterializer(
        source_reader=WrongRoleSource(), writer=DeterministicParquetWriter()
    )
    with pytest.raises(SnapshotWriterError, match="wrong role"):
        materializer.materialize(
            build=InMemoryDatasetBuildRepository(now_provider=lambda: UTC_TS).create_or_get(
                _request(), actor="test"
            ),
            attempt_id="attempt-1",
            store=store,
        )


def _pipeline(repository, store, source=None):  # type: ignore[no-untyped-def]
    class Source:
        @staticmethod
        def read(build):  # type: ignore[no-untyped-def]
            return {role: list(rows) for role, rows in _fixture_rows().items()}

    return DatasetSnapshotPipeline(
        repository=repository,
        materializer=DatasetSnapshotMaterializer(
            source_reader=source or Source(),
            writer=DeterministicParquetWriter(),
        ),
        store=store,
    )


def test_expired_materialize_attempt_recovers_and_reaches_sealed(tmp_path) -> None:  # type: ignore[no-untyped-def]
    now = [UTC_TS]
    repository = InMemoryDatasetBuildRepository(now_provider=lambda: now[0])
    build = repository.create_or_get(_request(), actor="test")
    expired_attempt = repository.start_attempt(
        build_id=build.build_id,
        operation=AttemptOperation.MATERIALIZE,
        expected_build_row_version=build.row_version,
        expected_checkpoint=build.checkpoint,
        lease_owner_id="dead-worker",
        lease_token="dead-lease",
        lease_seconds=1,
        operation_request_hash="1" * 64,
    )
    now[0] += timedelta(seconds=2)
    store = LocalContentAddressedStore(
        root=tmp_path / "store", repository_root=tmp_path / "repo", store_identity=_identity()
    )
    partial = store.staging_path(
        build_id=build.build_id,
        attempt_id=expired_attempt.attempt_id,
        logical_path="partial/file.parquet",
    )
    partial.parent.mkdir(parents=True)
    partial.write_bytes(b"partial")
    sealed = _pipeline(repository, store).run(build_id=build.build_id, actor="recovery-worker")
    assert sealed.checkpoint is BuildCheckpoint.SEALED
    assert not partial.exists()
    assert any(event.event_type.value == "RECOVERY_STARTED" for event in repository.events_for(build.build_id))


def test_base_snapshot_reuse_rechecks_invalidation_and_reuses_exact_blobs(tmp_path) -> None:  # type: ignore[no-untyped-def]
    allowed = [True]

    def validate(base):  # type: ignore[no-untyped-def]
        if not allowed[0]:
            raise DatasetBuildError("BASE_INVALIDATED", "base was invalidated")

    repository = InMemoryDatasetBuildRepository(
        now_provider=lambda: UTC_TS,
        base_snapshot_validator=validate,
    )
    store = LocalContentAddressedStore(
        root=tmp_path / "store", repository_root=tmp_path / "repo", store_identity=_identity()
    )
    first = repository.create_or_get(_request(), actor="test")
    first = _pipeline(repository, store).run(build_id=first.build_id, actor="test")
    first_files = repository.snapshot_files(str(first.sealed_snapshot_id))
    snapshot = repository._snapshots[str(first.sealed_snapshot_id)]
    base = BaseSnapshotIdentity(
        snapshot_id=str(first.sealed_snapshot_id),
        snapshot_content_hash=str(snapshot.snapshot_content_hash),
        manifest_sha256=str(first.promoted_manifest_hash),
        snapshot_source_revision_set_hash=snapshot.snapshot_source_revision_set_hash,
        capture_set_hash=snapshot.capture_set_hash,
        policy_compatibility_hash=_request().policy_compatibility_hash,
    )
    child_payload = _request().model_dump(mode="python", exclude={"build_request_hash"})
    child_payload.update(base_snapshot=base, code_commit="def456")
    child_request = type(_request()).model_validate(child_payload)
    child = repository.create_or_get(child_request, actor="test")
    child = _pipeline(repository, store).run(build_id=child.build_id, actor="test")
    assert [item.content_uri for item in repository.snapshot_files(str(child.sealed_snapshot_id))] == [
        item.content_uri for item in first_files
    ]

    pending_payload = _request().model_dump(mode="python", exclude={"build_request_hash"})
    pending_payload.update(base_snapshot=base, code_commit="ghi789")
    pending_request = type(_request()).model_validate(pending_payload)
    pending = repository.create_or_get(pending_request, actor="test")
    allowed[0] = False
    with pytest.raises(DatasetBuildError, match="BASE_INVALIDATED"):
        _pipeline(repository, store).run(build_id=pending.build_id, actor="test")
    assert repository.get_build(pending.build_id).checkpoint is BuildCheckpoint.REQUESTED


def test_seal_commit_timeout_is_resolved_by_exact_readback(tmp_path) -> None:  # type: ignore[no-untyped-def]
    class TimeoutAfterCommitRepository(InMemoryDatasetBuildRepository):
        raised = False

        def save_sealed_snapshot(self, snapshot, *, actor):  # type: ignore[no-untyped-def]
            result = super().save_sealed_snapshot(snapshot, actor=actor)
            if not self.raised:
                self.raised = True
                raise TimeoutError("client lost seal response")
            return result

    repository = TimeoutAfterCommitRepository(now_provider=lambda: UTC_TS)
    build = repository.create_or_get(_request(), actor="test")
    store = LocalContentAddressedStore(
        root=tmp_path / "store", repository_root=tmp_path / "repo", store_identity=_identity()
    )
    sealed = _pipeline(repository, store).run(build_id=build.build_id, actor="test")
    assert sealed.checkpoint is BuildCheckpoint.SEALED
    assert _pipeline(repository, store).run(build_id=build.build_id, actor="test").sealed_snapshot_id == sealed.sealed_snapshot_id


@pytest.mark.parametrize(
    ("method_name", "expected_checkpoint"),
    (
        ("complete_materialize", BuildCheckpoint.REQUESTED),
        ("complete_full_verify", BuildCheckpoint.MATERIALIZED),
        ("complete_promote", BuildCheckpoint.VERIFIED),
        ("save_sealed_snapshot", BuildCheckpoint.PROMOTED),
    ),
)
def test_each_repository_checkpoint_failure_resumes_without_fake_success(
    method_name, expected_checkpoint, monkeypatch, tmp_path  # type: ignore[no-untyped-def]
) -> None:
    repository = InMemoryDatasetBuildRepository(now_provider=lambda: UTC_TS)
    build = repository.create_or_get(_request(), actor="test")
    store = LocalContentAddressedStore(
        root=(tmp_path / method_name / "store").resolve(),
        repository_root=(tmp_path / method_name / "repo").resolve(),
        store_identity=_identity(),
    )
    pipeline = _pipeline(repository, store)
    original = getattr(repository, method_name)
    calls = [0]

    def fail_once(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls[0] += 1
        if calls[0] == 1:
            raise RuntimeError(f"crash before {method_name} checkpoint")
        return original(*args, **kwargs)

    monkeypatch.setattr(repository, method_name, fail_once)
    with pytest.raises(RuntimeError, match="crash before"):
        pipeline.run(build_id=build.build_id, actor="test")
    failed = repository.get_build(build.build_id)
    assert failed.checkpoint is expected_checkpoint
    assert failed.current_attempt_id is None
    monkeypatch.setattr(repository, method_name, original)
    assert pipeline.run(build_id=build.build_id, actor="test").checkpoint is BuildCheckpoint.SEALED


def test_partial_cas_publication_and_source_failure_resume_exactly(monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
    class Source:
        calls = 0

        @classmethod
        def read(cls, build):  # type: ignore[no-untyped-def]
            cls.calls += 1
            if cls.calls == 1:
                raise RuntimeError("source transaction interrupted")
            return {role: list(rows) for role, rows in _fixture_rows().items()}

    repository = InMemoryDatasetBuildRepository(now_provider=lambda: UTC_TS)
    build = repository.create_or_get(_request(), actor="test")
    store = LocalContentAddressedStore(
        root=(tmp_path / "store").resolve(),
        repository_root=(tmp_path / "repo").resolve(),
        store_identity=_identity(),
    )
    pipeline = _pipeline(repository, store, source=Source())
    with pytest.raises(RuntimeError, match="source transaction"):
        pipeline.run(build_id=build.build_id, actor="test")
    assert repository.get_build(build.build_id).checkpoint is BuildCheckpoint.REQUESTED

    original_put = store.put_blob_bytes
    calls = [0]

    def fail_publish(payload):  # type: ignore[no-untyped-def]
        calls[0] += 1
        if calls[0] == 3:
            raise LocalContentAddressedStoreError("CAS_INTERRUPTED", "partial publication")
        return original_put(payload)

    monkeypatch.setattr(store, "put_blob_bytes", fail_publish)
    with pytest.raises(LocalContentAddressedStoreError, match="CAS_INTERRUPTED"):
        pipeline.run(build_id=build.build_id, actor="test")
    assert repository.get_build(build.build_id).checkpoint is BuildCheckpoint.VERIFIED
    monkeypatch.setattr(store, "put_blob_bytes", original_put)
    assert pipeline.run(build_id=build.build_id, actor="test").checkpoint is BuildCheckpoint.SEALED


def test_verifier_rejects_descriptor_evidence_source_and_capability_drift(tmp_path) -> None:  # type: ignore[no-untyped-def]
    _, files = _write_full_fixture(tmp_path)
    verifier = FullParquetVerifier()
    data = next(item for item in files if item.logical_role == "canonical_signals")
    with pytest.raises(SnapshotWriterError, match="descriptor differs"):
        verifier.verify_files(
            build=_materialized_build(),
            files=tuple(item.model_copy(update={"compression": "snappy"}) if item == data else item for item in files),
            capability_manifest=_capability_manifest(),
        )
    source = next(item for item in files if item.logical_role == "source_revisions")
    with pytest.raises((SnapshotWriterError, ValueError)):
        verifier.verify_files(
            build=_materialized_build(),
            files=tuple(
                item.model_copy(update={"partition_content_hash": "0" * 64}) if item == source else item
                for item in files
            ),
            capability_manifest=_capability_manifest(),
        )


def test_canonical_receipt_models_reject_forged_hashes_duplicates_and_readiness(tmp_path) -> None:  # type: ignore[no-untyped-def]
    repository = InMemoryDatasetBuildRepository(now_provider=lambda: UTC_TS)
    build = repository.create_or_get(_request(), actor="test")
    store = LocalContentAddressedStore(
        root=tmp_path / "store", repository_root=tmp_path / "repo", store_identity=_identity()
    )
    pipeline = _pipeline(repository, store)
    sealed = pipeline.run(build_id=build.build_id, actor="test")
    verification, manifest, promotion, _ = pipeline._publish(sealed)

    identity = verification.files[0].file
    materialization_payload = {
        "build_id": build.build_id,
        "attempt_id": "attempt-1",
        "source_identity_hash": "1" * 64,
        "capture_set_hash": build.request.capture_set_hash,
        "source_revision_set_hash": build.request.snapshot_source_revision_set_hash,
        "files": (identity,),
        "file_set_hash": canonical_json_sha256([identity.canonical_identity()]),
    }
    materialization = MaterializationReceipt.model_validate(materialization_payload)
    assert materialization.receipt_hash
    for update, message in (
        ({"files": (identity, identity)}, "paths are not unique"),
        ({"file_set_hash": "0" * 64}, "file-set hash"),
        ({"receipt_hash": "0" * 64}, "receipt hash"),
    ):
        with pytest.raises(ValueError, match=message):
            MaterializationReceipt.model_validate({**materialization_payload, **update})

    verified_payload = verification.files[0].model_dump(mode="python")
    verified_payload["observed_size_bytes"] += 1
    with pytest.raises(ValueError, match="immutable descriptor"):
        VerifiedDatasetFile.model_validate(verified_payload)
    receipt_payload = verification.model_dump(mode="python", exclude={"receipt_hash"})
    for update, message in (
        ({"verification_contract_version": "wrong"}, "contract is invalid"),
        ({"files": (verification.files[0], verification.files[0])}, "paths are not unique"),
        ({"verified_content_set_hash": "0" * 64}, "content file-set hash"),
        ({"relational_closure_hash": "0" * 64}, "closure hash"),
        ({"receipt_hash": "0" * 64}, "receipt hash"),
    ):
        with pytest.raises(ValueError, match=message):
            FullParquetVerificationReceipt.model_validate({**receipt_payload, **update})

    rows = list(_capability_manifest().rows)
    with pytest.raises(ValueError, match="duplicate"):
        DatasetCapabilityManifest(rows=tuple(rows + [rows[0]]))
    wrong_readiness = tuple(
        row.model_copy(update={"status": "true"}) if row.capability == "MODEL_TRAINING_READY" else row
        for row in rows
    )
    with pytest.raises(ValueError, match="explicitly false"):
        DatasetCapabilityManifest(rows=wrong_readiness)
    with pytest.raises(ValueError, match="manifest hash"):
        DatasetCapabilityManifest(rows=tuple(rows), manifest_hash="0" * 64)
    assert DatasetCapabilityRow(component="X", capability="Y", status="FULL", reason_codes=("b", "a", "a")).reason_codes == (
        "a",
        "b",
    )

    core_payload = manifest.core.model_dump(mode="python", exclude={"manifest_core_sha256"})
    with pytest.raises(ValueError, match="files are not unique"):
        DatasetManifestCore.model_validate({**core_payload, "files": (manifest.core.files[0],) * 2})
    with pytest.raises(ValueError, match="core hash"):
        DatasetManifestCore.model_validate({**core_payload, "manifest_core_sha256": "0" * 64})
    manifest_payload = manifest.model_dump(mode="python", exclude={"manifest_sha256"})
    with pytest.raises(ValueError, match="manifest hash"):
        DatasetManifest.model_validate({**manifest_payload, "manifest_sha256": "0" * 64})
    promotion_payload = promotion.model_dump(mode="python", exclude={"receipt_sha256"})
    with pytest.raises(ValueError, match="blobs are not unique"):
        PromotionReceipt.model_validate({**promotion_payload, "blobs": (promotion.blobs[0],) * 2})
    foreign = promotion.blobs[0].model_copy(
        update={"blob": promotion.blobs[0].blob.model_copy(update={"store_backend_hash": "0" * 64})}
    )
    with pytest.raises(ValueError, match="backend differs"):
        PromotionReceipt.model_validate({**promotion_payload, "blobs": (foreign,)})
    with pytest.raises(ValueError, match="receipt hash"):
        PromotionReceipt.model_validate({**promotion_payload, "receipt_sha256": "0" * 64})


def test_writer_and_partition_helpers_reject_every_noncanonical_input(tmp_path) -> None:  # type: ignore[no-untyped-def]
    writer = DeterministicParquetWriter()
    canonical = _fixture_rows()["canonical_signals"][0]
    with pytest.raises(SnapshotWriterError, match="unknown Parquet"):
        writer.write_parquet(
            path=tmp_path / "unknown.parquet",
            logical_path="unknown/part-00000.parquet",
            logical_role="unknown",
            partition_key={},
            ordinal=0,
            rows=(),
        )
    mixed = _fixture_rows()["stage_candidates"][0]
    with pytest.raises(SnapshotWriterError, match="mixed logical roles"):
        writer.write_parquet(
            path=tmp_path / "mixed.parquet",
            logical_path="canonical_signals/part-00000.parquet",
            logical_role="canonical_signals",
            partition_key={},
            ordinal=0,
            rows=(mixed,),
        )
    with pytest.raises(SnapshotWriterError, match="caller sort key"):
        writer.write_parquet(
            path=tmp_path / "sort.parquet",
            logical_path="canonical_signals/part-00000.parquet",
            logical_role="canonical_signals",
            partition_key={},
            ordinal=0,
            rows=(canonical.model_copy(update={"sort_key": ("wrong",)}),),
        )
    with pytest.raises(SnapshotWriterError, match="sort/unique"):
        writer.write_parquet(
            path=tmp_path / "duplicate.parquet",
            logical_path="canonical_signals/part-00000.parquet",
            logical_role="canonical_signals",
            partition_key={},
            ordinal=0,
            rows=(canonical, canonical),
        )
    existing = tmp_path / "existing.parquet"
    existing.write_bytes(b"occupied")
    with pytest.raises(SnapshotWriterError, match="cannot overwrite"):
        writer.write_parquet(
            path=existing,
            logical_path="canonical_signals/part-00000.parquet",
            logical_role="canonical_signals",
            partition_key={},
            ordinal=0,
            rows=(canonical,),
        )
    with pytest.raises(SnapshotWriterError):
        DeterministicParquetWriter(writer_version="unsupported")

    assert _partition_key_from_logical_path(
        "canonical_signals/year=2026/month=07/part-00000.parquet",
        logical_role="canonical_signals",
    ) == {"year": "2026", "month": "07"}
    for path in (
        "wrong/part-00000.parquet",
        "canonical_signals/not-a-partition/part-00000.parquet",
        "canonical_signals/=2026/part-00000.parquet",
        "canonical_signals/year=/part-00000.parquet",
        "canonical_signals/year=2026/year=2027/part-00000.parquet",
    ):
        with pytest.raises(SnapshotWriterError, match="path|key"):
            _partition_key_from_logical_path(path, logical_role="canonical_signals")


def test_relational_verifier_rejects_each_broken_cross_file_closure() -> None:
    valid = {role: [dict(row.values) for row in rows] for role, rows in _fixture_rows().items()}
    summary, observations, labels = _validate_relational_rows(
        build=_materialized_build(),
        rows_by_role=valid,
        capability_manifest=_capability_manifest(),
    )
    assert summary["canonical_signal_count"] == len(observations) == len(labels) == 1

    mutations = []
    duplicate_signal = deepcopy(valid)
    duplicate_signal["canonical_signals"].append(deepcopy(duplicate_signal["canonical_signals"][0]))
    mutations.append(duplicate_signal)
    duplicate_version = deepcopy(valid)
    duplicate_version["observation_versions"].append(deepcopy(duplicate_version["observation_versions"][0]))
    mutations.append(duplicate_version)
    bad_lineage = deepcopy(valid)
    bad_lineage["lineage"][0]["observation_version_id"] = "missing"
    mutations.append(bad_lineage)
    bad_observation = deepcopy(valid)
    bad_observation["observation_versions"][0]["observation_status"] = "FAILED"
    mutations.append(bad_observation)
    missing_mapping = deepcopy(valid)
    missing_mapping["selected_observations"] = []
    mutations.append(missing_mapping)
    bad_candidate = deepcopy(valid)
    bad_candidate["stage_candidates"][0]["stage_evidence_id"] = "missing"
    mutations.append(bad_candidate)
    duplicate_outcome = deepcopy(valid)
    duplicate_outcome["outcome_labels"].append(deepcopy(duplicate_outcome["outcome_labels"][0]))
    mutations.append(duplicate_outcome)
    bad_label = deepcopy(valid)
    bad_label["selected_labels"][0]["selection_status"] = "MISSING"
    mutations.append(bad_label)
    duplicate_evidence = deepcopy(valid)
    duplicate_evidence["outcome_source_evidence"].append(deepcopy(duplicate_evidence["outcome_source_evidence"][0]))
    mutations.append(duplicate_evidence)
    mismatched_evidence = deepcopy(valid)
    mismatched_evidence["outcome_source_evidence"][0]["symbol"] = "999999.SZ"
    mutations.append(mismatched_evidence)
    invalid_evidence = deepcopy(valid)
    invalid_evidence["outcome_source_evidence"][0]["calculation_evidence_json"] = "not-json"
    mutations.append(invalid_evidence)
    missing_universe = deepcopy(valid)
    missing_universe["universe_outcomes"] = []
    mutations.append(missing_universe)
    bad_source = deepcopy(valid)
    bad_source["source_revisions"][0]["member_count"] = 2
    mutations.append(bad_source)
    for rows in mutations:
        with pytest.raises(SnapshotWriterError):
            _validate_relational_rows(
                build=_materialized_build(),
                rows_by_role=rows,
                capability_manifest=_capability_manifest(),
            )

    incomplete = DatasetCapabilityManifest(
        rows=tuple(row for row in _capability_manifest().rows if row.component != "canonical_signals")
    )
    with pytest.raises(SnapshotWriterError, match="capability rows differ"):
        _validate_capability_against_request(build=_materialized_build(), capability_manifest=incomplete)


def test_store_rejects_invalid_identity_capacity_and_noncanonical_blob(monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(LocalContentAddressedStoreError, match="absolute"):
        LocalContentAddressedStore(
            root=Path("relative"),
            repository_root=(tmp_path / "repo").resolve(),
            store_identity=_identity(),
        )
    bad_identity = dict(_identity(), atomic_publish_mode="RENAME")
    with pytest.raises(LocalContentAddressedStoreError, match="HARDLINK"):
        LocalContentAddressedStore(
            root=(tmp_path / "store").resolve(),
            repository_root=(tmp_path / "repo").resolve(),
            store_identity=bad_identity,
        )
    store = LocalContentAddressedStore(
        root=(tmp_path / "store").resolve(),
        repository_root=(tmp_path / "repo").resolve(),
        store_identity=_identity(),
    )
    monkeypatch.setattr(
        "backend.services.advisory_phase1.dataset_store.shutil.disk_usage",
        lambda path: SimpleNamespace(total=10 * 1024**3, free=1),
    )
    with pytest.raises(LocalContentAddressedStoreError, match="CAPACITY_INSUFFICIENT"):
        store.ensure_capacity(logical_source_bytes=1)
    stored = store.put_blob_bytes(b"canonical")
    document = store.put_document_bytes(kind="manifests", payload=b"canonical")
    with pytest.raises(LocalContentAddressedStoreError, match="outside the allowed store root"):
        store.read_blob_bytes(uri=document.uri, sha256=stored.sha256, size_bytes=stored.size_bytes)
    with pytest.raises(LocalContentAddressedStoreError, match="unsupported"):
        store.put_document_bytes(kind="unknown", payload=b"x")


def test_store_full_filesystem_readback_and_failure_contracts(monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
    repository_root = (tmp_path / "repo").resolve()
    repository_root.mkdir()
    with pytest.raises(LocalContentAddressedStoreError, match="cannot be empty"):
        LocalContentAddressedStore(
            root=(tmp_path / "empty-store").resolve(),
            repository_root=repository_root,
            store_identity={},
        )
    with pytest.raises(LocalContentAddressedStoreError, match="outside the repository"):
        LocalContentAddressedStore(
            root=(repository_root / "nested").resolve(),
            repository_root=repository_root,
            store_identity=_identity(),
        )
    bad_durability = dict(_identity(), durability_mode="NONE")
    with pytest.raises(LocalContentAddressedStoreError, match="durability mode"):
        LocalContentAddressedStore(
            root=(tmp_path / "bad-store").resolve(),
            repository_root=repository_root,
            store_identity=bad_durability,
        )
    store = LocalContentAddressedStore(
        root=(tmp_path / "store").resolve(), repository_root=repository_root, store_identity=_identity()
    )
    assert store.root.name == "store" and len(store.store_backend_hash) == 64
    with pytest.raises(LocalContentAddressedStoreError, match="cannot be negative"):
        store.ensure_capacity(logical_source_bytes=-1)
    monkeypatch.setattr(
        "backend.services.advisory_phase1.dataset_store.shutil.disk_usage",
        lambda path: (_ for _ in ()).throw(OSError("disk probe failed")),
    )
    with pytest.raises(LocalContentAddressedStoreError, match="cannot inspect"):
        store.ensure_capacity(logical_source_bytes=1)
    monkeypatch.undo()

    staging = store.staging_path(build_id="build-1", attempt_id="attempt-1", logical_path="role/part.parquet")
    staging.parent.mkdir(parents=True)
    staging.write_bytes(b"staging-payload")
    digest = hashlib.sha256(b"staging-payload").hexdigest()
    published = store.publish_staging_file(
        staging_uri=staging.as_uri(), sha256=digest, size_bytes=len(b"staging-payload")
    )
    assert store.read_blob_bytes(
        uri=published.uri, sha256=published.sha256, size_bytes=published.size_bytes
    ) == b"staging-payload"
    assert store.describe_blob(
        uri=published.uri, sha256=published.sha256, size_bytes=published.size_bytes
    ) == published
    assert store.is_canonical_blob_uri(uri=published.uri, sha256=published.sha256)
    assert not store.is_canonical_blob_uri(uri=staging.as_uri(), sha256=published.sha256)
    assert not store.is_canonical_blob_uri(uri="https://example.invalid/blob", sha256=published.sha256)
    assert not store.is_canonical_blob_uri(uri=published.uri, sha256="bad")
    with pytest.raises(LocalContentAddressedStoreError, match="descriptor is invalid"):
        store.read_bytes(uri=published.uri, sha256="bad", size_bytes=1)
    with pytest.raises(LocalContentAddressedStoreError, match="descriptor is invalid"):
        store.read_blob_bytes(uri=published.uri, sha256=published.sha256, size_bytes=0)
    with pytest.raises(LocalContentAddressedStoreError, match="local file URI"):
        store.path_from_uri("https://example.invalid/blob", allowed_root=store.root)
    with pytest.raises(LocalContentAddressedStoreError, match="cannot be resolved"):
        store.path_from_uri((tmp_path / "missing").as_uri(), allowed_root=store.root)
    with pytest.raises(LocalContentAddressedStoreError, match="backend identity"):
        store.verify_object(
            StoredCasObject(
                uri=published.uri,
                sha256=published.sha256,
                size_bytes=published.size_bytes,
                store_backend_hash="0" * 64,
            )
        )
    with pytest.raises(LocalContentAddressedStoreError, match="staging bytes"):
        store.publish_staging_file(staging_uri=staging.as_uri(), sha256="0" * 64, size_bytes=1)

    document = store.put_document_bytes(kind="manifests", payload=b"manifest")
    assert store.verify_document_bytes(kind="manifests", payload=b"manifest") == document
    with pytest.raises(LocalContentAddressedStoreError, match="unsupported"):
        store.verify_document_bytes(kind="bad", payload=b"manifest")
    document_path = store.path_from_uri(document.uri, allowed_root=store.root)
    document_path.unlink()
    with pytest.raises(LocalContentAddressedStoreError, match="document is missing"):
        store.verify_document_bytes(kind="manifests", payload=b"manifest")

    store.cleanup_attempt_staging(build_id="build-1", attempt_id="attempt-1")
    assert not staging.exists()
    store.cleanup_attempt_staging(build_id="build-1", attempt_id="attempt-1")
    with pytest.raises(LocalContentAddressedStoreError, match="safe path component"):
        store.cleanup_attempt_staging(build_id="..", attempt_id="attempt-1")
    with pytest.raises(LocalContentAddressedStoreError, match="unsafe"):
        store.staging_path(build_id="build-1", attempt_id="attempt-1", logical_path="../escape")

    conflict = store.put_blob_bytes(b"conflict")
    conflict_path = store.path_from_uri(conflict.uri, allowed_root=store.root)
    conflict_path.write_bytes(b"tampered")
    with pytest.raises(LocalContentAddressedStoreError, match="bytes do not match"):
        store.read_bytes(uri=conflict.uri, sha256=conflict.sha256, size_bytes=conflict.size_bytes)
    with pytest.raises(LocalContentAddressedStoreError, match="different bytes"):
        store.put_blob_bytes(b"conflict")
