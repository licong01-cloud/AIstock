from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any
from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonical_json_text,
)
from backend.services.advisory_phase1.dataset_build import (
    DatasetBlobHeader,
    DatasetSnapshotFile,
)
from backend.services.advisory_phase0b.errors import (
    Phase0BAuditError,
    REASON_SNAPSHOT_CHANGED,
    REASON_SNAPSHOT_INVALIDATED,
)
from backend.services.advisory_phase0b.snapshot_reader import (
    Phase0BClientDatabaseTargetV1,
    Phase0BDatabaseTargetReceiptV1,
    Phase0BSnapshotCatalogEntryV1,
    Phase0BSnapshotCatalogReceiptV1,
    Phase0BSnapshotReader,
    PostgresPhase0BSnapshotCatalog,
)
from backend.tests.advisory_phase0b.test_contracts import _request


_HASH = "a" * 64


def _client_target() -> Phase0BClientDatabaseTargetV1:
    return Phase0BClientDatabaseTargetV1(
        env_file_path_hash="1" * 64,
        configured_host_hash="2" * 64,
        configured_port=5432,
        configured_database_hash=hashlib.sha256(b"test_db").hexdigest(),
        configured_user_hash=hashlib.sha256(b"test_user").hexdigest(),
    )


def _database_target() -> Phase0BDatabaseTargetReceiptV1:
    return Phase0BDatabaseTargetReceiptV1(
        client_target=_client_target(),
        current_database_hash=hashlib.sha256(b"test_db").hexdigest(),
        server_address_hash=hashlib.sha256(b"127.0.0.1").hexdigest(),
        server_port=5432,
        server_version_num=160000,
        current_user_hash=hashlib.sha256(b"test_user").hexdigest(),
        transaction_read_only=True,
    )


def _file() -> DatasetSnapshotFile:
    return DatasetSnapshotFile(
        logical_path="canonical_signals/part-000.parquet",
        logical_role="canonical_signals",
        partition_key_hash="b" * 64,
        ordinal=0,
        content_uri="blobs/aa/" + _HASH,
        sha256=_HASH,
        size_bytes=10,
        row_count=1,
        schema_fingerprint="c" * 64,
        partition_content_hash="d" * 64,
        blob=DatasetBlobHeader(
            store_backend_hash="e" * 64,
            blob_sha256=_HASH,
            size_bytes=10,
        ),
    )


def _entry(*, header_marker: str = "v1") -> Phase0BSnapshotCatalogEntryV1:
    header = {
        "snapshot_id": "snapshot-1",
        "lineage_identity_type": "HISTORICAL_RANGE",
        "build_request_payload": {},
        "header_marker": header_marker,
        "manifest_sha256": _HASH,
        "snapshot_content_hash": "f" * 64,
        "snapshot_source_revision_set_hash": "6" * 64,
        "schema_fingerprint": "7" * 64,
        "maturity_coverage_hash": "8" * 64,
    }
    files = (_file(),)
    memberships = {"observations": (), "labels": (), "blob_refs": ()}
    return Phase0BSnapshotCatalogEntryV1(
        snapshot_id="snapshot-1",
        lineage_identity_type="HISTORICAL_RANGE",
        header_payload_json=canonical_json_text(header),
        files=files,
        observation_membership_json=(),
        label_membership_json=(),
        blob_ref_membership_json=(),
        header_hash=canonical_json_sha256(header),
        file_set_hash=canonical_json_sha256(
            tuple(item.model_dump(mode="json") for item in files)
        ),
        membership_hash=canonical_json_sha256(memberships),
        invalidation_query_hash=canonical_json_sha256(()),
    )


def _receipt(*, header_marker: str = "v1") -> Phase0BSnapshotCatalogReceiptV1:
    return Phase0BSnapshotCatalogReceiptV1(
        entries=(_entry(header_marker=header_marker),),
        database_target=_database_target(),
        observed_at=datetime(2026, 8, 1, tzinfo=UTC),
    )


def test_catalog_entry_requires_explicit_lineage_without_phase0a_default() -> None:
    entry = _entry()
    header = entry.header_payload()
    header.pop("lineage_identity_type")
    payload = entry.model_dump(mode="python")
    payload.update(
        {
            "lineage_identity_type": "PHASE0A",
            "header_payload_json": canonical_json_text(header),
            "header_hash": canonical_json_sha256(header),
            "catalog_content_hash": None,
        }
    )

    with pytest.raises(ValueError, match="explicit supported lineage type"):
        Phase0BSnapshotCatalogEntryV1.model_validate(payload)


class _FakeCursor:
    def __init__(self, *, invalidated: bool = False) -> None:
        self._rows: list[dict[str, Any]] = []
        self.invalidated = invalidated

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, _params: object) -> None:
        if "transaction_timestamp()" in query:
            self._rows = [
                {
                    "observed_at": datetime(2026, 8, 1, tzinfo=UTC),
                    "current_database": "test_db",
                    "server_address": "127.0.0.1",
                    "server_port": 5432,
                    "server_version_num": 160000,
                    "current_user": "test_user",
                    "transaction_read_only": "on",
                }
            ]
        elif "to_jsonb(snapshot)" in query:
            self._rows = [
                {
                    "snapshot_id": "snapshot-1",
                    "snapshot_header": {
                        "snapshot_id": "snapshot-1",
                        "snapshot_state": "SEALED",
                        "lineage_identity_type": "HISTORICAL_RANGE",
                        "manifest_sha256": _HASH,
                        "snapshot_content_hash": "f" * 64,
                    },
                    "build_request_payload_jsonb": {"range_lineage_scopes": []},
                }
            ]
        elif "advisory_dataset_snapshot_file" in query:
            self._rows = [{"snapshot_id": "snapshot-1", **_file().model_dump(mode="python"), "store_backend_hash": "e" * 64, "blob_sha256": _HASH}]
            self._rows[0].pop("blob")
        elif "advisory_dataset_snapshot_invalidation" in query:
            self._rows = (
                [{"snapshot_id": "snapshot-1", "payload": {"snapshot_id": "snapshot-1"}}]
                if self.invalidated
                else []
            )
        else:
            self._rows = []

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows

    def fetchone(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None


class _FakeConnection:
    def __init__(self, *, invalidated: bool = False) -> None:
        self.cursor_value = _FakeCursor(invalidated=invalidated)
        self.session: dict[str, object] | None = None
        self.rollback_count = 0
        self.close_count = 0

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def set_session(self, **kwargs: object) -> None:
        self.session = kwargs

    def cursor(self, **_kwargs: object) -> _FakeCursor:
        return self.cursor_value

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.close_count += 1


def test_catalog_uses_short_repeatable_read_read_only_transaction() -> None:
    connection = _FakeConnection()
    catalog = PostgresPhase0BSnapshotCatalog(
        conn_factory=lambda: connection,
        client_target=_client_target(),
    )

    receipt = catalog.read_once(snapshot_ids=("snapshot-1",))

    assert receipt.entries[0].snapshot_id == "snapshot-1"
    assert connection.session == {
        "isolation_level": "REPEATABLE READ",
        "readonly": True,
        "autocommit": False,
    }
    assert connection.rollback_count == 1
    assert connection.close_count == 1


def test_catalog_rejects_append_only_invalidation() -> None:
    connection = _FakeConnection(invalidated=True)
    catalog = PostgresPhase0BSnapshotCatalog(
        conn_factory=lambda: connection,
        client_target=_client_target(),
    )

    with pytest.raises(Phase0BAuditError) as captured:
        catalog.read_once(snapshot_ids=("snapshot-1",))

    assert captured.value.reason_code == REASON_SNAPSHOT_INVALIDATED


class _MembershipCursor:
    def __init__(self) -> None:
        self.query = ""
        self.params: object = None

    def execute(self, query: str, params: object) -> None:
        self.query = query
        self.params = params

    def fetchall(self) -> list[dict[str, Any]]:
        return [
            {"snapshot_id": "snapshot-1", "payload": {"member": "z-value"}},
            {"snapshot_id": "snapshot-1", "payload": {"member": "a-value"}},
        ]


def test_membership_query_materializes_payload_before_canonical_ordering() -> None:
    cursor = _MembershipCursor()

    rows = PostgresPhase0BSnapshotCatalog._canonical_rows_by_snapshot(
        cur=cursor,
        snapshot_ids=("snapshot-1",),
        table="app.advisory_dataset_snapshot_observation",
    )

    assert "SELECT snapshot_id, payload FROM (" in cursor.query
    assert ") membership ORDER BY snapshot_id, payload::text" in cursor.query
    assert cursor.params == (["snapshot-1"],)
    assert rows == {
        "snapshot-1": ('{"member":"a-value"}', '{"member":"z-value"}')
    }


class _ManifestMember:
    def __init__(self, payload: dict[str, str]) -> None:
        self.payload = payload

    def model_dump(self, *, mode: str) -> dict[str, str]:
        assert mode == "json"
        return self.payload


def test_manifest_memberships_sort_after_canonical_serialization() -> None:
    memberships = Phase0BSnapshotReader._canonical_manifest_memberships(
        (
            _ManifestMember({"candidate_stage_evidence_id": "z", "label_key_hash": "a"}),
            _ManifestMember({"candidate_stage_evidence_id": "a", "label_key_hash": "z"}),
        )
    )

    assert memberships == (
        '{"candidate_stage_evidence_id":"a","label_key_hash":"z"}',
        '{"candidate_stage_evidence_id":"z","label_key_hash":"a"}',
    )


class _ReceiptCatalog:
    def __init__(self, result: Phase0BSnapshotCatalogReceiptV1 | Phase0BAuditError) -> None:
        self.result = result

    def read_once(self, *, snapshot_ids: tuple[str, ...]) -> Phase0BSnapshotCatalogReceiptV1:
        assert snapshot_ids == ("snapshot-1",)
        if isinstance(self.result, Phase0BAuditError):
            raise self.result
        return self.result


def test_final_catalog_change_is_one_typed_changed_during_read_failure() -> None:
    reader = Phase0BSnapshotReader(
        catalog=_ReceiptCatalog(_receipt(header_marker="v2")),  # type: ignore[arg-type]
        dataset_store=object(),  # type: ignore[arg-type]
    )

    with pytest.raises(Phase0BAuditError) as captured:
        reader.confirm_unchanged(
            request=_request(),
            first_receipt=_receipt(header_marker="v1"),
        )

    assert captured.value.reason_code == REASON_SNAPSHOT_CHANGED


def test_final_invalidation_is_changed_during_read_not_initial_state_error() -> None:
    reader = Phase0BSnapshotReader(
        catalog=_ReceiptCatalog(  # type: ignore[arg-type]
            Phase0BAuditError(REASON_SNAPSHOT_INVALIDATED, "invalidated")
        ),
        dataset_store=object(),  # type: ignore[arg-type]
    )

    with pytest.raises(Phase0BAuditError) as captured:
        reader.confirm_unchanged(
            request=_request(),
            first_receipt=_receipt(),
        )

    assert captured.value.reason_code == REASON_SNAPSHOT_CHANGED


class _HistoricalLineageSpool:
    def distinct_target_lineages(
        self, *, snapshot_id: str
    ) -> tuple[tuple[str, str, str, None, str], ...]:
        assert snapshot_id == "snapshot-1"
        return (("package-1", _HASH, "multi_alpha", None, "9" * 64),)

def _historical_target_entry() -> Phase0BSnapshotCatalogEntryV1:
    entry = _entry()
    header = entry.header_payload()
    header["build_request_payload"] = {
        "range_lineage_scopes": [
            {"identity_id": "scope-day-1", "identity_hash": "1" * 64},
            {"identity_id": "scope-day-2", "identity_hash": "2" * 64},
        ]
    }
    payload = entry.model_dump(mode="python")
    payload.update(
        {
            "header_payload_json": canonical_json_text(header),
            "header_hash": canonical_json_sha256(header),
            "catalog_content_hash": None,
        }
    )
    return Phase0BSnapshotCatalogEntryV1.model_validate(payload)


def _historical_target_request() -> Any:
    target = SimpleNamespace(
        snapshot_id="snapshot-1",
        program_id="9" * 64,
        package_id="package-1",
        manifest_sha256=_HASH,
        alpha_mode="multi_alpha",
        target_hash="8" * 64,
    )
    return SimpleNamespace(audit_targets=(target,))


def test_historical_target_binds_frozen_program_without_conflating_daily_scopes() -> None:
    bindings = Phase0BSnapshotReader._verify_target_lineage(
        request=_historical_target_request(),
        entry=_historical_target_entry(),
        spool=_HistoricalLineageSpool(),  # type: ignore[arg-type]
    )

    assert len(bindings) == 1
    assert bindings[0].formal_program_id is None
    assert bindings[0].range_program_hash == "9" * 64
