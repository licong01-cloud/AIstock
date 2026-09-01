from __future__ import annotations

import asyncio

import pytest

from backend.services.quantevolver.long_trend_snapshot_resolver import (
    QELongTrendSnapshotResolutionError,
    QELongTrendSnapshotResolver,
)
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceDatasetIdentity,
    QEWorkspaceDatasetIdentityError,
)


def _identity(snapshot_id: str, manifest_sha256: str) -> QEWorkspaceDatasetIdentity:
    return QEWorkspaceDatasetIdentity(
        schema_version="qe_dataset_identity_v1",
        complete=True,
        reason_code=None,
        missing=(),
        acquisition_suggestions=(),
        dataset={"dataset_manifest_sha256": "f" * 64},
        long_trend_snapshot={
            "snapshot_id": snapshot_id,
            "manifest_sha256": manifest_sha256,
            "start_date": "2018-08-01",
            "end_date": "2026-06-30",
            "lineage_parent_ids": [],
            "files": {},
        },
    )


class _Client:
    def __init__(self, identities: dict[str, QEWorkspaceDatasetIdentity]) -> None:
        self.identities = identities
        self.calls: list[tuple[str, str]] = []

    async def get_dataset_identity(self, *, node_id: str, data_root_uri: str):
        self.calls.append((node_id, data_root_uri))
        return self.identities[data_root_uri]


def test_resolver_matches_only_server_allowlisted_roots() -> None:
    resolver = QELongTrendSnapshotResolver(
        root_provider=lambda _node: ["/current"],
        roots_env='{"wsl": ["/history"]}',
    )
    client = _Client(
        {
            "/current": _identity("snapshot-current", "a" * 64),
            "/history": _identity("snapshot-requested", "b" * 64),
        }
    )

    resolved = asyncio.run(
        resolver.resolve_requested_snapshot(
            node_id="wsl",
            requested_snapshot_id="snapshot-requested",
            client=client,
        )
    )

    assert resolved.resolved is True
    assert resolved.root_uri == "/history"
    assert client.calls == [("wsl", "/current"), ("wsl", "/history")]


def test_resolver_returns_visible_data_action_without_exposing_paths() -> None:
    resolver = QELongTrendSnapshotResolver(root_provider=lambda _node: ["/secret/root"], roots_env="")
    client = _Client({"/secret/root": _identity("different", "a" * 64)})

    resolved = asyncio.run(
        resolver.resolve_requested_snapshot(
            node_id="remote",
            requested_snapshot_id="snapshot-requested",
            client=client,
        )
    )

    assert resolved.resolved is False
    assert resolved.data_action is not None
    assert resolved.data_action["reason_code"] == "QELT_REQUESTED_OUTCOME_SNAPSHOT_UNAVAILABLE"
    assert "/secret/root" not in str(resolved.data_action)


def test_resolver_preserves_feature_role_in_missing_snapshot_action() -> None:
    resolver = QELongTrendSnapshotResolver(root_provider=lambda _node: ["/secret/root"], roots_env="")
    client = _Client({"/secret/root": _identity("different", "a" * 64)})

    resolved = asyncio.run(
        resolver.resolve_requested_snapshot(
            node_id="remote",
            requested_snapshot_id="archived-feature",
            client=client,
            snapshot_role="feature",
        )
    )

    assert resolved.resolved is False
    assert resolved.data_action == {
        "action": "register_requested_snapshot_in_qe_dataset_identity_roots",
        "reason_code": "QELT_REQUESTED_FEATURE_SNAPSHOT_UNAVAILABLE",
        "snapshot_role": "feature",
        "requested_snapshot_id": "archived-feature",
        "node_id": "remote",
        "attempts": [
            {
                "root_ref": resolved.data_action["attempts"][0]["root_ref"],
                "status": "different_snapshot",
                "snapshot_id": "different",
            }
        ],
    }
    assert "/secret/root" not in str(resolved.data_action)


def test_missing_archived_feature_is_explicit_and_path_free() -> None:
    resolved = QELongTrendSnapshotResolver.unresolved_archived_feature(node_id="wsl")

    assert resolved.resolved is False
    assert resolved.data_action == {
        "action": "archive_feature_snapshot_identity_for_qe_run",
        "reason_code": "QELT_ARCHIVED_FEATURE_SNAPSHOT_ID_UNAVAILABLE",
        "snapshot_role": "feature",
        "node_id": "wsl",
    }


def test_resolver_rejects_same_snapshot_id_with_different_manifests() -> None:
    resolver = QELongTrendSnapshotResolver(root_provider=lambda _node: ["/a", "/b"], roots_env="")
    client = _Client(
        {
            "/a": _identity("same", "a" * 64),
            "/b": _identity("same", "b" * 64),
        }
    )

    with pytest.raises(QELongTrendSnapshotResolutionError) as exc_info:
        asyncio.run(
            resolver.resolve_requested_snapshot(
                node_id="wsl",
                requested_snapshot_id="same",
                client=client,
            )
        )

    assert exc_info.value.reason_code == "QELT_SNAPSHOT_IDENTITY_AMBIGUOUS"


def test_resolver_rejects_path_shaped_snapshot_id() -> None:
    resolver = QELongTrendSnapshotResolver(root_provider=lambda _node: ["/a"], roots_env="")

    with pytest.raises(QELongTrendSnapshotResolutionError) as exc_info:
        asyncio.run(
            resolver.resolve_requested_snapshot(
                node_id="wsl",
                requested_snapshot_id="../../dataset",
                client=_Client({}),
            )
        )

    assert exc_info.value.reason_code == "QELT_SNAPSHOT_RESOLUTION_INVALID"


def test_root_configuration_validation_is_strict() -> None:
    with pytest.raises(QELongTrendSnapshotResolutionError, match="requires node_id"):
        QELongTrendSnapshotResolver(root_provider=lambda _node: ["/a"]).allowed_roots("")
    with pytest.raises(QELongTrendSnapshotResolutionError, match="no configured"):
        QELongTrendSnapshotResolver(root_provider=lambda _node: []).allowed_roots("wsl")
    with pytest.raises(QELongTrendSnapshotResolutionError, match="NUL"):
        QELongTrendSnapshotResolver(root_provider=lambda _node: ["/a\x00b"]).allowed_roots("wsl")
    with pytest.raises(QELongTrendSnapshotResolutionError, match="must be a JSON object"):
        QELongTrendSnapshotResolver(root_provider=lambda _node: ["/a"], roots_env="[").allowed_roots("wsl")
    with pytest.raises(QELongTrendSnapshotResolutionError, match="must map node ids"):
        QELongTrendSnapshotResolver(root_provider=lambda _node: ["/a"], roots_env="[]").allowed_roots("wsl")
    with pytest.raises(QELongTrendSnapshotResolutionError, match="array of strings"):
        QELongTrendSnapshotResolver(root_provider=lambda _node: ["/a"], roots_env='{"wsl": 1}').allowed_roots(
            "wsl"
        )
    with pytest.raises(QELongTrendSnapshotResolutionError, match="cannot read QE node dataset roots") as exc_info:
        QELongTrendSnapshotResolver(
            root_provider=lambda _node: (_ for _ in ()).throw(OSError("internal /secret/path"))
        ).allowed_roots("wsl")
    assert exc_info.value.reason_code == "QELT_SNAPSHOT_ROOTS_UNAVAILABLE"
    assert "/secret/path" not in str(exc_info.value)


def test_identity_errors_are_visible_attempts_and_do_not_expose_root() -> None:
    class _UnavailableClient:
        @staticmethod
        async def get_dataset_identity(**_kwargs):
            raise QEWorkspaceDatasetIdentityError("identity missing", reason_code="QE_DATASET_IDENTITY_MISSING")

    resolver = QELongTrendSnapshotResolver(root_provider=lambda _node: ["/secret/root"])
    resolved = asyncio.run(
        resolver.resolve_requested_snapshot(
            node_id="wsl",
            requested_snapshot_id="snapshot-1",
            client=_UnavailableClient(),
        )
    )

    assert resolved.data_action is not None
    assert resolved.data_action["attempts"][0]["status"] == "identity_unavailable"
    assert resolved.data_action["attempts"][0]["reason_code"] == "QE_DATASET_IDENTITY_MISSING"
    assert "/secret/root" not in str(resolved.data_action)


def test_node_factor_root_comes_from_compute_node_registry() -> None:
    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def execute(sql, params):  # type: ignore[no-untyped-def]
            assert "infra.compute_nodes" in sql
            assert params == ("wsl",)

        @staticmethod
        def fetchone():
            return ("/registry/factor-data",)

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def cursor():
            return _Cursor()

    resolver = QELongTrendSnapshotResolver(connection_provider=lambda: _Connection(), roots_env="")
    assert resolver.allowed_roots("wsl") == ("/registry/factor-data",)

    class _BrokenConnection:
        def __enter__(self):
            raise RuntimeError("db unavailable")

        def __exit__(self, *_args):
            return False

    broken = QELongTrendSnapshotResolver(connection_provider=lambda: _BrokenConnection(), roots_env="")
    with pytest.raises(QELongTrendSnapshotResolutionError, match="cannot read QE node dataset root"):
        broken.allowed_roots("wsl")
