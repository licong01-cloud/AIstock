"""Resolve F-014 dataset snapshot ids only through QE node allowlisted roots."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Literal, Mapping, Sequence

from backend.db.pg_pool import get_conn
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceDatasetIdentity,
    QEWorkspaceDatasetIdentityError,
)

ROOTS_ENV = "QE_DATASET_IDENTITY_ROOTS"
_SNAPSHOT_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,200}$")


class QELongTrendSnapshotResolutionError(RuntimeError):
    def __init__(self, message: str, *, reason_code: str, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


@dataclass(frozen=True)
class ResolvedDatasetSnapshot:
    node_id: str
    requested_snapshot_id: str
    root_uri: str | None
    identity: QEWorkspaceDatasetIdentity | None
    data_action: Mapping[str, Any] | None

    @property
    def resolved(self) -> bool:
        return self.root_uri is not None and self.identity is not None


class QELongTrendSnapshotResolver:
    def __init__(
        self,
        *,
        connection_provider: Callable[[], Any] | None = None,
        root_provider: Callable[[str], Sequence[str]] | None = None,
        roots_env: str | None = None,
    ) -> None:
        self._connection_provider = connection_provider or get_conn
        self._root_provider = root_provider
        self._roots_env = roots_env if roots_env is not None else os.getenv(ROOTS_ENV, "")

    def allowed_roots(self, node_id: str) -> tuple[str, ...]:
        normalized_node = str(node_id or "").strip()
        if not normalized_node:
            raise QELongTrendSnapshotResolutionError(
                "snapshot resolution requires node_id",
                reason_code="QELT_SNAPSHOT_RESOLUTION_INVALID",
            )
        if self._root_provider is not None:
            try:
                roots = list(self._root_provider(normalized_node))
            except QELongTrendSnapshotResolutionError:
                raise
            except Exception as exc:
                raise QELongTrendSnapshotResolutionError(
                    "cannot read QE node dataset roots",
                    reason_code="QELT_SNAPSHOT_ROOTS_UNAVAILABLE",
                    context={"node_id": normalized_node},
                ) from exc
        else:
            roots = [self._node_factor_data_root(normalized_node)]
        roots.extend(self._configured_roots(normalized_node))
        normalized: list[str] = []
        seen: set[str] = set()
        for value in roots:
            root = str(value or "").strip()
            if not root or root in seen:
                continue
            if "\x00" in root:
                raise QELongTrendSnapshotResolutionError(
                    "dataset identity root contains a NUL byte",
                    reason_code="QELT_SNAPSHOT_RESOLUTION_INVALID",
                )
            seen.add(root)
            normalized.append(root)
        if not normalized:
            raise QELongTrendSnapshotResolutionError(
                f"node {normalized_node} has no configured QE dataset identity roots",
                reason_code="QELT_SNAPSHOT_ROOTS_UNAVAILABLE",
            )
        return tuple(normalized)

    def primary_factor_data_root(self, node_id: str) -> str:
        """Return the node-owned primary factor root for normal QE execution.

        Extra registered roots are historical lookup candidates only.  A
        normal-loop profile must bind to the compute node's configured
        ``factor_data_dir`` and may never choose a caller supplied path.
        """

        return self._node_factor_data_root(str(node_id or "").strip())

    async def resolve_requested_snapshot(
        self,
        *,
        node_id: str,
        requested_snapshot_id: str,
        client: Any,
        snapshot_role: Literal["feature", "outcome"] = "outcome",
    ) -> ResolvedDatasetSnapshot:
        snapshot_id = str(requested_snapshot_id or "").strip()
        if not _SNAPSHOT_ID_RE.fullmatch(snapshot_id):
            raise QELongTrendSnapshotResolutionError(
                "requested outcome snapshot id has an invalid format",
                reason_code="QELT_SNAPSHOT_RESOLUTION_INVALID",
                context={"requested_snapshot_id": requested_snapshot_id},
            )
        matches: list[tuple[str, QEWorkspaceDatasetIdentity, str]] = []
        attempts: list[dict[str, Any]] = []
        for root_uri in self.allowed_roots(node_id):
            root_ref = hashlib.sha256(root_uri.encode("utf-8")).hexdigest()[:16]
            try:
                identity = await client.get_dataset_identity(node_id=node_id, data_root_uri=root_uri)
            except QEWorkspaceDatasetIdentityError as exc:
                attempts.append(
                    {
                        "root_ref": root_ref,
                        "status": "identity_unavailable",
                        "reason_code": exc.reason_code,
                    }
                )
                continue
            snapshot = identity.long_trend_snapshot
            actual_snapshot_id = str(snapshot.get("snapshot_id") or "") if isinstance(snapshot, Mapping) else ""
            attempts.append(
                {
                    "root_ref": root_ref,
                    "status": "matched" if actual_snapshot_id == snapshot_id else "different_snapshot",
                    "snapshot_id": actual_snapshot_id or None,
                }
            )
            if actual_snapshot_id == snapshot_id:
                manifest_sha = str(snapshot.get("manifest_sha256") or "")
                matches.append((root_uri, identity, manifest_sha))
        if not matches:
            return ResolvedDatasetSnapshot(
                node_id=node_id,
                requested_snapshot_id=snapshot_id,
                root_uri=None,
                identity=None,
                data_action={
                    "action": "register_requested_snapshot_in_qe_dataset_identity_roots",
                    "reason_code": f"QELT_REQUESTED_{snapshot_role.upper()}_SNAPSHOT_UNAVAILABLE",
                    "snapshot_role": snapshot_role,
                    "requested_snapshot_id": snapshot_id,
                    "node_id": node_id,
                    "attempts": attempts,
                },
            )
        manifest_hashes = {item[2] for item in matches}
        if len(manifest_hashes) != 1:
            raise QELongTrendSnapshotResolutionError(
                "requested snapshot id resolves to different manifests on the same QE node",
                reason_code="QELT_SNAPSHOT_IDENTITY_AMBIGUOUS",
                context={
                    "node_id": node_id,
                    "requested_snapshot_id": snapshot_id,
                    "matches": [
                        {
                            "root_ref": hashlib.sha256(root.encode("utf-8")).hexdigest()[:16],
                            "manifest_sha256": digest,
                        }
                        for root, _identity, digest in matches
                    ],
                },
            )
        root_uri, identity, _manifest_sha = matches[0]
        return ResolvedDatasetSnapshot(
            node_id=node_id,
            requested_snapshot_id=snapshot_id,
            root_uri=root_uri,
            identity=identity,
            data_action=None,
        )

    @staticmethod
    def unresolved_archived_feature(*, node_id: str) -> ResolvedDatasetSnapshot:
        """Represent missing archived feature identity without inventing a path or snapshot."""

        return ResolvedDatasetSnapshot(
            node_id=node_id,
            requested_snapshot_id="",
            root_uri=None,
            identity=None,
            data_action={
                "action": "archive_feature_snapshot_identity_for_qe_run",
                "reason_code": "QELT_ARCHIVED_FEATURE_SNAPSHOT_ID_UNAVAILABLE",
                "snapshot_role": "feature",
                "node_id": node_id,
            },
        )

    def _node_factor_data_root(self, node_id: str) -> str:
        try:
            with self._connection_provider() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT factor_data_dir FROM infra.compute_nodes WHERE node_id = %s", (node_id,))
                    row = cur.fetchone()
        except Exception as exc:
            raise QELongTrendSnapshotResolutionError(
                "cannot read QE node dataset root",
                reason_code="QELT_SNAPSHOT_ROOTS_UNAVAILABLE",
                context={"node_id": node_id},
            ) from exc
        root = str(row[0] if row else "").strip()
        if not root:
            raise QELongTrendSnapshotResolutionError(
                f"QE node {node_id} does not have factor_data_dir configured",
                reason_code="QELT_SNAPSHOT_ROOTS_UNAVAILABLE",
            )
        return root

    def _configured_roots(self, node_id: str) -> list[str]:
        raw = str(self._roots_env or "").strip()
        if not raw:
            return []
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise QELongTrendSnapshotResolutionError(
                f"{ROOTS_ENV} must be a JSON object: {exc}",
                reason_code="QELT_SNAPSHOT_ROOTS_CONFIG_INVALID",
            ) from exc
        if not isinstance(payload, Mapping):
            raise QELongTrendSnapshotResolutionError(
                f"{ROOTS_ENV} must map node ids to root arrays",
                reason_code="QELT_SNAPSHOT_ROOTS_CONFIG_INVALID",
            )
        roots: list[str] = []
        for key in ("*", node_id):
            values = payload.get(key, [])
            if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
                raise QELongTrendSnapshotResolutionError(
                    f"{ROOTS_ENV}[{key!r}] must be an array of strings",
                    reason_code="QELT_SNAPSHOT_ROOTS_CONFIG_INVALID",
                )
            roots.extend(values)
        return roots
