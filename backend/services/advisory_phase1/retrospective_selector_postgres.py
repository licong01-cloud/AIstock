"""PostgreSQL adapter for the range-only retrospective selector."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import psycopg2.extras

from backend.services.advisory_phase1.retrospective_selector import (
    RetrospectiveObservationVersion,
    RetrospectiveObservationSelector,
    RetrospectiveSelectedObservationMapping,
    RetrospectiveSelectionRequest,
)


class PostgresRetrospectiveObservationSelector:
    """Read exact range lineage rows in a read-only transaction, then select purely."""

    def __init__(
        self,
        *,
        conn_factory: Callable[[], Any],
        row_adapter: Callable[[dict[str, Any]], RetrospectiveObservationVersion],
    ) -> None:
        if conn_factory is None or row_adapter is None:
            raise ValueError("conn_factory and row_adapter are required")
        self._conn_factory = conn_factory
        self._row_adapter = row_adapter
        self._selector = RetrospectiveObservationSelector()

    def select_exact(
        self, *, request: RetrospectiveSelectionRequest
    ) -> tuple[RetrospectiveSelectedObservationMapping, ...]:
        candidate_hashes = tuple(
            item.semantic_content_hash for item in request.candidate_artifact_refs
        )
        with self._conn_factory() as conn:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT lineage.*, version.*
                    FROM app.advisory_signal_observation_lineage AS lineage
                    JOIN app.advisory_signal_observation_version AS version
                      ON version.observation_version_id = lineage.observation_version_id
                    WHERE lineage_source_type = 'HISTORICAL_RANGE_RESEARCH'
                      AND range_run_id = ANY(%s)
                      AND candidate_artifact_hash = ANY(%s)
                    ORDER BY lineage.canonical_signal_id,
                             version.observation_version_id,
                             lineage.lineage_id
                    """,
                    (list(request.range_run_ids), list(candidate_hashes)),
                )
                observations = tuple(
                    self._row_adapter(dict(row)) for row in cur.fetchall()
                )
            conn.rollback()
        return self._selector.select(request=request, observations=observations)
