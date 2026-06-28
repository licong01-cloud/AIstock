"""Seed provenance resolution for multi-alpha combine archive rows."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from .repository import QEArchiveRepository


_QEAR_RUN_RE = re.compile(r"^qear_run_[A-Za-z0-9_\-]+$")
_QE_LOOP_RE = re.compile(r"^(?P<task>qe_[A-Za-z0-9_\-]+)_L(?P<index>\d+)$")


@dataclass(frozen=True)
class SeedProvenance:
    seed_ref: str
    seed_ref_kind: str
    resolved: bool
    resolve_method: str
    resolve_note: str | None = None
    source_experiment_id: str | None = None
    source_task_id: str | None = None
    source_loop_id: str | None = None
    source_loop_index: int | None = None
    source_run_type: str | None = None
    source_model_type: str | None = None
    source_model_family: str | None = None
    source_factor_set_hash: str | None = None
    source_factor_names: list[str] | None = None
    source_factor_count: int | None = None
    source_freq: str | None = None
    source_label_horizon: int | None = None
    source_run_id: str | None = None
    source_created_at: Any | None = None
    source_updated_at: Any | None = None
    archived_at: Any | None = None
    source_row: Mapping[str, Any] | None = None

    def to_leg_source_row(self, *, run_id: str, leg_id: str, source_seq: int) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "leg_id": leg_id,
            "source_seq": source_seq,
            "seed_ref": self.seed_ref,
            "seed_ref_kind": self.seed_ref_kind,
            "source_experiment_id": self.source_experiment_id,
            "source_task_id": self.source_task_id,
            "source_loop_id": self.source_loop_id,
            "source_loop_index": self.source_loop_index,
            "source_run_type": self.source_run_type,
            "source_model_type": self.source_model_type,
            "source_factor_set_hash": self.source_factor_set_hash,
            "resolved": self.resolved,
            "resolve_method": self.resolve_method,
            "resolve_note": self.resolve_note,
        }

    def to_meta(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("source_row", None)
        return payload


class MultiAlphaProvenanceResolver:
    """Resolve macb seed ids into precise QE archive coordinates."""

    def __init__(self, repository: QEArchiveRepository | None = None) -> None:
        self._repository = repository or QEArchiveRepository()

    def resolve_seed(self, seed_ref: str) -> SeedProvenance:
        seed_ref = str(seed_ref or "").strip()
        if not seed_ref:
            return _unresolved("", kind="unknown", method="empty_seed_ref", note="seed reference is empty")
        if _QEAR_RUN_RE.match(seed_ref):
            return self._resolve_archive_run_id(seed_ref)
        match = _QE_LOOP_RE.match(seed_ref)
        if match:
            return self._resolve_evolution_loop_id(seed_ref, match.group("task"), int(match.group("index")))
        return _unresolved(
            seed_ref,
            kind="unknown",
            method="unsupported_seed_ref_format",
            note="seed reference does not match qear_run_<hash> or qe_<task>_L<idx>",
        )

    def _resolve_archive_run_id(self, seed_ref: str) -> SeedProvenance:
        row = self._repository.fetch_archive_run_for_seed(seed_ref)
        if not row:
            return _unresolved(
                seed_ref,
                kind="archive_run_id",
                method="archive_run_id_lookup",
                note="qe_archive.run row not found for qear_run seed reference",
            )
        missing = _required_coordinate_gaps(row)
        if missing:
            return _unresolved(
                seed_ref,
                kind="archive_run_id",
                method="archive_run_id_lookup",
                note="qe_archive.run row is missing required provenance coordinates: " + ",".join(missing),
                row=row,
            )
        return _resolved(seed_ref, kind="archive_run_id", method="archive_run_id_lookup", row=row)

    def _resolve_evolution_loop_id(self, seed_ref: str, task_id: str, loop_index: int) -> SeedProvenance:
        row = self._repository.resolve_evolution_loop_seed(task_id=task_id, loop_index=loop_index)
        if not row:
            return _unresolved(
                seed_ref,
                kind="evolution_loop_id",
                method="evolution_loop_id_lookup",
                note="qe_evolution_loops row not found for task_id+loop_index",
            )
        if not row.get("run_id"):
            note = "qe_evolution_loops row found but matching qe_archive.run row is missing"
            return _unresolved(
                seed_ref,
                kind="evolution_loop_id",
                method="evolution_loop_id_lookup",
                note=note,
                row=row,
            )
        missing = _required_coordinate_gaps(row)
        if missing:
            return _unresolved(
                seed_ref,
                kind="evolution_loop_id",
                method="evolution_loop_id_lookup",
                note="matched qe_archive.run row is missing required provenance coordinates: " + ",".join(missing),
                row=row,
            )
        return _resolved(seed_ref, kind="evolution_loop_id", method="evolution_loop_id_lookup", row=row)


def _resolved(seed_ref: str, *, kind: str, method: str, row: Mapping[str, Any]) -> SeedProvenance:
    return SeedProvenance(
        seed_ref=seed_ref,
        seed_ref_kind=kind,
        resolved=True,
        resolve_method=method,
        resolve_note=None,
        source_experiment_id=_text_or_none(row.get("experiment_id")),
        source_task_id=_text_or_none(row.get("task_id")),
        source_loop_id=_text_or_none(row.get("loop_id")),
        source_loop_index=_int_or_none(row.get("loop_index")),
        source_run_type=_text_or_none(row.get("run_type")),
        source_model_type=_text_or_none(row.get("model_type")),
        source_model_family=_text_or_none(row.get("model_family")),
        source_factor_set_hash=_text_or_none(row.get("factor_set_hash")),
        source_factor_names=_list_or_none(row.get("factor_names")),
        source_factor_count=_int_or_none(row.get("factor_count")),
        source_freq=_text_or_none(row.get("freq")),
        source_label_horizon=_int_or_none(row.get("label_horizon")),
        source_run_id=_text_or_none(row.get("run_id")),
        source_created_at=row.get("source_created_at"),
        source_updated_at=row.get("source_updated_at"),
        archived_at=row.get("archived_at"),
        source_row=dict(row),
    )


def _unresolved(
    seed_ref: str,
    *,
    kind: str,
    method: str,
    note: str,
    row: Mapping[str, Any] | None = None,
) -> SeedProvenance:
    return SeedProvenance(
        seed_ref=seed_ref,
        seed_ref_kind=kind,
        resolved=False,
        resolve_method=method,
        resolve_note=note,
        source_experiment_id=_text_or_none((row or {}).get("experiment_id")),
        source_task_id=_text_or_none((row or {}).get("task_id")),
        source_loop_id=_text_or_none((row or {}).get("loop_id")),
        source_loop_index=_int_or_none((row or {}).get("loop_index")),
        source_run_type=_text_or_none((row or {}).get("run_type")),
        source_model_type=_text_or_none((row or {}).get("model_type")),
        source_factor_set_hash=_text_or_none((row or {}).get("factor_set_hash")),
        source_factor_names=_list_or_none((row or {}).get("factor_names")),
        source_row=dict(row or {}),
    )


def _required_coordinate_gaps(row: Mapping[str, Any]) -> list[str]:
    gaps: list[str] = []
    if not _text_or_none(row.get("experiment_id")):
        gaps.append("experiment_id")
    if not _text_or_none(row.get("loop_id")):
        gaps.append("loop_id")
    if _int_or_none(row.get("loop_index")) is None:
        gaps.append("loop_index")
    if not _text_or_none(row.get("run_type")):
        gaps.append("run_type")
    return gaps


def _text_or_none(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _list_or_none(value: Any) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, (list, tuple)):
        return None
    items = [str(item).strip() for item in value if str(item).strip()]
    return items or None
