"""Read one immutable P3A sector candidate as a monthly release source.

The adapter is filesystem/read-only.  It accepts only the deterministic
candidate location below the profile's existing allowlisted candidate root,
revalidates the complete P3A writer/readback contract, proves the frozen
PIT/trading-day denominator, and exposes bounded date-partition row streams.
"""

from __future__ import annotations

import hashlib
import json
import re
import stat
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

from backend.services.sector_data_builder import (
    SECTOR_DATA_OPPORTUNITY_SCHEMA,
    SectorDataCandidateReadback,
    read_sector_data_candidate,
)

from .canonical import canonical_json_bytes, digest_named_fields
from .errors import DatasetReleaseError
from .pit import FrozenPitSnapshot
from .profile import DatasetProfile


SECTOR_CANDIDATE_SOURCE_SCHEMA = "dataset_release_p3a_sector_candidate_source_v1"
SECTOR_CANDIDATE_QUERY_VERSION = "sector_data_p3a_dual_authority_candidate_v1"
SECTOR_CANDIDATE_NAMESPACE = ".sector_data_authority"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CODE = re.compile(r"[0-9]{6}\.(?:SH|SZ)\Z")
_BARE_INDEX_CODE = re.compile(r"[0-9]{6}\Z")
_CANONICAL_INDEX_SYMBOL = re.compile(r"[0-9]{6}\.SI\Z")
_ZERO_SAFETY = {
    "database_writes": 0,
    "provider_database_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
    "candidate_writes": 0,
}

_SW_FIELD_MAP = {
    "open": "sw2_open",
    "high": "sw2_high",
    "low": "sw2_low",
    "close": "sw2_close",
    "pct_change": "sw2_pct_change",
    "vol": "sw2_vol",
    "amount": "sw2_amount",
    "pe": "sw2_pe",
    "pb": "sw2_pb",
    "total_mv": "sw2_total_mv",
}
_MONEYFLOW_FIELD_MAP = {
    "buy_sm_amount": "sw2_mf_buy_sm_amt",
    "sell_sm_amount": "sw2_mf_sell_sm_amt",
    "buy_md_amount": "sw2_mf_buy_md_amt",
    "sell_md_amount": "sw2_mf_sell_md_amt",
    "buy_lg_amount": "sw2_mf_buy_lg_amt",
    "sell_lg_amount": "sw2_mf_sell_lg_amt",
    "buy_elg_amount": "sw2_mf_buy_elg_amt",
    "sell_elg_amount": "sw2_mf_sell_elg_amt",
    "net_mf_amount": "sw2_mf_net_amt",
    "buy_elg_vol": "sw2_mf_buy_elg_vol",
    "sell_elg_vol": "sw2_mf_sell_elg_vol",
    "net_mf_vol": "sw2_mf_net_vol",
}


class SectorCandidateSourceError(DatasetReleaseError):
    code = "BLOCKED_P3A_SECTOR_CANDIDATE_INVALID"


@dataclass(frozen=True, slots=True)
class _DateSlice:
    start: int
    end: int


def sector_candidate_scope_key(sample_instruments: Sequence[str]) -> str:
    codes = tuple(sorted(str(value).strip().upper() for value in sample_instruments))
    if not codes:
        return "full"
    if len(codes) != len(set(codes)) or any(_CODE.fullmatch(code) is None for code in codes):
        raise SectorCandidateSourceError("P3A sample instruments are invalid or duplicated")
    digest = digest_named_fields("dataset_release_p3a_sample_scope_v1", {"codes": list(codes)})
    return f"sample-{digest}"


def sector_candidate_artifact_root(
    profile: DatasetProfile,
    *,
    cutoff: date,
    sample_instruments: Sequence[str] = (),
) -> Path:
    if not isinstance(cutoff, date):
        raise SectorCandidateSourceError("P3A candidate cutoff must be a date")
    return (
        Path(str(profile.candidate_root))
        / SECTOR_CANDIDATE_NAMESPACE
        / profile.profile
        / cutoff.isoformat()
        / sector_candidate_scope_key(sample_instruments)
    )


@dataclass(slots=True)
class SectorCandidateSource:
    profile: DatasetProfile
    cutoff: date
    sample_instruments: tuple[str, ...]
    artifact_root: Path
    readback: SectorDataCandidateReadback
    assignment_slices: Mapping[date, _DateSlice]
    fact_slices: Mapping[date, _DateSlice]

    @property
    def candidate_hash(self) -> str:
        return str(self.readback.manifest["candidate_hash"])

    @property
    def query_version(self) -> str:
        return f"{SECTOR_CANDIDATE_QUERY_VERSION}:{self.candidate_hash}"

    @property
    def source_table_identity(self) -> str:
        return f"artifact.{SECTOR_CANDIDATE_SOURCE_SCHEMA}.{self.candidate_hash}"

    @classmethod
    def load(
        cls,
        profile: DatasetProfile,
        *,
        cutoff: date,
        pit_snapshot: FrozenPitSnapshot,
        trading_dates: Sequence[date],
        sample_instruments: Sequence[str] = (),
    ) -> "SectorCandidateSource":
        samples = tuple(sorted(str(value).strip().upper() for value in sample_instruments))
        expected_root = sector_candidate_artifact_root(
            profile,
            cutoff=cutoff,
            sample_instruments=samples,
        )
        try:
            allowed_root = Path(str(profile.candidate_root)).resolve(strict=True)
            artifact_root = expected_root.resolve(strict=True)
        except OSError as exc:
            raise SectorCandidateSourceError("P3A candidate deterministic path is unavailable") from exc
        try:
            artifact_root.relative_to(allowed_root)
        except ValueError as exc:
            raise SectorCandidateSourceError("P3A candidate escapes the allowlisted candidate root") from exc
        if artifact_root != expected_root.resolve(strict=False):
            raise SectorCandidateSourceError("P3A candidate deterministic path resolves differently")
        _assert_plain_chain(allowed_root, artifact_root)
        repository_root = Path(__file__).resolve().parents[3]
        readback = read_sector_data_candidate(
            artifact_root=artifact_root,
            forbidden_roots=(repository_root, repository_root.parent / "AIstock_worktrees"),
        )
        expected_scope = "sample" if samples else "full"
        if readback.manifest.get("candidate_scope") != expected_scope:
            raise SectorCandidateSourceError("P3A candidate scope differs from the monthly request")
        _validate_denominator(
            readback,
            pit_snapshot=pit_snapshot,
            trading_dates=trading_dates,
            sample_instruments=samples,
        )
        assignments = _index_jsonl_dates(
            artifact_root / "assignments.jsonl",
            assignment_report=readback.report,
        )
        facts = _index_jsonl_dates(artifact_root / "sector_facts.jsonl")
        if tuple(assignments) != tuple(sorted(set(trading_dates))):
            raise SectorCandidateSourceError("P3A assignment trading-date coverage differs")
        source = cls(
            profile=profile,
            cutoff=cutoff,
            sample_instruments=samples,
            artifact_root=artifact_root,
            readback=readback,
            assignment_slices=assignments,
            fact_slices=facts,
        )
        source.verify_unchanged()
        return source

    def verify_unchanged(self) -> None:
        observed = read_sector_data_candidate(
            artifact_root=self.artifact_root,
            forbidden_roots=(
                Path(__file__).resolve().parents[3],
                Path(__file__).resolve().parents[4] / "AIstock_worktrees",
            ),
        )
        if observed.manifest != self.readback.manifest or observed.report != self.readback.report:
            raise SectorCandidateSourceError("P3A candidate changed during source freeze")

    def iter_rows(
        self,
        *,
        start: date,
        end: date,
        l2_code_map: Mapping[str, int],
    ) -> Iterator[Mapping[str, Any]]:
        if end < start:
            raise SectorCandidateSourceError("P3A source partition is inverted")
        assignment_dates = tuple(self.assignment_slices)
        left = bisect_left(assignment_dates, start)
        right = bisect_right(assignment_dates, end)
        for trade_date in assignment_dates[left:right]:
            facts = {
                str(row["row_hash"]): row
                for row in _read_slice_rows(
                    self.artifact_root / "sector_facts.jsonl",
                    self.fact_slices.get(trade_date),
                )
            }
            for assignment in _read_slice_rows(
                self.artifact_root / "assignments.jsonl",
                self.assignment_slices[trade_date],
            ):
                if assignment.get("status") != "resolved":
                    continue
                if assignment.get("alignment_state") != "aligned":
                    raise SectorCandidateSourceError("resolved P3A assignment is not authority-aligned")
                fact_hash = str(assignment.get("sector_fact_row_hash") or "")
                fact = facts.get(fact_hash)
                if fact is None:
                    raise SectorCandidateSourceError("resolved P3A assignment lacks its sector fact")
                classification = assignment.get("classification")
                identity_codes = (
                    classification.get("identity_codes") if isinstance(classification, Mapping) else None
                )
                l2_code = str(identity_codes.get("l2_code") or "") if isinstance(identity_codes, Mapping) else ""
                if l2_code != str(fact.get("classification_l2_code") or ""):
                    raise SectorCandidateSourceError("P3A assignment/fact classification identity differs")
                index_l2_symbol = _canonical_index_l2_symbol(fact.get("index_l2_code"))
                if index_l2_symbol not in l2_code_map:
                    raise SectorCandidateSourceError("P3A index L2 code is absent from the frozen catalog")
                sw_daily = fact.get("sw_daily")
                moneyflow = fact.get("moneyflow_aggregate")
                if not isinstance(sw_daily, Mapping) or not isinstance(moneyflow, Mapping):
                    raise SectorCandidateSourceError("P3A sector fact numeric payload is invalid")
                row: dict[str, Any] = {
                    "ts_code": str(assignment["canonical_symbol"]),
                    "trade_date": trade_date.isoformat(),
                    "l2_code_id": int(l2_code_map[index_l2_symbol]),
                }
                row.update({_SW_FIELD_MAP[key]: sw_daily[key] for key in _SW_FIELD_MAP})
                row.update({_MONEYFLOW_FIELD_MAP[key]: moneyflow[key] for key in _MONEYFLOW_FIELD_MAP})
                yield row

    def receipt(
        self,
        *,
        code_map_digest: str,
        classify_partitions: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        if _SHA256.fullmatch(code_map_digest) is None:
            raise SectorCandidateSourceError("P3A frozen L2 code-map digest is invalid")
        relative = self.artifact_root.relative_to(Path(str(self.profile.candidate_root)).resolve(strict=True))
        manifest = self.readback.manifest
        report = self.readback.report
        return {
            "schema_version": SECTOR_CANDIDATE_SOURCE_SCHEMA,
            "profile": self.profile.profile,
            "cutoff": self.cutoff.isoformat(),
            "candidate_root_id": self.profile.candidate_root_id,
            "candidate_root_relative_path": relative.as_posix(),
            "candidate_scope": manifest["candidate_scope"],
            "candidate_hash": manifest["candidate_hash"],
            "industry_bundle_hash": manifest["industry_bundle_hash"],
            "classification_authority_receipt_hash": manifest[
                "classification_authority_receipt_hash"
            ],
            "index_membership_authority_receipt_hash": manifest[
                "index_membership_authority_receipt_hash"
            ],
            "source_denominator_digest": manifest["source_denominator_digest"],
            "expected_opportunities": manifest["expected_opportunities"],
            "opportunity_digest": manifest["opportunity_digest"],
            "candidate_report_canonical_hash": report["canonical_hash"],
            "status_counts": dict(report["status_counts"]),
            "alignment_counts": dict(report["alignment_counts"]),
            "unavailable_by_reason": dict(report["unavailable_by_reason"]),
            "query_version": self.query_version,
            "code_map_digest": code_map_digest,
            "classify_partitions": [dict(value) for value in classify_partitions],
            "safety": dict(_ZERO_SAFETY),
        }


def _validate_denominator(
    readback: SectorDataCandidateReadback,
    *,
    pit_snapshot: FrozenPitSnapshot,
    trading_dates: Sequence[date],
    sample_instruments: Sequence[str],
) -> None:
    dates = tuple(sorted(set(trading_dates)))
    if (
        not dates
        or len(dates) != len(tuple(trading_dates))
        or dates[0] < pit_snapshot.scope_start
        or dates[-1] != pit_snapshot.cutoff
    ):
        raise SectorCandidateSourceError("P3A trading calendar differs from the frozen PIT scope")
    selected = frozenset(sample_instruments)
    pit_codes = {span.ts_code for span in pit_snapshot.spans}
    if selected and not selected.issubset(pit_codes):
        raise SectorCandidateSourceError("P3A sample instruments escape the frozen PIT snapshot")
    count = 0
    digest = hashlib.sha256()
    for trade_date in dates:
        symbols = sorted(
            span.ts_code
            for span in pit_snapshot.spans
            if span.eligible_start <= trade_date <= span.eligible_end
            and (not selected or span.ts_code in selected)
        )
        if not symbols:
            raise SectorCandidateSourceError("P3A frozen denominator has an empty trading day")
        if len(symbols) != len(set(symbols)):
            raise SectorCandidateSourceError("P3A frozen PIT spans overlap for a symbol/date")
        for symbol in symbols:
            digest.update(
                canonical_json_bytes(
                    {
                        "schema_version": SECTOR_DATA_OPPORTUNITY_SCHEMA,
                        "trade_date": trade_date.isoformat(),
                        "canonical_symbol": symbol,
                    }
                )
                + b"\n"
            )
            count += 1
    manifest = readback.manifest
    if count != manifest.get("expected_opportunities") or digest.hexdigest() != manifest.get(
        "opportunity_digest"
    ):
        raise SectorCandidateSourceError("P3A candidate denominator/opportunity identity differs")


def _index_jsonl_dates(
    path: Path,
    *,
    assignment_report: Mapping[str, Any] | None = None,
) -> Mapping[date, _DateSlice]:
    result: dict[date, _DateSlice] = {}
    previous: date | None = None
    statuses: Counter[str] = Counter()
    alignments: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    reasons_by_date: dict[str, Counter[str]] = defaultdict(Counter)
    reasons_by_sector: dict[str, Counter[str]] = defaultdict(Counter)
    with path.open("rb") as handle:
        while True:
            start = handle.tell()
            line = handle.readline()
            if not line:
                break
            end = handle.tell()
            try:
                value = json.loads(line)
                trade_date = date.fromisoformat(str(value["trade_date"]))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise SectorCandidateSourceError("P3A JSONL date index is invalid") from exc
            if previous is not None and trade_date < previous:
                raise SectorCandidateSourceError("P3A JSONL date order is not canonical")
            current = result.get(trade_date)
            result[trade_date] = _DateSlice(current.start if current else start, end)
            if assignment_report is not None:
                status = str(value.get("status") or "")
                alignment = str(value.get("alignment_state") or "")
                statuses[status] += 1
                alignments[alignment] += 1
                classification = value.get("classification")
                identity_codes = (
                    classification.get("identity_codes")
                    if isinstance(classification, Mapping)
                    else None
                )
                sector = (
                    str(identity_codes.get("l1_code") or "unavailable")
                    if isinstance(identity_codes, Mapping)
                    else "unavailable"
                )
                for raw_reason in value.get("unavailable_reasons") or ():
                    reason = str(raw_reason)
                    reasons[reason] += 1
                    reasons_by_date[trade_date.isoformat()][reason] += 1
                    reasons_by_sector[sector][reason] += 1
            previous = trade_date
    if assignment_report is not None:
        observed = {
            "status_counts": dict(sorted(statuses.items())),
            "alignment_counts": dict(sorted(alignments.items())),
            "unavailable_by_reason": dict(sorted(reasons.items())),
            "unavailable_by_date": {
                key: dict(sorted(value.items())) for key, value in sorted(reasons_by_date.items())
            },
            "unavailable_by_sector": {
                key: dict(sorted(value.items())) for key, value in sorted(reasons_by_sector.items())
            },
        }
        changed = sorted(
            key for key, value in observed.items() if assignment_report.get(key) != value
        )
        if changed:
            raise SectorCandidateSourceError(
                f"P3A assignment/report aggregate readback differs: {changed}"
            )
    return dict(sorted(result.items()))


def _read_slice_rows(path: Path, value: _DateSlice | None) -> Iterable[Mapping[str, Any]]:
    if value is None:
        return ()

    def rows() -> Iterator[Mapping[str, Any]]:
        with path.open("rb") as handle:
            handle.seek(value.start)
            while handle.tell() < value.end:
                raw = handle.readline()
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise SectorCandidateSourceError("P3A JSONL partition readback is invalid") from exc
                if not isinstance(parsed, Mapping):
                    raise SectorCandidateSourceError("P3A JSONL row is not a mapping")
                yield parsed

    return rows()


def _canonical_index_l2_symbol(value: Any) -> str:
    if not isinstance(value, str):
        raise SectorCandidateSourceError("P3A index L2 code is not a canonical string")
    if _BARE_INDEX_CODE.fullmatch(value):
        return f"{value}.SI"
    if _CANONICAL_INDEX_SYMBOL.fullmatch(value):
        return value
    raise SectorCandidateSourceError("P3A index L2 code is not canonical")


def _assert_plain_chain(allowed_root: Path, target: Path) -> None:
    current = allowed_root
    for part in target.relative_to(allowed_root).parts:
        current /= part
        metadata = current.lstat()
        attributes = int(getattr(metadata, "st_file_attributes", 0))
        reparse = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
        if stat.S_ISLNK(metadata.st_mode) or attributes & reparse:
            raise SectorCandidateSourceError("P3A candidate path contains a link/reparse point")
    if not target.is_dir():
        raise SectorCandidateSourceError("P3A candidate root is not a directory")


__all__ = [
    "SECTOR_CANDIDATE_QUERY_VERSION",
    "SECTOR_CANDIDATE_SOURCE_SCHEMA",
    "SectorCandidateSource",
    "SectorCandidateSourceError",
    "sector_candidate_artifact_root",
    "sector_candidate_scope_key",
]
