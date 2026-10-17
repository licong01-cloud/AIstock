"""Shared, fail-closed security identity resolution for frozen data exports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterable

import pandas as pd


SCHEMA_VERSION = "aistock_security_source_identity_v1"
DEFAULT_RESOLUTION = "canonical_same_code"
MONEYFLOW_DATASET = "market.moneyflow_ts"
SUPPORTED_SOURCE_DATASETS = frozenset(
    {
        "market.daily_basic",
        "market.kline_daily_raw",
        MONEYFLOW_DATASET,
        "market.stk_limit",
    }
)
DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parent / "manifests" / "security_source_identity_v1.json"
_TS_CODE_RE = re.compile(r"^[0-9]{6}\.(?:BJ|SH|SZ)$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class SecuritySourceIdentityError(ValueError):
    """The shared source identity authority is invalid or ambiguous."""


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class SecuritySourceResolution:
    security_identity_id: str
    canonical_ts_code: str
    source_dataset: str
    source_ts_code: str
    effective_start: date | None
    effective_end: date | None
    authority_ref: str
    authority_hash: str
    row_hash: str
    resolution_kind: str

    def evidence(self) -> dict[str, Any]:
        return {
            "security_identity_id": self.security_identity_id,
            "canonical_ts_code": self.canonical_ts_code,
            "source_dataset": self.source_dataset,
            "source_ts_code": self.source_ts_code,
            "effective_start": None if self.effective_start is None else self.effective_start.isoformat(),
            "effective_end": None if self.effective_end is None else self.effective_end.isoformat(),
            "authority_ref": self.authority_ref,
            "authority_hash": self.authority_hash,
            "row_hash": self.row_hash,
            "resolution_kind": self.resolution_kind,
        }


@dataclass(frozen=True, slots=True)
class SecuritySourceIdentityManifest:
    manifest_version: str
    default_resolution: str
    rows: tuple[SecuritySourceResolution, ...]
    manifest_sha256: str
    rows_sha256: str
    source_path: Path
    file_sha256: str

    def resolve(self, canonical_ts_code: str, trade_date: date, source_dataset: str) -> SecuritySourceResolution:
        _validate_ts_code(canonical_ts_code, "canonical_ts_code")
        if not isinstance(trade_date, date):
            raise SecuritySourceIdentityError("security source identity trade_date is invalid")
        _validate_source_dataset(source_dataset)
        matches = [
            row
            for row in self.rows
            if row.canonical_ts_code == canonical_ts_code
            and row.source_dataset == source_dataset
            and row.effective_start is not None
            and row.effective_end is not None
            and row.effective_start <= trade_date <= row.effective_end
        ]
        if len(matches) > 1:
            raise SecuritySourceIdentityError(
                f"ambiguous security source identity: {canonical_ts_code}/{trade_date}/{source_dataset}"
            )
        if matches:
            return matches[0]
        if self.default_resolution != DEFAULT_RESOLUTION:
            raise SecuritySourceIdentityError(
                f"unresolved security source identity: {canonical_ts_code}/{trade_date}/{source_dataset}"
            )
        body = {
            "default_resolution": self.default_resolution,
            "security_identity_id": f"canonical:{canonical_ts_code}",
            "canonical_ts_code": canonical_ts_code,
            "source_dataset": source_dataset,
            "source_ts_code": canonical_ts_code,
        }
        return SecuritySourceResolution(
            security_identity_id=body["security_identity_id"],
            canonical_ts_code=canonical_ts_code,
            source_dataset=source_dataset,
            source_ts_code=canonical_ts_code,
            effective_start=None,
            effective_end=None,
            authority_ref=f"manifest-default:{self.manifest_version}",
            authority_hash=self.manifest_sha256,
            row_hash=canonical_sha256(body),
            resolution_kind=DEFAULT_RESOLUTION,
        )

    def query_source_codes(
        self,
        canonical_codes: Iterable[str],
        start: date,
        end: date,
        source_dataset: str,
    ) -> list[str]:
        _validate_source_dataset(source_dataset)
        requested = {_validated_ts_code(value, "canonical_ts_code") for value in canonical_codes}
        result = set(requested)
        result.update(
            row.source_ts_code
            for row in self.rows
            if row.source_dataset == source_dataset
            and row.canonical_ts_code in requested
            and row.effective_start is not None
            and row.effective_end is not None
            and row.effective_start <= end
            and row.effective_end >= start
        )
        return sorted(result)

    def remap_source_rows(
        self,
        frame: pd.DataFrame,
        *,
        canonical_codes: Iterable[str],
        source_dataset: str,
        trade_date_column: str = "trade_date",
        source_code_column: str = "ts_code",
    ) -> pd.DataFrame:
        """Map source rows to canonical identities and reject any collision."""

        mapped = self.annotate_source_rows(
            frame,
            canonical_codes=canonical_codes,
            source_dataset=source_dataset,
            trade_date_column=trade_date_column,
            source_code_column=source_code_column,
        )
        mapped[source_code_column] = mapped.pop("_canonical_ts_code")
        return mapped

    def annotate_source_rows(
        self,
        frame: pd.DataFrame,
        *,
        canonical_codes: Iterable[str],
        source_dataset: str,
        trade_date_column: str = "trade_date",
        source_code_column: str = "ts_code",
    ) -> pd.DataFrame:
        """Select authorized source facts and attach their canonical identity."""

        _validate_source_dataset(source_dataset)
        requested = {_validated_ts_code(value, "canonical_ts_code") for value in canonical_codes}
        if frame.empty:
            out = frame.copy()
            out["_canonical_ts_code"] = pd.Series(dtype="object")
            return out
        missing = {trade_date_column, source_code_column} - set(frame.columns)
        if missing:
            raise SecuritySourceIdentityError(f"source frame misses identity columns: {sorted(missing)}")
        aliases_by_source: dict[str, list[SecuritySourceResolution]] = {}
        for row in self.rows:
            if row.source_dataset == source_dataset and row.canonical_ts_code in requested:
                aliases_by_source.setdefault(row.source_ts_code, []).append(row)

        mapped = frame.copy()
        canonical_values: list[str | None] = []
        for row in mapped[[trade_date_column, source_code_column]].itertuples(index=False, name=None):
            trade_date = pd.Timestamp(row[0]).date()
            source_code = _validated_ts_code(str(row[1]), "source_ts_code")
            matches = {
                alias.canonical_ts_code
                for alias in aliases_by_source.get(source_code, [])
                if alias.effective_start is not None
                and alias.effective_end is not None
                and alias.effective_start <= trade_date <= alias.effective_end
            }
            if source_code in requested:
                default_resolution = self.resolve(source_code, trade_date, source_dataset)
                if default_resolution.source_ts_code == source_code:
                    matches.add(source_code)
            if len(matches) > 1:
                raise SecuritySourceIdentityError(
                    f"source row maps to multiple canonical identities: {source_code}/{trade_date}/{sorted(matches)}"
                )
            canonical_values.append(next(iter(matches)) if matches else None)
        mapped["_canonical_ts_code"] = canonical_values
        mapped = mapped[mapped["_canonical_ts_code"].notna()].copy()
        duplicates = mapped.duplicated([trade_date_column, "_canonical_ts_code"], keep=False)
        if duplicates.any():
            sample = mapped.loc[duplicates, [trade_date_column, source_code_column, "_canonical_ts_code"]]
            sample = sample.head(5).to_dict("records")
            raise SecuritySourceIdentityError(f"duplicate canonical source facts after identity mapping: {sample}")
        return mapped

    def evidence(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "manifest_version": self.manifest_version,
            "default_resolution": self.default_resolution,
            "row_count": len(self.rows),
            "rows_sha256": self.rows_sha256,
            "canonical_sha256": self.manifest_sha256,
            "file_sha256": self.file_sha256,
        }


def _validated_ts_code(value: str, field: str) -> str:
    _validate_ts_code(value, field)
    return value


def _validate_ts_code(value: str, field: str) -> None:
    if not isinstance(value, str) or _TS_CODE_RE.fullmatch(value) is None:
        raise SecuritySourceIdentityError(f"{field} is invalid")


def _validate_source_dataset(value: str) -> None:
    if value not in SUPPORTED_SOURCE_DATASETS:
        raise SecuritySourceIdentityError(f"unsupported source_dataset={value}")


def _parse_date(value: Any, field: str) -> date:
    if not isinstance(value, str):
        raise SecuritySourceIdentityError(f"{field} is invalid")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SecuritySourceIdentityError(f"{field} is invalid") from exc


def _parse_row(value: Any) -> SecuritySourceResolution:
    required = {
        "security_identity_id",
        "canonical_ts_code",
        "source_dataset",
        "source_ts_code",
        "effective_start",
        "effective_end",
        "authority_ref",
        "authority_hash",
        "row_hash",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise SecuritySourceIdentityError("security source identity row schema is invalid")
    canonical = str(value["canonical_ts_code"])
    source = str(value["source_ts_code"])
    _validate_ts_code(canonical, "canonical_ts_code")
    _validate_ts_code(source, "source_ts_code")
    if canonical == source:
        raise SecuritySourceIdentityError("explicit security identity must change the source code")
    dataset = str(value["source_dataset"])
    _validate_source_dataset(dataset)
    start = _parse_date(value["effective_start"], "effective_start")
    end = _parse_date(value["effective_end"], "effective_end")
    if start > end:
        raise SecuritySourceIdentityError("security source identity interval is invalid")
    authority_hash = str(value["authority_hash"]).lower()
    row_hash = str(value["row_hash"]).lower()
    body = {key: value[key] for key in sorted(required - {"row_hash"})}
    if _SHA256_RE.fullmatch(authority_hash) is None or canonical_sha256(body) != row_hash:
        raise SecuritySourceIdentityError("security source identity row hash mismatch")
    return SecuritySourceResolution(
        security_identity_id=str(value["security_identity_id"]),
        canonical_ts_code=canonical,
        source_dataset=dataset,
        source_ts_code=source,
        effective_start=start,
        effective_end=end,
        authority_ref=str(value["authority_ref"]),
        authority_hash=authority_hash,
        row_hash=row_hash,
        resolution_kind="explicit_effective_alias",
    )


def load_security_source_identity_manifest(path: Path) -> SecuritySourceIdentityManifest:
    manifest_path = Path(path).resolve(strict=True)
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise SecuritySourceIdentityError("security source identity manifest must be a regular file")
    raw = manifest_path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SecuritySourceIdentityError("security source identity manifest cannot be decoded") from exc
    expected_fields = {"schema_version", "manifest_version", "default_resolution", "rows"}
    if not isinstance(payload, dict) or set(payload) != expected_fields:
        raise SecuritySourceIdentityError("security source identity manifest schema is invalid")
    if payload["schema_version"] != SCHEMA_VERSION or payload["default_resolution"] != DEFAULT_RESOLUTION:
        raise SecuritySourceIdentityError("security source identity manifest contract is invalid")
    rows_value = payload["rows"]
    if not isinstance(rows_value, list):
        raise SecuritySourceIdentityError("security source identity rows must be a list")
    rows = tuple(_parse_row(item) for item in rows_value)
    ordering = [(row.source_dataset, row.canonical_ts_code, row.effective_start, row.effective_end) for row in rows]
    if ordering != sorted(ordering) or len(ordering) != len(set(ordering)):
        raise SecuritySourceIdentityError("security source identity rows are not uniquely ordered")
    for previous, current in zip(rows, rows[1:], strict=False):
        if (
            previous.source_dataset == current.source_dataset
            and previous.canonical_ts_code == current.canonical_ts_code
            and previous.effective_end is not None
            and current.effective_start is not None
            and current.effective_start <= previous.effective_end
        ):
            raise SecuritySourceIdentityError("security source identity intervals overlap")
    manifest_version = str(payload["manifest_version"]).strip()
    if not manifest_version:
        raise SecuritySourceIdentityError("security source identity manifest_version is empty")
    return SecuritySourceIdentityManifest(
        manifest_version=manifest_version,
        default_resolution=DEFAULT_RESOLUTION,
        rows=rows,
        manifest_sha256=canonical_sha256(payload),
        rows_sha256=canonical_sha256(rows_value),
        source_path=manifest_path,
        file_sha256=hashlib.sha256(raw).hexdigest(),
    )


@lru_cache(maxsize=1)
def load_default_security_source_identity_manifest() -> SecuritySourceIdentityManifest:
    return load_security_source_identity_manifest(DEFAULT_MANIFEST_PATH)
