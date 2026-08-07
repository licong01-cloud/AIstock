"""Build ``qe_suspend_filter.json`` from the frozen suspend_d candidate dataset.

BUG-989 data-plane invariant: the QE/multi-alpha train, predict, backtest and
combine computation data plane must not access PostgreSQL.  The suspend filter
artifact is therefore derived exclusively from frozen files:

- ``qe_frozen_build_spec.json`` (workspace) -> window + suspend pin section
- ``<provider_uri_day>/calendars/day.txt`` -> pinned frozen trading calendar
- ``<suspend provider_uri>/suspend_d.parquet`` + ``manifest.json`` -> pinned
  per-day suspend rows (exported read-only during the offline dataset build
  phase by ``scripts/export_suspend_d_candidate.py``)

This helper runs inside the workspace before ``qrun`` (invoked from
``qrun_limit.py`` / ``qrun_limit_minute.py``) and rebuilds the runtime artifact
deterministically.  Any pin mismatch, identity drift, missing file, missing
field or coverage gap fails loud; there is no database fallback, no online
backfill and no silent degradation.  A missing optional runtime dependency
(pandas/pyarrow) also fails loud instead of disabling the suspend filter.

Date-completeness contract: the emitted ``suspended_by_date`` contains one key
for *every* trading day of the pinned frozen calendar inside the requested
window; days with zero suspensions map to an empty list.  The strict runtime
``QESuspendFilter`` raises on a missing date key, so a sparse parquet can never
be confused with "day not exported": the manifest ``daily_row_counts`` receipt
must cover every calendar day, and coverage mismatches fail closed here.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Any

SPEC_FILE = "qe_frozen_build_spec.json"
ARTIFACT_FILE = "qe_suspend_filter.json"
SPEC_SCHEMA_VERSION = "qe_frozen_build_spec_v1"
SUSPEND_PARQUET_NAME = "suspend_d.parquet"
SUSPEND_MANIFEST_NAME = "manifest.json"
MANIFEST_SCHEMA_VERSION = "suspend_d_dataset_manifest_v1"
CANONICAL_TS_CODE_RE = re.compile(r"^[0-9]{6}\.(SH|SZ)$")
REQUIRED_PARQUET_COLUMNS = ("trade_date", "ts_code", "suspend_type")


class FrozenSuspendFilterBuildError(RuntimeError):
    """Fail-loud builder error carrying a stable reason_code in the message."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_date(value: Any, *, field: str) -> date:
    try:
        return date.fromisoformat(str(value)[:10])
    except Exception as exc:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_build_spec_invalid: "
            f"field={field} value={value!r} is not an ISO date"
        ) from exc


def _load_spec(cwd: Path) -> dict[str, Any]:
    spec_path = cwd / SPEC_FILE
    with spec_path.open("r", encoding="utf-8") as handle:
        spec = json.load(handle)
    if not isinstance(spec, dict):
        raise FrozenSuspendFilterBuildError(
            f"reason_code=qe_frozen_build_spec_invalid: {spec_path} is not a JSON object"
        )
    if spec.get("schema_version") != SPEC_SCHEMA_VERSION:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_build_spec_invalid: "
            f"schema_version={spec.get('schema_version')!r} expected={SPEC_SCHEMA_VERSION!r}"
        )
    return spec


def _load_calendar_window(provider_dir: Path, pins: dict[str, Any], start: date, end: date) -> list[date]:
    calendar_path = provider_dir / "calendars" / "day.txt"
    if not calendar_path.is_file():
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_universe_file_missing: "
            f"path={calendar_path} (frozen qlib bin dataset is incomplete)"
        )
    expected = str(pins.get("calendar_sha256") or "")
    actual = _sha256_file(calendar_path)
    if not expected or actual != expected:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_universe_pin_mismatch: "
            f"path={calendar_path} actual_sha256={actual} expected_sha256={expected}"
        )
    days: list[date] = []
    with calendar_path.open("r", encoding="utf-8", newline="") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            day = _parse_date(text, field="calendar_day")
            if start <= day <= end:
                days.append(day)
    days.sort()
    if not days:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_calendar_window_empty: "
            f"calendar={calendar_path} window={start.isoformat()}..{end.isoformat()}"
        )
    return days


def _verify_suspend_dataset(
    suspend_dir: Path,
    suspend_spec: dict[str, Any],
) -> tuple[Path, dict[str, Any]]:
    parquet_path = suspend_dir / SUSPEND_PARQUET_NAME
    manifest_path = suspend_dir / SUSPEND_MANIFEST_NAME
    for path in (parquet_path, manifest_path):
        if not path.is_file():
            raise FrozenSuspendFilterBuildError(
                "reason_code=qe_frozen_suspend_file_missing: "
                f"path={path} (frozen suspend_d candidate dataset is incomplete)"
            )
    expected_hashes = {
        parquet_path: suspend_spec.get("parquet_sha256"),
        manifest_path: suspend_spec.get("manifest_sha256"),
    }
    for path, expected in expected_hashes.items():
        if not expected:
            raise FrozenSuspendFilterBuildError(
                "reason_code=qe_frozen_build_spec_invalid: "
                f"suspend pins missing sha256 for {path.name}"
            )
        actual = _sha256_file(path)
        if actual != str(expected):
            raise FrozenSuspendFilterBuildError(
                "reason_code=qe_frozen_suspend_pin_mismatch: "
                f"path={path} actual_sha256={actual} expected_sha256={expected}; "
                "the frozen suspend dataset does not match the deployed contract pins"
            )

    with manifest_path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_suspend_identity_mismatch: "
            f"manifest schema_version={manifest.get('schema_version')!r} "
            f"expected={MANIFEST_SCHEMA_VERSION!r}"
        )
    for key in ("dataset_id", "universe_key", "source_contract"):
        expected_value = str(suspend_spec.get(key) or "")
        actual_value = str(
            manifest.get(key)
            if key != "source_contract"
            else (manifest.get("source") or {}).get("contract") or ""
        )
        if not expected_value or actual_value != expected_value:
            raise FrozenSuspendFilterBuildError(
                "reason_code=qe_frozen_suspend_identity_mismatch: "
                f"field={key} actual={actual_value!r} expected={expected_value!r}"
            )
    return parquet_path, manifest


def _load_suspend_frame(parquet_path: Path):
    try:
        import pandas as pd  # noqa: PLC0415
    except ImportError as exc:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_suspend_dependency_missing: "
            "pandas is required to read the frozen suspend_d.parquet; "
            "the suspend filter is never silently disabled"
        ) from exc
    try:
        frame = pd.read_parquet(parquet_path)
    except ImportError as exc:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_suspend_dependency_missing: "
            f"a parquet engine (pyarrow) is required to read {parquet_path.name}: {exc}"
        ) from exc
    except Exception as exc:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_suspend_file_unreadable: "
            f"path={parquet_path} error={type(exc).__name__}: {exc}"
        ) from exc
    missing = [col for col in REQUIRED_PARQUET_COLUMNS if col not in frame.columns]
    if missing:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_suspend_field_missing: "
            f"path={parquet_path} missing_columns={missing} "
            f"required={list(REQUIRED_PARQUET_COLUMNS)}"
        )
    return frame


def build_suspend_filter_payload(spec: dict[str, Any]) -> dict[str, Any]:
    """Build the runtime suspend-filter artifact payload from frozen files."""

    suspend_spec = spec.get("suspend")
    if not isinstance(suspend_spec, dict):
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_suspend_spec_missing: "
            f"{SPEC_FILE} has no suspend pin section"
        )
    provider_uri = str(spec.get("provider_uri_day") or "").strip()
    if not provider_uri:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_build_spec_invalid: provider_uri_day is required"
        )
    provider_dir = Path(os.path.expanduser(provider_uri))
    if not provider_dir.is_dir():
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_universe_dir_missing: "
            f"provider_uri_day={provider_dir} is not a directory"
        )
    suspend_uri = str(suspend_spec.get("provider_uri") or "").strip()
    if not suspend_uri:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_build_spec_invalid: suspend.provider_uri is required"
        )
    suspend_dir = Path(os.path.expanduser(suspend_uri))
    if not suspend_dir.is_dir():
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_suspend_dir_missing: "
            f"suspend.provider_uri={suspend_dir} is not a directory"
        )

    start = _parse_date(spec.get("start_date"), field="start_date")
    end = _parse_date(spec.get("end_date"), field="end_date")
    if end < start:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_build_spec_invalid: "
            f"end_date={end.isoformat()} earlier than start_date={start.isoformat()}"
        )

    pins = spec.get("pins")
    if not isinstance(pins, dict):
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_build_spec_invalid: pins must be an object"
        )
    calendar_days = _load_calendar_window(provider_dir, pins, start, end)
    parquet_path, manifest = _verify_suspend_dataset(suspend_dir, suspend_spec)

    # Window coverage: the frozen suspend dataset must cover the whole request.
    manifest_start = _parse_date(manifest.get("start"), field="manifest.start")
    manifest_end = _parse_date(manifest.get("end"), field="manifest.end")
    if manifest_start > start or manifest_end < end:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_suspend_window_not_covered: "
            f"dataset={manifest_start.isoformat()}..{manifest_end.isoformat()} "
            f"requested={start.isoformat()}..{end.isoformat()}"
        )

    # Independent date-completeness receipt: the manifest must carry an
    # explicit row count for every pinned calendar day inside the window, so a
    # zero-suspension day is provably different from a day that was not
    # exported.
    daily_row_counts = manifest.get("daily_row_counts")
    if not isinstance(daily_row_counts, dict):
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_suspend_coverage_receipt_missing: "
            "manifest.daily_row_counts must map every trading day to a row count"
        )
    missing_days = [d.isoformat() for d in calendar_days if d.isoformat() not in daily_row_counts]
    if missing_days:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_suspend_coverage_receipt_incomplete: "
            f"manifest daily_row_counts missing {len(missing_days)} calendar days "
            f"first={missing_days[:3]}"
        )

    frame = _load_suspend_frame(parquet_path)
    frame = frame[frame["suspend_type"] == "S"]
    ts_codes = frame["ts_code"].astype(str)
    bad_codes = sorted({code for code in ts_codes if not CANONICAL_TS_CODE_RE.match(code)})
    if bad_codes:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_suspend_symbol_invalid: "
            f"non-canonical sh/sz ts_code values in frozen parquet: {bad_codes[:10]}"
        )

    trade_dates = frame["trade_date"]
    day_series = trade_dates.map(lambda value: _parse_date(value, field="trade_date"))
    out_of_window = sorted({d for d in day_series if d < manifest_start or d > manifest_end})
    if out_of_window:
        raise FrozenSuspendFilterBuildError(
            "reason_code=qe_frozen_suspend_date_outside_window: "
            f"parquet rows outside declared dataset window: "
            f"{[d.isoformat() for d in out_of_window[:3]]}"
        )

    by_date: dict[str, list[str]] = {}
    for day, code in zip(day_series, ts_codes, strict=True):
        key = day.isoformat()
        if start <= day <= end:
            bucket = by_date.setdefault(key, [])
            bucket.append(code)

    # Every pinned calendar day inside the window gets an explicit key; days
    # with zero suspensions map to an empty list (strict runtime contract).
    suspended_by_date: dict[str, list[str]] = {}
    total_rows = 0
    for day in calendar_days:
        key = day.isoformat()
        symbols = sorted(set(by_date.get(key, [])))
        expected_count = int(daily_row_counts[key])
        if len(symbols) != expected_count:
            raise FrozenSuspendFilterBuildError(
                "reason_code=qe_frozen_suspend_coverage_mismatch: "
                f"date={key} parquet_rows={len(symbols)} manifest_count={expected_count}"
            )
        suspended_by_date[key] = symbols
        total_rows += len(symbols)

    return {
        "enabled": True,
        "source": "frozen:suspend_d.parquet",
        "audit_dataset": str(suspend_spec.get("dataset_id") or ""),
        "source_contract": str(suspend_spec.get("source_contract") or ""),
        "strict_audit": True,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "trade_date_count": len(calendar_days),
        "suspended_row_count": total_rows,
        "suspend_d_parquet_sha256": str(suspend_spec.get("parquet_sha256") or ""),
        "suspend_d_manifest_sha256": str(suspend_spec.get("manifest_sha256") or ""),
        "suspended_by_date": suspended_by_date,
    }


def ensure_frozen_suspend_filter_artifact(
    cwd: str | os.PathLike[str] | None = None,
    *,
    print_fn=print,
) -> Path | None:
    """Rebuild ``qe_suspend_filter.json`` from the frozen suspend dataset.

    Returns the artifact path, or ``None`` when the workspace carries no
    frozen build spec or the spec has no suspend section (legacy workspace:
    leave existing artifacts untouched).  The rebuild is deterministic;
    identical inputs produce identical bytes.
    """

    base = Path(cwd or os.getcwd())
    spec_path = base / SPEC_FILE
    if not spec_path.is_file():
        return None
    spec = _load_spec(base)
    if not isinstance(spec.get("suspend"), dict):
        return None
    payload = build_suspend_filter_payload(spec)
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    artifact_path = base / ARTIFACT_FILE
    tmp_path = artifact_path.with_suffix(artifact_path.suffix + ".tmp")
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(artifact_path)
    print_fn(
        "[INFO] QE frozen suspend filter artifact built: "
        f"{artifact_path.name} trade_dates={payload['trade_date_count']} "
        f"suspended_rows={payload['suspended_row_count']} "
        f"dataset={payload['audit_dataset']}"
    )
    return artifact_path


if __name__ == "__main__":
    ensure_frozen_suspend_filter_artifact()
