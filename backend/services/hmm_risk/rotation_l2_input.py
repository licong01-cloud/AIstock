"""Frozen direct-v2 input builder for the L2 rotation contract.

The reader accepts one explicit active-profile path and one explicit candidate
root.  It never discovers ``latest`` data, queries a database, or repairs an
input.  All derived rows are bound to the release manifest and shared C-013
industry authorities before they reach the scoring code.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from backend.services.dataset_release.shared_sector_context import (
    load_release_sw_l2_code_map,
    load_sector_quote_availability,
    require_pinned_sector_context_files,
    validate_membership_frame,
    validate_sector_context_pins,
)
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.provider_absence import ProviderAbsenceManifest, load_provider_absence_manifest
from backend.services.hmm_risk.rotation_l2 import (
    DEVELOPMENT_END,
    DEVELOPMENT_START,
    FEATURE_DAYS,
    INPUT_SCHEMA,
    RotationL2Error,
    validate_input_bundle,
)
from backend.services.hmm_risk.security_identity import (
    SecuritySourceIdentityManifest,
    load_security_source_identity_manifest,
)


PROFILE_SCHEMA = "aistock_active_dataset_profile_v3"
MANIFEST_SCHEMA = "qe_dataset_manifest_v1"
EXPECTED_RELEASE = "qe_hmm_full_v2_20260831"
EXPECTED_CUTOFF = "2026-08-31"
BENCHMARK_CODE = "000300.SH"
FACTOR_ROOT = Path("components/factor_h5_static_candidate_v2")


def _fail(suffix: str, message: str, **context: Any) -> RotationL2Error:
    return RotationL2Error(f"hmm_risk_rotation_l2_{suffix}", message, context=context)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_link(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _require_file(root: Path, relative: str, expected_sha256: str) -> Path:
    path = root / Path(relative)
    if _is_link(path) or not path.is_file():
        raise _fail("input_identity_invalid", "pinned input is absent or linked", path=str(path))
    actual = _sha256_file(path)
    if actual != expected_sha256:
        raise _fail(
            "input_identity_invalid",
            "pinned input hash differs",
            path=str(path),
            expected=expected_sha256,
            actual=actual,
        )
    return path


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _fail("input_identity_invalid", "input JSON is invalid", path=str(path)) from exc
    if not isinstance(value, dict):
        raise _fail("input_identity_invalid", "input JSON root is not an object", path=str(path))
    return value


def _parse_calendar(path: Path) -> list[date]:
    try:
        values = [
            date.fromisoformat(line.strip()) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
        ]
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise _fail("calendar_invalid", "release calendar is invalid", path=str(path)) from exc
    if values != sorted(set(values)):
        raise _fail("calendar_invalid", "release calendar is not sorted and unique")
    if DEVELOPMENT_START not in values or DEVELOPMENT_END not in values:
        raise _fail("history_unavailable", "development boundaries are absent from release calendar")
    return values


def _parse_provider_spans(path: Path) -> dict[str, tuple[date, date]]:
    spans: dict[str, tuple[date, date]] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        for line in lines:
            if not line.strip():
                continue
            symbol, start_raw, end_raw = line.split("\t")
            symbol = symbol.strip().upper()
            if symbol in spans:
                raise ValueError(f"duplicate instrument {symbol}")
            spans[symbol] = (date.fromisoformat(start_raw), date.fromisoformat(end_raw))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise _fail("source_invalid", "provider instrument spans are invalid", path=str(path)) from exc
    return spans


def _hdf_slice(path: Path, *, start: date, end: date, columns: list[str]) -> pd.DataFrame:
    expression = f"(datetime>='{start.isoformat()}') & (datetime<='{end.isoformat()}')"
    try:
        with pd.HDFStore(path, mode="r") as store:
            if store.keys() != ["/data"]:
                raise ValueError("HDF must contain exactly /data")
            frame = store.select("/data", where=expression, columns=columns).reset_index()
    except (OSError, ValueError, KeyError) as exc:
        raise _fail("source_invalid", "pinned HDF cannot be read", path=str(path)) from exc
    if frame.empty:
        raise _fail("history_unavailable", "pinned HDF slice is empty", path=str(path))
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce").dt.date
    frame["instrument"] = frame["instrument"].astype(str).str.strip().str.upper()
    if frame[["datetime", "instrument"]].isna().any().any() or frame.duplicated(["datetime", "instrument"]).any():
        raise _fail("source_invalid", "pinned HDF identity is invalid or duplicated", path=str(path))
    return frame


def _availability(entries: Mapping[str, tuple[tuple[date, date], ...]], code: str, day: date) -> bool:
    return any(start <= day <= end for start, end in entries[code])


def _expand_expected(
    membership: pd.DataFrame,
    *,
    id_to_code: Mapping[int, str],
    provider_spans: Mapping[str, tuple[date, date]],
    source_days: Iterable[date],
    suspended: set[tuple[date, str]],
) -> tuple[pd.DataFrame, dict[tuple[date, str], int]]:
    source_days = tuple(source_days)
    rows: list[tuple[date, str, str]] = []
    member_counts: dict[tuple[date, str], int] = defaultdict(int)
    for raw in membership.itertuples(index=False):
        code = id_to_code[int(raw.l2_code_id)]
        provider = provider_spans.get(str(raw.instrument))
        if provider is None:
            raise _fail(
                "source_invalid",
                "membership instrument is absent from PIT provider universe",
                instrument=raw.instrument,
            )
        lower = max(raw.start_date, provider[0])
        upper = min(raw.end_date, provider[1])
        for day in source_days:
            if lower <= day <= upper:
                member_counts[(day, code)] += 1
                if (day, str(raw.instrument)) not in suspended:
                    rows.append((day, str(raw.instrument), code))
    expected = pd.DataFrame(rows, columns=["trade_date", "instrument", "sector_code"])
    if expected.empty or expected.duplicated(["trade_date", "instrument"]).any():
        raise _fail("source_invalid", "expanded PIT membership is empty or ambiguous")
    return expected, dict(member_counts)


def _qlib_amount_frame(
    qlib_root: Path,
    *,
    calendar: list[date],
    expected: pd.DataFrame,
) -> tuple[pd.DataFrame, str]:
    """Read only the pinned day-bin amount field for the exact PIT population."""

    if _is_link(qlib_root) or _is_link(qlib_root / "features"):
        raise _fail("input_identity_invalid", "Qlib amount root is linked")
    positions = {day: index for index, day in enumerate(calendar)}
    rows: list[tuple[date, str, float]] = []
    content_hashes: list[dict[str, str]] = []
    for symbol, group in expected.groupby("instrument", sort=True):
        path = qlib_root / "features" / str(symbol).lower() / "amount.day.bin"
        if _is_link(path.parent) or _is_link(path):
            raise _fail("input_identity_invalid", "Qlib amount input is linked", instrument=symbol)
        try:
            size = path.stat().st_size
            with path.open("rb") as handle:
                raw_start = np.frombuffer(handle.read(4), dtype="<f4")
        except (FileNotFoundError, OSError, ValueError) as exc:
            raise _fail("source_invalid", "Qlib amount bin is absent or unreadable", instrument=symbol) from exc
        if (
            size < 8
            or size % 4
            or len(raw_start) != 1
            or not math.isfinite(float(raw_start[0]))
            or float(raw_start[0]) % 1
        ):
            raise _fail("source_invalid", "Qlib amount bin header differs", instrument=symbol)
        start_index = int(raw_start[0])
        length = size // 4 - 1
        content_hashes.append(
            {
                "relative_path": path.relative_to(qlib_root).as_posix(),
                "sha256": _sha256_file(path),
            }
        )
        values = np.memmap(path, dtype="<f4", mode="r", offset=4, shape=(length,))
        try:
            for day in group["trade_date"]:
                global_index = positions.get(day)
                local_index = -1 if global_index is None else global_index - start_index
                if global_index is None or local_index < 0 or local_index >= length:
                    raise _fail(
                        "provider_absence_unknown",
                        "Qlib amount bin omits an expected PIT date",
                        trade_date=day.isoformat(),
                        instrument=symbol,
                    )
                amount = float(values[local_index])
                if not math.isfinite(amount) or amount < 0:
                    raise _fail(
                        "provider_absence_unknown",
                        "Qlib amount is absent or invalid without frozen authority",
                        trade_date=day.isoformat(),
                        instrument=symbol,
                    )
                rows.append((day, str(symbol), amount))
        finally:
            del values
    frame = pd.DataFrame(rows, columns=["trade_date", "instrument", "amount_cny"])
    if len(frame) != len(expected) or frame.duplicated(["trade_date", "instrument"]).any():
        raise _fail("source_invalid", "Qlib amount readback population differs")
    return frame, canonical_sha256(content_hashes)


def _daily_aggregates(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    membership: pd.DataFrame,
    id_to_code: Mapping[int, str],
    quote_entries: Mapping[str, tuple[tuple[date, date], ...]],
    catalog: list[str],
    source_days: list[date],
    provider_path: Path,
    suspend_path: Path,
    moneyflow_path: Path,
    qlib_root: Path,
    calendar: list[date],
    security_identity: SecuritySourceIdentityManifest,
    provider_absence: ProviderAbsenceManifest,
) -> tuple[list[dict[str, Any]], str]:
    del root, manifest
    provider = _parse_provider_spans(provider_path)
    suspend = pd.read_parquet(suspend_path, columns=["trade_date", "ts_code", "suspend_type", "suspend_timing"])
    suspend["trade_date"] = pd.to_datetime(suspend["trade_date"], errors="coerce").dt.date
    suspend["ts_code"] = suspend["ts_code"].astype(str).str.strip().str.upper()
    full_suspend = {
        (row.trade_date, row.ts_code)
        for row in suspend.itertuples(index=False)
        if row.suspend_type == "S" and pd.isna(row.suspend_timing)
    }
    expected, member_counts = _expand_expected(
        membership,
        id_to_code=id_to_code,
        provider_spans=provider,
        source_days=source_days,
        suspended=full_suspend,
    )
    try:
        expected["moneyflow_source"] = [
            security_identity.resolve(str(row.instrument), row.trade_date, "market.moneyflow_ts").source_ts_code
            for row in expected.itertuples(index=False)
        ]
    except (RuntimeError, ValueError) as exc:
        raise _fail("input_identity_invalid", "security identity cannot resolve the PIT population") from exc
    start, end = source_days[0], source_days[-1]
    moneyflow = _hdf_slice(moneyflow_path, start=start, end=end, columns=["mf_net_amt"]).rename(
        columns={"datetime": "trade_date", "instrument": "moneyflow_source", "mf_net_amt": "net_mf_amount_cny"}
    )
    amount, amount_set_sha256 = _qlib_amount_frame(qlib_root, calendar=calendar, expected=expected)
    joined = expected.merge(moneyflow, how="left", on=["trade_date", "moneyflow_source"], validate="many_to_one")
    joined = joined.merge(amount, how="left", on=["trade_date", "instrument"], validate="one_to_one")
    for column in ("net_mf_amount_cny", "amount_cny"):
        joined[column] = pd.to_numeric(joined[column], errors="coerce")
    missing_amount = joined[joined["amount_cny"].isna()]
    if not missing_amount.empty:
        sample = missing_amount[["trade_date", "instrument", "sector_code"]].head(10).to_dict("records")
        raise _fail("provider_absence_unknown", "expected Qlib amount rows are absent", sample=sample)
    authorized_absence: list[bool] = []
    for row in joined.itertuples(index=False):
        if not pd.isna(row.net_mf_amount_cny):
            authorized_absence.append(False)
            continue
        key = (str(row.instrument), "market.moneyflow_ts", str(row.moneyflow_source), row.trade_date)
        authorized_absence.append(key in provider_absence.by_key)
    joined["authorized_provider_absence"] = authorized_absence
    unknown = joined[joined["net_mf_amount_cny"].isna() & ~joined["authorized_provider_absence"]]
    if not unknown.empty:
        sample = unknown[["trade_date", "instrument", "moneyflow_source", "sector_code"]].head(10).to_dict("records")
        raise _fail(
            "provider_absence_unknown", "expected moneyflow rows are absent without frozen authority", sample=sample
        )
    if ((~joined["amount_cny"].map(math.isfinite)) | (joined["amount_cny"] < 0)).any():
        raise _fail("source_invalid", "moneyflow or amount contains invalid values")
    valid = joined[~joined["authorized_provider_absence"]].copy()
    if (~valid["net_mf_amount_cny"].map(math.isfinite)).any():
        raise _fail("source_invalid", "moneyflow contains an invalid non-absence value")
    grouped = valid.groupby(["trade_date", "sector_code"], sort=True)
    sums = grouped.agg(
        net_mf_amount_cny=("net_mf_amount_cny", "sum"),
        amount_cny=("amount_cny", "sum"),
        valid_contributors=("instrument", "nunique"),
        maximum_member_amount=("amount_cny", "max"),
    ).reset_index()
    aggregates = {(row.trade_date, row.sector_code): row for row in sums.itertuples(index=False)}
    expected_counts = expected.groupby(["trade_date", "sector_code"])["instrument"].nunique().to_dict()
    output: list[dict[str, Any]] = []
    for day in source_days:
        for code in catalog:
            raw = aggregates.get((day, code))
            member_count = member_counts.get((day, code), 0)
            quote_available = _availability(quote_entries, code, day)
            expected_count = int(expected_counts.get((day, code), 0))
            valid_count = int(raw.valid_contributors) if raw is not None else 0
            structural = member_count > 0 and quote_available
            reason: str | None = None
            eligible = False
            if member_count == 0:
                reason = "hmm_risk_rotation_l2_no_resolved_pit_members"
            elif not quote_available:
                reason = "hmm_risk_rotation_l2_quote_unavailable"
            elif expected_count == 0:
                reason = "hmm_risk_rotation_l2_all_members_suspended"
            elif valid_count / expected_count < 0.90:
                reason = "hmm_risk_rotation_l2_provider_coverage_insufficient"
            elif raw is None or not math.isfinite(float(raw.amount_cny)) or float(raw.amount_cny) <= 0:
                reason = "hmm_risk_rotation_l2_source_invalid"
            else:
                eligible = True
            output.append(
                {
                    "trade_date": day.isoformat(),
                    "sector_code": code,
                    "structural_eligible": structural,
                    "eligible": eligible,
                    "reason_code": reason,
                    "expected_contributors": expected_count,
                    "valid_contributors": valid_count,
                    "coverage": valid_count / expected_count if expected_count else None,
                    "net_mf_amount_cny": float(raw.net_mf_amount_cny) if raw is not None else None,
                    "amount_cny": float(raw.amount_cny) if raw is not None else None,
                    "maximum_member_amount_share": (
                        float(raw.maximum_member_amount) / float(raw.amount_cny)
                        if raw is not None and float(raw.amount_cny) > 0
                        else None
                    ),
                }
            )
    return output, amount_set_sha256


def _sector_returns(
    path: Path,
    *,
    id_to_code: Mapping[int, str],
    quote_entries: Mapping[str, tuple[tuple[date, date], ...]],
    catalog: list[str],
    calendar: list[date],
) -> list[dict[str, Any]]:
    start = DEVELOPMENT_START
    end = DEVELOPMENT_END
    frame = _hdf_slice(path, start=start, end=end, columns=["l2_code_id", "sw2_pct_change"])
    ids = pd.to_numeric(frame["l2_code_id"], errors="coerce")
    if ids.isna().any() or (ids % 1 != 0).any():
        raise _fail("source_invalid", "sector_data l2_code_id is invalid")
    frame["sector_code"] = ids.astype(int).map(id_to_code)
    if frame["sector_code"].isna().any():
        raise _fail("source_invalid", "sector_data references an unknown shared industry id")
    frame["sw2_pct_change"] = pd.to_numeric(frame["sw2_pct_change"], errors="coerce")
    resolved: dict[tuple[date, str], float | None] = {}
    for (day, code), group in frame.groupby(["datetime", "sector_code"], sort=True):
        finite = sorted({float(value) for value in group["sw2_pct_change"].dropna().tolist()})
        if len(finite) > 1:
            raise _fail("source_invalid", "sector_data duplicates disagree", trade_date=str(day), sector_code=code)
        resolved[(day, code)] = finite[0] if finite else None
    output: list[dict[str, Any]] = []
    for day in (value for value in calendar if start <= value <= end):
        for code in catalog:
            available = _availability(quote_entries, code, day)
            value = resolved.get((day, code))
            if available and (value is None or not math.isfinite(value)):
                raise _fail(
                    "source_invalid",
                    "official L2 quote is missing inside the availability authority",
                    trade_date=day.isoformat(),
                    sector_code=code,
                )
            if not available and value is not None:
                raise _fail(
                    "source_invalid",
                    "official L2 quote exists outside the availability authority",
                    trade_date=day.isoformat(),
                    sector_code=code,
                )
            output.append(
                {
                    "trade_date": day.isoformat(),
                    "sector_code": code,
                    "quote_available": available,
                    "pct_change": value,
                }
            )
    return output


def _benchmark_close(path: Path) -> list[dict[str, Any]]:
    try:
        frame = pd.read_hdf(path).reset_index(drop=True)
    except (OSError, ValueError, KeyError) as exc:
        raise _fail("source_invalid", "index HDF cannot be read", path=str(path)) from exc
    required = frame[frame["ts_code"].astype(str).str.upper() == BENCHMARK_CODE].copy()
    required["trade_date"] = pd.to_datetime(required["trade_date"], errors="coerce").dt.date
    required["close"] = pd.to_numeric(required["close"], errors="coerce")
    required = required[(required["trade_date"] >= DEVELOPMENT_START) & (required["trade_date"] <= DEVELOPMENT_END)]
    if required.empty or required.duplicated("trade_date").any():
        raise _fail("source_invalid", "CSI300 benchmark rows are empty or duplicated")
    if (
        required["close"].isna().any()
        or (~required["close"].map(math.isfinite)).any()
        or (required["close"] <= 0).any()
    ):
        raise _fail("source_invalid", "CSI300 close contains invalid values")
    return [
        {"trade_date": row.trade_date.isoformat(), "close": float(row.close)}
        for row in required.sort_values("trade_date").itertuples(index=False)
    ]


def build_rotation_l2_input_bundle(
    *,
    active_profile_path: Path,
    dataset_root: Path,
    source_commit: str,
    security_identity_manifest_path: Path,
    security_identity_sha256: str,
    provider_absence_manifest_path: Path,
    provider_absence_sha256: str,
) -> dict[str, Any]:
    """Build and validate one canonical input bundle from explicit frozen assets."""

    active_profile_path = Path(active_profile_path)
    dataset_root = Path(dataset_root)
    security_identity_manifest_path = Path(security_identity_manifest_path)
    provider_absence_manifest_path = Path(provider_absence_manifest_path)
    if not all(
        path.is_absolute()
        for path in (active_profile_path, dataset_root, security_identity_manifest_path, provider_absence_manifest_path)
    ):
        raise _fail("input_identity_invalid", "profile, root, and source authorities must be absolute")
    if _is_link(active_profile_path) or _is_link(dataset_root) or not dataset_root.is_dir():
        raise _fail("input_identity_invalid", "profile/root is absent or linked")
    profile = _json(active_profile_path)
    if profile.get("schema_version") != PROFILE_SCHEMA:
        raise _fail("input_identity_invalid", "active profile schema differs")
    if profile.get("release_id") != EXPECTED_RELEASE or profile.get("cutoff") != EXPECTED_CUTOFF:
        raise _fail("input_identity_invalid", "active release or cutoff differs")
    configured_root = Path(str(profile.get("controller_paths", {}).get("candidate_root") or ""))
    if configured_root.resolve() != dataset_root.resolve():
        raise _fail("input_identity_invalid", "explicit root differs from active profile")
    if len(source_commit) != 40 or any(character not in "0123456789abcdef" for character in source_commit):
        raise _fail("input_identity_invalid", "source commit is not a lowercase Git SHA")
    profile_sha = _sha256_file(active_profile_path)
    try:
        security_identity = load_security_source_identity_manifest(
            security_identity_manifest_path,
            expected_sha256=security_identity_sha256,
        )
        provider_absence = load_provider_absence_manifest(
            provider_absence_manifest_path,
            expected_sha256=provider_absence_sha256,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        raise _fail("input_identity_invalid", "security/provider authority is invalid") from exc
    components = profile.get("components")
    if not isinstance(components, Mapping):
        raise _fail("input_identity_invalid", "active profile components are absent")
    manifest_path = _require_file(
        dataset_root,
        "qe_dataset_manifest.json",
        str(components.get("dataset_manifest_file_sha256") or ""),
    )
    manifest = _json(manifest_path)
    if (
        manifest.get("schema_version") != MANIFEST_SCHEMA
        or manifest.get("availability_status") != "CANDIDATE_READY"
        or manifest.get("release_id") != EXPECTED_RELEASE
        or manifest.get("cutoff_trade_date") != EXPECTED_CUTOFF
        or manifest.get("dataset_manifest_sha256") != components.get("dataset_manifest_sha256")
    ):
        raise _fail("input_identity_invalid", "release manifest identity differs")
    manifest_components = manifest.get("components")
    if not isinstance(manifest_components, Mapping):
        raise _fail("input_identity_invalid", "manifest components are absent")
    pins = validate_sector_context_pins(components.get("sector_context_pins"))
    context_paths = require_pinned_sector_context_files(dataset_root, pins)
    code_map = load_release_sw_l2_code_map(context_paths["code_map"])
    quote = load_sector_quote_availability(
        context_paths["quote_availability"],
        code_map=code_map,
        required_end=date.fromisoformat(EXPECTED_CUTOFF),
    )
    if len(code_map.member_backed_codes) != 131:
        raise _fail("input_identity_invalid", "shared member-backed L2 catalog is not 131")
    membership = pd.read_parquet(context_paths["membership"])
    validate_membership_frame(
        membership,
        id_to_code=code_map.id_to_code,
        required_start=date(2024, 7, 1),
        required_end=date.fromisoformat(EXPECTED_CUTOFF),
    )
    membership["instrument"] = membership["instrument"].astype(str).str.strip().str.upper()
    membership["start_date"] = pd.to_datetime(membership["start_date"]).dt.date
    membership["end_date"] = pd.to_datetime(membership["end_date"]).dt.date

    calendar_component = manifest_components.get("day_calendar")
    day_meta_component = manifest_components.get("day_meta_export")
    provider_component = manifest_components.get("day_provider_catalog")
    index_component = manifest_components.get("index_daily")
    suspend_component = manifest_components.get("suspend_data")
    sector_component = manifest_components.get("sector_data")
    factor_content_component = manifest_components.get("factor_content_manifest")
    if not all(
        isinstance(value, Mapping)
        for value in (
            calendar_component,
            day_meta_component,
            provider_component,
            index_component,
            suspend_component,
            sector_component,
            factor_content_component,
        )
    ):
        raise _fail("input_identity_invalid", "required direct-v2 component pins are absent")
    calendar_path = _require_file(dataset_root, str(calendar_component["path"]), str(calendar_component["sha256"]))
    _require_file(dataset_root, str(day_meta_component["path"]), str(day_meta_component["sha256"]))
    provider_path = _require_file(dataset_root, str(provider_component["path"]), str(provider_component["sha256"]))
    index_path = _require_file(dataset_root, str(index_component["path"]), str(index_component["sha256"]))
    suspend_path = _require_file(dataset_root, str(suspend_component["path"]), str(suspend_component["sha256"]))
    sector_path = _require_file(dataset_root, str(sector_component["path"]), str(sector_component["sha256"]))
    moneyflow_path = dataset_root / FACTOR_ROOT / "moneyflow.h5"
    qlib_root = dataset_root / "components/daily_bin_candidate"
    factor_inventory_path = _require_file(
        dataset_root,
        str(factor_content_component["path"]),
        str(factor_content_component["sha256"]),
    )
    factor_inventory = _json(factor_inventory_path)
    files = factor_inventory.get("files")
    if not isinstance(files, list):
        raise _fail("input_identity_invalid", "factor content manifest files are absent")
    factor_pins = {str(row.get("path")): str(row.get("sha256")) for row in files if isinstance(row, Mapping)}
    for path in (moneyflow_path,):
        relative = path.relative_to(dataset_root).as_posix()
        expected = factor_pins.get(relative)
        if expected is None:
            raise _fail("input_identity_invalid", "factor file is not pinned", path=relative)
        _require_file(dataset_root, relative, expected)

    calendar = _parse_calendar(calendar_path)
    start_index = calendar.index(DEVELOPMENT_START)
    if start_index < FEATURE_DAYS:
        raise _fail("history_unavailable", "25 source sessions before development are unavailable")
    source_days = calendar[start_index - FEATURE_DAYS : calendar.index(DEVELOPMENT_END)]
    contract_calendar = calendar[start_index - FEATURE_DAYS : calendar.index(DEVELOPMENT_END) + 1]
    catalog = list(code_map.member_backed_codes)
    daily, amount_set_sha256 = _daily_aggregates(
        root=dataset_root,
        manifest=manifest,
        membership=membership,
        id_to_code=code_map.id_to_code,
        quote_entries=quote.entries,
        catalog=catalog,
        source_days=source_days,
        provider_path=provider_path,
        suspend_path=suspend_path,
        moneyflow_path=moneyflow_path,
        qlib_root=qlib_root,
        calendar=calendar,
        security_identity=security_identity,
        provider_absence=provider_absence,
    )
    body: dict[str, Any] = {
        "schema_version": INPUT_SCHEMA,
        "identity": {
            "source_commit": hashlib.sha256(source_commit.encode("ascii")).hexdigest(),
            "source_git_commit": source_commit,
            "profile_sha256": profile_sha,
            "manifest_sha256": str(manifest["dataset_manifest_sha256"]),
            "mapping_hash": code_map.member_backed_digest,
            "quote_authority_hash": quote.quote_availability_digest,
            "calendar_hash": str(calendar_component["sha256"]),
            "release_id": EXPECTED_RELEASE,
            "generation": str(profile.get("generation") or ""),
            "cutoff": EXPECTED_CUTOFF,
            "sector_display_name_authority": "canonical_sw_l2_code_only",
            "source_file_hashes": {
                "moneyflow_h5": factor_pins[moneyflow_path.relative_to(dataset_root).as_posix()],
                "factor_content_manifest": str(factor_content_component["sha256"]),
                "daily_bin_amount_set": amount_set_sha256,
                "daily_bin_meta": str(day_meta_component["sha256"]),
                "sector_data_h5": str(sector_component["sha256"]),
                "index_daily_h5": str(index_component["sha256"]),
                "membership": str(pins["membership_sha256"]),
                "suspend": str(suspend_component["sha256"]),
                "security_identity": security_identity.manifest_sha256,
                "provider_absence": provider_absence.manifest_sha256,
            },
        },
        "catalog": [{"sector_code": code, "sector_name": code} for code in catalog],
        "calendar": [value.isoformat() for value in contract_calendar],
        "daily_aggregates": daily,
        "sector_returns": _sector_returns(
            sector_path,
            id_to_code=code_map.id_to_code,
            quote_entries=quote.entries,
            catalog=catalog,
            calendar=calendar,
        ),
        "benchmark_close": _benchmark_close(index_path),
    }
    body["input_hash"] = canonical_sha256(body)
    validate_input_bundle(body)
    return body


__all__ = ["build_rotation_l2_input_bundle"]
