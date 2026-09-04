"""Build ``qe_event_risk_policy.json`` from the frozen qlib bin universe.

BUG-989 data-plane invariant: the QE/multi-alpha train, predict, backtest and
combine computation data plane must not access PostgreSQL.  The ST PIT risk
policy artifact is therefore derived exclusively from the frozen qlib bin
dataset referenced by ``qlib_init.provider_uri.day``:

- ``instruments/all.txt``    -> PIT eligible spans (ts_code, start, end)
- ``calendars/day.txt``      -> frozen trading calendar
- ``meta_export.json``       -> frozen snapshot identity

The composer writes ``qe_frozen_build_spec.json`` (window, risk profile,
dataset identity, sha256 pins) into the workspace at assembly time.  This
helper runs inside the workspace before ``qrun`` and rebuilds the runtime
artifact deterministically.  Any pin mismatch, identity drift, missing file or
empty coverage fails loud; there is no database fallback, no online backfill
and no silent degradation.

Stdlib-only on purpose: the workspace python environment is minimal.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

SPEC_FILE = "qe_frozen_build_spec.json"
ARTIFACT_FILE = "qe_event_risk_policy.json"
SPEC_SCHEMA_VERSION = "qe_frozen_build_spec_v1"
SPEC_KIND = "qe_event_risk_policy"


class FrozenRiskPolicyBuildError(RuntimeError):
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
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_build_spec_invalid: "
            f"field={field} value={value!r} is not an ISO date"
        ) from exc


def _load_spec(cwd: Path) -> dict[str, Any]:
    spec_path = cwd / SPEC_FILE
    with spec_path.open("r", encoding="utf-8") as handle:
        spec = json.load(handle)
    if not isinstance(spec, dict):
        raise FrozenRiskPolicyBuildError(
            f"reason_code=qe_frozen_build_spec_invalid: {spec_path} is not a JSON object"
        )
    if spec.get("schema_version") != SPEC_SCHEMA_VERSION:
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_build_spec_invalid: "
            f"schema_version={spec.get('schema_version')!r} expected={SPEC_SCHEMA_VERSION!r}"
        )
    if spec.get("kind") != SPEC_KIND:
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_build_spec_invalid: "
            f"kind={spec.get('kind')!r} expected={SPEC_KIND!r}"
        )
    return spec


def _verify_frozen_snapshot(provider_dir: Path, pins: dict[str, Any]) -> dict[str, Any]:
    meta_path = provider_dir / "meta_export.json"
    instruments_file = str(pins.get("instruments_file") or "all.txt")
    if instruments_file not in {"all.txt", "stock_universe.txt"}:
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_build_spec_invalid: "
            f"unsupported instruments_file={instruments_file!r}"
        )
    instruments_path = provider_dir / "instruments" / instruments_file
    calendar_path = provider_dir / "calendars" / "day.txt"
    for path in (meta_path, instruments_path, calendar_path):
        if not path.is_file():
            raise FrozenRiskPolicyBuildError(
                "reason_code=qe_frozen_universe_file_missing: "
                f"path={path} (frozen qlib bin dataset is incomplete)"
            )

    expected_hashes = {
        meta_path: pins.get("meta_export_sha256"),
        instruments_path: pins.get("instruments_sha256"),
        calendar_path: pins.get("calendar_sha256"),
    }
    for path, expected in expected_hashes.items():
        if not expected:
            raise FrozenRiskPolicyBuildError(
                "reason_code=qe_frozen_build_spec_invalid: "
                f"pins missing sha256 for {path.name}"
            )
        actual = _sha256_file(path)
        if actual != str(expected):
            raise FrozenRiskPolicyBuildError(
                "reason_code=qe_frozen_universe_pin_mismatch: "
                f"path={path} actual_sha256={actual} expected_sha256={expected}; "
                "the frozen dataset does not match the deployed contract pins"
            )

    with meta_path.open("r", encoding="utf-8") as handle:
        meta = json.load(handle)
    snapshot_id = str(meta.get("snapshot_id") or "")
    if snapshot_id != str(pins.get("snapshot_id") or ""):
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_universe_identity_mismatch: "
            f"snapshot_id={snapshot_id!r} expected={pins.get('snapshot_id')!r}"
        )
    universe_key = str(meta.get("universe_key") or "")
    if universe_key != str(pins.get("universe_key") or ""):
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_universe_identity_mismatch: "
            f"universe_key={universe_key!r} expected={pins.get('universe_key')!r}"
        )
    return {
        "meta": meta,
        "instruments_path": instruments_path,
        "calendar_path": calendar_path,
        "instruments_sha256": str(pins["instruments_sha256"]),
    }


def _load_trade_dates(calendar_path: Path, start: date, end: date) -> list[date]:
    dates: list[date] = []
    with calendar_path.open("r", encoding="utf-8", newline="") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            day = _parse_date(text, field="calendar_day")
            if start <= day <= end:
                dates.append(day)
    dates.sort()
    if not dates:
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_calendar_window_empty: "
            f"calendar={calendar_path} window={start.isoformat()}..{end.isoformat()}"
        )
    return dates


def _load_spans(instruments_path: Path, start: date, end: date) -> list[dict[str, Any]]:
    spans: list[dict[str, Any]] = []
    with instruments_path.open("r", encoding="utf-8", newline="") as handle:
        for lineno, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            parts = text.replace(",", "\t").split("\t")
            parts = [part.strip() for part in parts if part.strip()]
            if len(parts) < 3:
                raise FrozenRiskPolicyBuildError(
                    "reason_code=qe_frozen_universe_span_invalid: "
                    f"path={instruments_path} line={lineno} content={text!r}"
                )
            ts_code = parts[0].upper()
            span_start = _parse_date(parts[1], field="eligible_start")
            span_end = _parse_date(parts[2], field="eligible_end")
            if span_start > span_end:
                raise FrozenRiskPolicyBuildError(
                    "reason_code=qe_frozen_universe_span_invalid: "
                    f"path={instruments_path} line={lineno} start after end"
                )
            if span_start <= end and span_end >= start:
                spans.append(
                    {
                        "ts_code": ts_code,
                        "eligible_start": span_start.isoformat(),
                        "eligible_end": span_end.isoformat(),
                    }
                )
    spans.sort(key=lambda item: (item["ts_code"], item["eligible_start"], item["eligible_end"]))
    if not spans:
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_universe_window_empty: "
            f"instruments={instruments_path} window={start.isoformat()}..{end.isoformat()}"
        )
    return spans


def build_risk_policy_payload(spec: dict[str, Any]) -> dict[str, Any]:
    """Build the runtime artifact payload from the spec and frozen files."""

    provider_uri = str(spec.get("provider_uri_day") or "").strip()
    if not provider_uri:
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_build_spec_invalid: provider_uri_day is required"
        )
    start = _parse_date(spec.get("start_date"), field="start_date")
    end = _parse_date(spec.get("end_date"), field="end_date")
    if end < start:
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_build_spec_invalid: "
            f"end_date={end.isoformat()} earlier than start_date={start.isoformat()}"
        )

    profile = spec.get("profile")
    if not isinstance(profile, dict):
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_build_spec_invalid: profile must be an object"
        )
    providers = [str(item) for item in (profile.get("providers") or [])]
    if "st_pit" not in providers:
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_build_spec_invalid: profile.providers must include st_pit"
        )
    hard_actions = [str(item) for item in (profile.get("hard_actions") or [])]

    dataset = spec.get("dataset") if isinstance(spec.get("dataset"), dict) else {}
    pins = spec.get("pins")
    if not isinstance(pins, dict):
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_build_spec_invalid: pins must be an object"
        )

    provider_dir = Path(os.path.expanduser(provider_uri))
    if not provider_dir.is_dir():
        raise FrozenRiskPolicyBuildError(
            "reason_code=qe_frozen_universe_dir_missing: "
            f"provider_uri_day={provider_dir} is not a directory"
        )
    verified = _verify_frozen_snapshot(provider_dir, pins)
    trade_dates = _load_trade_dates(verified["calendar_path"], start, end)
    raw_spans = _load_spans(verified["instruments_path"], start, end)

    rule_version = str(dataset.get("rule_version") or "frozen_qlib_bin_universe_v1")
    spans = [
        {
            "ts_code": span["ts_code"],
            "eligible_start": span["eligible_start"],
            "eligible_end": span["eligible_end"],
            "entry_reason": None,
            "exit_reason": None,
            "rule_version": rule_version,
            "metadata": {},
        }
        for span in raw_spans
    ]

    meta = verified["meta"]
    return {
        "enabled": True,
        "contract": str(profile.get("contract") or "stock_event_risk_policy_v1"),
        "source": f"frozen:qlib_bin/instruments/{verified['instruments_path'].name}",
        "providers": providers,
        "hard_actions": hard_actions,
        "visible_time_mode": profile.get("visible_time_mode"),
        "strict_data_ready": bool(profile.get("strict_data_ready", True)),
        "dataset_contract_id": dataset.get("contract_id"),
        "st_universe_key": dataset.get("st_universe_key"),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "trade_date_count": len(trade_dates),
        "span_count": len(spans),
        "active_spans": spans,
        "state": {
            "universe_key": str(meta.get("universe_key") or ""),
            "rule_version": rule_version,
            "scope": "frozen_dataset_file",
            "status": "frozen",
            "dirty": False,
            "source_fingerprint_sha256": verified["instruments_sha256"],
            "generated_at": meta.get("generated_at"),
        },
    }


def ensure_frozen_risk_policy_artifact(
    cwd: str | os.PathLike[str] | None = None,
    *,
    print_fn=print,
) -> Path | None:
    """Rebuild ``qe_event_risk_policy.json`` from the frozen dataset.

    Returns the artifact path, or ``None`` when the workspace carries no
    frozen build spec (legacy workspace: leave existing artifacts untouched).
    The rebuild is deterministic; identical inputs produce identical bytes.
    """

    base = Path(cwd or os.getcwd())
    spec_path = base / SPEC_FILE
    if not spec_path.is_file():
        return None
    spec = _load_spec(base)
    payload = build_risk_policy_payload(spec)
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    artifact_path = base / ARTIFACT_FILE
    tmp_path = artifact_path.with_suffix(artifact_path.suffix + ".tmp")
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(artifact_path)
    print_fn(
        "[INFO] QE frozen risk policy artifact built: "
        f"{artifact_path.name} spans={payload['span_count']} "
        f"trade_dates={payload['trade_date_count']} "
        f"snapshot={payload['state']['universe_key']} "
        f"fingerprint={payload['state']['source_fingerprint_sha256']}"
    )
    return artifact_path


if __name__ == "__main__":
    ensure_frozen_risk_policy_artifact()
