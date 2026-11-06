"""Request-driven official classification observations; no scheduler or activation.

The content store and its observation pointer are HMM source evidence, not a
completed business watermark. A failed refresh never returns the old pointer as
success. The inference kernel never performs HTTP or reads this mutable pointer.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
import hashlib
import os
from pathlib import Path
import ssl
import tempfile
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

import pandas as pd

from backend.services.dataset_release.canonical import digest_named_fields

from backend.services.hmm_risk import observed_industry_authority as authority
from backend.services.hmm_risk.contracts import canonical_json_bytes, canonical_sha256
from backend.services.hmm_risk.formal_state_executor import validate_output_location, write_once
from backend.services.hmm_risk.industry_pit_adapter import HMM_L2_CODE_PROJECTION_SCHEMA

VERSION = "hmm_risk_official_classification_observation_v1"
MAX_BYTES = 8 * 1024 * 1024  # Transport bound, not a business coverage threshold.


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _download() -> bytes:
    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, *_args, **_kwargs):
            raise ValueError("official classification redirect forbidden")

    request = Request(authority.OFFICIAL_URL, headers={"User-Agent": "AIstock-HMM/1.0"})
    opener = build_opener(HTTPSHandler(context=ssl.create_default_context()), NoRedirect())
    with opener.open(request, timeout=20) as response:
        authority.require(
            response.status == 200 and response.geturl() == authority.OFFICIAL_URL,
            "official classification HTTP status/redirect differs",
        )
        raw = response.read(MAX_BYTES + 1)
    authority.require(0 < len(raw) <= MAX_BYTES, "official classification response size invalid")
    return raw


def _capture_body(path: Path, sha256: str) -> tuple[dict, bytes]:
    body = authority._object(authority.pinned_bytes(path, sha256))
    authority.require(body.get("schema_version") in (None, VERSION), "unknown official capture schema")
    if body.get("schema_version") == VERSION:
        authority.require(
            body.get("first_publication_verified") is False
            and body.get("business_preflight_passed") is False
            and body.get("runtime_activation") is False,
            "observation must not claim publication, business readiness or activation",
        )
    authority.require(
        body.get("status") == 200 and body.get("tls_verification_disabled") is False,
        "official capture transport unverified",
    )
    authority.require(
        body.get("source_url") == body.get("final_url") == authority.OFFICIAL_URL, "official capture URL differs"
    )
    captured = authority._timestamp(body.get("captured_at"))
    authority.require(captured <= _now(), "official capture is in the future")
    raw = authority.pinned_bytes(Path(body["path"]), body["sha256"])
    authority.require(len(raw) == body.get("bytes"), "captured workbook size differs")
    frame = authority.read_workbook(raw)
    authority.require(len(frame) == body.get("row_count"), "captured workbook row count differs")
    return body, raw


def load_capture(path: Path, sha256: str) -> tuple[dict, bytes]:
    """Also accepts the existing, explicitly pinned HTTPS capture receipts."""
    body, raw = _capture_body(path, sha256)
    first = body.get("first_observed_at", body["captured_at"])
    origin = body.get("first_observation")
    authority.require(
        authority._timestamp(first) <= authority._timestamp(body["captured_at"]),
        "first observation is later than verification",
    )
    if origin is not None:
        original, _ = _capture_body(Path(origin["path"]), origin["sha256"])
        authority.require(
            original.get("first_observation") is None
            and original["captured_at"] == first
            and original["sha256"] == body["sha256"],
            "first observation evidence differs",
        )
    else:
        authority.require(first == body["captured_at"], "earlier observation has no evidence")
    previous_pin = body.get("previous_observation")
    if previous_pin:
        previous, _ = _capture_body(Path(previous_pin["path"]), previous_pin["sha256"])
        authority.require(
            authority._timestamp(previous["captured_at"]) <= authority._timestamp(body["captured_at"]),
            "observation chain moves backwards",
        )
        if previous["sha256"] == body["sha256"]:
            authority.require(
                first == previous.get("first_observed_at", previous["captured_at"])
                and origin == (previous.get("first_observation") or previous_pin),
                "unchanged observation lost its original evidence",
            )
        else:
            authority.require(
                origin is None and first == body["captured_at"], "changed source must start a new observation segment"
            )
    elif body.get("schema_version") == VERSION:
        authority.require(origin is None, "first observation cannot skip its predecessor")
    return {**body, "first_observed_at": first}, raw


def read_current(root: Path) -> dict | None:
    path = validate_output_location(root / "current.json")
    if not path.exists():
        return None
    pin = authority._object(path.read_bytes())
    authority.require(set(pin) == {"path", "sha256"}, "observation pointer schema differs")
    capture_path = Path(pin["path"])
    authority.require(capture_path.parent == root / "observations", "observation pointer escapes store")
    load_capture(capture_path, pin["sha256"])
    return pin


@contextmanager
def _locked(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    path = validate_output_location(root / "refresh.lock")
    with path.open("a+b") as handle:
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            def lock():
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)

            def unlock():
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            def lock():
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            def unlock():
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

        try:
            lock()
        except OSError as exc:
            raise ValueError("official observation refresh already in progress") from exc
        try:
            yield
        finally:
            handle.seek(0)
            unlock()


def refresh(root: Path) -> dict:
    """Capture once, reuse source bytes, atomically advance observation only."""
    root = validate_output_location(root)
    with _locked(root):
        prior_pin = read_current(root)
        prior = load_capture(Path(prior_pin["path"]), prior_pin["sha256"])[0] if prior_pin else None
        raw = _download()
        captured_at = _now().isoformat()
        frame = authority.read_workbook(raw)
        # Validate the source identities before marking the capture successful.
        authority.latest_assignments(frame, date.max)
        if prior:
            authority.require(
                authority._timestamp(prior["captured_at"]) <= authority._timestamp(captured_at),
                "observation clock moved backwards",
            )
        digest = hashlib.sha256(raw).hexdigest()
        workbook = validate_output_location(root / "workbooks" / (digest + ".xls"))
        workbook.parent.mkdir(parents=True, exist_ok=True)
        if workbook.exists():
            authority.pinned_bytes(workbook, digest)
        else:
            with workbook.open("xb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            authority.pinned_bytes(workbook, digest)
        unchanged = prior is not None and prior["sha256"] == digest
        capture = {
            "schema_version": VERSION,
            "source_url": authority.OFFICIAL_URL,
            "final_url": authority.OFFICIAL_URL,
            "status": 200,
            "tls_verification_disabled": False,
            "path": str(workbook),
            "sha256": digest,
            "bytes": len(raw),
            "row_count": len(frame),
            "captured_at": captured_at,
            "first_observed_at": prior["first_observed_at"] if unchanged else captured_at,
            "first_observation": (prior.get("first_observation") or prior_pin) if unchanged else None,
            "previous_observation": prior_pin,
            "first_publication_verified": False,
            "business_preflight_passed": False,
            "runtime_activation": False,
        }
        output = validate_output_location(root / "observations" / (canonical_sha256(capture) + ".json"))
        if not output.exists():
            write_once(output, capture)
        pin = {"path": str(output), "sha256": hashlib.sha256(output.read_bytes()).hexdigest()}
        load_capture(output, pin["sha256"])
        current = validate_output_location(root / "current.json")
        with tempfile.NamedTemporaryFile(mode="wb", dir=root, prefix=".current-", delete=False) as handle:
            handle.write(canonical_json_bytes(pin) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        try:
            os.replace(temporary, current)
        finally:
            temporary.unlink(missing_ok=True)
        return pin


def observation_from_capture(
    path: Path, sha256: str, base_authority: dict, *, data_date: date, symbols: set[str]
) -> authority.Observation:
    """Project official taxonomy to existing text IDs using the supplied PIT set."""
    capture, raw = load_capture(path, sha256)
    frame = authority.read_workbook(raw)
    latest = authority.latest_assignments(frame, data_date)
    projection = base_authority["l2_projection"]
    authority.require(
        digest_named_fields(
            HMM_L2_CODE_PROJECTION_SCHEMA, {k: v for k, v in projection.items() if k != "canonical_hash"}
        )
        == projection["canonical_hash"],
        "official L2 projection hash differs",
    )
    by_taxonomy = {row["taxonomy_l2_code"]: row["canonical_l2_code"] for row in projection["rows"]}
    authority.require(
        len(by_taxonomy) == len(projection["rows"]) == len(set(by_taxonomy.values())),
        "ambiguous official L2 projection",
    )
    authority.require(bool(symbols) and len({s[:6] for s in symbols}) == len(symbols), "ambiguous target symbols")
    missing = sorted(s for s in symbols if s[:6] not in latest)
    authority.require(not missing, f"official classification missing {len(missing)} target symbols: {missing[:20]}")
    rows = {}
    for symbol in sorted(symbols):
        authority.require_symbol(symbol)
        included, codes, position = latest[symbol[:6]]
        authority.require(len(codes) == 1, f"ambiguous latest official assignment: {symbol}")
        l3 = next(iter(codes))
        l2 = l3[:4] + "00"
        authority.require(l2 in by_taxonomy, f"unknown official L2 projection: {symbol}/{l2}")
        updated = pd.Timestamp(frame.iloc[position, 3])
        rows[symbol] = {
            "symbol": symbol,
            "canonical_l2_code": by_taxonomy[l2],
            "taxonomy_l2_code": l2,
            "taxonomy_l3_code": l3,
            "source_excel_row": position + 2,
            "source_reported_inclusion_at": included.isoformat(),
            "source_reported_update_at": None if pd.isna(updated) else updated.isoformat(),
            "publication_at": None,
            "publication_time_quality": "FIRST_PUBLICATION_UNVERIFIED",
            "first_proven_observation_at": capture["first_observed_at"],
        }
    return authority.Observation(
        authority._timestamp(capture["first_observed_at"]),
        data_date,
        rows,
        (sha256, capture["sha256"], projection["canonical_hash"]),
        (),
        authority._timestamp(capture["captured_at"]),
    )
