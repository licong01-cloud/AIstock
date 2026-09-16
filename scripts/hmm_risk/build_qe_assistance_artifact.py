"""Build one immutable HMM QE-assistance coefficient artifact.

All inputs are explicit frozen files.  The command performs no database,
network, tail, training, or runtime action.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.dataset_release.canonical import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.industry_pit_adapter import HMMIndustryPitAdapter  # noqa: E402
from backend.services.hmm_risk.qe_assistance_adapter import (  # noqa: E402
    EXPECTED_SOURCE_FILE_SHA256,
    FORMAL_WINDOW_END,
    FORMAL_WINDOW_START,
    QEAssistanceContractError,
    build_qe_assistance_artifact,
    validate_qe_assistance_artifact,
)


def _object(path: Path) -> dict[str, Any]:
    if not path.is_absolute() or path.is_symlink():
        raise RuntimeError(f"input must be an absolute non-symlink file: {path}")
    try:
        value = json.loads(path.resolve(strict=True).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"JSON input cannot be read: {path}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON input must be an object: {path}")
    return value


def _calendar(path: Path) -> list:
    value = _object(path)
    rows = value.get("trade_dates")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("calendar JSON must contain a non-empty trade_dates list")
    return rows


def _state_rows(value: dict[str, Any], model_contract: str) -> tuple[list[dict[str, Any]], str]:
    payload = value.get("reproducibility_payload") if isinstance(value.get("reproducibility_payload"), dict) else value
    rows = payload.get("oof_prediction_rows")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("v1.6 report lacks oof_prediction_rows")
    if not all(isinstance(row, dict) for row in rows):
        raise RuntimeError("v1.6 OOF rows must all be objects")
    model_hashes = {row.get("model_hash") for row in rows}
    if len(model_hashes) != 1:
        raise RuntimeError("v1.6 OOF rows do not have one model identity")
    normalized: list[dict[str, Any]] = []
    for row in rows:
        try:
            timestamp = pd.Timestamp(row.get("trade_date"))
            if pd.isna(timestamp):
                raise ValueError("NaT")
            trade_date = timestamp.date()
        except (TypeError, ValueError) as exc:
            raise RuntimeError("v1.6 OOF row trade_date is invalid") from exc
        if FORMAL_WINDOW_START <= trade_date <= FORMAL_WINDOW_END:
            normalized.append({**row, "model_contract": model_contract})
    if not normalized:
        raise RuntimeError("v1.6 report has no rows in the approved QE window")
    return normalized, str(next(iter(model_hashes)))


def _symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if len(text) == 8 and text[:2] in {"SH", "SZ", "BJ"} and text[2:].isdigit():
        return f"{text[2:]}.{text[:2]}"
    return text


def _prediction_rows(path: Path, calendar: list[Any]) -> list[dict[str, Any]]:
    if not path.is_absolute() or path.is_symlink():
        raise RuntimeError("prediction pickle must be an absolute non-symlink file")
    resolved = path.resolve(strict=True)
    digest = hashlib.sha256(resolved.read_bytes()).hexdigest()
    if digest != EXPECTED_SOURCE_FILE_SHA256:
        raise RuntimeError("prediction pickle SHA-256 differs from approved Loop2 source")
    frame = pd.read_pickle(resolved)
    if (
        not isinstance(frame, pd.DataFrame)
        or not isinstance(frame.index, pd.MultiIndex)
        or frame.index.nlevels != 2
        or list(frame.columns) != ["score"]
        or frame.index.has_duplicates
    ):
        raise RuntimeError("prediction pickle schema differs")
    ordered_calendar = [pd.Timestamp(value).date() for value in calendar]
    successors = dict(zip(ordered_calendar, ordered_calendar[1:], strict=False))
    rows: list[dict[str, Any]] = []
    for (raw_date, raw_symbol), raw_score in frame["score"].items():
        source_date = pd.Timestamp(raw_date).date()
        trade_date = successors.get(source_date)
        if trade_date is None:
            continue
        if trade_date < FORMAL_WINDOW_START or trade_date > FORMAL_WINDOW_END:
            continue
        rows.append(
            {
                "source_date": source_date.isoformat(),
                "trade_date": trade_date.isoformat(),
                "instrument": _symbol(raw_symbol),
                "score": float(raw_score),
            }
        )
    return rows


def _adapter(authority: dict[str, Any]) -> HMMIndustryPitAdapter:
    expected = {"artifact_root", "identity", "research_basis", "l1_projection", "l2_projection"}
    if set(authority) != expected:
        raise RuntimeError("industry authority schema differs")
    adapter = HMMIndustryPitAdapter.from_artifact_root(
        artifact_root=Path(str(authority["artifact_root"])),
        forbidden_roots=(ROOT,),
        expected_identity=authority["identity"],
    )
    adapter.bind_research_basis_contract(authority["research_basis"])
    adapter.bind_l1_code_projection(authority["l1_projection"])
    adapter.bind_l2_code_projection(authority["l2_projection"])
    return adapter


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prediction-pickle", type=Path, required=True)
    parser.add_argument("--v16-report", type=Path, required=True)
    parser.add_argument("--calendar", type=Path, required=True)
    parser.add_argument("--industry-pit-authority", type=Path, required=True)
    parser.add_argument("--source-model-contract", required=True)
    parser.add_argument("--source-mapping-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    calendar = _calendar(args.calendar)
    state_rows, model_hash = _state_rows(_object(args.v16_report), args.source_model_contract)
    authority = _object(args.industry_pit_authority)
    artifact = build_qe_assistance_artifact(
        raw_prediction_rows=_prediction_rows(args.prediction_pickle, calendar),
        state_rows=state_rows,
        calendar=calendar,
        industry_adapter=_adapter(authority),
        source_model_contract=args.source_model_contract,
        model_hash=model_hash,
        source_mapping_sha256=args.source_mapping_sha256,
        source_prediction_file_sha256=EXPECTED_SOURCE_FILE_SHA256,
        authority_identity=authority["identity"],
        canonical_l1_codes=[row["canonical_l1_code"] for row in authority["l1_projection"]["rows"]],
    )
    validate_qe_assistance_artifact(artifact)
    output = args.output
    if not output.is_absolute() or output.is_symlink():
        raise RuntimeError("output must be an absolute non-symlink path")
    output.parent.resolve(strict=True)
    with output.open("xb") as handle:
        handle.write(canonical_json_bytes(artifact) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    print(json.dumps({"status": "complete", "artifact_sha256": artifact["artifact_sha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (QEAssistanceContractError, RuntimeError) as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason_code": getattr(exc, "reason_code", "hmm_risk_qe_assistance_input_invalid"),
                    "message": str(exc),
                    "tail_accessed": False,
                    "database_write_performed": False,
                    "runtime_action_performed": False,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        raise SystemExit(2) from exc
