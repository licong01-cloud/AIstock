"""Standalone runtime reader for QE sector-risk overlay artifacts."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


SUPPORTED_MODES = {"none", "entry_gate", "bounded_de_risk", "exit_reentry"}
SUPPORTED_STATES = {"NORMAL", "CAUTION", "HIGH", "CRITICAL", "UNMAPPED"}
REQUIRED_COLUMNS = {
    "signal_date",
    "effective_trade_date",
    "instrument",
    "l2_code_id",
    "risk_score",
    "risk_state",
    "rs_turn_risk",
    "breadth_deterioration",
    "flow_divergence_risk",
    "leadership_concentration",
    "vol_crowding_risk",
}
DEFAULT_MULTIPLIERS = {
    "none": {"NORMAL": 1.0, "CAUTION": 1.0, "HIGH": 1.0, "CRITICAL": 1.0, "UNMAPPED": 1.0},
    "entry_gate": {"NORMAL": 1.0, "CAUTION": 1.0, "HIGH": 1.0, "CRITICAL": 1.0, "UNMAPPED": 1.0},
    "bounded_de_risk": {"NORMAL": 1.0, "CAUTION": 0.75, "HIGH": 0.50, "CRITICAL": 0.25, "UNMAPPED": 1.0},
    "exit_reentry": {"NORMAL": 1.0, "CAUTION": 0.75, "HIGH": 0.50, "CRITICAL": 0.0, "UNMAPPED": 1.0},
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class QESectorRiskOverlayPolicy:
    """Validated, deterministic stock-date sector-risk policy."""

    def __init__(
        self,
        *,
        enabled=False,
        mode="none",
        manifest_file=None,
        data_file=None,
        strict=True,
        reentry_confirm_days=3,
        state_multipliers=None,
    ):
        self.enabled = bool(enabled)
        self.mode = str(mode)
        self.strict = bool(strict)
        self.reentry_confirm_days = int(reentry_confirm_days)
        if self.mode not in SUPPORTED_MODES:
            raise RuntimeError(f"unsupported QE sector-risk overlay mode: {self.mode}")
        if self.reentry_confirm_days < 1:
            raise RuntimeError("sector-risk reentry_confirm_days must be >= 1")
        self.multipliers = dict(DEFAULT_MULTIPLIERS[self.mode])
        if state_multipliers is not None:
            if not isinstance(state_multipliers, dict):
                raise RuntimeError("sector-risk state_multipliers must be a mapping")
            unknown = set(state_multipliers) - SUPPORTED_STATES
            if unknown:
                raise RuntimeError(f"sector-risk state_multipliers contains unknown states: {sorted(unknown)}")
            self.multipliers.update({str(k): float(v) for k, v in state_multipliers.items()})
        if any(not 0.0 <= float(value) <= 1.0 for value in self.multipliers.values()):
            raise RuntimeError("sector-risk state multipliers must be in [0, 1]")
        self.manifest = None
        self.frame = pd.DataFrame()
        self._lookup = None
        if self.enabled:
            self._load(manifest_file=manifest_file, data_file=data_file)

    def _load(self, *, manifest_file, data_file) -> None:
        if not manifest_file or not data_file:
            raise RuntimeError("enabled QE sector-risk overlay requires manifest_file and data_file")
        manifest_path = Path(str(manifest_file)).expanduser().resolve()
        data_path = Path(str(data_file)).expanduser().resolve()
        if not manifest_path.is_file() or not data_path.is_file():
            raise RuntimeError(
                f"QE sector-risk overlay artifact missing: manifest={manifest_path} data={data_path}"
            )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("schema_version") != "qe_sector_risk_overlay_manifest_v1":
            raise RuntimeError(f"invalid QE sector-risk manifest schema: {manifest.get('schema_version')}")
        runtime_meta = (manifest.get("artifacts") or {}).get("runtime") or {}
        expected_hash = str(runtime_meta.get("sha256") or "")
        actual_hash = _sha256(data_path)
        if not expected_hash or actual_hash != expected_hash:
            raise RuntimeError(
                f"QE sector-risk runtime hash mismatch: expected={expected_hash} actual={actual_hash}"
            )
        frame = pd.read_parquet(data_path)
        missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
        if missing:
            raise RuntimeError(f"QE sector-risk runtime missing columns: {missing}")
        frame = frame.copy()
        frame["effective_trade_date"] = pd.to_datetime(frame["effective_trade_date"], errors="coerce").dt.normalize()
        frame["signal_date"] = pd.to_datetime(frame["signal_date"], errors="coerce").dt.normalize()
        frame["instrument"] = frame["instrument"].astype(str)
        frame["risk_state"] = frame["risk_state"].astype(str)
        if frame["effective_trade_date"].isna().any() or frame["signal_date"].isna().any():
            raise RuntimeError("QE sector-risk runtime contains invalid dates")
        unknown_states = set(frame["risk_state"].unique()) - SUPPORTED_STATES
        if unknown_states:
            raise RuntimeError(f"QE sector-risk runtime contains unknown states: {sorted(unknown_states)}")
        if frame.duplicated(["effective_trade_date", "instrument"]).any():
            raise RuntimeError("QE sector-risk runtime contains duplicate effective stock-date keys")
        frame = frame.sort_values(["effective_trade_date", "instrument"], kind="mergesort")
        self._attach_reentry_ready(frame)
        self.manifest = manifest
        self.frame = frame
        self._lookup = frame.set_index(["effective_trade_date", "instrument"], verify_integrity=True)

    def _attach_reentry_ready(self, frame: pd.DataFrame) -> None:
        sector_daily = frame.loc[frame["l2_code_id"].ge(0), ["effective_trade_date", "l2_code_id", "risk_state"]]
        state_counts = sector_daily.groupby(
            ["effective_trade_date", "l2_code_id"], sort=False, observed=True
        )["risk_state"].nunique(dropna=False)
        if state_counts.gt(1).any():
            raise RuntimeError("QE sector-risk runtime has conflicting sector states")
        sector_daily = sector_daily.drop_duplicates(["effective_trade_date", "l2_code_id"])
        sector_daily = sector_daily.sort_values(["l2_code_id", "effective_trade_date"], kind="mergesort")
        readiness = {}
        for sector_id, group in sector_daily.groupby("l2_code_id", sort=False):
            low_count = 0
            for row in group.itertuples():
                if row.risk_state in {"NORMAL", "CAUTION"}:
                    low_count += 1
                else:
                    low_count = 0
                readiness[(row.effective_trade_date, int(sector_id))] = low_count >= self.reentry_confirm_days
        frame["reentry_ready"] = [
            True
            if int(row.l2_code_id) < 0
            else bool(readiness.get((row.effective_trade_date, int(row.l2_code_id)), False))
            for row in frame.itertuples()
        ]

    @staticmethod
    def _date_key(trade_date) -> pd.Timestamp:
        return pd.Timestamp(trade_date).normalize()

    def row(self, instrument, trade_date):
        if not self.enabled:
            return None
        key = (self._date_key(trade_date), str(instrument))
        try:
            return self._lookup.loc[key]
        except KeyError:
            if self.strict:
                raise RuntimeError(
                    f"QE sector-risk runtime has no row for trade_date={key[0].date()} instrument={key[1]}"
                )
            return None

    def state(self, instrument, trade_date) -> str:
        row = self.row(instrument, trade_date)
        return str(row["risk_state"]) if row is not None else "UNMAPPED"

    def multiplier(self, instrument, trade_date) -> float:
        return float(self.multipliers[self.state(instrument, trade_date)]) if self.enabled else 1.0

    def entry_allowed(self, instrument, trade_date) -> bool:
        if not self.enabled or self.mode == "none":
            return True
        row = self.row(instrument, trade_date)
        if row is None:
            return True
        state = str(row["risk_state"])
        if state in {"HIGH", "CRITICAL"}:
            return False
        if self.mode == "exit_reentry" and not bool(row["reentry_ready"]):
            return False
        return True
