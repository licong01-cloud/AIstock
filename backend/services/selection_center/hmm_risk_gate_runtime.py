"""HMM Risk Gate artifact loader for Selection Center runtime.

Loads precomputed hmm_risk_gate_v1 artifacts and provides gate decisions
for a given trade_date and set of symbols.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from pydantic import BaseModel


class HMMRiskGateEntry(BaseModel):
    state: str
    confidence: float
    blocked: bool
    block_reason: str | None = None


class HMMRiskGateArtifact(BaseModel):
    artifact_type: str
    model_path: str
    preset_key: str
    test_start: str
    backtest_end: str
    sector_count: int
    gate_config: dict[str, Any]
    stock_sector_map: dict[str, str]
    daily_gates: dict[str, dict[str, dict[str, Any]]]
    daily_triggers: dict[str, list[dict[str, Any]]]


class HMMRiskGateArtifactLoader:
    """Load and validate hmm_risk_gate artifacts. Fail-fast if missing."""

    def __init__(self) -> None:
        self._cache: dict[str, HMMRiskGateArtifact] = {}

    def load(self, *, artifact_path: str, trade_date: date) -> HMMRiskGateArtifact:
        if artifact_path in self._cache:
            artifact = self._cache[artifact_path]
        else:
            path = Path(artifact_path)
            if not path.exists():
                raise DataUnavailableError(
                    f"HMM risk gate artifact not found: {artifact_path}"
                )
            raw = json.loads(path.read_text(encoding="utf-8"))
            if raw.get("artifact_type") != "hmm_risk_gate_v1":
                raise StrategyPackageValidationError(
                    f"Invalid artifact type: {raw.get('artifact_type')}, expected hmm_risk_gate_v1"
                )
            artifact = HMMRiskGateArtifact(**raw)
            self._cache[artifact_path] = artifact

        d_iso = trade_date.isoformat()
        if d_iso not in artifact.daily_gates:
            raise DataUnavailableError(
                f"HMM risk gate artifact does not cover trade_date={d_iso}. "
                f"Range: {artifact.test_start} ~ {artifact.backtest_end}"
            )
        return artifact

    def get_blocked_sectors(
        self, artifact: HMMRiskGateArtifact, trade_date: date
    ) -> set[str]:
        d_iso = trade_date.isoformat()
        gates = artifact.daily_gates.get(d_iso, {})
        return {
            sector_code
            for sector_code, gate in gates.items()
            if gate.get("blocked", False)
        }

    def get_symbol_sector(
        self, artifact: HMMRiskGateArtifact, symbol: str
    ) -> str | None:
        return artifact.stock_sector_map.get(symbol)


class DataUnavailableError(RuntimeError):
    pass


class StrategyPackageValidationError(ValueError):
    pass
