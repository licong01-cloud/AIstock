"""HMM Risk Gate Decision Provider for Selection Center.

Blocks new buys in sectors that recently transitioned to fading state.
Existing holdings are never affected (hold continues).

This provider reads precomputed risk gate artifacts and produces RiskDecision
objects compatible with the StockRiskPolicyService merge pipeline.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from backend.services.selection_center.hmm_risk_gate_runtime import (
    DataUnavailableError,
    HMMRiskGateArtifactLoader,
)


class HMMRiskGateDecisionProvider:
    """HMM sector risk gate: blocks new buys in recently-fading sectors."""

    source_name = "hmm_risk_gate"

    def __init__(
        self, artifact_loader: HMMRiskGateArtifactLoader | None = None
    ) -> None:
        self._loader = artifact_loader or HMMRiskGateArtifactLoader()

    def evaluate(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        profile: Any,
        current_positions: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Evaluate risk gate for given symbols on trade_date.

        Returns dict[symbol, RiskDecision] for symbols that should be blocked.
        Only blocks NEW buys — symbols in current_positions are not affected.
        """
        from backend.services.selection_center.risk_policy import RiskDecision

        artifact_path = getattr(profile, "hmm_risk_gate_artifact_path", None)
        if not artifact_path:
            return {}

        try:
            artifact = self._loader.load(
                artifact_path=artifact_path, trade_date=trade_date
            )
        except DataUnavailableError:
            return {}

        blocked_sectors = self._loader.get_blocked_sectors(artifact, trade_date)
        if not blocked_sectors:
            return {}

        protect_top = artifact.gate_config.get("protect_top", 30)
        current_holdings = set(current_positions.keys()) if current_positions else set()
        decisions: dict[str, Any] = {}

        # protect_top is applied by the caller (Selection Center ranks candidates
        # before calling risk policy). The provider blocks all non-holding symbols
        # in fading sectors; the caller should exclude top-ranked candidates.
        for symbol in symbols:
            if symbol in current_holdings:
                continue

            sector = self._loader.get_symbol_sector(artifact, symbol)
            if sector and sector in blocked_sectors:
                d_iso = trade_date.isoformat()
                gate_info = artifact.daily_gates.get(d_iso, {}).get(sector, {})
                decisions[symbol] = RiskDecision(
                    can_buy=False,
                    reason_code="hmm_risk_gate_fading_transition",
                    source_events=[
                        {
                            "source": self.source_name,
                            "sector_code": sector,
                            "state": gate_info.get("state", "fading"),
                            "confidence": gate_info.get("confidence", 0.0),
                            "block_reason": gate_info.get("block_reason"),
                            "trade_date": d_iso,
                        }
                    ],
                )

        return decisions
