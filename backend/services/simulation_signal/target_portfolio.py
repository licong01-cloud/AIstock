"""Pure construction of desired weights from frozen daily selection evidence."""

from __future__ import annotations


from .contracts import DailySelectionEvidence, TargetPortfolio, canonical_json_sha256


class TargetPortfolioService:
    def build_equal_weight(
        self,
        *,
        evidence: DailySelectionEvidence,
        symbols: tuple[str, ...],
    ) -> TargetPortfolio:
        canonical = tuple(sorted(set(symbols)))
        if canonical != symbols:
            raise ValueError("target symbols must be unique and sorted")
        if evidence.candidate_count != len(symbols):
            raise ValueError("target symbols must exactly match the frozen evidence candidate count")
        weight = 0.0 if not canonical else 1.0 / len(canonical)
        weights = {symbol: weight for symbol in canonical}
        payload = {
            "evidence_id": evidence.evidence_id,
            "trade_date": evidence.target_trade_date.isoformat(),
            "weights": weights,
        }
        return TargetPortfolio(
            evidence_id=evidence.evidence_id,
            trade_date=evidence.target_trade_date,
            weights=weights,
            target_hash=canonical_json_sha256(payload),
        )
