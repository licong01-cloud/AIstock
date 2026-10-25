"""Tradability filtering for package-based selection and Paper v2."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Callable, Iterator, Protocol

from backend.db.pg_pool import get_conn
from backend.services.selection_center.industry_provider import (
    DbSwIndustryLookupProvider,
    IndustryInfo,
    IndustryLookupProvider,
    industry_info_from_candidate,
)
from backend.services.selection_center.models import SelectionCandidate, SelectionExclusion
from backend.services.selection_center.prospective_evidence import (
    CandidateStageName,
    StageReceiptStatus,
    TradabilityResult,
    build_stage_receipt,
)
from backend.services.selection_center.suspension_provider import project_selection_suspension_days
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    DataUnavailableError,
    RuntimeConfigInvalidError,
)

ConnFactory = Callable[[], Iterator[Any]]


class SuspendLookupProvider(Protocol):
    def get_suspended_symbols(self, symbols: list[str], trade_date: date) -> dict[str, dict[str, Any]]:
        ...


class _SuspensionLookup(dict):
    """One invocation's evidence, never mutable provider-wide cached state."""

    def __init__(self, receipt):
        super().__init__()
        self.receipt = receipt


class DbSuspendLookupProvider:
    """Batch lookup for confirmed suspensions in ``market.suspend_d``."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def get_suspended_symbols(self, symbols: list[str], trade_date: date) -> dict[str, dict[str, Any]]:
        normalized = []
        for symbol in symbols:
            symbol = str(symbol or "").strip()
            if symbol and symbol not in normalized:
                normalized.append(symbol)
        if not normalized:
            return {}
        try:
            with self._conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT ts_code, suspend_type, suspend_timing
                        FROM market.suspend_d
                        WHERE ts_code = ANY(%s)
                          AND trade_date = %s
                        ORDER BY ts_code, suspend_type, suspend_timing NULLS FIRST
                        """,
                        (normalized, trade_date),
                    )
                    rows = cur.fetchall()
        except Exception as exc:
            raise DataUnavailableError(
                "suspend_d tradability lookup failed",
                context={"trade_date": trade_date.isoformat(), "symbol_count": len(normalized)},
            ) from exc
        projected, receipt = project_selection_suspension_days(
            [{"ts_code": str(symbol), "trade_date": trade_date, "suspend_type": kind, "suspend_timing": timing}
             for symbol, kind, timing in rows],
            symbols=normalized, required_dates=[trade_date], usage="D_EFFECTIVE_MARKET_STATE",
        )
        unresolved = [row for row in projected if row["state"] == "UNRESOLVED"]
        if unresolved:
            raise DataUnavailableError(
                "suspend_d effective day is unresolved",
                context={"trade_date": trade_date.isoformat(), "reason_code": "SUSPENSION_DAY_UNRESOLVED",
                         "suspension_day_authority": receipt},
            )
        suspended = _SuspensionLookup(receipt)
        for row in projected:
            if row["state"] in {"FULL_DAY_SUSPENDED", "INTRADAY_SUSPENDED"}:
                facts = [r for r in receipt["raw_events"] if r["ts_code"] == row["ts_code"] and r["suspend_type"] == "S"]
                suspended[row["ts_code"]] = {
                    "suspend_type": "S", "suspend_timing": facts[0]["suspend_timing"] if facts else None,
                    "source": "market.suspend_d", "suspension_day_authority": row,
                }
        return suspended


class TradabilityFilter:
    """Turn a full ranked signal list into tradable top-k candidates.

    Strategy runtime output is treated as the raw rank. The returned candidates
    are compactly re-ranked after confirmed suspensions are removed, while the
    original rank remains in ``component_scores.raw_rank`` for traceability.
    """

    def __init__(
        self,
        suspend_provider: SuspendLookupProvider | None = None,
        industry_provider: IndustryLookupProvider | None = None,
    ) -> None:
        self.suspend_provider = suspend_provider or DbSuspendLookupProvider()
        self.industry_provider = industry_provider or DbSwIndustryLookupProvider()

    def filter_candidates(
        self,
        *,
        candidates: list[SelectionCandidate],
        trade_date: date,
        top_k: int,
        package_id: str,
        manifest_sha256: str,
        enabled: bool = True,
        industry_blacklist: list[str] | None = None,
    ) -> tuple[list[SelectionCandidate], list[SelectionExclusion]]:
        result = self.filter_candidates_with_receipt(
            candidates=candidates,
            trade_date=trade_date,
            top_k=top_k,
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            enabled=enabled,
            industry_blacklist=industry_blacklist,
        )
        return result.candidates, result.exclusions

    def filter_candidates_with_receipt(
        self,
        *,
        candidates: list[SelectionCandidate],
        trade_date: date,
        top_k: int,
        package_id: str,
        manifest_sha256: str,
        enabled: bool = True,
        industry_blacklist: list[str] | None = None,
        allow_empty: bool = False,
        exhaustive_evidence: bool = False,
        industry_trade_date: date | None = None,
    ) -> TradabilityResult:
        """Run the current filter once and preserve its exact in-memory trace."""
        # Eligibility remains labelled D. Only the PIT industry observation may
        # be explicitly bounded by an earlier C; old direct calls stay unchanged.
        if industry_trade_date is not None and (
            not isinstance(industry_trade_date, date) or isinstance(industry_trade_date, datetime)
            or industry_trade_date > trade_date
        ):
            raise RuntimeConfigInvalidError("industry observation date must not exceed eligibility trade_date")
        industry_date = industry_trade_date if industry_trade_date is not None else trade_date
        if top_k <= 0:
            raise RuntimeConfigInvalidError(
                "tradability filter requires positive top_k",
                context={"package_id": package_id, "top_k": top_k},
            )
        ordered = sorted(candidates, key=lambda item: (item.rank, -item.score, item.symbol))
        normalized_industry_blacklist = self._normalize_industry_blacklist(industry_blacklist)
        if not enabled and not normalized_industry_blacklist:
            selected = self._rerank(
                ordered[:top_k],
                package_id=package_id,
                manifest_sha256=manifest_sha256,
                exclude_suspended=False,
                industry_blacklist=[],
            )
            tail_exclusions = [
                self._top_k_exclusion(
                    candidate,
                    trade_date=trade_date,
                    package_id=package_id,
                    manifest_sha256=manifest_sha256,
                    top_k=top_k,
                )
                for candidate in ordered[top_k:]
            ] if exhaustive_evidence else []
            return self._result_with_receipt(
                candidates=selected,
                exclusions=tail_exclusions,
                candidate_pool_count=len(ordered),
                inspected_count=len(ordered) if exhaustive_evidence else len(selected),
                enabled=False,
                industry_blacklist=[],
                trade_date=trade_date,
                package_id=package_id,
                manifest_sha256=manifest_sha256,
                industry_trade_date=industry_trade_date,
            )

        suspended = (
            self.suspend_provider.get_suspended_symbols(
                [item.symbol for item in ordered],
                trade_date,
            )
            if enabled
            else {}
        )
        industry_map: dict[str, IndustryInfo] = {}
        if normalized_industry_blacklist:
            industry_map = self.industry_provider.get_industries(
                [item.symbol for item in ordered],
                industry_date,
            )
        selected: list[SelectionCandidate] = []
        excluded: list[SelectionExclusion] = []
        for candidate in ordered:
            if len(selected) >= top_k:
                if exhaustive_evidence:
                    excluded.append(
                        self._top_k_exclusion(
                            candidate,
                            trade_date=trade_date,
                            package_id=package_id,
                            manifest_sha256=manifest_sha256,
                            top_k=top_k,
                        )
                    )
                    continue
                break
            suspension = suspended.get(candidate.symbol)
            if suspension is not None:
                excluded.append(
                    SelectionExclusion(
                        symbol=candidate.symbol,
                        score=candidate.score,
                        rank=candidate.rank,
                        reason="suspended_by_suspend_d",
                        source=str(suspension.get("source") or "market.suspend_d"),
                        context={
                            "package_id": package_id,
                            "manifest_sha256": manifest_sha256,
                            "trade_date": trade_date.isoformat(),
                            "raw_rank": candidate.rank,
                            "suspend_type": suspension.get("suspend_type"),
                            "suspend_timing": suspension.get("suspend_timing"),
                            **({"suspension_day_authority": suspension["suspension_day_authority"]}
                               if "suspension_day_authority" in suspension else {}),
                        },
                    )
                )
                continue
            if normalized_industry_blacklist:
                industry = industry_map.get(candidate.symbol) or industry_info_from_candidate(
                    candidate.symbol,
                    candidate.component_scores,
                )
                if industry is None:
                    raise DataUnavailableError(
                        "industry blacklist requires PIT industry metadata",
                        context={
                            "package_id": package_id,
                            "manifest_sha256": manifest_sha256,
                            "trade_date": trade_date.isoformat(),
                            "symbol": candidate.symbol,
                            "raw_rank": candidate.rank,
                            "industry_provider": type(self.industry_provider).__name__,
                            "industry_trade_date": industry_date.isoformat(),
                            "required_component_score_keys": [
                                "industry",
                                "sector",
                                "industry_name",
                                "sw_l1",
                                "sw_l1_name",
                                "cs_industry",
                            ],
                        },
                    )
                match = industry.match_blacklist(normalized_industry_blacklist)
                if match:
                    matched_blacklist, matched_level = match
                    excluded.append(
                        SelectionExclusion(
                            symbol=candidate.symbol,
                            score=candidate.score,
                            rank=candidate.rank,
                            reason="industry_blacklisted",
                            source="runtime_profile.industry_blacklist",
                            context={
                                "package_id": package_id,
                                "manifest_sha256": manifest_sha256,
                                "trade_date": trade_date.isoformat(),
                                "raw_rank": candidate.rank,
                                "industry_blacklist": normalized_industry_blacklist,
                                "matched_blacklist": matched_blacklist,
                                "matched_level": matched_level,
                                **({"industry_trade_date": industry_date.isoformat()} if industry_trade_date is not None else {}),
                                **industry.to_context(),
                            },
                        )
                    )
                    continue
            selected.append(candidate)

        if not selected:
            reasons = sorted({item.reason for item in excluded})
            if allow_empty:
                return self._result_with_receipt(
                    candidates=[],
                    exclusions=excluded,
                    candidate_pool_count=len(ordered),
                    inspected_count=len(ordered),
                    enabled=enabled,
                    industry_blacklist=normalized_industry_blacklist,
                    trade_date=trade_date,
                    package_id=package_id,
                    manifest_sha256=manifest_sha256,
                    industry_trade_date=industry_trade_date,
                )
            if reasons == ["suspended_by_suspend_d"]:
                raise DataUnavailableError(
                    "all ranked candidates are suspended by suspend_d",
                    context={
                        "package_id": package_id,
                        "manifest_sha256": manifest_sha256,
                        "trade_date": trade_date.isoformat(),
                        "candidate_count": len(candidates),
                        "excluded_count": len(excluded),
                    },
                )
            raise ArtifactGenerationFailedError(
                "all ranked candidates are excluded by runtime tradability filters",
                context={
                    "package_id": package_id,
                    "manifest_sha256": manifest_sha256,
                    "trade_date": trade_date.isoformat(),
                    "candidate_count": len(candidates),
                    "excluded_count": len(excluded),
                    "exclusion_reasons": reasons,
                },
            )
        reranked = self._rerank(
            selected,
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            exclude_suspended=enabled,
            industry_blacklist=normalized_industry_blacklist,
        )
        return self._result_with_receipt(
            candidates=reranked,
            exclusions=excluded,
            candidate_pool_count=len(ordered),
            inspected_count=(len(ordered) if exhaustive_evidence else len(reranked) + len(excluded)),
            enabled=enabled,
            industry_blacklist=normalized_industry_blacklist,
            trade_date=trade_date,
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            industry_trade_date=industry_trade_date,
            suspension_receipt=getattr(suspended, "receipt", None),
        )

    @staticmethod
    def _top_k_exclusion(
        candidate: SelectionCandidate,
        *,
        trade_date: date,
        package_id: str,
        manifest_sha256: str,
        top_k: int,
    ) -> SelectionExclusion:
        return SelectionExclusion(
            symbol=candidate.symbol,
            score=candidate.score,
            rank=candidate.rank,
            reason="outside_selection_top_k",
            source="runtime_profile.selection.top_k",
            context={
                "package_id": package_id,
                "manifest_sha256": manifest_sha256,
                "trade_date": trade_date.isoformat(),
                "raw_rank": candidate.rank,
                "top_k": top_k,
            },
        )

    def select_top_k_with_receipt(
        self,
        *,
        candidates: list[SelectionCandidate],
        top_k: int,
        trade_date: date,
        package_id: str,
        manifest_sha256: str,
        exhaustive_evidence: bool = False,
    ) -> TradabilityResult:
        """Record the existing no-filter top-k branch without mutating rows."""
        if top_k <= 0:
            raise RuntimeConfigInvalidError(
                "tradability filter requires positive top_k",
                context={"package_id": package_id, "top_k": top_k},
            )
        selected = list(candidates[:top_k])
        exclusions = [
            self._top_k_exclusion(
                candidate,
                trade_date=trade_date,
                package_id=package_id,
                manifest_sha256=manifest_sha256,
                top_k=top_k,
            )
            for candidate in candidates[top_k:]
        ] if exhaustive_evidence else []
        return self._result_with_receipt(
            candidates=selected,
            exclusions=exclusions,
            candidate_pool_count=len(candidates),
            inspected_count=len(candidates) if exhaustive_evidence else len(selected),
            enabled=False,
            industry_blacklist=[],
            trade_date=trade_date,
            package_id=package_id,
            manifest_sha256=manifest_sha256,
        )

    @staticmethod
    def _result_with_receipt(
        *,
        candidates: list[SelectionCandidate],
        exclusions: list[SelectionExclusion],
        candidate_pool_count: int,
        inspected_count: int,
        enabled: bool,
        industry_blacklist: list[str],
        trade_date: date,
        package_id: str,
        manifest_sha256: str,
        industry_trade_date: date | None = None,
        suspension_receipt: dict[str, Any] | None = None,
    ) -> TradabilityResult:
        if inspected_count != len(candidates) + len(exclusions):
            raise RuntimeConfigInvalidError(
                "tradability receipt counts do not reconcile",
                context={
                    "package_id": package_id,
                    "candidate_pool_count": candidate_pool_count,
                    "inspected_count": inspected_count,
                    "output_count": len(candidates),
                    "excluded_count": len(exclusions),
                },
            )
        universe_metadata = {
            "status": "COMPLETE",
            "enabled": enabled,
            "industry_blacklist": list(industry_blacklist),
            "trade_date": trade_date.isoformat(),
            "package_id": package_id,
            "manifest_sha256": manifest_sha256,
            "candidate_pool_count": candidate_pool_count,
            "inspected_count": inspected_count,
            "unprocessed_tail_count": candidate_pool_count - inspected_count,
        }
        if industry_trade_date is not None:
            universe_metadata["industry_trade_date"] = industry_trade_date.isoformat()
        if suspension_receipt is not None:
            universe_metadata["suspension_day_authority"] = suspension_receipt
        receipt = build_stage_receipt(
            stage=CandidateStageName.SELECTION_EFFECTIVE,
            status=StageReceiptStatus.COMPLETE,
            input_count=inspected_count,
            candidates=candidates,
            exclusions=exclusions,
            semantic_payload=universe_metadata,
        )
        return TradabilityResult(
            candidates=candidates,
            exclusions=exclusions,
            receipt=receipt,
            universe_metadata=universe_metadata,
        )

    @staticmethod
    def _rerank(
        candidates: list[SelectionCandidate],
        *,
        package_id: str,
        manifest_sha256: str,
        exclude_suspended: bool,
        industry_blacklist: list[str],
    ) -> list[SelectionCandidate]:
        reranked: list[SelectionCandidate] = []
        for final_rank, candidate in enumerate(candidates, start=1):
            component_scores = dict(candidate.component_scores or {})
            component_scores.setdefault("raw_rank", candidate.rank)
            component_scores["tradability_filter"] = {
                "exclude_suspended": exclude_suspended,
                "industry_blacklist": industry_blacklist,
                "package_id": package_id,
                "manifest_sha256": manifest_sha256,
            }
            reranked.append(
                candidate.model_copy(
                    update={
                        "rank": final_rank,
                        "component_scores": component_scores,
                        "reason": candidate.reason or "tradable_selection",
                    }
                )
            )
        return reranked

    @staticmethod
    def _normalize_industry_blacklist(value: list[str] | None) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for item in value or []:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            normalized.append(text)
            seen.add(text)
        return normalized
