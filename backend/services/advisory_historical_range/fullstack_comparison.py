"""Immutable repo-external artifacts for the A/B/C historical comparison."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from statistics import mean, median, stdev
from typing import Any, Mapping, Sequence

from backend.services.advisory_list_transition import (
    AdvisoryListTransitionEngine,
    AdvisoryTransitionCandidateV1,
    AdvisoryTransitionEpisodeV1,
    AdvisoryTransitionPolicyV1,
    AdvisoryTransitionRankObservationV1,
    REVIEW_REASON_NOT_IN_CURRENT_TOPK,
)

from backend.services.advisory_historical_range.canonical import canonical_json_text
from backend.services.advisory_historical_range.model_challenger import (
    HistoricalMetaLabelChallengerArtifactV1,
    HistoricalModelChallengerArtifactV1,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeCandidateArtifactPayloadV2,
)


@dataclass(frozen=True)
class HistoricalComparisonArtifactRefV1:
    artifact_kind: str
    artifact_hash: str
    relative_path: str
    file_sha256: str
    size_bytes: int


@dataclass(frozen=True)
class HistoricalComparisonLifecycleDayV1:
    decision_trade_date: date
    next_trade_date: date
    entry_candidates: tuple[AdvisoryTransitionCandidateV1, ...]
    review_rank_by_symbol: Mapping[str, int | None]
    exit_mark_by_symbol: Mapping[str, float | None]
    exit_mark_available_by_symbol: Mapping[str, bool]
    observed_max_selection_rank: int


class HistoricalComparisonArtifactStore:
    def __init__(self, *, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("comparison artifact root must be absolute")
        root.mkdir(parents=True, exist_ok=True)
        self._root = root.resolve(strict=True)

    def publish_challenger(
        self,
        artifact: HistoricalModelChallengerArtifactV1,
    ) -> HistoricalComparisonArtifactRefV1:
        return self._publish_model_artifact(
            artifact=artifact,
            artifact_kind="MODEL_CHALLENGER",
            directory="model-challenger",
        )

    def publish_meta_label_challenger(
        self,
        artifact: HistoricalMetaLabelChallengerArtifactV1,
    ) -> HistoricalComparisonArtifactRefV1:
        return self._publish_model_artifact(
            artifact=artifact,
            artifact_kind="META_LABEL_CHALLENGER",
            directory="meta-label-challenger",
        )

    def _publish_model_artifact(
        self,
        *,
        artifact: (
            HistoricalModelChallengerArtifactV1
            | HistoricalMetaLabelChallengerArtifactV1
        ),
        artifact_kind: str,
        directory: str,
    ) -> HistoricalComparisonArtifactRefV1:
        identity = str(artifact.artifact_hash)
        relative = Path(directory) / f"{identity}.json"
        destination = (self._root / relative).resolve()
        try:
            destination.relative_to(self._root)
        except ValueError as exc:
            raise ValueError(
                "comparison artifact path escapes configured root"
            ) from exc
        destination.parent.mkdir(parents=True, exist_ok=True)
        content = (canonical_json_text(artifact.model_dump(mode="json")) + "\n").encode(
            "utf-8"
        )
        if destination.exists():
            persisted = destination.read_bytes()
            if persisted != content:
                raise RuntimeError("ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT")
        else:
            descriptor, temp_name = tempfile.mkstemp(
                prefix=f".{identity}.", suffix=".tmp", dir=destination.parent
            )
            temporary = Path(temp_name)
            try:
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
                try:
                    os.link(temporary, destination)
                except FileExistsError:
                    if destination.read_bytes() != content:
                        raise RuntimeError("ADVISORY_COMPARISON_EXACT_RETRY_CONFLICT")
                if destination.read_bytes() != content:
                    raise RuntimeError("ADVISORY_COMPARISON_ARTIFACT_READBACK_MISMATCH")
            finally:
                temporary.unlink(missing_ok=True)
        return HistoricalComparisonArtifactRefV1(
            artifact_kind=artifact_kind,
            artifact_hash=identity,
            relative_path=relative.as_posix(),
            file_sha256=_sha256_bytes(content),
            size_bytes=len(content),
        )

    def load_challenger(
        self,
        ref: HistoricalComparisonArtifactRefV1,
    ) -> HistoricalModelChallengerArtifactV1:
        if ref.artifact_kind != "MODEL_CHALLENGER":
            raise ValueError("comparison ref is not a model challenger")
        path = (self._root / ref.relative_path).resolve()
        try:
            path.relative_to(self._root)
        except ValueError as exc:
            raise ValueError(
                "comparison artifact path escapes configured root"
            ) from exc
        raw = path.read_bytes()
        if len(raw) != ref.size_bytes or _sha256_bytes(raw) != ref.file_sha256:
            raise RuntimeError("ADVISORY_COMPARISON_ARTIFACT_READBACK_MISMATCH")
        payload = json.loads(raw.decode("utf-8"))
        artifact = HistoricalModelChallengerArtifactV1.model_validate(payload)
        if artifact.artifact_hash != ref.artifact_hash:
            raise RuntimeError("ADVISORY_COMPARISON_ARTIFACT_READBACK_MISMATCH")
        return artifact

    def load_meta_label_challenger(
        self,
        ref: HistoricalComparisonArtifactRefV1,
    ) -> HistoricalMetaLabelChallengerArtifactV1:
        if ref.artifact_kind != "META_LABEL_CHALLENGER":
            raise ValueError("comparison ref is not a meta-label challenger")
        path = (self._root / ref.relative_path).resolve()
        try:
            path.relative_to(self._root)
        except ValueError as exc:
            raise ValueError(
                "comparison artifact path escapes configured root"
            ) from exc
        raw = path.read_bytes()
        if len(raw) != ref.size_bytes or _sha256_bytes(raw) != ref.file_sha256:
            raise RuntimeError("ADVISORY_COMPARISON_ARTIFACT_READBACK_MISMATCH")
        payload = json.loads(raw.decode("utf-8"))
        artifact = HistoricalMetaLabelChallengerArtifactV1.model_validate(payload)
        if artifact.artifact_hash != ref.artifact_hash:
            raise RuntimeError("ADVISORY_COMPARISON_ARTIFACT_READBACK_MISMATCH")
        return artifact


def compare_day_ranks(
    *,
    control: HistoricalRangeCandidateArtifactPayloadV2,
    enhanced: HistoricalRangeCandidateArtifactPayloadV2,
    challenger: HistoricalModelChallengerArtifactV1,
) -> dict[str, Any]:
    if control.decision_trade_date != enhanced.decision_trade_date or (
        challenger.decision_trade_date != control.decision_trade_date
    ):
        raise ValueError("comparison day identities differ")
    a = {item.symbol: item for item in control.candidates}
    b = {item.symbol: item for item in enhanced.candidates}
    if set(a) != set(b):
        raise ValueError("A/B raw candidate symbol sets differ")
    raw_mismatches = [
        symbol
        for symbol in a
        if a[symbol].alpha_raw_rank != b[symbol].alpha_raw_rank
        or a[symbol].alpha_raw_score != b[symbol].alpha_raw_score
    ]
    if raw_mismatches:
        raise ValueError("A/B raw candidate evidence differs")
    model = {item.symbol: item for item in challenger.candidates}
    expected_model_symbols = {
        item.symbol
        for item in control.candidates
        if (item.selection_effective_rank or 10**9) <= 20
    }
    if set(model) != expected_model_symbols:
        raise ValueError("C parent Top20 differs from A")
    a5 = _top_symbols(a.values(), "selection_effective_rank", 5)
    b5 = _top_symbols(b.values(), "selection_effective_rank", 5)
    c5 = {item.symbol for item in challenger.candidates if item.model_rank <= 5}
    a20 = _top_symbols(a.values(), "selection_effective_rank", 20)
    b20 = _top_symbols(b.values(), "selection_effective_rank", 20)
    return {
        "decision_trade_date": control.decision_trade_date.isoformat(),
        "raw_candidate_count": len(a),
        "hmm_rank_changed_count": _changed_count(
            b.values(), "alpha_raw_rank", "hmm_adjusted_rank"
        ),
        "risk_rank_changed_count": _changed_count(
            b.values(), "hmm_adjusted_rank", "risk_policy_adjusted_rank"
        ),
        "selection_rank_changed_count": _changed_count(
            b.values(), "risk_policy_adjusted_rank", "selection_effective_rank"
        ),
        "excluded_count": sum(
            item.membership_status == "EXCLUDED" for item in b.values()
        ),
        "a20_b20_overlap": len(a20 & b20),
        "a20_b20_changed": 20 - len(a20 & b20),
        "a5_b5_overlap": len(a5 & b5),
        "a5_c5_overlap": len(a5 & c5),
        "b5_c5_overlap": len(b5 & c5),
        "model_rank_changed_count": sum(
            item.selection_rank != item.model_rank for item in challenger.candidates
        ),
        "model_mean_absolute_rank_change": mean(
            abs(item.selection_rank - item.model_rank) for item in challenger.candidates
        ),
        "a5": sorted(a5),
        "b5": sorted(b5),
        "c5": sorted(c5),
        "a20": sorted(a20),
        "b20": sorted(b20),
    }


def summarize_return_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = [
        {
            "trade_date": _date_value(item["trade_date"]),
            "value": float(item["value"]),
        }
        for item in records
        if item.get("value") is not None
    ]
    if not normalized:
        return {"status": "NO_VALID_SAMPLE", "sample_count": 0}
    daily: dict[date, list[float]] = {}
    monthly: dict[str, list[float]] = {}
    values: list[float] = []
    for item in normalized:
        trade_date = item["trade_date"]
        value = item["value"]
        values.append(value)
        daily.setdefault(trade_date, []).append(value)
        monthly.setdefault(trade_date.strftime("%Y-%m"), []).append(value)
    daily_values = [mean(items) for _, items in sorted(daily.items())]
    return {
        "status": "AVAILABLE",
        "sample_count": len(values),
        "positive_count": sum(value > 0 for value in values),
        "win_rate": sum(value > 0 for value in values) / len(values),
        "mean_return": mean(values),
        "median_return": median(values),
        "max_consecutive_losses": _max_consecutive_losses(values),
        "daily_observation_count": len(daily_values),
        "mean_daily_equal_weight_return": mean(daily_values),
        "daily_win_rate": sum(value > 0 for value in daily_values) / len(daily_values),
        "daily": {
            trade_date.isoformat(): {
                "sample_count": len(items),
                "win_rate": sum(value > 0 for value in items) / len(items),
                "mean_return": mean(items),
            }
            for trade_date, items in sorted(daily.items())
        },
        "monthly": {
            month: {
                "sample_count": len(items),
                "win_rate": sum(value > 0 for value in items) / len(items),
                "mean_return": mean(items),
            }
            for month, items in sorted(monthly.items())
        },
    }


def summarize_paired_daily_delta(
    baseline: Mapping[str, Any],
    current: Mapping[str, Any],
) -> dict[str, Any]:
    baseline_daily = baseline.get("daily") or {}
    current_daily = current.get("daily") or {}
    dates = sorted(set(baseline_daily) & set(current_daily))
    if not dates:
        return {"status": "NO_PAIRED_DAY", "paired_day_count": 0}
    return_differences = [
        float(current_daily[value]["mean_return"])
        - float(baseline_daily[value]["mean_return"])
        for value in dates
    ]
    win_differences = [
        float(current_daily[value]["win_rate"])
        - float(baseline_daily[value]["win_rate"])
        for value in dates
    ]
    return {
        "status": "AVAILABLE",
        "paired_day_count": len(dates),
        "mean_return_difference": mean(return_differences),
        "mean_return_difference_ci95": _normal_mean_ci95(return_differences),
        "win_rate_difference": mean(win_differences),
        "win_rate_difference_ci95": _normal_mean_ci95(win_differences),
        "ci_method": "PAIRED_DAILY_NORMAL_95",
    }


def replay_matched_lifecycle(
    *,
    group_name: str,
    days: Sequence[HistoricalComparisonLifecycleDayV1],
    policy: AdvisoryTransitionPolicyV1,
) -> dict[str, Any]:
    """Replay one matched-capacity list while separating entry and review ranks."""

    if not group_name.strip() or not days:
        raise ValueError("matched lifecycle requires a group name and at least one day")
    ordered = tuple(sorted(days, key=lambda item: item.decision_trade_date))
    if len({item.decision_trade_date for item in ordered}) != len(ordered):
        raise ValueError("matched lifecycle decision dates must be unique")
    engine = AdvisoryListTransitionEngine()
    active: tuple[AdvisoryTransitionEpisodeV1, ...] = ()
    entry_sequence: dict[str, int] = {}
    daily_rows: list[dict[str, Any]] = []
    completed_episodes: list[dict[str, Any]] = []
    action_counts: dict[str, int] = {}
    exit_reason_counts: dict[str, int] = {}

    for day in ordered:
        active_by_symbol = {item.symbol: item for item in active}
        if len(active_by_symbol) != len(active):
            raise ValueError("matched lifecycle active symbols must be unique")
        candidates_by_symbol = {item.symbol: item for item in day.entry_candidates}
        if len(candidates_by_symbol) != len(day.entry_candidates):
            raise ValueError("matched lifecycle entry candidates must be unique")
        effective_candidates: dict[str, AdvisoryTransitionCandidateV1] = {}
        for symbol, candidate in candidates_by_symbol.items():
            review_rank = day.review_rank_by_symbol.get(symbol)
            if symbol in active_by_symbol:
                effective_candidates[symbol] = replace(
                    candidate,
                    rank=review_rank or day.observed_max_selection_rank + 1,
                )
            else:
                effective_candidates[symbol] = candidate
        for symbol, episode in active_by_symbol.items():
            if symbol in effective_candidates:
                continue
            review_rank = day.review_rank_by_symbol.get(symbol)
            effective_candidates[symbol] = AdvisoryTransitionCandidateV1(
                symbol=symbol,
                rank=review_rank or day.observed_max_selection_rank + 1,
                score=episode.current_score,
                entry_mark=None,
                exit_mark=day.exit_mark_by_symbol.get(symbol),
                entry_mark_available=False,
                exit_mark_available=bool(
                    day.exit_mark_available_by_symbol.get(symbol, False)
                ),
                reason_code=REVIEW_REASON_NOT_IN_CURRENT_TOPK,
                evidence={
                    "group_name": group_name,
                    "active_episode_id": episode.episode_id,
                },
            )
        active_review_ranks = {
            symbol: day.review_rank_by_symbol.get(symbol)
            or day.observed_max_selection_rank + 1
            for symbol in active_by_symbol
        }

        def allocate_episode(candidate: AdvisoryTransitionCandidateV1) -> str:
            sequence = entry_sequence.get(candidate.symbol, 0) + 1
            entry_sequence[candidate.symbol] = sequence
            return f"{group_name}:{candidate.symbol}:{sequence}"

        transition = engine.transition(
            policy=policy,
            decision_trade_date=day.decision_trade_date,
            candidates=tuple(
                effective_candidates[key] for key in sorted(effective_candidates)
            ),
            active_episodes=active,
            rank_observation=AdvisoryTransitionRankObservationV1(
                status="COMPLETE",
                observed_max_selection_rank=day.observed_max_selection_rank,
                active_rank_by_symbol=active_review_ranks,
            ),
            episode_identity_allocator=allocate_episode,
            effective_entry_date=lambda _candidate, value=day.next_trade_date: value,
            effective_exit_date=lambda _episode, value=day.next_trade_date: value,
            defer_stop_before_effective_entry=False,
            historical_mode=True,
        )
        if transition.blocking_diagnostics:
            raise RuntimeError(
                "ADVISORY_COMPARISON_LIFECYCLE_INPUT_INCOMPLETE: "
                f"group={group_name} decision_trade_date={day.decision_trade_date.isoformat()} "
                + ",".join(transition.blocking_diagnostics)
            )
        actions: list[dict[str, Any]] = []
        for decision in transition.decisions:
            action_counts[decision.action] = action_counts.get(decision.action, 0) + 1
            actions.append(
                {
                    "symbol": decision.symbol,
                    "action": decision.action,
                    "reason_code": decision.reason_code,
                    "episode_id": (
                        decision.episode.episode_id if decision.episode else None
                    ),
                }
            )
            if decision.action == "EXIT" and decision.episode is not None:
                exit_reason_counts[decision.reason_code] = (
                    exit_reason_counts.get(decision.reason_code, 0) + 1
                )
                exit_price = decision.exit_price
                gross_return = (
                    exit_price / decision.episode.entry_price - 1.0
                    if exit_price is not None
                    else None
                )
                completed_episodes.append(
                    {
                        "episode_id": decision.episode.episode_id,
                        "symbol": decision.symbol,
                        "entry_trade_date": decision.episode.effective_entry_date.isoformat(),
                        "exit_trade_date": day.next_trade_date.isoformat(),
                        "exit_reason": decision.reason_code,
                        "gross_return": gross_return,
                        "holding_trading_days": decision.episode.holding_trading_days,
                        "max_runup_bps": decision.episode.max_runup_bps,
                        "max_drawdown_bps": decision.episode.max_drawdown_bps,
                    }
                )
        active = transition.active_episodes
        daily_rows.append(
            {
                "decision_trade_date": day.decision_trade_date.isoformat(),
                "active_symbols": sorted(item.symbol for item in active),
                "actions": sorted(
                    actions, key=lambda item: (item["symbol"], item["action"])
                ),
                "replacement_budget_used": transition.replacement_budget_used,
            }
        )
    completed_returns = [
        float(item["gross_return"])
        for item in completed_episodes
        if item["gross_return"] is not None
    ]
    holding_days = [
        float(item["holding_trading_days"])
        for item in completed_episodes
        if item["holding_trading_days"] is not None
    ]
    max_runups = [
        float(item["max_runup_bps"])
        for item in completed_episodes
        if item["max_runup_bps"] is not None
    ]
    max_drawdowns = [
        float(item["max_drawdown_bps"])
        for item in completed_episodes
        if item["max_drawdown_bps"] is not None
    ]
    return {
        "group_name": group_name,
        "day_count": len(daily_rows),
        "daily": daily_rows,
        "action_counts": dict(sorted(action_counts.items())),
        "completed_episode_count": len(completed_episodes),
        "completed_episodes": completed_episodes,
        "exit_reason_counts": dict(sorted(exit_reason_counts.items())),
        "active_at_end_count": len(active),
        "active_at_end": sorted(item.symbol for item in active),
        "episode_return_basis": "GROSS_DECISION_MARK",
        "episode_win_rate": (
            sum(item > 0 for item in completed_returns) / len(completed_returns)
            if completed_returns
            else None
        ),
        "mean_episode_return": mean(completed_returns) if completed_returns else None,
        "median_episode_return": (
            median(completed_returns) if completed_returns else None
        ),
        "max_consecutive_losses": _max_consecutive_losses(completed_returns),
        "mean_holding_trading_days": mean(holding_days) if holding_days else None,
        "mean_max_runup_bps": mean(max_runups) if max_runups else None,
        "mean_max_drawdown_bps": mean(max_drawdowns) if max_drawdowns else None,
    }


def _top_symbols(rows: Any, rank_field: str, count: int) -> set[str]:
    return {
        item.symbol
        for item in rows
        if item.membership_status == "INCLUDED"
        and getattr(item, rank_field) is not None
        and int(getattr(item, rank_field)) <= count
    }


def _changed_count(rows: Any, first: str, second: str) -> int:
    return sum(
        getattr(item, first) is not None
        and getattr(item, second) is not None
        and getattr(item, first) != getattr(item, second)
        for item in rows
    )


def _max_consecutive_losses(values: Sequence[float]) -> int:
    longest = 0
    current = 0
    for value in values:
        if value < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _normal_mean_ci95(values: Sequence[float]) -> tuple[float, float]:
    center = mean(values)
    if len(values) < 2:
        return (center, center)
    half_width = 1.96 * stdev(values) / math.sqrt(len(values))
    return (center - half_width, center + half_width)


def _date_value(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()
