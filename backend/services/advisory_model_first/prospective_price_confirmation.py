from __future__ import annotations

import json
import math
import os
import random
from pathlib import Path
from statistics import mean, median
from typing import Sequence

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.prospective_price_evaluation import (
    SETTLEMENT_ROOT_NAME,
    AdvisoryPriceProspectiveSettlementArtifact,
    read_settlement_artifact,
)
from backend.services.advisory_model_first.prospective_price_evaluation_contracts import (
    AdvisoryPriceProspectiveConfirmationV1,
    AdvisoryPriceProspectiveOutcomeCandidateV1,
    build_confirmation,
)


MINIMUM_TARGET_DATES = 20
MINIMUM_AVAILABLE_CANDIDATES = 300
MINIMUM_CANDIDATES_PER_SUPPORTED_DATE = 5
MINIMUM_SUPPORTED_DATE_RATIO = 0.8
BOOTSTRAP_RESAMPLES = 2000


def build_price_prospective_confirmation(
    *,
    model_root: str | Path,
    price_range_bundle_id: str,
) -> AdvisoryPriceProspectiveConfirmationV1:
    settlement_root = Path(model_root).resolve() / SETTLEMENT_ROOT_NAME
    artifacts = _matching_settlements(
        settlement_root,
        price_range_bundle_id=price_range_bundle_id,
    )
    target_dates = tuple(sorted(artifact.settlement.target_trade_date for artifact in artifacts))
    if len(target_dates) != len(set(target_dates)):
        raise _confirmation_error(
            "prospective confirmation contains duplicate target dates",
            "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_IDENTITY_MISMATCH",
        )
    packages = {artifact.settlement.package_id for artifact in artifacts}
    policies = {artifact.settlement.review_policy_sha256 for artifact in artifacts}
    if len(packages) > 1 or len(policies) > 1:
        raise _confirmation_error(
            "prospective confirmation mixes package or policy lineage",
            "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_IDENTITY_MISMATCH",
        )
    supported_by_date = {
        artifact.settlement.target_trade_date: artifact.settlement.metrics.model_available_market_available_count
        for artifact in artifacts
    }
    support_date_count = sum(
        count >= MINIMUM_CANDIDATES_PER_SUPPORTED_DATE for count in supported_by_date.values()
    )
    target_date_count = len(target_dates)
    support_date_ratio = support_date_count / target_date_count if target_date_count else 0.0
    available_candidate_count = sum(supported_by_date.values())
    not_applicable_count = sum(artifact.settlement.metrics.not_applicable_count for artifact in artifacts)
    gaps: list[str] = []
    if target_date_count < MINIMUM_TARGET_DATES:
        gaps.append(f"target_dates:{target_date_count}/{MINIMUM_TARGET_DATES}")
    if available_candidate_count < MINIMUM_AVAILABLE_CANDIDATES:
        gaps.append(f"available_candidates:{available_candidate_count}/{MINIMUM_AVAILABLE_CANDIDATES}")
    if support_date_ratio < MINIMUM_SUPPORTED_DATE_RATIO:
        gaps.append(
            f"supported_date_ratio:{support_date_ratio:.6f}/{MINIMUM_SUPPORTED_DATE_RATIO:.6f}"
        )

    common = {
        "price_range_bundle_id": price_range_bundle_id,
        "package_id": next(iter(packages), None),
        "review_policy_sha256": next(iter(policies), None),
        "target_trade_dates": target_dates,
        "target_date_count": target_date_count,
        "available_candidate_count": available_candidate_count,
        "not_applicable_count": not_applicable_count,
        "support_date_count": support_date_count,
        "support_date_ratio": support_date_ratio,
        "support_gaps": tuple(gaps),
    }
    if gaps:
        return build_confirmation(status="ACCUMULATING", metrics=None, cluster_bootstrap=None, **common)

    rows = [
        row
        for artifact in artifacts
        for row in artifact.settlement.candidates
        if row.market_outcome_status == "AVAILABLE" and row.model_prediction_status == "AVAILABLE"
    ]
    metrics = _aggregate_metrics(rows)
    bootstrap = _cluster_bootstrap(artifacts, price_range_bundle_id=price_range_bundle_id)
    return build_confirmation(
        status="CONFIRMATION_EVIDENCE_READY",
        metrics=metrics,
        cluster_bootstrap=bootstrap,
        **common,
    )


def write_price_prospective_confirmation(
    confirmation: AdvisoryPriceProspectiveConfirmationV1,
    path: str | Path,
) -> Path:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = confirmation.model_dump(mode="json")
    if target.exists():
        try:
            existing = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise _confirmation_error(
                "existing confirmation output cannot be read",
                "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_ARTIFACT_CONFLICT",
            ) from exc
        if existing != payload:
            raise _confirmation_error(
                "confirmation output already contains different content",
                "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_ARTIFACT_CONFLICT",
            )
        return target
    temporary = target.with_name(f".{target.name}.tmp")
    if temporary.exists():
        raise _confirmation_error(
            "confirmation temporary path is already occupied",
            "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_ARTIFACT_CONFLICT",
        )
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def _matching_settlements(
    root: Path,
    *,
    price_range_bundle_id: str,
) -> list[AdvisoryPriceProspectiveSettlementArtifact]:
    if not root.exists():
        return []
    artifacts: list[AdvisoryPriceProspectiveSettlementArtifact] = []
    for path in sorted(root.iterdir(), key=lambda item: item.name):
        if not path.is_dir() or path.name.startswith("."):
            continue
        artifact = read_settlement_artifact(path)
        if artifact.settlement.price_range_bundle_id == price_range_bundle_id:
            artifacts.append(artifact)
    return artifacts


def _aggregate_metrics(
    rows: Sequence[AdvisoryPriceProspectiveOutcomeCandidateV1],
) -> dict[str, float]:
    if not rows:
        raise _confirmation_error(
            "ready confirmation has no supported candidates",
            "ADVISORY_PRICE_PROSPECTIVE_CONFIRMATION_SUPPORT_INSUFFICIENT",
        )
    covered = [float(bool(row.covered)) for row in rows]
    lower = [float(bool(row.lower_miss)) for row in rows]
    upper = [float(bool(row.upper_miss)) for row in rows]
    widths = [float(row.interval_width_bps) for row in rows if row.interval_width_bps is not None]
    errors = [float(row.absolute_mid_error_bps) for row in rows if row.absolute_mid_error_bps is not None]
    return {
        "calibrated_coverage": mean(covered),
        "lower_miss_rate": mean(lower),
        "upper_miss_rate": mean(upper),
        "mean_interval_width_bps": mean(widths),
        "median_interval_width_bps": median(widths),
        "mean_absolute_mid_error_bps": mean(errors),
        "median_absolute_mid_error_bps": median(errors),
    }


def _cluster_bootstrap(
    artifacts: Sequence[AdvisoryPriceProspectiveSettlementArtifact],
    *,
    price_range_bundle_id: str,
) -> dict[str, float]:
    daily: list[tuple[float, float]] = []
    for artifact in artifacts:
        rows = [
            row
            for row in artifact.settlement.candidates
            if row.market_outcome_status == "AVAILABLE" and row.model_prediction_status == "AVAILABLE"
        ]
        if not rows:
            continue
        daily.append(
            (
                mean(float(bool(row.covered)) for row in rows),
                mean(float(row.absolute_mid_error_bps) for row in rows if row.absolute_mid_error_bps is not None),
            )
        )
    seed = int(price_range_bundle_id[:16], 16)
    rng = random.Random(seed)
    coverage_draws: list[float] = []
    error_draws: list[float] = []
    for _ in range(BOOTSTRAP_RESAMPLES):
        sample = [daily[rng.randrange(len(daily))] for _ in daily]
        coverage_draws.append(mean(value[0] for value in sample))
        error_draws.append(mean(value[1] for value in sample))
    coverage_draws.sort()
    error_draws.sort()
    return {
        "method": "TARGET_TRADE_DATE_CLUSTER_BOOTSTRAP_V1",
        "resamples": float(BOOTSTRAP_RESAMPLES),
        "cluster_count": float(len(daily)),
        "coverage_mean": mean(coverage_draws),
        "coverage_ci95_low": _quantile(coverage_draws, 0.025),
        "coverage_ci95_high": _quantile(coverage_draws, 0.975),
        "absolute_mid_error_bps_mean": mean(error_draws),
        "absolute_mid_error_bps_ci95_low": _quantile(error_draws, 0.025),
        "absolute_mid_error_bps_ci95_high": _quantile(error_draws, 0.975),
    }


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("quantile values cannot be empty")
    position = (len(values) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(values[lower])
    weight = position - lower
    return float(values[lower] * (1.0 - weight) + values[upper] * weight)


def _confirmation_error(message: str, reason_code: str) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(message, reason_code=reason_code)
