"""Freeze the current file-source C-010 panels into the formal train/D6 request.

Only complete, validated C-010/A5 source receipts may cross this boundary.
The smaller rotation-product bundle (nine features) is not a substitute.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, BASE_FEATURES, canonical_sha256
from backend.services.hmm_risk.formal_state_calendar import build_calendar_carrier
from backend.services.hmm_risk.formal_state_executor import ACTIVE_GENERATION, ACTIVE_MANIFEST, TRAIN_CALENDAR_HASH
from backend.services.hmm_risk.formal_state_model import CONTRACTS, FAMILIES, VERSION, FormalStateError, receipt
from backend.services.hmm_risk.stock_fact_observation import validate_c010_policy_manifest


def _frame(panel: pd.DataFrame, *, codes: Sequence[str], calendar: Sequence[str]) -> pd.DataFrame:
    if not isinstance(panel, pd.DataFrame) or not isinstance(panel.index, pd.MultiIndex):
        raise FormalStateError("hmm_risk_formal_input_invalid", "direct feature panel must have date/sector index")
    if panel.index.nlevels != 2 or panel.index.has_duplicates:
        raise FormalStateError("hmm_risk_formal_input_invalid", "direct feature panel key is not unique")
    expected = pd.MultiIndex.from_product([pd.to_datetime(calendar), codes], names=panel.index.names)
    if not panel.index.sort_values().equals(expected):
        raise FormalStateError("hmm_risk_formal_input_invalid", "panel must retain full calendar/sector denominator")
    if not set(ALL_CORE_FEATURES) <= set(panel.columns) or "benchmark_return" not in panel:
        raise FormalStateError("hmm_risk_formal_input_invalid", "full 20D source and benchmark required")
    return panel.reindex(expected)


def prepare_request(
    *,
    panels: Mapping[str, pd.DataFrame],
    calendar: Sequence[str],
    sector_codes: Mapping[str, Sequence[str]],
    source_identity: Mapping[str, Any],
    industry_authority: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    """No fitting; full validation calendar is retained with compact finite payloads."""
    policy = validate_c010_policy_manifest(policy)
    if (
        policy["schema_version"] != "hmm_risk_c010_feature_domain_policy_v2"
        or policy["dataset_manifest_hash"] != source_identity["dataset_manifest_hash"]
        or source_identity["generation"] != ACTIVE_GENERATION
        or source_identity["manifest_sha256"] != ACTIVE_MANIFEST
    ):
        raise FormalStateError("hmm_risk_c010_policy_identity_mismatch", "current A5/source identity required")
    if list(calendar) != sorted(set(calendar)) or any(date.fromisoformat(d) > date(2025, 4, 30) for d in calendar):
        raise FormalStateError("hmm_risk_formal_input_invalid", "source calendar exceeds approved utility watermark")
    train_calendar = [d for d in calendar if "2022-01-01" <= d <= "2024-06-30"]
    validation_calendar = [d for d in calendar if "2024-07-01" <= d <= "2025-03-31"]
    if (
        len(train_calendar) != 601
        or canonical_sha256(train_calendar) != TRAIN_CALENDAR_HASH
        or len(validation_calendar) != 182
    ):
        raise FormalStateError("hmm_risk_formal_input_invalid", "frozen calendars differ")
    if set(panels) != {"L1", "L2"} or set(sector_codes) != {"L1", "L2"}:
        raise FormalStateError("hmm_risk_formal_input_invalid", "both direct levels required")
    series = {}
    insufficient = []
    for level, count in (("L1", 31), ("L2", 131)):
        codes = list(sector_codes[level])
        if len(codes) != count or codes != sorted(set(codes)):
            raise FormalStateError("hmm_risk_formal_input_invalid", f"{level} canonical denominator differs")
        panel = _frame(panels[level], codes=codes, calendar=calendar)
        for family in FAMILIES:
            features = list(BASE_FEATURES if family == FAMILIES[0] else ALL_CORE_FEATURES)
            key = f"{family}:{level}"
            series[key] = {}
            for code in codes:
                frame = panel.xs(code, level=1)
                train = frame.reindex(pd.to_datetime(train_calendar))[features]
                train_mask = np.isfinite(train.to_numpy(dtype=np.float64)).all(axis=1)
                train = train.loc[train_mask]
                if len(train) < 120:
                    insufficient.append({"family": family, "level": level, "sector": code, "rows": len(train)})
                validation = frame.reindex(pd.to_datetime(validation_calendar))[features].to_numpy(dtype=np.float64)
                daily = frame["daily_return"].to_numpy(dtype=np.float64)
                benchmark = frame["benchmark_return"].to_numpy(dtype=np.float64)
                calendar_position = {day: index for index, day in enumerate(calendar)}
                components = {}
                for horizon in (5, 10, 20):
                    available_positions, utility = [], []
                    for p, day in enumerate(validation_calendar):
                        i = calendar_position[day]
                        # Existing C-007-A utility: forward SUM of daily excess,
                        # not a newly introduced compounded-return target.
                        sector_window = daily[i + 1 : i + horizon + 1]
                        market_window = benchmark[i + 1 : i + horizon + 1]
                        if (
                            len(sector_window) == horizon
                            and np.isfinite(sector_window).all()
                            and np.isfinite(market_window).all()
                        ):
                            value = float(np.sum(sector_window - market_window))
                            if np.isfinite(value):
                                available_positions.append(p)
                                utility.append(value)
                    components[f"excess_return_{horizon}d"] = {"positions": available_positions, "values": utility}
                body = {
                    "feature_names": features,
                    "train_dates": [d.date().isoformat() for d in train.index],
                    "train_values": train.to_numpy(dtype=np.float64).tolist(),
                    "validation": build_calendar_carrier(
                        dates=validation_calendar,
                        feature_names=features,
                        observations=validation,
                        components=components,
                        source_identity_sha256=canonical_sha256(source_identity),
                        source_receipt_sha256=policy["receipt_sha256"],
                    ),
                }
                series[key][code] = {**body, "source_receipt_sha256": canonical_sha256(body)}
    if insufficient:
        raise FormalStateError(
            "hmm_risk_model_train_coverage_insufficient", "complete 31/131 grid cannot be frozen", evidence=insufficient
        )
    return receipt(
        {
            "schema_version": VERSION,
            "contracts": CONTRACTS,
            "source_identity": dict(source_identity),
            "industry_authority": dict(industry_authority),
            "policy": policy,
            "train_calendar": train_calendar,
            "validation_calendar": validation_calendar,
            "sector_codes": dict(sector_codes),
            "series": series,
        }
    )
