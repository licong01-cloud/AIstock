from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.turnover_constrained_utility_pipeline import (
    _evaluate_constraint_blocks,
    _oracle_priorities,
    _verify_label_status_identity,
)
from backend.tests.advisory_model_first.test_turnover_constrained_utility_contracts import _request


def _labels() -> pd.DataFrame:
    rows = []
    for date in ("2026-01-05", "2026-01-06"):
        for rank in range(1, 21):
            rows.append(
                {
                    "decision_as_of_trade_date": date,
                    "target_trade_date": "2026-01-07",
                    "instrument": f"{rank:06d}.SZ",
                    "selection_rank": rank,
                    "label_status": "MATURED",
                    "holding_trading_days": float(rank),
                    "net_excess_return_bps": float(21 - rank),
                }
            )
    return pd.DataFrame(rows)


def test_oracle_priorities_require_exact_20_matured_rows() -> None:
    labels = _labels()
    priorities = _oracle_priorities(
        labels=labels,
        decision_dates=pd.to_datetime(["2026-01-05", "2026-01-06"]),
        target_count=5,
        shadow_price=100.0,
    )
    assert priorities.groupby("decision_as_of_trade_date").size().eq(20).all()
    assert priorities["selection_effective_rank"].between(1, 20).all()
    labels.loc[0, "label_status"] = "NOT_ENTERED_LIMIT_UP"
    with pytest.raises(AdvisoryModelFirstError, match="exact-20 matured"):
        _oracle_priorities(
            labels=labels,
            decision_dates=pd.to_datetime(["2026-01-05"]),
            target_count=5,
            shadow_price=100.0,
        )


def test_constraint_evaluation_resets_portfolio_at_each_cpcv_block(monkeypatch) -> None:
    calls: list[tuple[str, ...]] = []

    def fake_replay_shadow_portfolio(**kwargs):
        dates = tuple(value.date().isoformat() for value in kwargs["candidate_decision_dates"])
        calls.append(dates)
        return SimpleNamespace(
            daily=pd.DataFrame(
                {
                    "trade_date": pd.to_datetime(list(dates)),
                    "turnover_fraction": [0.1] * len(dates),
                }
            )
        )

    monkeypatch.setattr(
        "backend.services.advisory_model_first.turnover_constrained_utility_pipeline."
        "replay_shadow_portfolio",
        fake_replay_shadow_portfolio,
    )
    dates = pd.to_datetime(["2026-01-05", "2026-01-06"])
    priorities = _oracle_priorities(
        labels=_labels(),
        decision_dates=dates,
        target_count=5,
        shadow_price=100.0,
    )
    receipt = _evaluate_constraint_blocks(
        rankings=pd.DataFrame(),
        candidate_daily=pd.DataFrame(),
        benchmark=pd.DataFrame(),
        suspend=pd.DataFrame(),
        calendar=pd.DataFrame(),
        policy=object(),
        policy_sha256="a" * 64,
        cost=object(),
        request_id="req",
        calibration_dates=pd.DatetimeIndex(dates),
        entry_priorities=priorities,
        block_by_date={"2026-01-05": 1, "2026-01-06": 2},
    )
    assert calls == [("2026-01-05",), ("2026-01-06",)]
    assert receipt["mean_turnover_fraction"] == pytest.approx(0.1)
    assert len(receipt["block_metrics"]) == 2


def test_label_status_identity_rejects_frozen_p0c_drift() -> None:
    request = _request()
    labels = pd.DataFrame(
        {
            "label_status": [
                *(["MATURED"] * 7716),
                *(["NOT_ENTERED_LIMIT_UP"] * 3),
                "CENSORED_RIGHT_BOUNDARY",
            ]
        }
    )
    _verify_label_status_identity(request, labels)
    labels.loc[0, "label_status"] = "CENSORED_RIGHT_BOUNDARY"
    with pytest.raises(AdvisoryModelFirstError, match="differs from frozen P0-C"):
        _verify_label_status_identity(request, labels)
