from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.position_timing.action_value_execution_audit import (
    AUDIT_FIELDS,
    audit_minute_execution,
)
from backend.services.position_timing.contracts import canonical_sha256


def _minute_candidate(root: Path) -> Path:
    (root / "calendars").mkdir(parents=True)
    (root / "instruments").mkdir()
    (root / "features" / "000001.sz").mkdir(parents=True)
    (root / "meta_export.json").write_text(
        json.dumps(
            {
                "start": "2026-08-31",
                "end": "2026-08-31",
                "required_minute_fields": list(AUDIT_FIELDS),
            }
        ),
        encoding="utf-8",
    )
    (root / "calendars" / "1min.txt").write_text(
        "2026-08-31 09:30:00\n2026-08-31 09:31:00\n", encoding="utf-8"
    )
    (root / "instruments" / "all.txt").write_text(
        "000001.SZ\t2026-08-31\t2026-08-31\n", encoding="utf-8"
    )
    values = {
        "open": 10,
        "high": 10.1,
        "low": 9.9,
        "close": 10,
        "factor": 1,
        "up_limit_price": 11,
        "down_limit_price": 9,
    }
    for field, value in values.items():
        np.asarray([0, value, value], dtype="<f4").tofile(
            root / "features" / "000001.sz" / f"{field}.1min.bin"
        )
    return root


def test_minute_audit_checks_same_frozen_plan_without_creating_signal(tmp_path: Path) -> None:
    minute = _minute_candidate(tmp_path / "minute")
    rows = pd.DataFrame(
        [
            {
                "sleeve_id": "s1",
                "symbol": "000001.SZ",
                "target_trade_date": "2026-08-31",
                "baseline": "BUY_AND_HOLD",
                "planned_delta_qty": 100,
                "plan_reference_raw": 10,
                "plan_risk_exit": False,
                "pre_quantity": 0,
                "pre_sellable_qty": 0,
                "fill_status": "FILLED",
                "fill_price_raw": 10,
            },
            # The second baseline is the same policy plan and must not double count.
            {
                "sleeve_id": "s1",
                "symbol": "000001.SZ",
                "target_trade_date": "2026-08-31",
                "baseline": "FROZEN_L1_V1",
                "planned_delta_qty": 100,
                "plan_reference_raw": 10,
                "plan_risk_exit": False,
                "pre_quantity": 0,
                "pre_sellable_qty": 0,
                "fill_status": "FILLED",
                "fill_price_raw": 10,
            },
        ]
    )
    result = audit_minute_execution(rows, minute_root=minute)
    assert result["population_count"] == 1
    assert result["paired_count"] == 1
    assert result["fill_status_agreement_ratio"] == 1
    assert result["audit_role"] == "DIAGNOSTIC_ONLY_NOT_MINUTE_SIGNAL"
    assert result["execution_audit_sha256"] == canonical_sha256(
        {key: value for key, value in result.items() if key != "execution_audit_sha256"}
    )
