"""Immutable dataset identity shared by all QE backtest consumers.

The live selection and paper-trading runtimes intentionally do not import this
module.  Updating these defaults is part of deploying a new QE dataset; normal
QE experiment creation only consumes the resulting immutable contract.
"""

from __future__ import annotations

import datetime as dt
import os
import re


_DEFAULT_DATASET_ID = "qlib_st_pit_active_h5_daily_candidate_20180801_20260630_moneyflow_v2"
_DATASET_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,95}$")


def _dataset_id() -> str:
    value = os.getenv("QE_DATASET_CONTRACT_ID", _DEFAULT_DATASET_ID).strip().lower()
    if not _DATASET_ID_RE.fullmatch(value):
        raise RuntimeError(
            "QE_DATASET_CONTRACT_ID must match [a-z0-9][a-z0-9_.-]{0,95}; "
            f"got {value!r}"
        )
    return value


QE_DATASET_CONTRACT_ID = _dataset_id()
QE_DATASET_START_DATE = dt.date(2018, 8, 1)
QE_DATASET_SIGNAL_END_DATE = dt.date(2026, 6, 30)
QE_ST_PIT_UNIVERSE_KEY = f"shsz_st_pit_qe_dataset_{QE_DATASET_CONTRACT_ID}"


def require_qe_dataset_window(*, start_date: dt.date, end_date: dt.date) -> None:
    """Fail fast when a QE consumer escapes the deployed dataset contract."""

    if start_date < QE_DATASET_START_DATE or end_date > QE_DATASET_SIGNAL_END_DATE:
        raise ValueError(
            "QE request window is outside the deployed immutable dataset contract: "
            f"requested={start_date}..{end_date}, "
            f"dataset={QE_DATASET_START_DATE}..{QE_DATASET_SIGNAL_END_DATE}, "
            f"dataset_id={QE_DATASET_CONTRACT_ID}"
        )
    if end_date < start_date:
        raise ValueError(f"QE request end date {end_date} is earlier than start date {start_date}")
