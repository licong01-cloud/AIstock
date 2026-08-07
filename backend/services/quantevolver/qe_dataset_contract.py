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

# Frozen qlib-bin universe pins for the deployed dataset (BUG-989 zero-DB data
# plane).  These values are computed over the frozen bin files at dataset
# deploy time: sha256 of instruments/all.txt, calendars/day.txt and
# meta_export.json plus the snapshot identity they must declare.  The QE
# computation data plane verifies these pins instead of querying market.*
# tables; a mismatch, like any missing frozen input, fails closed.  Deploying
# a new dataset means updating every constant in this module together.
QE_FROZEN_BIN_SNAPSHOT_ID = "qlib_bin_st_pit_active_daily_candidate_20180801_20260630"
QE_FROZEN_BIN_UNIVERSE_KEY = "shsz_st_pit_active_v1"
QE_FROZEN_INSTRUMENTS_SHA256 = "94c9d82de1ba60446d7d6114b39b1066fa3bda3f2a7b9787bb7f0ad4a2a05ca4"
QE_FROZEN_CALENDAR_SHA256 = "6ab71db126fd8c0173831162d5413691c33bfecbbc81db687d8a2de7cc776031"
QE_FROZEN_META_EXPORT_SHA256 = "66c5c070b368ec1352ce4031dce4be982d44894e58968bb3ead0cdc5b65eefb3"
# Universe fingerprint stamped onto QE factor caches: the frozen span file
# digest, not any market.stock_universe_pit_state row.
QE_FROZEN_UNIVERSE_FINGERPRINT_SHA256 = QE_FROZEN_INSTRUMENTS_SHA256

# Frozen suspend_d candidate dataset pins (BUG-989 continuation).  The suspend
# sidecar lives in a versioned candidate directory that is a *sibling* of the
# frozen qlib bin directory on every compute node (same layout on WSL and
# node1): ``<parent of provider_uri_day>/suspend_d_daily_candidate_20180801_20260630``.
# It was exported read-only from market.suspend_d (suspend_type='S', sh/sz
# only, BJ excluded) by scripts/export_suspend_d_candidate.py and carries a
# per-trading-day completeness receipt in manifest.json.  The QE computation
# data plane verifies these pins instead of querying market.suspend_d; a
# mismatch, like any missing frozen input, fails closed.
QE_FROZEN_SUSPEND_DATASET_ID = "suspend_d_daily_candidate_20180801_20260630"
QE_FROZEN_SUSPEND_PARQUET_SHA256 = "493f694312f514d39960aefc275c23ddc6bb60a6a2606cd57e39c428090cc33d"
QE_FROZEN_SUSPEND_MANIFEST_SHA256 = "eea71b92e9098c598db671e6c4bc4f0195516805cb657e3b85a978d60f047ff0"
QE_FROZEN_SUSPEND_SOURCE_CONTRACT = "tushare_suspend_d_shsz_S_v1"


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
