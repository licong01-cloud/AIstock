"""Shared QE suspension-filter helpers for Qlib strategy runtime.

The filter consumes a pre-generated local JSON artifact so daily strategy
selection never queries PostgreSQL during Qlib backtests.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

logger = logging.getLogger(__name__)


class QESuspendFilter:
    """Filter ranked signal scores using a local suspend_d artifact."""

    def __init__(self, enabled=False, suspend_filter_file=None, strict=True, logger_obj=None):
        self.enabled = bool(enabled)
        self.suspend_filter_file = suspend_filter_file
        self.strict = bool(strict)
        self.logger = logger_obj or logger
        self._loaded = False
        self._by_date = {}
        self._metadata = {}

    def _load(self):
        if self._loaded:
            return
        if not self.enabled:
            self._loaded = True
            return
        if not self.suspend_filter_file:
            raise RuntimeError("filter_suspended_on_signal=True requires suspend_filter_file")
        path = Path(str(self.suspend_filter_file))
        if not path.exists():
            raise RuntimeError(f"suspend_filter_file does not exist: {path}")
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not payload.get("enabled"):
            raise RuntimeError(f"suspend_filter_file is not enabled: {path}")
        by_date = payload.get("suspended_by_date")
        if not isinstance(by_date, dict):
            raise RuntimeError(f"suspend_filter_file missing suspended_by_date: {path}")
        self._by_date = {
            str(k): self._expand_symbol_set(v or [])
            for k, v in by_date.items()
        }
        self._metadata = payload
        self._loaded = True

    @staticmethod
    def _date_key(trade_date) -> str:
        return str(pd.Timestamp(trade_date).date())

    @staticmethod
    def _symbol_aliases(symbol) -> set[str]:
        """Return common Tushare and Qlib aliases for one A-share symbol."""
        raw = str(symbol).strip().upper()
        aliases = {raw}
        if "." in raw:
            code, exch = raw.split(".", 1)
            exch = {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}.get(exch, exch)
            aliases.add(f"{code}.{exch}")
            aliases.add(f"{exch}{code}")
        elif len(raw) >= 8 and raw[:2] in {"SH", "SZ", "BJ"} and raw[2:].isdigit():
            aliases.add(f"{raw[2:]}.{raw[:2]}")
        return aliases

    @classmethod
    def _expand_symbol_set(cls, symbols: Iterable[str]) -> set[str]:
        expanded: set[str] = set()
        for symbol in symbols:
            expanded.update(cls._symbol_aliases(symbol))
        return expanded

    def suspended_symbols(self, trade_date) -> set[str]:
        self._load()
        if not self.enabled:
            return set()
        key = self._date_key(trade_date)
        if key not in self._by_date:
            if self.strict:
                raise RuntimeError(
                    "suspend filter artifact has no entry for trade date "
                    f"{key}; regenerate QE config after refreshing suspend_d audit"
                )
            self.logger.warning("[QESuspendFilter] missing date %s; no suspension filter applied", key)
            return set()
        return set(self._by_date.get(key) or set())

    def is_suspended(self, symbol, trade_date) -> bool:
        """Return whether one symbol is suspended on the given PIT trade date."""
        if not self.enabled:
            return False
        suspended = self.suspended_symbols(trade_date)
        if not suspended:
            return False
        return bool(self._symbol_aliases(symbol) & suspended)

    def filter_scores(self, scores, trade_date):
        """Return scores with suspended instruments removed.

        ``scores`` must be a pandas Series indexed by instrument. A DataFrame is
        accepted only when it has a ``score`` column, matching QE strategy input.
        """
        if not self.enabled:
            return scores
        if scores is None:
            return scores
        if isinstance(scores, pd.DataFrame):
            if "score" not in scores.columns:
                raise RuntimeError("suspend filter received DataFrame without 'score' column")
            base = scores["score"]
        else:
            base = scores
        if not isinstance(base, pd.Series):
            raise RuntimeError(f"suspend filter expected pandas Series, got {type(base).__name__}")
        suspended = self.suspended_symbols(trade_date)
        if not suspended or base.empty:
            return base
        mask = pd.Series(
            [not (self._symbol_aliases(idx) & suspended) for idx in base.index],
            index=base.index,
        )
        excluded = int((~mask).sum())
        if excluded:
            self.logger.info(
                "[QESuspendFilter] trade_date=%s excluded=%d suspended_by_suspend_d",
                self._date_key(trade_date), excluded,
            )
        return base.loc[mask]


def filter_scores_by_suspend_artifact(scores, trade_date, artifact_path, strict=True, logger_obj=None):
    return QESuspendFilter(True, artifact_path, strict, logger_obj).filter_scores(scores, trade_date)
