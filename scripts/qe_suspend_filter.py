"""Shared QE suspension-filter helpers for Qlib strategy runtime.

The filter consumes a pre-generated local JSON artifact so daily strategy
selection never queries PostgreSQL during Qlib backtests.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
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
        self._execution_exclusions = []
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
        exclusions = payload.get("execution_data_exclusions", [])
        if not isinstance(exclusions, list):
            raise RuntimeError(f"suspend_filter_file has invalid execution_data_exclusions: {path}")
        normalized_exclusions = []
        for index, item in enumerate(exclusions):
            if not isinstance(item, dict):
                raise RuntimeError(
                    f"suspend_filter_file execution_data_exclusions[{index}] is invalid: {path}"
                )
            instrument = str(item.get("instrument") or "").strip().upper()
            start_date = str(item.get("start_date") or "")
            end_date = str(item.get("end_date") or "")
            evidence_sha256 = str(item.get("evidence_sha256") or "").strip().lower()
            if (
                item.get("schema_version") != "qe_execution_data_exclusion_v1"
                or item.get("scope") != "full_backtest_window"
                or item.get("reason_code") != "minute_source_gap_confirmed_unfillable"
                or not re.fullmatch(r"[0-9]{6}\.(SH|SZ)", instrument)
                or not start_date
                or not end_date
                or end_date < start_date
                or not re.fullmatch(r"[0-9a-f]{64}", evidence_sha256)
            ):
                raise RuntimeError(
                    f"suspend_filter_file execution_data_exclusions[{index}] is invalid: {path}"
                )
            normalized_exclusions.append(
                {
                    "aliases": self._expand_symbol_set([instrument]),
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )
        if exclusions:
            expected_contract_sha256 = str(
                payload.get("execution_data_exclusion_contract_sha256") or ""
            ).lower()
            actual_contract_sha256 = hashlib.sha256(
                json.dumps(
                    exclusions,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            if expected_contract_sha256 != actual_contract_sha256:
                raise RuntimeError(
                    "suspend_filter_file execution_data_exclusion contract hash mismatch: "
                    f"{path}"
                )
        self._execution_exclusions = normalized_exclusions
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
        """Return whether one symbol is blocked by suspend or run exclusion."""
        if not self.enabled:
            return False
        blocked = self.suspended_symbols(trade_date) | self.execution_excluded_symbols(trade_date)
        if not blocked:
            return False
        return bool(self._symbol_aliases(symbol) & blocked)

    def execution_excluded_symbols(self, trade_date) -> set[str]:
        """Return explicit run-scoped execution exclusions active on one date."""

        self._load()
        if not self.enabled:
            return set()
        key = self._date_key(trade_date)
        excluded: set[str] = set()
        for item in self._execution_exclusions:
            if item["start_date"] <= key <= item["end_date"]:
                excluded.update(item["aliases"])
        return excluded

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
        execution_excluded = self.execution_excluded_symbols(trade_date)
        blocked = suspended | execution_excluded
        if not blocked or base.empty:
            return base
        mask = pd.Series(
            [not (self._symbol_aliases(idx) & blocked) for idx in base.index],
            index=base.index,
        )
        excluded = int((~mask).sum())
        if excluded:
            self.logger.info(
                "[QESuspendFilter] trade_date=%s excluded=%d suspended=%d execution_data=%d",
                self._date_key(trade_date),
                excluded,
                len(suspended),
                len(execution_excluded),
            )
        return base.loc[mask]


def filter_scores_by_suspend_artifact(scores, trade_date, artifact_path, strict=True, logger_obj=None):
    return QESuspendFilter(True, artifact_path, strict, logger_obj).filter_scores(scores, trade_date)
