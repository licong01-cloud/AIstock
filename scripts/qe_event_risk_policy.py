"""QE event-risk policy helpers for Qlib strategy runtime.

The helper consumes a frozen local JSON artifact. Qlib backtests must not
query PostgreSQL while generating trade decisions, and they must fail fast if
the artifact does not cover the requested trade date.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

logger = logging.getLogger(__name__)


class QEEventRiskPolicy:
    """Evaluate PIT buy eligibility and forced exits from a local artifact."""

    def __init__(self, enabled=False, risk_policy_file=None, strict=True, logger_obj=None):
        self.enabled = bool(enabled)
        self.risk_policy_file = risk_policy_file
        self.strict = bool(strict)
        self.logger = logger_obj or logger
        self._loaded = False
        self._payload = {}
        self._spans_by_alias = {}
        self._active_cache = {}

    def _load(self):
        if self._loaded:
            return
        if not self.enabled:
            self._loaded = True
            return
        if not self.risk_policy_file:
            raise RuntimeError("risk_policy_enabled=True requires risk_policy_file")
        path = Path(str(self.risk_policy_file))
        if not path.exists():
            raise RuntimeError(f"risk_policy_file does not exist: {path}")
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not payload.get("enabled"):
            raise RuntimeError(f"risk_policy_file is not enabled: {path}")
        if payload.get("contract") != "stock_event_risk_policy_v1":
            raise RuntimeError(
                "risk_policy_file has unsupported contract: "
                f"{payload.get('contract')!r}"
            )
        providers = set(payload.get("providers") or [])
        if "announcement_risk" in providers:
            raise RuntimeError("announcement_risk provider is not implemented for QE runtime yet")
        if "st_pit" not in providers:
            raise RuntimeError(f"risk_policy_file providers must include st_pit: {sorted(providers)}")
        spans = payload.get("active_spans")
        if not isinstance(spans, list):
            raise RuntimeError(f"risk_policy_file missing active_spans: {path}")
        spans_by_alias = {}
        for span in spans:
            ts_code = str(span.get("ts_code") or "").strip().upper()
            start = span.get("eligible_start")
            end = span.get("eligible_end")
            if not ts_code or not start or not end:
                raise RuntimeError(f"risk_policy_file has invalid span row for {path}: {span!r}")
            normalized_span = (pd.Timestamp(start).date(), pd.Timestamp(end).date(), ts_code)
            for alias in self._symbol_aliases(ts_code):
                spans_by_alias.setdefault(alias, []).append(normalized_span)
        self._payload = payload
        self._spans_by_alias = spans_by_alias
        self._loaded = True

    @staticmethod
    def _date_key(trade_date) -> str:
        return str(pd.Timestamp(trade_date).date())

    @staticmethod
    def _symbol_aliases(symbol) -> set[str]:
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

    def _ensure_date_covered(self, trade_date) -> None:
        if not self.enabled:
            return
        current = pd.Timestamp(trade_date).date()
        start = pd.Timestamp(self._payload.get("start_date")).date()
        end = pd.Timestamp(self._payload.get("end_date")).date()
        if start <= current <= end:
            return
        message = (
            "risk policy artifact does not cover trade date "
            f"{current}; covered range is {start}..{end}"
        )
        if self.strict:
            raise RuntimeError(message)
        self.logger.warning("[QEEventRiskPolicy] %s", message)

    def is_buy_allowed(self, symbol, trade_date) -> bool:
        self._load()
        if not self.enabled:
            return True
        self._ensure_date_covered(trade_date)
        current = pd.Timestamp(trade_date).date()
        for alias in self._symbol_aliases(symbol):
            for start, end, _ts_code in self._spans_by_alias.get(alias, []):
                if start <= current <= end:
                    return True
        return False

    def blocked_symbols(self, symbols: Iterable[str], trade_date) -> set[str]:
        self._load()
        if not self.enabled:
            return set()
        blocked = set()
        for symbol in symbols:
            if not self.is_buy_allowed(symbol, trade_date):
                blocked.add(str(symbol))
        return blocked

    def force_exit_symbols(self, symbols: Iterable[str], trade_date) -> set[str]:
        self._load()
        if not self.enabled:
            return set()
        hard_actions = set(self._payload.get("hard_actions") or [])
        if "force_exit" not in hard_actions:
            return set()
        return self.blocked_symbols(symbols, trade_date)

    def filter_scores(self, scores, trade_date):
        """Return scores with hard-risk buy-blocked instruments removed."""
        if not self.enabled:
            return scores
        if scores is None:
            return scores
        if isinstance(scores, pd.DataFrame):
            if "score" not in scores.columns:
                raise RuntimeError("risk policy received DataFrame without 'score' column")
            base = scores["score"]
        else:
            base = scores
        if not isinstance(base, pd.Series):
            raise RuntimeError(f"risk policy expected pandas Series, got {type(base).__name__}")
        self._load()
        hard_actions = set(self._payload.get("hard_actions") or [])
        if "block_buy" not in hard_actions or base.empty:
            return base
        blocked = self.blocked_symbols(base.index, trade_date)
        if not blocked:
            return base
        mask = pd.Series([str(idx) not in blocked for idx in base.index], index=base.index)
        excluded = int((~mask).sum())
        if excluded:
            self.logger.info(
                "[QEEventRiskPolicy] trade_date=%s excluded=%d risk_policy_block_buy",
                self._date_key(trade_date),
                excluded,
            )
        return base.loc[mask]
