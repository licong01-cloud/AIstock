"""T16: tests for regime_label_daily.

Covers:
- fetch_percentile: rank computation against mocked history
- classify_simple_quadrant: 5 quadrant boundary cases
- upsert_regime_label: PK (trade_date, source_method) multi-method coexistence
- compute_regime_for_date: end-to-end with mocked conn

Run:
    cd /f/Dev/AIstock-worktrees/dw-foundation-20260510
    .venv/Scripts/python -m pytest scripts/test_regime_label.py -v
"""

from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock

import pytest

from regime_label_daily import (
    RegimeLabel,
    RegimeSignal,
    classify_simple_quadrant,
    compute_regime_for_date,
    fetch_percentile,
    upsert_regime_label,
)


def _mock_conn_with_rows(rows):
    """Build a psycopg2-style mock conn whose cursor.fetchall() returns `rows`."""
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


# ---------- fetch_percentile ----------

class TestFetchPercentile:
    def _hist(self, n, value_fn=lambda i: float(i)):
        base = dt.date(2024, 1, 1)
        return [(base + dt.timedelta(days=i), value_fn(i)) for i in range(n)]

    def test_value_at_median_returns_half(self):
        history = self._hist(100)
        conn, _ = _mock_conn_with_rows(history)
        pct = fetch_percentile(conn, dt.date(2026, 5, 10), 49.0, "ret_6m")
        # values <= 49.0 are 0..49 -> 50 of 100
        assert pct == 0.5

    def test_value_above_all_history_returns_one(self):
        history = self._hist(100)
        conn, _ = _mock_conn_with_rows(history)
        pct = fetch_percentile(conn, dt.date(2026, 5, 10), 999.0, "vol_60d")
        assert pct == 1.0

    def test_value_below_all_history_returns_zero(self):
        history = self._hist(100, lambda i: float(i + 1))
        conn, _ = _mock_conn_with_rows(history)
        pct = fetch_percentile(conn, dt.date(2026, 5, 10), -1.0, "ret_6m")
        assert pct == 0.0

    def test_insufficient_history_returns_none(self):
        history = self._hist(30)
        conn, _ = _mock_conn_with_rows(history)
        pct = fetch_percentile(conn, dt.date(2026, 5, 10), 5.0, "ret_6m")
        assert pct is None

    def test_unknown_signal_raises(self):
        conn, _ = _mock_conn_with_rows([])
        with pytest.raises(ValueError, match="unknown signal"):
            fetch_percentile(conn, dt.date(2026, 5, 10), 0.0, "garbage")

    def test_drops_null_history_rows(self):
        base = dt.date(2024, 1, 1)
        history = (
            [(base + dt.timedelta(days=i), None) for i in range(50)]
            + [(base + dt.timedelta(days=50 + i), float(i)) for i in range(80)]
        )
        conn, _ = _mock_conn_with_rows(history)
        pct = fetch_percentile(conn, dt.date(2026, 5, 10), 39.0, "vol_60d")
        # 80 non-null observations, 40 of them <= 39.0
        assert pct == 0.5


# ---------- classify_simple_quadrant ----------

class TestClassifySimpleQuadrant:
    def _signal(self, ret_pct, vol_pct):
        return RegimeSignal(
            trade_date=dt.date(2026, 5, 10),
            csi300_6m_ret=0.0,
            csi300_60d_vol=0.0,
            ret_pct_5y=ret_pct,
            vol_pct_5y=vol_pct,
        )

    def test_bull_high_ret_low_vol(self):
        regime, conf = classify_simple_quadrant(self._signal(0.8, 0.2))
        assert regime == "bull"
        assert 0.0 < conf <= 1.0

    def test_bear_low_ret_high_vol(self):
        regime, _ = classify_simple_quadrant(self._signal(0.2, 0.8))
        assert regime == "bear"

    def test_high_vol_only(self):
        regime, _ = classify_simple_quadrant(self._signal(0.5, 0.7))
        assert regime == "high_vol"

    def test_low_vol_only(self):
        regime, _ = classify_simple_quadrant(self._signal(0.5, 0.3))
        assert regime == "low_vol"

    def test_oscillation_centre(self):
        regime, conf = classify_simple_quadrant(self._signal(0.5, 0.5))
        assert regime == "oscillation"
        assert conf == 0.0

    def test_missing_percentiles_yields_oscillation_zero_conf(self):
        regime, conf = classify_simple_quadrant(self._signal(None, 0.5))
        assert regime == "oscillation"
        assert conf == 0.0

    def test_confidence_clamped_to_one(self):
        # extreme corner -> distance > 0.5 / 0.5 == 1.0 cap
        _, conf = classify_simple_quadrant(self._signal(1.0, 0.0))
        assert conf <= 1.0


# ---------- upsert_regime_label & multi-method coexistence ----------

class TestUpsertMultiMethod:
    def _make_label(self, trade_date, method, regime="bull"):
        sig = RegimeSignal(
            trade_date=trade_date,
            csi300_6m_ret=0.10,
            csi300_60d_vol=0.15,
            ret_pct_5y=0.7,
            vol_pct_5y=0.3,
        )
        return RegimeLabel(
            trade_date=trade_date,
            regime=regime,
            confidence=0.5,
            source_method=method,
            source_signal=sig,
        )

    def test_upsert_uses_on_conflict_pk(self):
        conn, cur = _mock_conn_with_rows([])
        label = self._make_label(dt.date(2026, 5, 10), "simple_quadrant")
        upsert_regime_label(conn, label)
        sql_text = cur.execute.call_args.args[0]
        assert "ON CONFLICT (trade_date, source_method)" in sql_text
        assert "DO UPDATE" in sql_text
        conn.commit.assert_called_once()

    def test_two_methods_same_date_emit_distinct_writes(self):
        conn, cur = _mock_conn_with_rows([])
        d = dt.date(2026, 5, 10)
        upsert_regime_label(conn, self._make_label(d, "simple_quadrant", "bull"))
        upsert_regime_label(conn, self._make_label(d, "hmm_viterbi", "bear"))
        # Both writes happened with distinct method params
        methods = [call.args[1]["method"] for call in cur.execute.call_args_list]
        assert methods == ["simple_quadrant", "hmm_viterbi"]
        regimes = [call.args[1]["regime"] for call in cur.execute.call_args_list]
        assert regimes == ["bull", "bear"]


# ---------- compute_regime_for_date end-to-end ----------

class TestComputeRegimeForDate:
    def test_unsupported_method_raises(self):
        conn = MagicMock()
        with pytest.raises(NotImplementedError):
            compute_regime_for_date(conn, dt.date(2026, 5, 10), method="hmm_viterbi")

    def test_missing_csi300_data_raises(self, monkeypatch):
        import regime_label_daily as mod

        monkeypatch.setattr(mod, "fetch_csi300_6m_return", lambda c, d: None)
        monkeypatch.setattr(mod, "fetch_csi300_60d_volatility", lambda c, d: 0.2)
        with pytest.raises(ValueError, match="missing CSI300 data"):
            compute_regime_for_date(MagicMock(), dt.date(2026, 5, 10))

    def test_full_path_bull(self, monkeypatch):
        import regime_label_daily as mod

        monkeypatch.setattr(mod, "fetch_csi300_6m_return", lambda c, d: 0.20)
        monkeypatch.setattr(mod, "fetch_csi300_60d_volatility", lambda c, d: 0.10)
        monkeypatch.setattr(
            mod,
            "fetch_percentile",
            lambda c, d, v, sig: 0.85 if sig == "ret_6m" else 0.15,
        )
        label = compute_regime_for_date(MagicMock(), dt.date(2026, 5, 10))
        assert label.regime == "bull"
        assert label.source_method == "simple_quadrant"
        assert label.source_signal.csi300_6m_ret == 0.20
        assert label.source_signal.ret_pct_5y == 0.85
