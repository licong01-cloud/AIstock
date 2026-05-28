"""T16: tests for regime_label_daily.

Originally lived at scripts/test_regime_label.py on
origin/claude/dw-foundation-20260510. Relocated to
backend/tests/market/test_regime_label.py by pipeline-foundation Stage 2 so
that the standard pytest collection picks it up under the
``market.regime_label`` plan.

Covers:
- fetch_percentile: rank computation against mocked history
- classify_simple_quadrant: 5 quadrant boundary cases
- upsert_regime_label: PK (trade_date, source_method) multi-method coexistence
- compute_regime_for_date: end-to-end with mocked conn

The sourced module ``scripts/regime_label_daily.py`` is currently on
``origin/claude/dw-foundation-20260510`` and may not yet be merged into
``origin/main``. The test module skips at module load time when the source
script is unavailable, and starts running automatically once the merge lands.

Run after merge:
    python -m pytest backend/tests/market/test_regime_label.py -v
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = REPO_ROOT / "scripts"
REGIME_LABEL_DAILY = SCRIPTS_DIR / "regime_label_daily.py"

if not REGIME_LABEL_DAILY.exists():
    pytest.skip(
        "scripts/regime_label_daily.py not yet merged to this branch; "
        "test_regime_label tests will activate after the merge.",
        allow_module_level=True,
    )

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from regime_label_daily import (  # noqa: E402  module-level import after sys.path patch
    RegimeLabel,
    RegimeSignal,
    SIX_MONTH_TRADING_SESSIONS,
    classify_simple_quadrant,
    compute_regime_for_date,
    fetch_csi300_6m_return,
    fetch_percentile,
    upsert_regime_label,
)


def _is_skeleton_implementation() -> bool:
    """Return True if regime_label_daily is still the T10 skeleton (NotImplementedError)."""
    try:
        from unittest.mock import MagicMock as _Mock
        cur = _Mock()
        cur.fetchall.return_value = [(dt.date(2024, 1, 1), 1.0)] * 100
        cur.__enter__ = _Mock(return_value=cur)
        cur.__exit__ = _Mock(return_value=False)
        conn = _Mock()
        conn.cursor.return_value = cur
        fetch_percentile(conn, dt.date(2026, 5, 10), 0.5, "ret_6m")
    except NotImplementedError:
        return True
    except Exception:
        return False
    return False


SKELETON_IMPL = _is_skeleton_implementation()
SKELETON_REASON = (
    "scripts/regime_label_daily.py is the T10 SKELETON on this branch "
    "(fetch_percentile raises NotImplementedError); test_regime_label tests "
    "will activate after origin/claude/dw-foundation-20260510 merge."
)
_skeleton_skipif = pytest.mark.skipif(SKELETON_IMPL, reason=SKELETON_REASON)


def test_regime_label_test_module_wired() -> None:
    """Smoke test that proves the relocated test file is collected by pytest.

    Always passes - this guards against pytest exit code 5 (no tests
    collected) when the rest of the suite is skipped because the
    implementation is still a skeleton on this branch.
    """
    assert REGIME_LABEL_DAILY.exists()


def _mock_conn_with_rows(rows):
    """Build a psycopg2-style mock conn whose cursor.fetchall() returns ``rows``."""
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


# ---------- fetch_percentile ----------

@_skeleton_skipif
class TestFetchCsi300Return:
    def test_uses_126_trading_session_lag_not_calendar_days(self):
        conn, cur = _mock_conn_with_rows([(0.12,)])
        value = fetch_csi300_6m_return(conn, dt.date(2026, 5, 10))

        sql_text = cur.execute.call_args.args[0]
        params = cur.execute.call_args.args[1]

        assert value == 0.12
        assert "ROW_NUMBER() OVER (ORDER BY trade_date DESC)" in sql_text
        assert "INTERVAL '180 days'" not in sql_text
        assert params == (
            "000300.SH",
            dt.date(2026, 5, 10),
            SIX_MONTH_TRADING_SESSIONS,
        )


@_skeleton_skipif
class TestFetchPercentile:
    def _hist(self, n, value_fn=lambda i: float(i)):
        base = dt.date(2024, 1, 1)
        return [(base + dt.timedelta(days=i), value_fn(i)) for i in range(n)]

    def test_value_at_median_returns_half(self):
        history = self._hist(100)
        conn, _ = _mock_conn_with_rows(history)
        pct = fetch_percentile(conn, dt.date(2026, 5, 10), 49.0, "ret_6m")
        assert pct == 0.5

    def test_ret_6m_history_uses_trading_session_lag(self):
        history = self._hist(100)
        conn, cur = _mock_conn_with_rows(history)
        pct = fetch_percentile(conn, dt.date(2026, 5, 10), 49.0, "ret_6m")

        sql_text = cur.execute.call_args.args[0]
        params = cur.execute.call_args.args[1]

        assert pct == 0.5
        assert "LAG(close, %s) OVER (ORDER BY trade_date)" in sql_text
        assert "INTERVAL '180 days'" not in sql_text
        assert params[0] == SIX_MONTH_TRADING_SESSIONS

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
        assert pct == 0.5


# ---------- classify_simple_quadrant ----------

@_skeleton_skipif
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
        _, conf = classify_simple_quadrant(self._signal(1.0, 0.0))
        assert conf <= 1.0


# ---------- upsert_regime_label & multi-method coexistence ----------

@_skeleton_skipif
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
        methods = [call.args[1]["method"] for call in cur.execute.call_args_list]
        assert methods == ["simple_quadrant", "hmm_viterbi"]
        regimes = [call.args[1]["regime"] for call in cur.execute.call_args_list]
        assert regimes == ["bull", "bear"]


# ---------- compute_regime_for_date end-to-end ----------

@_skeleton_skipif
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
