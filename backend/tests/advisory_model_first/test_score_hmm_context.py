from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.score_hmm_admission_contracts import (
    SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS,
    build_default_score_hmm_arms,
)
from backend.services.advisory_model_first.score_hmm_admission_pipeline import (
    _FiniteAbsoluteDeltaConvergenceMonitor,
    _build_parent_context_exposure,
    _causal_forward_filter,
    _contiguous_blocks,
    _fit_market_hmm_path,
    _validated_hmm_final_delta,
    _validate_market_pit_snapshot,
    build_raw_market_shape,
    freeze_score_hmm_market_pit_snapshot,
)
from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID
from backend.tests.advisory_model_first.test_score_hmm_admission_pipeline import build_test_request
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SCOPE,
    CANONICAL_PIT_UNIVERSE_KEY,
    canonical_rule_parameters_digest,
)
from backend.services.dataset_release.pit import freeze_pit_snapshot


def _raw_market_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DatetimeIndex]:
    calendar = pd.bdate_range("2024-01-02", periods=70)
    pit_symbols = [f"{index:06d}.SZ" for index in range(1, 102)]
    all_symbols = [*pit_symbols, "999999.SZ"]
    index = pd.MultiIndex.from_product([calendar, all_symbols], names=["datetime", "instrument"])
    day_number = np.repeat(np.arange(len(calendar), dtype=float), len(all_symbols))
    symbol_number = np.tile(np.arange(len(all_symbols), dtype=float), len(calendar))
    close = 10.0 + day_number * 0.01 + symbol_number * 0.0001
    market = pd.DataFrame(
        {
            "close": close,
            "prev_close": close / 1.001,
            "volume": 1_000.0,
            "amount": 10_000.0,
            "limit_up": 0.0,
        },
        index=index,
    )
    benchmark = pd.DataFrame(
        {"open": 100.0 + np.arange(len(calendar)), "close": 100.5 + np.arange(len(calendar))},
        index=pd.Index(calendar, name="datetime"),
    )
    suspend = pd.DataFrame(
        {"trade_date": [calendar[-1]], "instrument": [pit_symbols[-1]], "suspend_type": ["S"]}
    )
    spans = pd.DataFrame(
        {
            "ts_code": pit_symbols,
            "eligible_start": calendar[0].date(),
            "eligible_end": calendar[-1].date(),
            "entry_reason": None,
            "exit_reason": None,
        }
    )
    return market, benchmark, suspend, spans, calendar


def test_raw_market_uses_pit_members_and_excludes_verified_suspensions_from_denominator() -> None:
    market, benchmark, suspend, spans, calendar = _raw_market_inputs()
    result = build_raw_market_shape(
        market_daily=market,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        pit_snapshot=spans,
        calendar=calendar,
    )
    final = result.coverage.iloc[-1]
    assert final["valid_market_member_count"] == 100
    assert final["status"] == "AVAILABLE"
    assert result.features.iloc[-1]["market_up_ratio"] == 1.0
    assert result.pit_filter_receipt["removed_by_reason"]["no_span"] == len(calendar)


class _TwoStateEmissionModel:
    startprob_ = np.array([0.5, 0.5])
    transmat_ = np.array([[0.9, 0.1], [0.2, 0.8]])

    def _compute_log_likelihood(self, matrix: np.ndarray) -> np.ndarray:
        signal = matrix[:, 0]
        return np.column_stack((signal, -signal))


def test_causal_hmm_prefix_is_unchanged_by_future_poison_and_disjoint_blocks_are_explicit() -> None:
    model = _TwoStateEmissionModel()
    prefix = np.array([[0.2], [0.4], [-0.1], [0.8]])
    baseline = _causal_forward_filter(model, np.vstack((prefix, [[0.0], [0.0]])))
    poisoned = _causal_forward_filter(model, np.vstack((prefix, [[999.0], [-999.0]])))
    np.testing.assert_allclose(baseline[: len(prefix)], poisoned[: len(prefix)], rtol=0.0, atol=0.0)

    calendar = pd.bdate_range("2024-01-02", periods=10)
    dates = calendar[[0, 1, 4, 5, 9]]
    blocks = _contiguous_blocks(dates, calendar)
    assert [len(block) for block in blocks] == [2, 2, 1]
    assert _causal_forward_filter(model, np.array([[0.3]]))[0].sum() == 1.0


def test_outer_hmm_fit_resets_discontinuous_training_sequences(monkeypatch) -> None:
    observed: dict[str, object] = {}

    class _FakeGaussianHMM:
        def __init__(self, **_: object) -> None:
            self.tol = 1e-4
            self.startprob_ = np.array([0.5, 0.5])
            self.transmat_ = np.array([[0.9, 0.1], [0.1, 0.9]])
            self.means_ = np.vstack((np.zeros(8), np.ones(8)))
            self.covars_ = np.stack((np.eye(8), np.eye(8)))
            self.monitor_ = SimpleNamespace(converged=True, history=[-100.0, -99.99995])

        def fit(self, matrix: np.ndarray, *, lengths: list[int]):
            observed["row_count"] = len(matrix)
            observed["lengths"] = lengths
            return self

        def _compute_log_likelihood(self, matrix: np.ndarray) -> np.ndarray:
            return np.column_stack((matrix[:, 0], -matrix[:, 0]))

    hmm_package = ModuleType("hmmlearn")
    hmm_module = ModuleType("hmmlearn.hmm")
    hmm_module.GaussianHMM = _FakeGaussianHMM
    hmm_package.hmm = hmm_module
    monkeypatch.setitem(sys.modules, "hmmlearn", hmm_package)
    monkeypatch.setitem(sys.modules, "hmmlearn.hmm", hmm_module)

    calendar = pd.bdate_range("2024-01-02", periods=130)
    raw = pd.DataFrame(
        {
            name: np.sin(np.arange(len(calendar)) / (index + 2.0)) + np.arange(len(calendar)) / 100.0
            for index, name in enumerate(
                (
                    "csi300_ret_1",
                    "csi300_ret_5",
                    "csi300_ret_20",
                    "csi300_drawdown_20",
                    "csi300_drawdown_60",
                    "market_up_ratio",
                    "market_limit_up_ratio",
                    "market_cross_section_vol",
                )
            )
        },
        index=calendar,
    )
    train_dates = calendar[60:70].append(calendar[80:90])
    validation_dates = calendar[100:105]
    output, receipt = _fit_market_hmm_path(
        raw=raw,
        path={"path_id": "path-00", "train_dates": train_dates, "validation_dates": validation_dates},
        calendar=calendar,
        warmup_days=60,
    )

    assert observed == {"row_count": 20, "lengths": [10, 10]}
    assert receipt["train_block_lengths"] == [10, 10]
    assert receipt["convergence_rule"] == "FINITE_ABS_FINAL_LOG_LIKELIHOOD_DELTA_LT_TOL"
    assert not output.empty


def test_hmm_convergence_requires_finite_final_delta_inside_tolerance() -> None:
    converged = SimpleNamespace(tol=1e-4, monitor_=SimpleNamespace(history=[-100.0, -99.99995]))
    converged_after_tiny_regression = SimpleNamespace(
        tol=1e-4,
        monitor_=SimpleNamespace(history=[-100.0, -100.00005]),
    )
    exhausted = SimpleNamespace(tol=1e-4, monitor_=SimpleNamespace(history=[-100.0, -99.0]))

    assert _validated_hmm_final_delta(converged, path_id="path-00") == pytest.approx(0.00005)
    assert _validated_hmm_final_delta(converged_after_tiny_regression, path_id="path-00") == pytest.approx(-0.00005)
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _validated_hmm_final_delta(exhausted, path_id="path-00")
    assert caught.value.reason_code == "ADVISORY_SCORE_HMM_MARKET_HMM_INVALID"


def test_hmm_monitor_continues_after_negative_delta_outside_absolute_tolerance() -> None:
    delegate = SimpleNamespace(
        tol=1e-4,
        n_iter=200,
        iter=2,
        history=[-100.0, -100.000126259],
    )
    monitor = _FiniteAbsoluteDeltaConvergenceMonitor(delegate)

    assert monitor.converged is False

    delegate.iter += 1
    delegate.history.append(-100.00008)
    assert monitor.converged is True

    delegate.iter = delegate.n_iter
    delegate.history[-1] = -99.0
    assert monitor.converged is True


def test_sector_arms_are_typed_unavailable_without_blocking_three_executable_arms() -> None:
    arms = build_default_score_hmm_arms((84, 85, 86, 87, 88))
    assert [item.run_status for item in arms[:3]] == ["RUN", "RUN", "RUN"]
    assert tuple(item.arm_id for item in arms[3:]) == SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS
    assert {item.run_status for item in arms[3:]} == {"NOT_RUN_SOURCE_UNAVAILABLE"}


def test_parent_hmm_exposure_is_lineage_conservative_not_name_based() -> None:
    common = {
        "package_id": "pkg_test",
        "manifest_sha256": "1" * 64,
        "selection_runtime_semantics_hash": "2" * 64,
        "terminal_weights": {LSTM_LEG_ID: 0.7, FUND_LEG_ID: 0.3},
        "representative_model_asset_sha256": {LSTM_LEG_ID: "3" * 64, FUND_LEG_ID: "4" * 64},
    }
    explicit_absence = _build_parent_context_exposure(
        SimpleNamespace(selection_runtime_semantics={"hmm_enabled": False}, **common),
        request=build_test_request(),
    )
    unknown_lineage = _build_parent_context_exposure(
        SimpleNamespace(selection_runtime_semantics={"hmm_enabled": True}, **common),
        request=build_test_request(),
    )
    assert explicit_absence["market_hmm_attribution_status"] == "ATTRIBUTABLE_NO_EXPLICIT_PARENT_HMM_OUTPUT"
    assert unknown_lineage["market_hmm_attribution_status"] == "UNATTRIBUTABLE_UNKNOWN_PARENT_HMM_LINEAGE"
    assert unknown_lineage["name_only_duplicate_detection_used"] is False


def _pit_snapshot(
    *,
    scope_start: date,
    fingerprint: str = "a" * 64,
    eligible_end: date = date(2026, 6, 30),
):
    return freeze_pit_snapshot(
        [
            {
                "ts_code": "000001.SZ",
                "eligible_start": date(2023, 1, 1),
                "eligible_end": eligible_end,
                "entry_reason": "ipo_252td",
                "exit_reason": "generation_end",
            }
        ],
        universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        scope_start=scope_start,
        cutoff=date(2026, 3, 10),
        state_identity="state-v1",
        source_fingerprint_sha256=fingerprint,
        parameter_hash=canonical_rule_parameters_digest(),
    )


def test_market_warmup_pit_must_be_an_exact_wider_projection_of_n1() -> None:
    n1 = _pit_snapshot(scope_start=date(2024, 7, 4))
    market = _pit_snapshot(scope_start=date(2023, 9, 1))
    _validate_market_pit_snapshot(market_snapshot=market, n1_snapshot=n1)
    reason_only_lineage_drift = _pit_snapshot(scope_start=date(2023, 9, 1), fingerprint="b" * 64)
    _validate_market_pit_snapshot(market_snapshot=reason_only_lineage_drift, n1_snapshot=n1)
    drifted = _pit_snapshot(scope_start=date(2023, 9, 1), eligible_end=date(2026, 2, 27))
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _validate_market_pit_snapshot(market_snapshot=drifted, n1_snapshot=n1)
    assert caught.value.reason_code == "ADVISORY_SCORE_HMM_MARKET_PIT_NOT_READY"


def test_market_warmup_pit_freeze_is_read_only_repeatable_and_restart_safe(tmp_path: Path) -> None:
    statements: list[str] = []
    sessions: list[dict[str, object]] = []
    connection_options: list[dict[str, object]] = []

    class _Cursor:
        description: list[tuple[str]] = []

        def __enter__(self):
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def execute(self, sql: str, _: object) -> None:
            statements.append(" ".join(sql.split()))
            if "stock_universe_pit_state" in sql:
                self.description = [(name,) for name in (
                    "universe_key",
                    "rule_version",
                    "scope",
                    "start_date",
                    "end_date",
                    "status",
                    "dirty",
                    "source_fingerprint_sha256",
                    "generated_at",
                    "updated_at",
                )]
            else:
                self.description = [(name,) for name in (
                    "ts_code",
                    "eligible_start",
                    "eligible_end",
                    "entry_reason",
                    "exit_reason",
                )]

        def fetchone(self):
            return (
                CANONICAL_PIT_UNIVERSE_KEY,
                CANONICAL_PIT_RULE_VERSION,
                CANONICAL_PIT_SCOPE,
                date(2018, 1, 1),
                date(2026, 6, 30),
                "ready",
                False,
                "a" * 64,
                datetime(2026, 8, 30, tzinfo=timezone.utc),
                datetime(2026, 8, 30, tzinfo=timezone.utc),
            )

        def fetchall(self):
            return [("000001.SZ", date(2023, 1, 1), date(2026, 6, 30), "ipo_252td", "generation_end")]

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def set_session(self, **kwargs: object) -> None:
            sessions.append(kwargs)

        def cursor(self):
            return _Cursor()

    def _connection_factory(**kwargs: object) -> _Connection:
        connection_options.append(kwargs)
        return _Connection()

    output = tmp_path / "market-pit.json"
    first = freeze_score_hmm_market_pit_snapshot(output_path=output, connection_factory=_connection_factory)
    second = freeze_score_hmm_market_pit_snapshot(output_path=output, connection_factory=_connection_factory)

    assert first["status"] == "WRITTEN"
    assert second["status"] == "EXACT_NOOP"
    assert all(statement.upper().startswith("SELECT ") for statement in statements)
    assert connection_options == [
        {"autocommit": False, "manage_transaction": True},
        {"autocommit": False, "manage_transaction": True},
    ]
    assert sessions == [{"isolation_level": "REPEATABLE READ", "readonly": True}] * 2
    assert first["database_write"] is False
