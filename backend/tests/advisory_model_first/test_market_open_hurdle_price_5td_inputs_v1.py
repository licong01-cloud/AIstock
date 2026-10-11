"""Market projection/clock contracts. All source prices are synthetic."""
from contextlib import contextmanager
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_original_slot_hurdle_price_5td_inputs_v1 import source_case
from backend.services.advisory_model_first import market_open_hurdle_price_5td_inputs_v1 as inputs
from backend.services.advisory_model_first.market_open_hurdle_price_5td_contracts_v1 import (
    KEY, MARKET_COLUMN, MarketOpenHurdleStudyPlanV1,
)


class MarketSession:
    def __init__(self, *, corruption=None):
        self.calls, self.corruption, self.closed = [], corruption, False

    @contextmanager
    def connection(self):
        yield self

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params):
        self.calls.append((sql, params))
        field = "open" if "SELECT trade_date, open " in sql else "close"
        self.description = [SimpleNamespace(name=n) for n in ("trade_date", field)]
        values = [(d, 4000. if field == "close" else 4000*(1+(-120+(d.toordinal()%9)*40)/10000)) for d in params[1]]
        if self.corruption == "missing" and field == "open":
            values = values[1:]
        elif self.corruption == "duplicate":
            values.append(values[0])
        elif self.corruption == "invalid":
            values[0] = (values[0][0], -1.)
        elif self.corruption == "foreign":
            values[0] = (pd.Timestamp("2026-02-02").date(), values[0][1])
        self.values = values

    def fetchall(self):
        return self.values

    def close(self):
        self.closed = True


@pytest.fixture(scope="session")
def prepared_study(tmp_path_factory):
    old, _, original = source_case(tmp_path_factory.mktemp("market_source"))
    from backend.services.advisory_model_first.market_open_hurdle_price_5td_pipeline_v1 import implementation_sha256
    declared = old.model_dump()
    declared.update(schema_version="market_open_hurdle_price_5td_study_v1", previous_physical_fit_count=142,
        implementation_sha256=implementation_sha256(), temporary_root_uri="X:/AIstock_temp/advisory_market_open_hurdle_20261011/unit_temp")
    plan = MarketOpenHurdleStudyPlanV1.model_validate(declared)
    p = inputs.prepare_hurdle_inputs_v1(plan=plan, session_factory=MarketSession)
    return plan, p, original


def test_exact_field_clock_projection_no_future_market_decode(prepared_study):
    plan, p, _ = prepared_study
    session = MarketSession()
    dates = p["encoding"]["clocks"]["train_dates"][:3]
    frame, receipt = inputs.read_market_open_v1(plan=plan, dates=dates, calendar=p["calendar"], session_factory=lambda: session)
    assert session.closed and len(session.calls) == 2 and len(frame) == 3
    for field, (sql, params) in zip(("close", "open"), session.calls, strict=True):
        assert sql.startswith("SELECT trade_date, "+field+" FROM") and "*" not in sql
        assert params[0] == "000300.SH" and len(params[1]) == 3 and params[2] == 4
    assert session.calls[0][1][1] == list(pd.to_datetime(frame[KEY[0]]).dt.date)
    assert session.calls[1][1][1] == list(pd.to_datetime(frame[KEY[1]]).dt.date)
    assert receipt["database_written"] is False and receipt["native_identity"] == "UNPROVEN"
    with pytest.raises(ValueError, match="sealed"):
        inputs.read_market_open_v1(plan=plan, dates=["2026-02-02"], calendar=p["calendar"], session_factory=MarketSession)


@pytest.mark.parametrize("corruption", ["duplicate", "invalid", "foreign"])
def test_market_error_never_becomes_unknown_or_retry(prepared_study, corruption):
    plan, p, _ = prepared_study
    session = MarketSession(corruption=corruption)
    with pytest.raises(ValueError):
        inputs.read_market_open_v1(plan=plan, dates=p["encoding"]["clocks"]["train_dates"][:3],
            calendar=p["calendar"], session_factory=lambda: session)
    assert session.closed and len(session.calls) == 1


def test_missing_market_keeps_slots_and_common_supervision(prepared_study):
    plan, p, _ = prepared_study
    missing = inputs.prepare_hurdle_inputs_v1(plan=plan, session_factory=lambda: MarketSession(corruption="missing"))
    assert len(missing["roster"]) == len(p["roster"]) and len(missing["domain"]) == len(p["domain"])
    assert missing["domain"][MARKET_COLUMN].isna().sum() == 10
    from backend.services.advisory_model_first.market_open_hurdle_price_5td_model_v1 import supervision_v1
    selected, _, weights = supervision_v1(missing["domain"], missing["encoding"])
    assert selected[MARKET_COLUMN].notna().all() and selected.groupby("label_cluster").cluster_mass.sum().eq(1).all()
    assert np.sum(weights) == selected.label_cluster.nunique()


def test_market_encoding_uses_train_input_not_label_or_test(prepared_study):
    _, p, _ = prepared_study
    from backend.services.advisory_model_first.market_open_hurdle_price_5td_model_v1 import fit_encoding_v1
    changed = p["domain"].copy()
    changed["net_return_bps"] = np.inf
    e = fit_encoding_v1(changed, p["encoding"]["core"]["intervals_bps"])
    for field in ("market_mean", "market_scale", "market_buckets", "market_min_bps", "market_max_bps"):
        assert e[field] == p["encoding"][field]
    assert p["market_train"][KEY[1]].max() <= pd.Timestamp(p["encoding"]["clocks"]["train_end"])
