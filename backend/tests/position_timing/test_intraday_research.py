"""Study population, immutable reuse and real Qlib conversion business checks."""
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.intraday.contracts import PriceConstraint, PriceInterval, payload
from backend.services.position_timing.intraday.inputs import file_sha256
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import freeze, inspect, prepare, run
from backend.services.position_timing.intraday.research_inputs import CandidateReader
from backend.services.position_timing.intraday.store import IntradayError, IntradayStore
from test_intraday_core import at, case


def study(tmp_path, *, with_price=True, days=1):
    price = PriceConstraint(mode="PREFERRED", intervals=(PriceInterval(lower="9.80", upper="10.20"),),
                            source_ref="frozen-before-replay", source_sha256="c" * 64,
                            valid_from=at("09:30:00"), valid_until=at("14:57:00")) if with_price else None
    plan, market = case(source="FROZEN_REPLAY", prices=("10",) * 120, price=price)
    afternoon = tuple(b.model_copy(update={"start": b.start + timedelta(minutes=210),
                                           "end": b.end + timedelta(minutes=210),
                                           "received_at": b.received_at + timedelta(minutes=210)}) for b in market.bars[:117])
    market = market.model_copy(update={"bars": (*market.bars, *afternoon)})
    plan = plan.model_copy(update={"target_quantity": 1000, "quantity_source_ref": "frozen-quantity"})
    plans, markets = [], []
    def shift(value, delta):
        if isinstance(value, dict):
            return {k: shift(v, delta) for k, v in value.items()}
        if isinstance(value, list):
            return [shift(v, delta) for v in value]
        if isinstance(value, str) and value.startswith('2026-10-22'):
            return (plan.target_trade_date + delta).isoformat() + value[10:]
        return value
    for index in range(days):
        item = shift(payload(plan), timedelta(days=index))
        item['plan_id'] += str(index)
        plans.append({'available_at': (at('09:00:00') + timedelta(days=index)).isoformat(), 'plan': item})
        markets.append(shift(payload(market), timedelta(days=index)))
    spec = {"input_kind": "FUNCTIONAL_FIXTURE", "source_commit": "1" * 40,
            "candidate_manifest": freeze(tmp_path / "candidate.json", {"schema_version": "functional_fixture_only"}),
            "plans": freeze(tmp_path / "plans.json", plans),
            "fixture_markets": freeze(tmp_path / "markets.json", markets),
            "cost_policy": freeze(tmp_path / "cost.json", {"open_rate": "0.000095", "close_rate": "0.000595", "minimum_cny": "5"})}
    return freeze(tmp_path / "spec.json", spec).path


def test_full_population_all_arms_delays_and_immutable_retry(tmp_path):
    result = run(prepare(study(tmp_path, days=2), tmp_path / "artifacts")["request"])
    verified = inspect(result["bundle"])
    retry = run(IntradayStore().read(Path(result["bundle"]) / "manifest.json")["request"])
    assert result["manifest_sha256"] == verified["manifest_sha256"] == retry["manifest_sha256"]
    assert retry["status"] == "ALREADY_MATERIALIZED"
    assert verified["receipt"]["case_count"] == verified["receipt"]["applicable_case_count"] == 24
    assert verified["receipt"]["child_count"] == 6
    assert verified["receipt"]["economic_evaluation"] == "NOT_RUN_LOCALSIM_ADAPTER_PENDING"
    assert not verified["receipt"]["outcomes_read"]


def test_no_price_preserves_inapplicable_population_and_source_drift_fails(tmp_path):
    result = run(prepare(study(tmp_path, with_price=False), tmp_path / "artifacts")["request"])
    assert result["receipt"]["case_count"] == 12 and result["receipt"]["applicable_case_count"] == 8
    (tmp_path / "cost.json").write_text('{}', encoding='utf-8')
    with pytest.raises(IntradayError, match="SOURCE_IDENTITY_MISMATCH"):
        inspect(result["bundle"])


def qlib_fixture(tmp_path, *, factor=2, upper=11, header=0):
    components = {}
    def record(relative, data):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        components[relative] = {"path": relative, "size": path.stat().st_size, "sha256": file_sha256(path)}
    daily = "components/daily_bin_candidate"
    minute = "components/minute_bin_candidate"
    record(daily + '/calendars/day.txt', b'2026-10-22\n')
    record(minute + '/calendars/1min.txt', b'2026-10-22 09:31:00\n')
    record(minute + '/instruments/all.txt', b'000001.SZ\t2026-10-22 09:31:00\t2026-10-22 15:00:00\n')
    for field, value in {'prev_close': 10, 'up_limit_price': upper, 'down_limit_price': 9}.items():
        record(f'{daily}/features/000001.sz/{field}.day.bin', np.array([0, value], dtype='<f4').tobytes())
    for field, value in {'open': 20, 'high': 20.2, 'low': 19.8, 'close': 20, 'factor': factor, 'volume': 500, 'amount': 10000}.items():
        record(f'{minute}/features/000001.sz/{field}.1min.bin', np.array([header, value], dtype='<f4').tobytes())
    path = tmp_path / 'components/suspend_d_daily_candidate_v2/suspend_d.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({'ts_code': pd.Series(dtype=str), 'trade_date': pd.Series(dtype='datetime64[ns]'),
                  'suspend_type': pd.Series(dtype=str), 'suspend_timing': pd.Series(dtype=str)}).to_parquet(path)
    components[path.relative_to(tmp_path).as_posix()] = {'path': path.relative_to(tmp_path).as_posix(), 'size': path.stat().st_size, 'sha256': file_sha256(path)}
    manifest = tmp_path / 'qe_dataset_manifest.json'
    manifest.write_text(json.dumps({'schema_version': 'qe_dataset_manifest_v1', 'components': components}), encoding='utf-8')
    return FileRef(path=str(manifest), sha256=file_sha256(manifest))


def test_qlib_raw_price_volume_and_producer_hash_are_authoritative(tmp_path):
    manifest = qlib_fixture(tmp_path)
    market = CandidateReader(manifest).market('000001.SZ', at().date())
    assert market.bars[0].close == Decimal('10') and market.bars[0].volume_shares == Decimal('1000')
    assert market.day_facts.limit_up == Decimal('11') and market.day_facts.is_suspended is False
    path = tmp_path / 'components/minute_bin_candidate/features/000001.sz/factor.1min.bin'
    path.write_bytes(np.array([0, 3], dtype='<f4').tobytes())
    with pytest.raises(ValueError, match='differs'):
        CandidateReader(manifest).market('000001.SZ', at().date())


@pytest.mark.parametrize('kwargs,reason', [({'factor': 0}, 'invalid file adjustment factor'),
                                         ({'upper': 8}, 'LIMIT_ORDER_INVALID'),
                                         ({'header': 1}, 'BIN_OUTSIDE_CALENDAR')])
def test_invalid_market_economics_fail_before_replay(tmp_path, kwargs, reason):
    with pytest.raises((ValueError, IntradayError), match=reason):
        CandidateReader(qlib_fixture(tmp_path, **kwargs)).market('000001.SZ', at().date())
