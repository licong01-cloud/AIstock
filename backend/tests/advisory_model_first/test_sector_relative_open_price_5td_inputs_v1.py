"""PIT, field projection and complete peer-population contracts, not earnings proof."""
import hashlib
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.sector_relative_open_price_5td_contracts_v1 import KEY, sha
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.sector_relative_open_price_5td_inputs_v1 import (
    SectorOpenFileProjectionV1, build_peer_panel_v1, build_sector_context_inputs_v1, measure_sector_finance_v1,
)


def file_binding(root):
    root.mkdir()
    calendar = pd.bdate_range("2024-06-03", periods=45)
    symbols = [f"{600000+i}.SH" for i in range(8)]
    frame = pd.DataFrame(index=pd.MultiIndex.from_product((calendar, symbols), names=["datetime", "instrument"]))
    for name, ratio in (("open", 1.01), ("high", 1.05), ("low", .95), ("close", 1.)):
        frame[name] = (np.repeat(10+np.arange(len(calendar))*.01, len(symbols))*ratio).round(3)
    frame["volume"], frame["factor"] = 10000., 1.
    frame.to_hdf(root/"daily.h5", key="data", format="table", data_columns=True)
    index = pd.DataFrame(dict(trade_date=calendar.strftime("%Y-%m-%d"), ts_code="000300.SH",
        open=3001., close=3000., high=float("inf"), low=float("nan"), volume=1., amount=1.))
    index.to_hdf(root/"index.h5", key="data", format="fixed")
    (root/"calendar.txt").write_text("\n".join(d.date().isoformat() for d in calendar), encoding="utf-8")
    (root/"pool.txt").write_text("\n".join(f"{s} {calendar[0].date()} {calendar[-1].date()}" for s in symbols), encoding="utf-8")
    (root/"meta.json").write_text('{"schema_version":"qe_direct_factor_h5_static_v2"}', encoding="utf-8")
    (root/"day_meta.json").write_text("{}", encoding="utf-8")
    for symbol in symbols:
        folder = root/"features"/symbol.lower()
        folder.mkdir(parents=True)
        for name, value in (("up_limit_price", 100.), ("down_limit_price", 1.)):
            (folder/(name+".day.bin")).write_bytes(np.asarray([0., *([value]*len(calendar))], dtype="<f4").tobytes())
    pd.DataFrame(dict(instrument=symbols, start_date=calendar[0], end_date=calendar[-1], l2_code_id=1)).to_parquet(root/"members.parquet")
    (root/"codes.json").write_text(json.dumps(dict(entries=[dict(l2_code_id=1, canonical_l2_code="test")])), encoding="utf-8")
    pd.DataFrame(columns=["ts_code", "trade_date", "suspend_type", "suspend_timing"]).to_parquet(root/"suspend.parquet")
    mapping = dict(daily_pv="daily.h5", index_daily="index.h5", day_calendar="calendar.txt",
        day_selection_universe="pool.txt", factor_meta="meta.json", day_meta_export="day_meta.json",
        sector_membership_spans="members.parquet", sector_code_map="codes.json", suspend_data="suspend.parquet")
    body = dict(schema_version="advisory_outcome_frozen_files_v1",
        profile=dict(controller_paths=dict(candidate_root=str(root))), files={k: dict(path=p,
            sha256=hashlib.sha256((root/p).read_bytes()).hexdigest()) for k, p in mapping.items()})
    return dict(body, binding_hash=sha(body)), calendar, symbols


def peer_packet(n=7, change=.02):
    symbols = [f"{600000+i}.SH" for i in range(n)]
    d, t = pd.Timestamp("2024-07-04"), pd.Timestamp("2024-07-05")
    return dict(membership=pd.DataFrame(dict(instrument=symbols, l2_code_id=1)), pool=symbols,
        d_close=pd.DataFrame(dict(trade_date=d, instrument=symbols, raw_close_cny=10., adj_factor=1.)),
        t_open=pd.DataFrame(dict(trade_date=t, instrument=symbols, raw_open_cny=10*(1+change), adj_factor=1.)),
        candidates=[symbols[0]], decision_date=d, target_date=t)


def test_peer_is_leave_one_out_new_information_and_members_are_not_dropped():
    packet = peer_packet()
    first, panel = build_peer_panel_v1(**packet)
    packet["t_open"].loc[0, "raw_open_cny"] = 99.
    second, _ = build_peer_panel_v1(**packet)
    pd.testing.assert_frame_equal(first, second)
    opposite, _ = build_peer_panel_v1(**peer_packet(change=-.02))
    assert first.peer_open_median_bps.iloc[0] == pytest.approx(200.)
    assert opposite.peer_open_median_bps.iloc[0] == pytest.approx(-200.)
    assert len(panel) == 7 and first.peer_count.iloc[0] == 6


@pytest.mark.parametrize("count,quotes,coverage", [(1, 0, None), (5, 0, 0.), (6, 5, 1.)])
def test_empty_peers_and_unobserved_quotes_are_not_cash_or_stock_ineligibility(count, quotes, coverage):
    packet = peer_packet(n=count)
    packet["t_open"].loc[1+quotes:, "raw_open_cny"] = np.nan
    result, panel = build_peer_panel_v1(**packet)
    row = result.iloc[0]
    assert len(panel) == count
    if coverage is None:
        assert pd.isna(row.peer_quote_coverage)
    else:
        assert row.peer_quote_coverage == coverage
    assert row.peer_status == ("KNOWN" if quotes >= 5 else "UNKNOWN_PEER_SUPPORT")


def test_duplicate_mapping_and_known_bad_price_fail_instead_of_removing_stock():
    packet = peer_packet()
    packet["membership"] = pd.concat([packet["membership"], packet["membership"].iloc[:1]])
    with pytest.raises(ValueError, match="membership"):
        build_peer_panel_v1(**packet)
    packet = peer_packet()
    packet["t_open"].loc[1, "raw_open_cny"] = -1.
    with pytest.raises(AdvisoryModelFirstError):
        build_peer_panel_v1(**packet)


def test_native_fixed_index_projects_numeric_field_before_future_ohlc_validation(tmp_path):
    binding, calendar, _ = file_binding(tmp_path/"asset")
    reader = SectorOpenFileProjectionV1(binding)
    close = reader.index(dates=[calendar[20]], field="close")
    opened = reader.index(dates=[calendar[21]], field="open")
    assert close.iloc[0] == 3000. and opened.iloc[0] == 3001.
    assert all(r["fields"] in (["close"], ["open"]) for r in reader.read_receipts)


def test_shared_batch_preserves_original_package_order_and_only_reads_three_daily_roles(tmp_path):
    binding, calendar, symbols = file_binding(tmp_path/"asset")
    reader = SectorOpenFileProjectionV1(binding)
    roster = pd.DataFrame([dict(package_id=p, manifest_sha256="a"*64, run_id=p, source_id=p,
        **{KEY[0]: d, KEY[1]: calendar[calendar.get_loc(d)+1], KEY[2]: s},
        selection_effective_rank=i+1, candidate_group_size=5)
        for p in ("second", "first") for d in calendar[20:22] for i, s in enumerate(symbols[:5])])
    values, panel = build_sector_context_inputs_v1(roster=roster, reader=reader,
        calendar=[d.date().isoformat() for d in calendar], cutoff=calendar[22].date())
    pd.testing.assert_frame_equal(values.loc[:, roster.columns], roster)
    assert len(panel) == 16 and len(values) == 20
    assert values.peer_observed_count.eq(7).all()
    roles = [r["role"] for r in reader.read_receipts if r["role"] in ("D_HISTORY", "PEER_D_CLOSE", "T_OPEN")]
    assert roles == ["D_HISTORY", "PEER_D_CLOSE", "T_OPEN"]
    assert not any(r["role"].endswith("FINANCE") for r in reader.read_receipts)


def test_role_date_boundaries_and_hash_drift_are_explicit_errors(tmp_path):
    binding, calendar, symbols = file_binding(tmp_path/"asset")
    reader = SectorOpenFileProjectionV1(binding)
    with pytest.raises(ValueError, match="projection"):
        reader.daily(start=calendar[21], end=calendar[21], cutoff=calendar[20],
            columns=["open", "factor"], members=symbols, role="T_OPEN")
    with pytest.raises(ValueError, match="projection"):
        reader.daily(start=calendar[20], end=calendar[20], cutoff=calendar[20],
            columns=["open", "close", "factor"], members=symbols, role="T_OPEN")
    (tmp_path/"asset"/"pool.txt").write_text("changed", encoding="utf-8")
    with pytest.raises(Exception, match="changed"):
        reader.assert_unchanged()


def test_native_valuation_retains_halts_unknowns_and_original_slots(tmp_path):
    root = tmp_path/"asset"
    binding, calendar, symbols = file_binding(root)
    frame = pd.read_hdf(root/"daily.h5", key="data")
    for day, symbol in ((calendar[22], symbols[1]), (calendar[23], symbols[1]),
            (calendar[22], symbols[2]), (calendar[21], symbols[3])):
        frame.loc[(day, symbol), ["open", "high", "low", "close"]] = np.nan
    frame.to_hdf(root/"daily.h5", key="data", mode="w", format="table", data_columns=True)
    pd.DataFrame(dict(ts_code=[symbols[1], symbols[1], symbols[3]], trade_date=calendar[[22, 23, 21]],
        suspend_type="S", suspend_timing="")).to_parquet(root/"suspend.parquet")
    for key in ("daily_pv", "suspend_data"):
        binding["files"][key]["sha256"] = hashlib.sha256((root/binding["files"][key]["path"]).read_bytes()).hexdigest()
    binding["binding_hash"] = sha({k: v for k, v in binding.items() if k != "binding_hash"})
    reader = SectorOpenFileProjectionV1(binding)
    roster = pd.DataFrame([dict(package_id="p", manifest_sha256="a"*64, run_id="r", source_id="r",
        **{KEY[0]: calendar[20], KEY[1]: calendar[21], KEY[2]: s},
        selection_effective_rank=i+1, candidate_group_size=5) for i, s in enumerate(symbols[:5])])
    inputs, _ = build_sector_context_inputs_v1(roster=roster, reader=reader,
        calendar=[str(d.date()) for d in calendar], cutoff=calendar[25].date())
    contexts = inputs.filter(regex="peer_|market_open_gap").copy()
    rows = measure_sector_finance_v1(inputs=inputs, reader=reader, calendar=[str(d.date()) for d in calendar],
        cutoff=calendar[25].date(), role="TRAIN_FINANCE")
    assert rows.valuation_status.tolist() == ["AVAILABLE", "AVAILABLE", "UNKNOWN", "CASH_ENTRY_NOT_EXECUTABLE", "AVAILABLE"]
    assert rows.carried_valuation_sessions.iloc[1] == 2 and rows.net_return_bps.iloc[3] == 0
    assert pd.isna(rows.net_return_bps.iloc[2]) and rows.valuation_reason.iloc[2] == "UNKNOWN_TRADED_PRICE_OR_COORDINATE"
    assert rows.label_information_end.eq(calendar[25]).all() and len(rows) == 5
    pd.testing.assert_frame_equal(inputs.filter(regex="peer_|market_open_gap"), contexts)
    assert reader.receipt()["sql_queries"] == 0 and not reader.receipt()["new_native_receipt"]


def test_future_price_and_factor_do_not_enter_earlier_context(tmp_path):
    binding, calendar, symbols = file_binding(tmp_path/"asset")
    root = tmp_path/"asset"
    frame = pd.read_hdf(root/"daily.h5", key="data")
    # T close/high/low and T+1 factors are not legal at this T-open clock.
    frame.loc[(calendar[21], slice(None)), ["high", "low", "close"]] = float("inf")
    frame.loc[(calendar[22], slice(None)), "factor"] = -999.
    frame.to_hdf(root/"daily.h5", key="data", mode="w", format="table", data_columns=True)
    binding["files"]["daily_pv"]["sha256"] = hashlib.sha256((root/"daily.h5").read_bytes()).hexdigest()
    binding["binding_hash"] = sha({k: v for k, v in binding.items() if k != "binding_hash"})
    reader = SectorOpenFileProjectionV1(binding)
    roster = pd.DataFrame([dict(package_id="p", manifest_sha256="a"*64, run_id="r", source_id="r",
        **{KEY[0]: calendar[20], KEY[1]: calendar[21], KEY[2]: s}, selection_effective_rank=i+1,
        candidate_group_size=5) for i, s in enumerate(symbols[:5])])
    values, _ = build_sector_context_inputs_v1(roster=roster, reader=reader,
        calendar=[str(d.date()) for d in calendar], cutoff=calendar[21].date())
    assert values.peer_status.eq("KNOWN").all() and len(values) == 5


def test_industry_migration_after_D_cannot_change_frozen_peer_members(tmp_path):
    binding, calendar, symbols = file_binding(tmp_path/"asset")
    root = tmp_path/"asset"
    mapping = pd.read_parquet(root/"members.parquet")
    mapping.loc[0, "end_date"] = calendar[20]
    future = mapping.iloc[:1].assign(start_date=calendar[21], end_date=calendar[-1], l2_code_id=999)
    pd.concat([mapping, future], ignore_index=True).to_parquet(root/"members.parquet")
    binding["files"]["sector_membership_spans"]["sha256"] = hashlib.sha256((root/"members.parquet").read_bytes()).hexdigest()
    binding["binding_hash"] = sha({k: v for k, v in binding.items() if k != "binding_hash"})
    reader = SectorOpenFileProjectionV1(binding)
    roster = pd.DataFrame([dict(package_id="p", manifest_sha256="a"*64, run_id="r", source_id="r",
        **{KEY[0]: calendar[20], KEY[1]: calendar[21], KEY[2]: symbols[0]}, selection_effective_rank=1, candidate_group_size=1)])
    values, _ = build_sector_context_inputs_v1(roster=roster, reader=reader,
        calendar=[str(d.date()) for d in calendar], cutoff=calendar[21].date())
    assert values.peer_count.iloc[0] == 7 and values.peer_status.iloc[0] == "KNOWN"
