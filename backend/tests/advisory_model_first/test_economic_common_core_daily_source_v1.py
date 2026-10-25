"""One reused day fixture, focused transaction and bounded-read contracts."""
from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_common_core_daily_source_v1 import EconomicCommonCoreReadonlyDailySourceV1
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import RAW_FIELDS
from backend.services.advisory_model_first.economic_entry_timing_features_v1 import TIMING_FEATURES
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_daily_feature_core_v1 import packet as core_packet


@pytest.fixture
def packet():
    return core_packet.__wrapped__()


class Database:
    def __init__(self, packet):
        self.original = deepcopy(packet)
        self.calls, self.options, self.rollbacks, self.seconds = [], {}, 0, 0.
        self.poison, self.extra_calendar, self.fail, self.delay = None, None, False, 0.
        self.rollback_fail = False

    @contextmanager
    def connection(self):
        yield self

    def set_session(self, **options):
        self.options = options

    def rollback(self):
        self.rollbacks += 1
        if self.rollback_fail:
            raise RuntimeError("rollback text must not leak")

    @contextmanager
    def cursor(self):
        yield self

    def execute(self, sql, params):
        self.calls.append((sql,params))
        if sql.startswith("SET LOCAL"):
            return
        if self.fail:
            raise RuntimeError("database text must not be copied to client errors")
        self.seconds += self.delay
        if "trading_calendar" in sql:
            self.names = ["packet", "trade_date"]
            self.rows = [(index,day) for index,first,last in zip(*params[:3], strict=True)
                for day in sorted(set(self.original["calendar"]) | ({self.extra_calendar} if self.extra_calendar else set())) if first <= day <= last]
        else:
            phase = "raw_daily" if "price.open_li" in sql else "benchmark_daily" if "index_daily" in sql else "suspend_rows" if "suspend_d" in sql else "market_daily"
            columns = ("trade_date", "instrument", *RAW_FIELDS) if phase == "raw_daily" else ("trade_date", "instrument", "suspend_type", "suspend_timing") if phase == "suspend_rows" else ("trade_date", "instrument", "close")
            frame = self.original[phase].copy()
            if phase == "suspend_rows" and "suspend_timing" not in frame:
                frame["suspend_timing"] = None
            if phase in ("raw_daily", "suspend_rows"):
                keys = {(pd.Timestamp(day),symbol) for day,symbol in zip(params[0],params[1],strict=True)}
                mask = [(pd.Timestamp(day),symbol) in keys for day,symbol in frame[["trade_date", "instrument"]].to_numpy()]
            else:
                days = params[0] if phase == "benchmark_daily" else params[2]
                mask = frame.trade_date.isin(pd.DatetimeIndex(days))
            frame = frame.loc[mask,columns].head(params[-1])
            self.names, self.rows = list(columns), list(frame.itertuples(index=False,name=None))
            if phase == "raw_daily" and self.poison:
                rows = [list(row) for row in self.rows]
                if self.poison == "foreign":
                    rows[0][1] = "999999.SH"
                elif self.poison == "future":
                    rows[0][0] = self.original["calendar"][-1]
                elif self.poison == "duplicate":
                    rows[-1] = rows[0]
                elif self.poison == "budget":
                    rows += [rows[0]]
                else:
                    self.names[-1] = "future_return"
                self.rows = rows
        self.description = [SimpleNamespace(name=name) for name in self.names]

    def fetchall(self):
        return self.rows

    def source(self):
        return EconomicCommonCoreReadonlyDailySourceV1(connection_context_factory=self.connection,monotonic=lambda:self.seconds)


def day_packet(packet):
    return {key:deepcopy(packet[key]) for key in ("candidates", "calendar", "component_roles", "terminal_weights")}


def test_single_and_batch_share_exact_inputs_five_queries_and_readonly_snapshot(packet):
    single_db, batch_db = Database(packet), Database(packet)
    day = day_packet(packet)
    previous = deepcopy(day)
    first = (pd.Timestamp(day["calendar"][0])-pd.offsets.BDay()).date()
    previous["calendar"] = (first,*day["calendar"][:-1])
    previous["candidates"]["decision_as_of_trade_date"] = pd.Timestamp(previous["calendar"][-2])
    previous["candidates"]["target_trade_date"] = pd.Timestamp(previous["calendar"][-1])
    batch_db.extra_calendar = first
    single, one = single_db.source().load_day(**day)
    (_, earlier),(batch,two) = batch_db.source().load_batch(packets=[previous,day])
    pd.testing.assert_frame_equal(single,batch)
    for key in ("input_sha256", "feature_sha256", "information_sha256", "unknown_fields"):
        assert one[key] == two[key]
    assert earlier["candidate_count"] == len(day["candidates"])
    for database, receipt in ((single_db,one),(batch_db,two)):
        assert database.options == dict(isolation_level="REPEATABLE READ",readonly=True,autocommit=False)
        assert database.rollbacks == 1 and receipt["db_source"]["select_count"] == 5
        sql = [statement for statement,_ in database.calls if not statement.startswith("SET LOCAL")]
        assert len(sql) == 5 and not any(word in " ".join(sql).upper() for word in ("UPDATE ","INSERT ","DELETE ","TRUNCATE "))
        assert not receipt["db_source"]["database_written"] and not receipt["db_source"]["native_capture"]
        assert receipt["source_evidence"] == "COMPUTATION_ONLY" and not receipt["deployable"]
        assert all(1 <= params[0] <= 15000 for statement,params in database.calls if statement.startswith("SET LOCAL"))


def test_missing_candidate_and_legal_empty_roster_are_not_deleted_or_fake_model_success(packet):
    database = Database(packet)
    database.original["raw_daily"] = database.original["raw_daily"].loc[~database.original["raw_daily"].instrument.eq("000001.SZ")]
    frame,receipt = database.source().load_day(**day_packet(packet))
    assert frame.instrument.tolist() == ["000001.SZ","000002.SZ"] and pd.isna(frame.ret_1.iloc[0])
    empty = day_packet(packet)
    empty["candidates"] = empty["candidates"].iloc[:0]
    result,receipt = Database(packet).source().load_day(**empty)
    assert result.empty and receipt["status"] == "NO_CANDIDATES" and receipt["db_source"]["select_count"] == 1


@pytest.mark.parametrize("poison", ["foreign","future","duplicate","budget","schema"])
def test_source_cannot_hide_bad_rows_by_per_day_filtering(packet,poison):
    database = Database(packet)
    database.poison = poison
    with pytest.raises(AdvisoryModelFirstError):
        database.source().load_day(**day_packet(packet))
    assert database.rollbacks == 1


@pytest.mark.parametrize("failure", ["query","time","calendar","rollback"])
def test_failed_reads_and_authoritative_calendar_mismatch_always_rollback(packet,failure):
    database = Database(packet)
    database.fail = failure == "query"
    database.delay = 31 if failure == "time" else 0
    database.rollback_fail = failure == "rollback"
    day = day_packet(packet)
    if failure == "calendar":
        database.original["calendar"] = database.original["calendar"][:-1]
    with pytest.raises(AdvisoryModelFirstError) as error:
        database.source().load_day(**day)
    assert database.rollbacks == 1 and "database text" not in str(error.value)


@pytest.mark.parametrize("invalid", ["batch","future_clock","symbol"])
def test_unsafe_requests_are_rejected_before_any_database_read(packet,invalid):
    database = Database(packet)
    day = day_packet(packet)
    if invalid == "future_clock":
        day["candidates"]["decision_as_of_trade_date"] = pd.Timestamp(day["calendar"][-1])
    elif invalid == "symbol":
        day["candidates"].loc[0,"instrument"] = "invalid"
    with pytest.raises(AdvisoryModelFirstError):
        database.source().load_batch(packets=[day]*21 if invalid == "batch" else [day])
    assert database.calls == [] and database.rollbacks == 0


def test_timing_entry_reuses_original_five_queries_and_preserves_default_core(packet):
    default_db, single_db, batch_db = Database(packet), Database(packet), Database(packet)
    day = day_packet(packet)
    core, original = default_db.source().load_day(**day)
    single, first = single_db.source().load_timing_day(**day)
    (batch, second), _ = batch_db.source().load_timing_batch(packets=[day,deepcopy(day)])
    pd.testing.assert_frame_equal(single, batch)
    pd.testing.assert_frame_equal(single.drop(columns=list(TIMING_FEATURES)), core)
    assert first["core"]["input_sha256"] == original["input_sha256"]
    assert first["timing"] == second["timing"] and first["feature_sha256"] == second["feature_sha256"]
    for database, receipt in ((single_db, first), (batch_db, second)):
        assert database.rollbacks == 1 and receipt["db_source"]["select_count"] == 5
        assert [sql for sql,_ in database.calls] == [sql for sql,_ in default_db.calls]
        assert database.options == default_db.options
        assert not receipt["deployable"] and not receipt["new_native_receipt"]


def test_timing_missing_raw_and_zero_volume_do_not_remove_candidates(packet):
    database = Database(packet)
    bars = database.original["raw_daily"]
    bars.loc[bars.instrument.eq("000001.SZ"), "volume_hand"] = 0.
    value, receipt = database.source().load_timing_day(**day_packet(packet))
    assert value.instrument.tolist() == ["000001.SZ", "000002.SZ"]
    assert value.loc[0,list(TIMING_FEATURES)].isna().all()
    assert value.loc[1,list(TIMING_FEATURES)].notna().all()
    assert receipt["timing"]["unknown_fields"][0]["fields"] and database.rollbacks == 1
    empty = day_packet(packet)
    empty["candidates"] = empty["candidates"].iloc[:0]
    value, receipt = Database(packet).source().load_timing_day(**empty)
    assert value.empty and receipt["status"] == "NO_CANDIDATES" and receipt["db_source"]["select_count"] == 1


def test_timing_source_d_close_unknown_keeps_independent_overnight_volatility(packet):
    database = Database(packet)
    bars = database.original["raw_daily"]
    mask = bars.instrument.eq("000001.SZ") & bars.trade_date.eq(pd.Timestamp(packet["calendar"][-2]))
    bars.loc[mask, "close_li"] = float("nan")
    value, receipt = database.source().load_timing_day(**day_packet(packet))
    assert pd.isna(value[TIMING_FEATURES[0]].iloc[0]) and pd.notna(value[TIMING_FEATURES[1]].iloc[0])
    assert set(receipt["timing"]["unknown_fields"][0]["fields"]) == {TIMING_FEATURES[0]}
