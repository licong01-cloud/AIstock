"""Reuse core/sector packets; test only the new source's read and join contracts."""
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_sector_daily_core_v1 import QUOTE_FIELDS, build_sector_daily_features_v1
from backend.services.advisory_model_first.economic_sector_daily_source_v1 import EconomicSectorReadonlyDailySourceV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
from backend.tests.advisory_model_first.test_economic_common_core_daily_source_v1 import Database, packet as packet
from backend.tests.advisory_model_first.test_economic_sector_daily_core_v1 import sector_packet as sector_packet


class SectorDatabase(Database):
    def __init__(self, packet, sector_packet):
        super().__init__(packet)
        self.extra_calendar = sector_packet['calendar'][0]
        self.quotes = sector_packet['sector_quotes'].copy()

    def execute(self, sql, params):
        if 'market.sw_daily' not in sql:
            return super().execute(sql, params)
        self.calls.append((sql, params))
        if self.fail:
            raise RuntimeError('database detail must not leak')
        self.seconds += self.delay
        wanted = {(pd.Timestamp(day), identity) for day, identity in zip(params[0], params[1], strict=True)}
        rows = self.quotes.loc[[(pd.Timestamp(day), identity) in wanted for day, identity in
                              self.quotes[['datetime', 'l2_code_id']].itertuples(index=False, name=None)]]
        self.names = list(QUOTE_FIELDS)
        self.rows = [list(row) for row in rows.itertuples(index=False, name=None)]
        for row in self.rows:
            row[2] = None if pd.isna(row[2]) else Decimal(str(row[2]))
        if self.poison == 'foreign':
            self.rows[0][1] = 999
        elif self.poison == 'future':
            self.rows[0][0] = self.original['calendar'][-1]
        elif self.poison == 'duplicate':
            self.rows[-1] = list(self.rows[0])
        elif self.poison == 'budget':
            self.rows += [list(self.rows[0])]
        elif self.poison == 'schema':
            self.names[-1] = 'future_return'
        self.description = [SimpleNamespace(name=name) for name in self.names]


def build(packet, sector_packet):
    core_db, sector_db = Database(packet), SectorDatabase(packet, sector_packet)
    source = EconomicSectorReadonlyDailySourceV1(crosswalk=sector_packet['crosswalk'],
        expected_crosswalk_values_sha256=sector_packet['expected_crosswalk_values_sha256'],
        index_codes={0: '801001.SI', 1: '801002.SI'}, core_source=core_db.source(),
        connection_context_factory=sector_db.connection, monotonic=lambda: core_db.seconds+sector_db.seconds)
    day = {key: deepcopy(sector_packet['core_inputs'][key]) for key in ('candidates', 'component_roles', 'terminal_weights')}
    day.update(calendar=sector_packet['calendar'], classification_rows=sector_packet['classification_rows'].copy())
    return source, core_db, sector_db, day


def test_single_batch_and_pure_math_match_with_seven_selects_and_id_zero(packet, sector_packet):
    sector_packet['sector_quotes']['sw2_close'] += 0.12345
    source, core_db, sector_db, day = build(packet, sector_packet)
    original = deepcopy(day)
    single, one = source.load_day(**day)
    (batch, two), _ = source.load_batch(packets=[day, deepcopy(day)])
    normalized = deepcopy(sector_packet)
    normalized['sector_quotes']['sw2_close'] = normalized['sector_quotes'].sw2_close.astype('float32').astype('float64')
    expected, _ = build_sector_daily_features_v1(**normalized)
    pd.testing.assert_frame_equal(single, expected)
    pd.testing.assert_frame_equal(single, batch)
    pd.testing.assert_frame_equal(day['candidates'], original['candidates'])
    for key in ('input_sha256', 'feature_sha256', 'unknown_fields'):
        assert one[key] == two[key]
    assert one['db_source']['select_count'] == 7
    assert one['identity_sha256'] == sha({key: value for key, value in one.items() if key != 'identity_sha256'})
    for database in (core_db, sector_db):
        assert database.options == dict(isolation_level='REPEATABLE READ', readonly=True, autocommit=False)
        assert database.rollbacks == 2
        assert not any(word in ' '.join(sql for sql, _ in database.calls).upper() for word in ('UPDATE ', 'INSERT ', 'DELETE ', 'TRUNCATE '))
    quote_calls = [params for sql, params in sector_db.calls if 'sw_daily' in sql]
    assert all(len(params[0]) == 42 and 0 in params[1] and params[-1] == 43 for params in quote_calls)
    assert one['native_identity'] == 'UNPROVEN' and not one['outcomes_read']


def test_normal_missing_classification_quote_and_empty_roster_remain_explicit(packet, sector_packet):
    source, _, database, day = build(packet, sector_packet)
    day['classification_rows'].loc[1, 'classification_l2_code'] = None
    database.quotes = database.quotes.loc[database.quotes.datetime.ne(sector_packet['calendar'][5])]
    frame, receipt = source.load_day(**day)
    assert frame.instrument.tolist() == day['candidates'].instrument.tolist() and frame.sector_ret5.isna().all()
    assert {row['sector_status'] for row in receipt['unknown_fields']} == {'UNKNOWN_SECTOR_QUOTE', 'UNKNOWN_CLASSIFICATION_OR_MAPPING'}
    day['candidates'], day['classification_rows'] = day['candidates'].iloc[:0], day['classification_rows'].iloc[:0]
    frame, receipt = source.load_day(**day)
    assert frame.empty and receipt['status'] == 'NO_CANDIDATES' and receipt['db_source']['select_count'] == 2


@pytest.mark.parametrize('poison', ['foreign', 'future', 'duplicate', 'budget', 'schema'])
def test_quote_poison_cannot_be_hidden_by_per_packet_filtering(packet, sector_packet, poison):
    source, _, database, day = build(packet, sector_packet)
    database.poison = poison
    with pytest.raises(AdvisoryModelFirstError):
        source.load_day(**day)
    assert database.rollbacks == 1


@pytest.mark.parametrize('failure', ['query', 'time', 'calendar', 'rollback'])
def test_failure_always_rolls_back_without_exposing_database_details(packet, sector_packet, failure):
    source, _, database, day = build(packet, sector_packet)
    database.fail, database.delay, database.rollback_fail = failure == 'query', 31 if failure == 'time' else 0, failure == 'rollback'
    if failure == 'calendar':
        database.extra_calendar = None
    with pytest.raises(AdvisoryModelFirstError) as error:
        source.load_day(**day)
    assert database.rollbacks == 1 and 'database detail' not in str(error.value)


@pytest.mark.parametrize('invalid', ['future_classification', 'malformed_clock', 'candidate_missing', 'batch', 'calendar'])
def test_bad_original_inputs_fail_before_either_source_reads(packet, sector_packet, invalid):
    source, core, database, day = build(packet, sector_packet)
    if invalid == 'future_classification':
        day['classification_rows'].loc[0, 'classification_known_from'] = pd.Timestamp(day['calendar'][-1])
    elif invalid == 'malformed_clock':
        day['classification_rows']['classification_known_from'] = '2020-01-01T00:00:00'
    elif invalid == 'candidate_missing':
        day['classification_rows'] = day['classification_rows'].iloc[:1]
    elif invalid == 'calendar':
        day['calendar'] = day['calendar'][1:]
    with pytest.raises((AdvisoryModelFirstError, ValueError)):
        source.load_batch(packets=[day]*21 if invalid == 'batch' else [day])
    assert core.calls == database.calls == []


def test_malformed_index_namespace_is_not_a_default_or_package_qualification(packet, sector_packet):
    with pytest.raises(AdvisoryModelFirstError):
        EconomicSectorReadonlyDailySourceV1(crosswalk=sector_packet['crosswalk'],
            expected_crosswalk_values_sha256=sector_packet['expected_crosswalk_values_sha256'],
            index_codes={0: '801001.SI', 1: '801001.SI'})
