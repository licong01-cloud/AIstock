"""Read only the original daily candidates' sector quotes; no package admission."""
from copy import deepcopy
from datetime import date, datetime, timezone
import math
from numbers import Real
import re
import time

import pandas as pd

from backend.db.pg_pool import get_conn
from backend.services.advisory_model_first.economic_common_core_daily_source_v1 import (
    EconomicCommonCoreReadonlyDailySourceV1, _packets as validate_core_packets,
)
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import _number
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _frame
from backend.services.advisory_model_first.economic_sector_daily_core_v1 import (
    CLASS_FIELDS, QUOTE_FIELDS, _calendar, _crosswalk, compose_sector_daily_features_v1,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def _invalid(message):
    raise AdvisoryModelFirstError(message, reason_code='ADVISORY_SECTOR_DAILY_SOURCE_INVALID')


class EconomicSectorReadonlyDailySourceV1:
    """Two bounded sector SELECTs per batch, reusing the existing five-query core."""

    def __init__(self, *, crosswalk, expected_crosswalk_values_sha256, index_codes,
                 core_source=None, connection_context_factory=None, budget_seconds=30., monotonic=None):
        self._mapping = _crosswalk(crosswalk, expected_crosswalk_values_sha256)
        known = {value for value in self._mapping.values() if value is not None}
        if (not isinstance(index_codes, dict) or set(index_codes) != known
                or any(type(key) is not int or not isinstance(code, str)
                       or re.fullmatch(r'\d{6}\.SI', code) is None for key, code in index_codes.items())
                or len(set(index_codes.values())) != len(index_codes)
                or isinstance(budget_seconds, bool) or not isinstance(budget_seconds, Real)
                or not math.isfinite(budget_seconds) or not 0 < budget_seconds <= 30):
            _invalid('sector quote namespace or finite time budget is malformed')
        self._codes, self._pin = dict(index_codes), expected_crosswalk_values_sha256
        self._budget, self._clock = float(budget_seconds), monotonic or time.monotonic
        self._connection = connection_context_factory or (lambda: get_conn(autocommit=False, manage_transaction=False))
        self._core = core_source or EconomicCommonCoreReadonlyDailySourceV1(
            connection_context_factory=self._connection, budget_seconds=self._budget, monotonic=self._clock)

    def load_day(self, **packet):
        return self.load_batch(packets=[packet])[0]

    def load_batch(self, *, packets):
        if not isinstance(packets, (list, tuple)) or not 1 <= len(packets) <= 20:
            _invalid('sector daily source needs one to twenty original day packets')
        deadline, packets = self._clock()+self._budget, deepcopy(packets)
        core_packets, per_packet_pairs = [], []
        for packet in packets:
            if not isinstance(packet, dict) or set(packet) != {
                'candidates', 'calendar', 'component_roles', 'terminal_weights', 'classification_rows',
            }:
                _invalid('sector daily packet schema differs')
            _calendar(packet['calendar'])
            core_packets.append({key: value for key, value in packet.items() if key != 'classification_rows'}
                                | {'calendar': tuple(packet['calendar'][1:])})
        validate_core_packets(core_packets)
        for packet in packets:
            classified = packet['classification_rows']
            if (not isinstance(classified, pd.DataFrame) or not classified.columns.is_unique
                    or set(classified.columns) != set(CLASS_FIELDS) or len(classified) > 20):
                _invalid('sector classifications must preserve the original candidate schema')
            classified = _frame(classified.loc[:, CLASS_FIELDS], KEY, set(CLASS_FIELDS))
            roster = _frame(packet['candidates'].loc[:, KEY], KEY, set(KEY))
            if set(classified[KEY].itertuples(index=False, name=None)) != set(roster[KEY].itertuples(index=False, name=None)):
                _invalid('sector classifications must preserve all original candidate keys')
            day = pd.Timestamp(packet['calendar'][-2])
            for code, clock in classified[['classification_l2_code', 'classification_known_from']].itertuples(index=False, name=None):
                if not pd.isna(clock) and (not isinstance(clock, (date, pd.Timestamp, str))
                        or isinstance(clock, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}', clock) is None):
                    _invalid('sector company classification needs an explicit session clock')
                if (not pd.isna(code) and (not isinstance(code, str) or re.fullmatch(r'\d{6}', code) is None or pd.isna(clock))
                        or not pd.isna(clock) and _day(clock) > day):
                    _invalid('sector company classification is malformed or not visible at D')
            ids = {self._mapping[code] for code in classified.classification_l2_code.dropna()
                   if code in self._mapping and self._mapping[code] is not None}
            per_packet_pairs.append({(pd.Timestamp(day), identity) for day in packet['calendar'][:-1] for identity in ids})
        pairs = sorted(set().union(*per_packet_pairs))
        if len(pairs) > 8400:
            _invalid('sector quote requests exceed the explicit candidate-key budget')
        try:
            cores = self._core.load_batch(packets=core_packets)
        except AdvisoryModelFirstError:
            raise
        except Exception as exc:
            raise AdvisoryModelFirstError('sector core read-only query failed', reason_code='ADVISORY_SECTOR_DAILY_SOURCE_QUERY_FAILED',
                context={'error_type': type(exc).__name__, 'phase': 'core'}) from exc
        if len(cores) != len(packets):
            _invalid('sector core results changed the original packet count')
        expected_calendar = {(i, pd.Timestamp(day)) for i, packet in enumerate(packets) for day in packet['calendar']}
        queries = []
        with self._connection() as conn:
            try:
                conn.set_session(isolation_level='REPEATABLE READ', readonly=True, autocommit=False)
                with conn.cursor() as cursor:
                    def read(phase, sql, params, columns, maximum):
                        began = self._clock()
                        if deadline <= began:
                            _invalid('sector daily total time budget exceeded')
                        cursor.execute('SET LOCAL statement_timeout = %s', (max(1, min(15000, int((deadline-began)*1000))),))
                        cursor.execute(sql, (*params, maximum+1))
                        rows, names = cursor.fetchall(), [item.name for item in cursor.description]
                        if len(rows) > maximum or names != list(columns) or self._clock() > deadline:
                            _invalid('sector daily row/schema/time budget differs')
                        queries.append(dict(phase=phase, rows=len(rows), seconds=round(self._clock()-began, 6)))
                        return pd.DataFrame(rows, columns=names)

                    calendar = read('sector_calendar', '''SELECT request.packet,cal.cal_date AS trade_date
                        FROM unnest(%s::integer[],%s::date[],%s::date[]) request(packet,first_day,target)
                        JOIN market.trading_calendar cal ON cal.cal_date BETWEEN request.first_day AND request.target
                        WHERE cal.is_trading=TRUE ORDER BY request.packet,cal.cal_date LIMIT %s''',
                        (list(range(len(packets))), [p['calendar'][0] for p in packets], [p['calendar'][-1] for p in packets]),
                        ('packet', 'trade_date'), len(expected_calendar))
                    dates = pd.to_datetime(calendar.trade_date, errors='coerce')
                    calendar['trade_date'] = dates
                    if (dates.isna().any() or dates.dt.tz is not None or not dates.eq(dates.dt.normalize()).all()
                            or not calendar.packet.map(lambda value: type(value) is int).all()
                            or calendar.duplicated(['packet', 'trade_date']).any()
                            or set(calendar.itertuples(index=False, name=None)) != expected_calendar):
                        _invalid('sector authoritative twenty-one D sessions or next T differs')
                    quotes = pd.DataFrame(columns=QUOTE_FIELDS)
                    if pairs:
                        quotes = read('sector_quotes', '''SELECT quote.trade_date AS datetime,request.identity AS l2_code_id,
                            quote.close AS sw2_close FROM market.sw_daily quote
                            JOIN unnest(%s::date[],%s::integer[],%s::text[]) request(day,identity,index_code)
                              ON quote.trade_date=request.day AND quote.ts_code=request.index_code
                            WHERE quote.trade_date BETWEEN %s AND %s
                            ORDER BY quote.trade_date,request.identity LIMIT %s''',
                            ([day.date() for day, _ in pairs], [identity for _, identity in pairs],
                             [self._codes[identity] for _, identity in pairs], pairs[0][0].date(), pairs[-1][0].date()),
                            QUOTE_FIELDS, len(pairs))
            except AdvisoryModelFirstError:
                raise
            except Exception as exc:
                raise AdvisoryModelFirstError('sector read-only query failed', reason_code='ADVISORY_SECTOR_DAILY_SOURCE_QUERY_FAILED',
                    context={'error_type': type(exc).__name__, 'completed_queries': queries}) from exc
            finally:
                try:
                    conn.rollback()
                except Exception as exc:
                    raise AdvisoryModelFirstError('sector rollback failed', reason_code='ADVISORY_SECTOR_DAILY_SOURCE_QUERY_FAILED',
                        context={'error_type': type(exc).__name__, 'phase': 'rollback'}) from exc
        dates = pd.to_datetime(quotes.datetime, errors='coerce')
        quotes['datetime'] = dates
        if (dates.isna().any() or dates.dt.tz is not None or not dates.eq(dates.dt.normalize()).all()
                or not quotes.l2_code_id.map(lambda value: type(value) is int).all()
                or quotes.duplicated(['datetime', 'l2_code_id']).any()
                or not set(quotes[['datetime', 'l2_code_id']].itertuples(index=False, name=None)).issubset(set(pairs))):
            _invalid('sector quote contains a foreign, future, duplicate or malformed key')
        # Match M1's existing H5 close representation before applying its math.
        # Decimal DB quotes otherwise differ from the trained float32 inputs.
        quotes['sw2_close'] = quotes.sw2_close.map(lambda value: _number(value, positive=True)).astype('float32').astype('float64')
        results, read_at = [], datetime.now(timezone.utc).isoformat()
        for packet, (core, core_receipt), wanted in zip(packets, cores, per_packet_pairs, strict=True):
            if self._clock() > deadline:
                _invalid('sector daily total time budget exceeded')
            selected = quotes.loc[[(day, identity) in wanted for day, identity in
                                  quotes[['datetime', 'l2_code_id']].itertuples(index=False, name=None)]]
            frame, receipt = compose_sector_daily_features_v1(core_frame=core, core_receipt=core_receipt,
                calendar=packet['calendar'], classification_rows=packet['classification_rows'], sector_quotes=selected,
                crosswalk=self._mapping, expected_crosswalk_values_sha256=self._pin)
            receipt['db_source'] = dict(read_at=read_at, isolation='REPEATABLE READ', readonly=True,
                database_written=False, native_capture=False, snapshots='CORE_AND_SECTOR_SEPARATE_READONLY',
                select_count=len(queries)+core_receipt['db_source']['select_count'], sector_queries=queries,
                batch_packet_count=len(packets), index_codes_sha256=sha(self._codes),
                quote_value_projection='FLOAT32_THEN_FLOAT64_MATCH_M1_H5')
            receipt['identity_sha256'] = sha({key: value for key, value in receipt.items() if key != 'identity_sha256'})
            results.append((frame, receipt))
        if self._clock() > deadline:
            _invalid('sector daily total time budget exceeded')
        return results
