"""Read-only structural identifiers and strictly D-visible sector quotes."""
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _frame
from backend.services.advisory_model_first.economic_entry_pipeline import _verify_reference, file_sha256

SECTOR_FEATURES = ('sector_ret5', 'sector_vol20', 'relative_ret5_sector')


def structural_crosswalk_v1(snapshot, *, code_map, taxonomy):
    """Versioned code translation, never a current company membership lookup."""
    if (snapshot.get('schema_version') != 'advisory_sw2021_structural_crosswalk_v1'
            or snapshot.get('taxonomy_version') != 'SW2021'
            or snapshot.get('stability_basis') != 'SAME_SW2021_STANDARD_EXPLICIT_CODE_PAIRS'
            or snapshot.get('source_table') != 'market.sw_index_classify'
            or snapshot.get('official_url') != 'https://tushare.pro/document/2?doc_id=181'
            or not snapshot.get('captured_at') or snapshot.get('historical_capture_proven') is not False
            or taxonomy.get('version') != 'SW2021'
            or taxonomy.get('contract_id') != 'sw2021_classification_catalog_v1'):
        raise ValueError('sector structural version/authority is unproved')

    def pairs(rows):
        result = {}
        for row in rows:
            industry, index = row.get('industry_code'), row.get('index_code')
            if (row.get('level') != 'L2' or row.get('src') != 'SW2021'
                    or not isinstance(industry, str) or not re.fullmatch(r'\d{6}', industry)
                    or not isinstance(index, str) or not re.fullmatch(r'\d{6}\.SI', index)
                    or industry in result or index in result.values()):
                raise ValueError('sector structural mapping is not explicit one-to-one')
            result[industry] = index
        if len(result) != 134:
            raise ValueError('sector SW2021 structural table is incomplete')
        return result

    db, official = pairs(snapshot['rows']), pairs(snapshot['official_pairs'])
    if db != official:
        raise ValueError('sector database and official standard disagree')
    ids = {}
    for entry in code_map['entries']:
        code, identity = entry['canonical_l2_code'], entry['l2_code_id']
        if (code not in db.values() or type(identity) is not int or identity < 0
                or code in ids or identity in ids.values()):
            raise ValueError('sector profile code namespace conflicts')
        ids[code] = identity
    if not ids:
        raise ValueError('sector profile has no quote namespace')
    return {industry: ids.get(code) for industry, code in db.items()}


def sector_quotes_v1(chunks, *, namespace, first_day, last_day):
    """One quote per date/index, ignoring H5 instrument-to-company assignment."""
    parts = []
    for frame in chunks:
        if 'datetime' in frame.index.names:
            frame = frame.reset_index()
        required = {'datetime', 'l2_code_id', 'sw2_close'}
        if not required.issubset(frame.columns):
            raise ValueError('sector H5 quote schema differs')
        frame = frame.loc[:, ['datetime', 'l2_code_id', 'sw2_close']].copy()
        frame['datetime'] = pd.to_datetime(frame.datetime)
        frame = frame.loc[frame.datetime.between(first_day, last_day)]
        ids = frame.l2_code_id
        known = ids.notna()
        if (ids.loc[known].map(lambda v: isinstance(v, (bool, np.bool_))).any()
                or not np.isfinite(ids.loc[known].to_numpy(dtype=float)).all()
                or ids.loc[known].mod(1).ne(0).any()
                or not ids.loc[known].isin(namespace).all()):
            raise ValueError('sector quote id contradicts pinned namespace')
        # Null id is declared unclassified; zero is a VALID id when mapped.
        frame = frame.loc[known & frame.sw2_close.notna()].copy()
        if (frame.sw2_close.map(lambda v: isinstance(v, (bool, np.bool_))).any()
                or not np.isfinite(frame.sw2_close.to_numpy(dtype=float)).all()
                or frame.sw2_close.le(0).any()
                or not frame.datetime.eq(frame.datetime.dt.normalize()).all()):
            raise ValueError('sector known quote is invalid')
        frame['l2_code_id'] = frame.l2_code_id.astype(int)
        grouped = frame.groupby(['datetime', 'l2_code_id']).sw2_close
        if grouped.nunique().gt(1).any():
            raise ValueError('sector duplicate date/id quote contradiction')
        parts.append(grouped.first().reset_index())
    if not parts:
        return pd.DataFrame(columns=['datetime', 'l2_code_id', 'sw2_close'])
    combined = pd.concat(parts, ignore_index=True)
    grouped = combined.groupby(['datetime', 'l2_code_id']).sw2_close
    if grouped.nunique().gt(1).any():
        raise ValueError('sector cross-chunk quote contradiction')
    return grouped.first().reset_index()


def sector_dynamic_rows_v1(*, rows, quotes, calendar, crosswalk):
    """Keep all original keys; 21 COMPLETE sessions ending at D or UNKNOWN."""
    fields = [*KEY, 'ret_5', 'classification_l2_code', 'classification_known_from']
    base = _frame(rows.loc[:, fields], KEY, set(fields))
    days = pd.DatetimeIndex([_day(day) for day in calendar])
    if not days.is_unique or not days.is_monotonic_increasing:
        raise ValueError('sector original trading calendar differs')
    if not base[KEY[0]].isin(days).all():
        raise ValueError('sector D absent from original calendar')
    known = base.classification_l2_code.notna()
    clocks = base.classification_known_from.map(lambda v: pd.NaT if pd.isna(v) else _day(v))
    if (known & (clocks.isna() | clocks.gt(base[KEY[0]]))).any():
        raise ValueError('sector classification is not known by D')
    if not base.loc[known, 'classification_l2_code'].map(lambda v: isinstance(v, str) and bool(re.fullmatch(r'\d{6}', v))).all():
        raise ValueError('sector classification requires exact industry code')
    quotes = quotes.loc[quotes.datetime.le(base[KEY[0]].max())]
    if quotes.duplicated(['datetime', 'l2_code_id']).any():
        raise ValueError('sector normalized quote keys are not unique')
    series = {identity: group.set_index('datetime').sw2_close.reindex(days)
        for identity, group in quotes.groupby('l2_code_id')}
    computed, output = {}, []
    for item in base.to_dict('records'):
        day, industry = item[KEY[0]], item['classification_l2_code']
        identity = crosswalk.get(industry)
        state, values = 'UNKNOWN_CLASSIFICATION_OR_MAPPING', [None]*3
        if identity is not None:
            cache_key = (day, identity)
            if cache_key not in computed:
                position = days.get_loc(day)
                if position < 20:
                    computed[cache_key] = ('UNKNOWN_WARMUP', None)
                else:
                    window = days[position-20:position+1]
                    closes = series.get(identity, pd.Series(dtype=float)).reindex(window).to_numpy(dtype=float)
                    if not np.isfinite(closes).all() or (closes <= 0).any():
                        computed[cache_key] = ('UNKNOWN_SECTOR_QUOTE', None)
                    else:
                        returns = closes[1:]/closes[:-1]-1
                        computed[cache_key] = ('AVAILABLE', [float(closes[-1]/closes[-6]-1), float(returns.std(ddof=1))])
            state, block = computed[cache_key]
            if block is not None:
                ret = item['ret_5']
                if pd.isna(ret):
                    state = 'UNKNOWN_STOCK_FEATURE'
                elif isinstance(ret, (bool, np.bool_)) or not np.isfinite(float(ret)):
                    raise ValueError('sector stock return is invalid')
                else:
                    values = [*block, float(ret)-block[0]]
        output.append({**{key: item[key] for key in KEY}, **dict(zip(SECTOR_FEATURES, values, strict=True)),
            'sector_feature_status': state, 'sector_feature_visible_through': day})
    return pd.DataFrame(output)


def load_sector_dynamic_rows_v1(*, plan, rows, calendar, authority_root):
    """Profile-bound immutable H5; no database access, rebuilding or activation."""
    profile_path = Path(plan.profile_path)
    if file_sha256(profile_path) != plan.profile_sha256:
        raise ValueError('sector profile hash changed')
    profile = json.loads(profile_path.read_text(encoding='utf-8'))
    root = Path(profile['controller_paths']['candidate_root'])
    pins = profile['components']['sector_context_pins']
    component = root/pins['component_root']
    map_path = component/pins['code_map_file']
    h5 = root/'components/factor_h5_static_candidate_v2/sector_data.h5'
    taxonomy_path = Path(authority_root)/'taxonomy_catalog.json'
    snapshot_path = _verify_reference(plan.crosswalk_ref)
    snapshot = json.loads(snapshot_path.read_text(encoding='utf-8'))
    expected = {profile_path: plan.profile_sha256, map_path: pins['code_map_sha256'], h5: pins['sector_data_sha256'],
        taxonomy_path: snapshot['taxonomy_catalog_sha256'], snapshot_path: plan.crosswalk_ref.sha256}
    for path, digest in expected.items():
        if file_sha256(path) != digest:
            raise ValueError('sector pinned source hash differs')
    crosswalk = structural_crosswalk_v1(snapshot, code_map=json.loads(map_path.read_text(encoding='utf-8')),
        taxonomy=json.loads(taxonomy_path.read_text(encoding='utf-8')))
    days = pd.DatetimeIndex([_day(day) for day in calendar])
    last = pd.to_datetime(rows[KEY[0]]).max()
    with pd.HDFStore(h5, mode='r') as store:
        chunks = store.select('data', where=[f"datetime >= '{days.min().date()}'", f"datetime <= '{last.date()}'"],
            columns=['l2_code_id', 'sw2_close'], chunksize=100000)
        quotes = sector_quotes_v1(chunks, namespace=set(value for value in crosswalk.values() if value is not None),
            first_day=days.min(), last_day=last)
    result = sector_dynamic_rows_v1(rows=rows, quotes=quotes, calendar=calendar, crosswalk=crosswalk)
    for path, digest in expected.items():
        if file_sha256(path) != digest:
            raise ValueError('sector source changed while reading')
    summary = dict(candidate_rows=len(result), quote_rows=len(quotes), feature_fields=list(SECTOR_FEATURES),
        availability=result.sector_feature_status.value_counts().to_dict(), source_sha256={str(path): digest for path, digest in expected.items()},
        structural_taxonomy='SW2021', company_membership='ORIGINAL_STRICT_AS_PUBLISHED_PIT', native_identity='UNPROVEN',
        sealed_accessed=False, database_written=False)
    return result, summary
