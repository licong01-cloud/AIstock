"""Fixed PIT sector path shape beyond the original sector return and volatility."""
import json
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import Field, model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_pipeline import file_sha256
from backend.services.advisory_model_first.economic_sector_moneyflow_v1 import _number
from backend.services.advisory_model_first.economic_sector_price_source_v1 import SECTOR_FEATURES, sector_quotes_v1, structural_crosswalk_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPricePlanV1
from backend.services.advisory_model_first.economic_volume_context_price_v1 import _roster_calendar
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

PATH_FEATURES = ('sector_ret1_D', 'sector_drawdown20_D')
INFORMATION_FEATURES = (*SECTOR_FEATURES, *PATH_FEATURES)
STATUS = 'sector_path_feature_status'
CLOCK = 'sector_path_feature_visible_through'


class SectorPathPricePlanV1(SectorPricePlanV1):
    schema_version: Literal['economic_sector_path_price_v1'] = 'economic_sector_path_price_v1'
    campaign_id: Literal['advisory_sector_path_price_v1_20261006'] = 'advisory_sector_path_price_v1_20261006'
    model_id: Literal['M25'] = 'M25'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1
    sector_prepared_manifest_ref: EvidenceReferenceV1
    prior_fit_journal_sha256: str = Field(pattern=r'^[a-f0-9]{64}$')

    @model_validator(mode='after')
    def validate_path_sources(self):
        for ref, role, stage in (
            (self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
            (self.predecessor_manifest_ref, 'sector_path_price_predecessor', 'evaluated'),
            (self.sector_prepared_manifest_ref, 'sector_path_price_base_snapshot', 'prepared')):
            path = Path(ref.artifact_uri)
            if (ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:'
                    or path.name != 'manifest.json' or path.parent.name != stage
                    or path.parent.parent.parent.resolve() != self.campaign_root.resolve()):
                raise ValueError('sector path original source/budget anchor differs')
        return self

    def model_copy(self, *, update=None, deep=False):
        return type(self).model_validate(super().model_copy(update=update, deep=deep).model_dump())

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return {**super().parameters, 'hypothesis': 'H-SECTOR-PATH-SHAPE-CONDITIONAL-PRICE-1',
            'information_features': list(INFORMATION_FEATURES), 'matched_information_features': list(SECTOR_FEATURES),
            'matched_dimension': 16, 'candidate_dimension': 18, 'campaign_fit_budget': 99,
            'source': 'EXISTING_FROZEN_M1_SECTOR_PATH_QUOTES', 'source_select_budget': 0,
            'maximum_ranking_rows': 20000, 'maximum_quote_rows': 60000, 'h5_chunk_rows': 100000,
            'path_window_sessions': 21}

    @property
    def experiment_id(self):
        return 'advsectorpath_'+self.plan_sha256[:24]


def load_sector_path_quotes_v1(*, plan, base_rows, calendar):
    """Read the original quote identities once during prepare, never current membership."""
    prepared = Path(plan.sector_prepared_manifest_ref.artifact_uri).parent
    summary = json.loads((prepared/'source_summary.json').read_text(encoding='utf-8'))
    expected = {Path(name): digest for name, digest in summary['source_sha256'].items()}
    profile_path = Path(plan.profile_path)
    profile = json.loads(profile_path.read_text(encoding='utf-8'))
    root = Path(profile['controller_paths']['candidate_root'])
    pins = profile['components']['sector_context_pins']
    map_path = root/pins['component_root']/pins['code_map_file']
    h5 = root/'components/factor_h5_static_candidate_v2/sector_data.h5'
    snapshot_path = Path(plan.crosswalk_ref.artifact_uri)
    taxonomies = [path for path in expected if path.name == 'taxonomy_catalog.json']
    if (len(taxonomies) != 1 or set(expected) != {profile_path, map_path, h5, snapshot_path, *taxonomies}
            or expected.get(profile_path) != plan.profile_sha256
            or expected.get(snapshot_path) != plan.crosswalk_ref.sha256
            or expected.get(map_path) != pins['code_map_sha256']
            or expected.get(h5) != pins['sector_data_sha256']):
        raise ValueError('sector path original source summary/profile pins differ')
    for path, digest in expected.items():
        if file_sha256(path) != digest:
            raise ValueError('sector path original source changed before reading')
    crosswalk = structural_crosswalk_v1(json.loads(snapshot_path.read_text(encoding='utf-8')),
        code_map=json.loads(map_path.read_text(encoding='utf-8')),
        taxonomy=json.loads(taxonomies[0].read_text(encoding='utf-8')))
    _, days = _roster_calendar(base_rows[KEY], calendar)
    last = pd.to_datetime(base_rows[KEY[0]]).max()
    with pd.HDFStore(h5, mode='r') as store:
        chunks = store.select('data', where=[f"datetime >= '{days.min().date()}'", f"datetime <= '{last.date()}'"],
            columns=['l2_code_id', 'sw2_close'], chunksize=plan.parameters['h5_chunk_rows'])
        quotes = sector_quotes_v1(chunks, namespace={v for v in crosswalk.values() if v is not None},
            first_day=days.min(), last_day=last)
    if len(quotes) > plan.parameters['maximum_quote_rows']:
        raise ValueError('sector path unique quote budget differs')
    for path, digest in expected.items():
        if file_sha256(path) != digest:
            raise ValueError('sector path original source changed while reading')
    return quotes, crosswalk, dict(quote_rows=len(quotes), source_sha256=summary['source_sha256'],
        feature_fields=list(PATH_FEATURES), company_membership='ORIGINAL_M1_D_CLASSIFICATION',
        native_identity='UNPROVEN', source_evidence='RECOVERED_LIMITED_NON_VINTAGE',
        selects=0, database_accessed=False, database_written=False)


def sector_path_price_rows_v1(*, base_rows, quotes, candidates, calendar, crosswalk):
    roster, days = _roster_calendar(candidates, calendar)
    expected = set(roster[KEY].itertuples(index=False, name=None))
    if (not len(roster) or len(base_rows) > 7720 or len(quotes) > 60000 or base_rows.duplicated(KEY).any()
            or set(base_rows[KEY].itertuples(index=False, name=None)) != expected
            or set((*PATH_FEATURES, STATUS, CLOCK)) & set(base_rows.columns)):
        raise ValueError('sector path original keys/budget/field collision differs')
    rows = base_rows.reset_index(drop=True).copy()
    if (not pd.to_datetime(rows.sector_feature_visible_through).eq(rows[KEY[0]]).all()
            or not rows.sector_feature_status.map(lambda v: isinstance(v, str) and (v == 'AVAILABLE' or v.startswith('UNKNOWN'))).all()):
        raise ValueError('sector path original clock/status differs')
    known = rows.classification_l2_code.notna()
    clocks = pd.to_datetime(rows.classification_known_from)
    if (known & (clocks.isna() | clocks.gt(rows[KEY[0]]))).any():
        raise ValueError('sector path classification is not known by D')
    if not rows.loc[known, 'classification_l2_code'].map(lambda v: isinstance(v, str) and len(v) == 6 and v.isdecimal()).all():
        raise ValueError('sector path original classification code differs')
    rows.loc[:, SECTOR_FEATURES] = rows.loc[:, SECTOR_FEATURES].map(_number)
    frame = quotes.loc[pd.to_datetime(quotes.datetime).le(rows[KEY[0]].max())].copy()
    frame['datetime'] = pd.to_datetime(frame.datetime)
    if (frame.duplicated(['datetime', 'l2_code_id']).any() or not frame.datetime.eq(frame.datetime.dt.normalize()).all()):
        raise ValueError('sector path normalized quote key/clock differs')
    series = {code: group.set_index('datetime').sw2_close for code, group in frame.groupby('l2_code_id')}
    cache, result = {}, []
    for item in rows.to_dict('records'):
        d, code = item[KEY[0]], crosswalk.get(item['classification_l2_code'])
        key = (d, code)
        if key not in cache:
            values = [np.nan, np.nan]
            position = days.get_loc(d)
            if code is not None and code in series and position >= 20:
                window = days[position-20:position+1]
                closes = series[code].reindex(window).map(_number).to_numpy(dtype=float)
                if (np.isfinite(closes) & (closes <= 0)).any():
                    raise ValueError('sector path known close must be positive')
                if np.isfinite(closes).all():
                    with np.errstate(over='ignore', invalid='ignore', divide='ignore'):
                        values = [closes[-1]/closes[-2]-1., closes[-1]/closes.max()-1.]
                    if not np.isfinite(values).all():
                        raise ValueError('sector path finite formula overflow')
            cache[key] = values
        result.append(cache[key])
    rows.loc[:, PATH_FEATURES] = np.asarray(result, dtype=float)
    available = rows.sector_feature_status.eq('AVAILABLE') & np.isfinite(rows.loc[:, INFORMATION_FEATURES].to_numpy(dtype=float)).all(axis=1)
    rows[STATUS] = np.where(available, 'AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')
    rows[CLOCK] = rows[KEY[0]]
    return rows


def sector_path_price_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    if (recipe.get('sector_path_price_features') != list(INFORMATION_FEATURES)
            or recipe.get('matched_information_features') != list(SECTOR_FEATURES)
            or recipe.get('d_features') != list(D_FEATURES)
            or set(models) != {'matched_mean', 'matched_path', 'candidate_mean', 'candidate_path'}
            or any(body.get('features') != (16 if name.startswith('matched') else 18) for name, body in models.items())):
        raise ValueError('sector parent raw actual 16/18 recipe/heads differ')
    return information_fit_identity_v1(recipe, models, support, model_id='M25')


def sector_path_price_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    if arm not in ('matched', 'candidate') or not rows.index.is_unique or len(rows) > 500000:
        raise ValueError('sector parent raw query arm/index/budget differs')
    if sector_path_price_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('sector parent raw fitted identity changed')
    query = rows.copy()
    for name in (*D_FEATURES, *INFORMATION_FEATURES, 'actual_gap_bps'):
        query[name] = query[name].map(_number).astype(float)
    if STATUS in query:
        if not query[STATUS].isin(('AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')).all():
            raise ValueError('sector parent raw query status differs')
        query.loc[query[STATUS].ne('AVAILABLE'), list(INFORMATION_FEATURES)] = np.nan
    if query.empty:
        return pd.DataFrame(columns=['status', 'expected_net_bps', 'downside_q90_bps'], index=query.index)
    return information_nodes_v1(fitted=fitted, rows=query, arm=arm, model_id='M25',
        information_features=INFORMATION_FEATURES, matched_information_features=SECTOR_FEATURES)


def train_sector_path_price_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M25', information_features=INFORMATION_FEATURES, status_column=STATUS,
        matched_information_features=SECTOR_FEATURES)


def sector_path_price_price_set_v1(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny=.01):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_price_set_v1
    if sector_path_price_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('sector parent raw fitted identity changed')
    features = {name: _number(value) for name, value in d_features.items()}
    return information_price_set_v1(fitted=fitted, d_features=features, arm=arm,
        reference_cny=reference_cny, legal_low_cny=legal_low_cny, legal_high_cny=legal_high_cny,
        tick_cny=tick_cny, model_id='M25', information_features=INFORMATION_FEATURES, matched_information_features=SECTOR_FEATURES)

