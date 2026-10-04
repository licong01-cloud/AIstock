"""Fixed sector-information comparison: 13D versus 16D, four JSON heads."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import ARMS, GBDT, PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_price_campaign_models_v2 import export_gbdt_v2, predict_json_v2
from backend.services.advisory_model_first.economic_sector_price_source_v1 import SECTOR_FEATURES
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, ValueAnchorEstimateV1, ValueAnchorPriceSetV1, finite_number, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.economic_value_anchor_inference_v1 import build_value_anchor_gap_support_v1, evaluate_value_anchor_price_v1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


class SectorPricePlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_sector_price_value_v1'] = 'economic_sector_price_value_v1'
    model_id: Literal['M1'] = 'M1'
    crosswalk_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_crosswalk(self):
        if self.crosswalk_ref.role != 'sector_structural_crosswalk':
            raise ValueError('sector crosswalk role differs')
        return self

    @property
    def parameters(self):
        return dict(hypothesis='H-SECTOR-DYNAMIC-2', d_features=list(D_FEATURES), sector_features=list(SECTOR_FEATURES),
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=15, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advsectorvalue_'+self.plan_sha256[:24]


@dataclass(frozen=True)
class SectorPriceFitV1:
    recipe: dict
    models: dict
    support: object
    diagnostics: dict
    model_sha256: str


def _information_key(model_id, information_features):
    if model_id == 'M1' and tuple(information_features) == SECTOR_FEATURES:
        return 'sector_features'
    if model_id == 'M5':
        from backend.services.advisory_model_first.economic_selection_state_price_v1 import STATE_FEATURES
        if tuple(information_features) == STATE_FEATURES:
            return 'selection_features'
    if model_id == 'M6':
        from backend.services.advisory_model_first.economic_moneyflow_price_v1 import MONEYFLOW_FEATURES
        if tuple(information_features) == MONEYFLOW_FEATURES:
            return 'moneyflow_features'
    if model_id == 'M7':
        from backend.services.advisory_model_first.economic_price_path_value_v1 import PRICE_PATH_FEATURES
        if tuple(information_features) == PRICE_PATH_FEATURES:
            return 'price_path_features'
    if model_id == 'M8':
        from backend.services.advisory_model_first.economic_market_risk_price_v1 import MARKET_RISK_FEATURES
        if tuple(information_features) == MARKET_RISK_FEATURES:
            return 'market_risk_features'
    if model_id == 'M9':
        from backend.services.advisory_model_first.economic_volume_context_price_v1 import VOLUME_CONTEXT_FEATURES
        if tuple(information_features) == VOLUME_CONTEXT_FEATURES:
            return 'volume_context_features'
    if model_id == 'M10':
        from backend.services.advisory_model_first.economic_breadth_state_price_v1 import BREADTH_STATE_FEATURES
        if tuple(information_features) == BREADTH_STATE_FEATURES:
            return 'breadth_state_features'
    raise ValueError('fixed information block/model differs')


def information_fit_identity_v1(recipe, models, support, *, model_id):
    if model_id not in ('M1', 'M5', 'M6', 'M7', 'M8', 'M9', 'M10'):
        raise ValueError('fixed information model differs')
    return sha(dict(model_id=model_id, recipe=recipe, models=models, support=list(support.intervals_bps)))


def information_matrix_v1(rows, *, arm, information_features):
    if arm not in ARMS:
        raise ValueError('sector model arm differs')
    fields = [*D_FEATURES, *(information_features if arm == 'candidate' else ()), 'actual_gap_bps']
    raw = rows.loc[:, fields]
    if raw.map(lambda value: isinstance(value, (bool, np.bool_))).any().any():
        raise ValueError('sector boolean is not numeric')
    values = raw.to_numpy(dtype=float)
    if not np.isfinite(values).all() or (values[:, -1] <= -10000).any():
        raise ValueError('sector model requires finite positive-price coordinates')
    values[:, -1] /= 100
    return values


def predict_information_price_v1(*, fitted, rows, arm, model_id, information_features):
    information_key = _information_key(model_id, information_features)
    if (information_fit_identity_v1(fitted.recipe, fitted.models, fitted.support, model_id=model_id) != fitted.model_sha256
            or fitted.recipe['d_features'] != list(D_FEATURES) or fitted.recipe[information_key] != list(information_features)):
        raise ValueError('sector fitted identity/order differs')
    matrix = information_matrix_v1(rows, arm=arm, information_features=information_features)
    mean, lower = (predict_json_v2(fitted.models[arm+'_'+head], matrix) for head in ('mean', 'path'))
    return [ValueAnchorEstimateV1(float(a), float(b)) for a, b in zip(mean, lower, strict=True)]


def information_nodes_v1(*, fitted, rows, arm, model_id, information_features):
    _information_key(model_id, information_features)
    if arm not in ARMS or not rows.index.is_unique or len(rows) > 500000:
        raise ValueError('sector query arm/index/budget differs')
    if information_fit_identity_v1(fitted.recipe, fitted.models, fitted.support, model_id=model_id) != fitted.model_sha256:
        raise ValueError('sector fitted identity changed')
    fields = [*D_FEATURES, *information_features, 'actual_gap_bps']
    raw = rows.loc[:, fields]
    if raw.map(lambda value: isinstance(value, (bool, np.bool_))).any().any():
        raise ValueError('sector boolean is not numeric')
    values = raw.to_numpy(dtype=float)
    available = np.isfinite(values).all(axis=1) & np.array([np.isfinite(gap) and fitted.support.contains(float(gap)) for gap in values[:, -1]])
    result = pd.DataFrame(dict(status=['UNKNOWN_INPUT_OR_SUPPORT']*len(rows), expected_net_bps=[None]*len(rows),
        downside_q90_bps=[None]*len(rows)), index=rows.index)
    if available.any():
        selected = rows.loc[available]
        for index, estimate in zip(selected.index, predict_information_price_v1(fitted=fitted, rows=selected, arm=arm,
                model_id=model_id, information_features=information_features), strict=True):
            point = evaluate_value_anchor_price_v1(estimate=estimate, reference_cny=1.,
                price_cny=1+float(rows.loc[index, 'actual_gap_bps'])/10000, support=fitted.support)
            result.loc[index] = [point.status, point.expected_net_bps, point.downside_q90_bps]
    return result


def information_price_set_v1(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny,
        model_id, information_features, tick_cny=.01):
    reference, low, high, tick = (finite_number(value, positive=True) for value in (reference_cny, legal_low_cny, legal_high_cny, tick_cny))
    if low > high or set(d_features) != set(D_FEATURES)|set(information_features):
        raise ValueError('sector legal bounds/D schema differs')
    step = Decimal(str(tick))
    first, last = (int((Decimal(str(value))/step).to_integral_value(rounding=mode))
        for value, mode in ((low, ROUND_CEILING), (high, ROUND_FLOOR)))
    if last-first+1 > 100000 or last < first:
        raise ValueError('sector legal price tick budget differs')
    prices = [float(step*index) for index in range(first, last+1)]
    rows = pd.DataFrame([{**d_features, 'actual_gap_bps': float((step*index/Decimal(str(reference))-1)*10000)} for index in range(first, last+1)])
    nodes = information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id=model_id, information_features=information_features)
    intervals, start, end = [], None, None
    for price, status in zip(prices, nodes.status, strict=True):
        if status == 'ACCEPTABLE':
            if start is None:
                start = price
            end = price
        elif start is not None:
            intervals.append((start, end))
            start = end = None
    if start is not None:
        intervals.append((start, end))
    status = 'ACCEPTABLE' if intervals else 'AVOID' if nodes.status.eq('AVOID').any() else 'UNKNOWN_INPUT_OR_SUPPORT'
    return ValueAnchorPriceSetV1(status, tuple(intervals), valuation_semantics='OBSERVED_PRICE_CONDITIONAL_NOT_CAUSAL_LIMIT_FILL')


def train_information_price_v1(*, rows, configuration, before_fit, model_id, information_features, status_column):
    import scipy
    import sklearn
    from sklearn.ensemble import GradientBoostingRegressor
    from threadpoolctl import threadpool_limits
    if (sklearn.__version__, scipy.__version__) != ('1.8.0', '1.16.3'):
        raise ValueError('sector exact fit runtime differs')
    information_key = _information_key(model_id, information_features)
    if status_column != {'M1': 'sector_feature_status', 'M5': 'state_feature_status', 'M6': 'moneyflow_feature_status', 'M7': 'price_path_feature_status', 'M8': 'market_risk_feature_status', 'M9': 'volume_context_feature_status', 'M10': 'breadth_state_feature_status'}[model_id]:
        raise ValueError('fixed information availability contract differs')
    domain = rows.loc[rows.split.eq('train') & rows.values_available].copy()
    domain.loc[domain[KEY[1]].gt(pd.Timestamp(configuration.train_end)), 'actual_gap_bps'] = np.nan
    support = build_value_anchor_gap_support_v1(domain)
    finite_information = np.isfinite(rows.loc[:, information_features].to_numpy(dtype=float)).all(axis=1)
    common = rows.training_eligible & rows.values_available & finite_information & rows[status_column].eq('AVAILABLE') & rows.actual_gap_bps.map(
        lambda gap: bool(pd.notna(gap) and support.contains(float(gap))))
    train = rows.loc[rows.split.eq('train') & common].sort_values(KEY)
    ends = pd.to_datetime(train.label_information_end)
    if (len(train) < 100 or train[KEY[0]].nunique() < 20 or not support.intervals_bps
            or ends.isna().any() or ends.gt(pd.Timestamp(configuration.train_end)).any()
            or train[KEY[1]].gt(pd.Timestamp(configuration.train_end)).any()):
        raise ValueError('sector lacks mature common training/support')
    recipe = dict(d_features=list(D_FEATURES), **{information_key: list(information_features)},
        common_supervision_sha256=sha([[str(value) for value in key] for key in train[[*KEY, 'label_information_end']].itertuples(index=False, name=None)]))
    models, count = {}, 0
    for arm in ARMS:
        matrix = information_matrix_v1(train, arm=arm, information_features=information_features)
        for head, target in (('mean', train.gross_value_ratio), ('path', train.path_min_value_ratio)):
            estimator = GradientBoostingRegressor(**GBDT, loss='quantile' if head == 'path' else 'squared_error', alpha=.1)
            before_fit(arm+'_'+head)
            count += 1
            with threadpool_limits(limits=2):
                estimator.fit(matrix, target)
            body = export_gbdt_v2(estimator)
            if not np.allclose(predict_json_v2(body, matrix), estimator.predict(matrix), rtol=1e-10, atol=1e-10):
                raise ValueError('sector public tree/JSON parity differs')
            models[arm+'_'+head] = body
    diagnostics = dict(train_rows=len(train), train_days=train[KEY[0]].nunique(), candidate_rows=len(rows),
        fitted_head_count=count, index_build_count=0, candidate_count=1, test_used_for_training_or_calibration=False,
        decision_use='NAVIGATION_ONLY', deployable=False)
    fitted = SectorPriceFitV1(recipe, models, support, diagnostics, information_fit_identity_v1(recipe, models, support, model_id=model_id))
    validation = rows.loc[rows.split.eq('validation') & common]
    diagnostics['validation_diagnostics_only'] = {}
    if not validation.empty:
        for arm in ARMS:
            estimates = predict_information_price_v1(fitted=fitted, rows=validation, arm=arm,
                model_id=model_id, information_features=information_features)
            means = np.array([value.mean_gross_value_ratio for value in estimates])
            lower = np.array([value.path_min_ratio_q10 for value in estimates])
            error = validation.path_min_value_ratio.to_numpy()-lower
            diagnostics['validation_diagnostics_only'][arm] = dict(supported_rows=len(validation), used_for_selection=False,
                mean_squared_error=float(np.mean((validation.gross_value_ratio.to_numpy()-means)**2)),
                path_pinball_loss=float(np.mean(np.maximum(.1*error, -.9*error))),
                path_lower_coverage=float(np.mean(validation.path_min_value_ratio.to_numpy() < lower)))
    return fitted


# Existing M1 signatures, recipe fields and identity formula stay exact.
def sector_fit_identity_v1(recipe, models, support):
    return information_fit_identity_v1(recipe, models, support, model_id='M1')


def sector_matrix_v1(rows, *, arm):
    return information_matrix_v1(rows, arm=arm, information_features=SECTOR_FEATURES)


def predict_sector_price_v1(*, fitted, rows, arm):
    return predict_information_price_v1(fitted=fitted, rows=rows, arm=arm, model_id='M1', information_features=SECTOR_FEATURES)


def sector_nodes_v1(*, fitted, rows, arm):
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M1', information_features=SECTOR_FEATURES)


def sector_price_set_v1(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny=.01):
    return information_price_set_v1(fitted=fitted, d_features=d_features, arm=arm, reference_cny=reference_cny,
        legal_low_cny=legal_low_cny, legal_high_cny=legal_high_cny, tick_cny=tick_cny, model_id='M1', information_features=SECTOR_FEATURES)


def train_sector_price_v1(*, rows, configuration, before_fit):
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M1', information_features=SECTOR_FEATURES, status_column='sector_feature_status')
