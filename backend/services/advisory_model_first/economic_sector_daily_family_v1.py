"""M1 15D plus observed-price query, using the original frozen JSON heads."""
from copy import deepcopy
from datetime import date
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import _number, _records
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_sector_classification_source_v1 import original_daily_keys_v1
from backend.services.advisory_model_first.economic_sector_daily_core_v1 import FEATURES, RECIPE, _calendar
from backend.services.advisory_model_first.economic_sector_price_value_v1 import sector_nodes_v1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.price_range_regulatory import resolve_regulatory_price_range
from backend.services.advisory_model_first.realtime_feature_source import PriceRangeRealtimeContext
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

FAMILY = 'M1_SECTOR_PRICE_VALUE_V1'
SCOPE_KEYS = ('package_id', 'program_id', 'manifest_sha256', 'parent_policy_identity',
              'policy_identity', 'universe_selection')
PACKET_KEYS = {'candidates', 'calendar', 'component_roles', 'terminal_weights', 'scope', 'price_contexts'}


def _invalid(message):
    raise AdvisoryModelFirstError(message, reason_code='ADVISORY_SECTOR_DAILY_FAMILY_INVALID')


def _price_set(fitted, features, context, decision, target):
    empty = dict(intervals=[], legal_node_count=0, unknown_node_count=0)
    if context is None:
        return dict(status='QUERY_DOMAIN_UNAVAILABLE', **empty)
    if (not isinstance(context, PriceRangeRealtimeContext) or type(context.decision_price_trade_date) is not date
            or context.decision_price_trade_date != decision or type(context.list_date) is not date
            or context.list_date > decision or type(context.listed_trading_days) is not int
            or context.listed_trading_days <= 0 or type(context.target_is_st) is not bool
            or context.board_type not in {'MAIN', 'CHINEXT', 'STAR', 'BSE'}
            or context.decision_price_source != 'market.kline_daily_raw.close_li'
            or context.price_unit_divisor != 1000.):
        _invalid('M1 price context is not the original D raw-CNY coordinate')
    close, multiplier, tick = (_number(value, positive=True, required=True) for value in
                              (context.decision_raw_close, context.target_raw_price_multiplier, context.tick_size))
    reference = close * multiplier
    if not np.isfinite(reference):
        _invalid('M1 price reference is nonfinite')
    domain = resolve_regulatory_price_range(context, target_trade_date=target)
    if domain.status != 'LIMITED':
        return dict(status='QUERY_DOMAIN_UNAVAILABLE', rule_id=domain.rule_id, **empty)
    step = Decimal(str(tick))
    first, last = (int((Decimal(str(value))/step).to_integral_value(rounding=mode))
                   for value, mode in ((domain.low, ROUND_CEILING), (domain.high, ROUND_FLOOR)))
    count = last-first+1
    if not 0 < count <= 5000:
        return dict(status='QUERY_DOMAIN_OVER_BUDGET', requested_node_count=count, rule_id=domain.rule_id, **empty)
    prices = [float(step*index) for index in range(first, last+1)]
    rows = pd.DataFrame([{**features, 'actual_gap_bps': float((step*index/Decimal(str(reference))-1)*10000)}
                         for index in range(first, last+1)])
    nodes = sector_nodes_v1(fitted=fitted, rows=rows, arm='candidate')
    intervals, group = [], []

    def finish():
        if group:
            selected = nodes.iloc[group]
            intervals.append(dict(low_cny=prices[group[0]], high_cny=prices[group[-1]],
                tick_cny=tick, node_count=len(group), expected_net_bps_min=float(selected.expected_net_bps.min()),
                expected_net_bps_max=float(selected.expected_net_bps.max()),
                downside_q90_bps_max=float(selected.downside_q90_bps.max())))
            group.clear()

    for index, status in enumerate(nodes.status):
        if status == 'ACCEPTABLE':
            group.append(index)
        else:
            finish()
    finish()
    status = 'ACCEPTABLE_PRICE_SET' if intervals else 'NO_ACCEPTABLE_PRICE' if nodes.status.eq('AVOID').any() else 'UNKNOWN_INPUT_OR_SUPPORT'
    return dict(status=status, intervals=intervals, legal_node_count=count,
        unknown_node_count=int(nodes.status.eq('UNKNOWN_INPUT_OR_SUPPORT').sum()),
        reference_cny=reference, rule_id=domain.rule_id)


class EconomicSectorPriceDailyFamilyV1:
    """One computation path for single-day and bounded historical batches."""

    def __init__(self, *, bundle, source, classification_source):
        if bundle.plan.model_id != 'M1':
            _invalid('M1 daily family requires the actual M1 bundle')
        self._bundle, self._source, self._classification = bundle, source, classification_source

    def predict_day(self, **packet):
        return self.predict_batch(packets=[packet])[0]

    def predict_batch(self, *, packets):
        if not isinstance(packets, (list, tuple)) or not 1 <= len(packets) <= 20:
            _invalid('M1 daily batch needs one to twenty original day packets')
        self._bundle.verify_unchanged()
        packets, inputs, class_receipts = deepcopy(packets), [], []
        for packet in packets:
            if not isinstance(packet, dict) or set(packet) != PACKET_KEYS:
                _invalid('M1 daily packet schema differs')
            _calendar(packet['calendar'])
            keys = original_daily_keys_v1(packet['candidates'])
            if (not keys[KEY[0]].eq(pd.Timestamp(packet['calendar'][-2])).all()
                    or not keys[KEY[1]].eq(pd.Timestamp(packet['calendar'][-1])).all()
                    or not isinstance(packet['scope'], dict) or set(packet['scope']) != set(SCOPE_KEYS)
                    or any(packet['scope'][key] != self._bundle.scope[key] for key in SCOPE_KEYS)
                    or not isinstance(packet['price_contexts'], dict)
                    or set(packet['price_contexts']) - set(keys.instrument)
                    or any(value is not None and (not isinstance(value, PriceRangeRealtimeContext) or value.symbol != symbol)
                           for symbol, value in packet['price_contexts'].items())):
                _invalid('M1 daily input scope, dates or price-context keys differ')
            classification, receipt = self._classification.load_day(candidates=packet['candidates'])
            inputs.append({key: packet[key] for key in ('candidates', 'calendar', 'component_roles', 'terminal_weights')}
                          | {'classification_rows': classification})
            class_receipts.append(receipt)
        outputs = self._source.load_batch(packets=inputs)
        if len(outputs) != len(packets):
            _invalid('M1 daily source changed the number of original day packets')
        results = []
        for packet, (features, receipt), classification_receipt in zip(packets, outputs, class_receipts, strict=True):
            keys = original_daily_keys_v1(packet['candidates'])
            if (not isinstance(features, pd.DataFrame) or list(features.columns) != [*KEY, *FEATURES]
                    or not isinstance(receipt, dict)
                    or len(features) != len(keys)
                    or len(keys) and not features[KEY].reset_index(drop=True).equals(keys.reset_index(drop=True))
                    or receipt.get('candidate_count') != len(keys)
                    or receipt.get('semantics_sha256') != sha(RECIPE)
                    or receipt.get('feature_sha256') != sha(_records(features))
                    or receipt.get('identity_sha256') != sha({key: value for key, value in receipt.items() if key != 'identity_sha256'})):
                _invalid('M1 daily source content or original candidate order differs')
            features.loc[:, FEATURES].map(_number)
            candidates = []
            for position, row in enumerate(features.to_dict('records')):
                symbol = row['instrument']
                context = packet['price_contexts'].get(symbol)
                candidates.append(dict(instrument=symbol, selection_effective_rank=int(packet['candidates'].iloc[position].selection_effective_rank),
                    **_price_set(self._bundle.fitted, {key: row[key] for key in FEATURES}, context,
                                 packet['calendar'][-2], packet['calendar'][-1])))
            result = dict(schema_version='economic_sector_daily_prediction_v1', model_family=FAMILY,
                decision_date=packet['calendar'][-2].isoformat(), target_date=packet['calendar'][-1].isoformat(),
                scope=packet['scope'], model_sha256=self._bundle.fitted.model_sha256,
                bundle_sha256=self._bundle.bundle_sha256, feature_receipt=receipt, classification_receipt=classification_receipt,
                candidates=candidates, status='COMPUTED' if candidates else 'NO_CANDIDATES',
                valuation_semantics='OBSERVED_PRICE_CONDITIONAL_NOT_CAUSAL_LIMIT_FILL',
                expected_net_semantics='CONDITIONAL_NET_BPS_NOT_WIN_PROBABILITY',
                downside_semantics='PATH_LOSS_Q90_NOT_CONFIDENCE_INTERVAL_OR_EXIT_TARGET',
                decision_use=self._bundle.decision_use, deployable=self._bundle.deployable,
                model_source_evidence=self._bundle.source_evidence, model_native_identity=self._bundle.native_identity,
                package_qualification_rechecked=False, database_written=False, outcomes_read=False, fit_count=0)
            result['prediction_sha256'] = sha(result)
            results.append(result)
        self._bundle.verify_unchanged()
        return results
