"""M8 immutable daily consumer; one readonly index query, original stock snapshot."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _frame, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, _verify_reference, read_stage
from backend.services.advisory_model_first.economic_market_risk_price_v1 import MARKET_RISK_FEATURES, MarketRiskPricePlanV1, market_risk_fit_identity_v1, market_risk_nodes_v1, market_risk_rows_v1, train_market_risk_price_v1
from backend.services.advisory_model_first.economic_moneyflow_price_pipeline_v1 import verify_moneyflow_budget_v1
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import MoneyflowPricePlanV1
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _ledger, _publish, _record, _root, campaign_sources_v2, implementation_sha256_v2
from backend.services.advisory_model_first.economic_price_path_value_v1 import PricePathPlanV1
from backend.services.advisory_model_first.economic_sector_price_pipeline_v1 import _fit_event, evaluate_information_study_v1, information_actual_decisions_v1, information_source_receipt_v1, load_information_fit_v1, sector_implementation_sha256_v1, train_information_study_v1
from backend.services.advisory_model_first.economic_selection_state_pipeline_v1 import SOURCE_FIELDS
from backend.services.advisory_model_first.entry_price_daily_service import BoundedEntryReadSession, EntryWorkBudget
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

NAMES = ('economic_market_risk_price_v1.py', 'economic_market_risk_price_pipeline_v1.py')


def market_risk_implementation_sha256_v1():
    names = (*NAMES, 'economic_moneyflow_price_pipeline_v1.py', 'economic_price_path_value_v1.py',
        'economic_moneyflow_price_v1.py', 'economic_selection_state_pipeline_v1.py', 'entry_price_daily_service.py')
    return sha(dict(algorithm='UTF8_LF_BYTES_V1', common=sector_implementation_sha256_v1(),
        files={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in names}))


def market_risk_sources_v1(plan):
    if plan.implementation_sha256 != market_risk_implementation_sha256_v1():
        raise ValueError('market risk implementation changed')
    predecessor = _verify_reference(plan.predecessor_manifest_ref)
    previous = PricePathPlanV1.model_validate_json((predecessor.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    old_path = _verify_reference(previous.predecessor_manifest_ref)
    original = MoneyflowPricePlanV1.model_validate_json((old_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    verify_moneyflow_budget_v1(original, price_path_extension=previous, market_risk_extension=plan)
    adapter = PriceCampaignPlanV2(**{name: getattr(plan, name) for name in SOURCE_FIELDS},
        model_id='M2', implementation_sha256=implementation_sha256_v2())
    return campaign_sources_v2(adapter)


def _market_risk_root(plan, output_root):
    root = _root(plan, output_root)
    if root.parent != plan.campaign_root.resolve():
        raise ValueError('market risk output cannot reset original budget')
    return root


def load_market_risk_index_v1(*, dates, session_factory=None):
    days = pd.DatetimeIndex([_day(day) for day in dates])
    if days.empty or len(days) > 10000 or not days.is_unique or not days.is_monotonic_increasing:
        raise ValueError('market risk index request dates differ')
    session = (session_factory or (lambda: BoundedEntryReadSession(EntryWorkBudget(30))))()
    query_at = datetime.now(timezone.utc).isoformat()
    try:
        with session.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''SELECT trade_date,ts_code AS instrument,close FROM market.index_daily
                    WHERE ts_code=%s AND trade_date=ANY(%s::date[]) AND trade_date BETWEEN %s AND %s
                    ORDER BY trade_date LIMIT %s''', ('000300.SH', [day.date() for day in days], days[0].date(), days[-1].date(), 10001))
                values = cursor.fetchall()
                names = [item.name for item in cursor.description]
    finally:
        session.close()
    if names != ['trade_date', 'instrument', 'close'] or len(values) > 10000:
        raise ValueError('market risk index source schema/row budget differs')
    frame = _frame(pd.DataFrame(values, columns=names), names[:2], set(names))
    if not frame.trade_date.isin(days).all() or not frame.instrument.eq('000300.SH').all():
        raise ValueError('market risk index returned foreign date/instrument')
    receipt = dict(query_at=query_at, selects=1, requested_days=len(days), returned_rows=len(frame),
        source='market.index_daily', instrument='000300.SH', price_unit='index_points',
        first_day=str(days[0].date()), last_day=str(days[-1].date()), source_evidence='CURRENT_DB_HISTORICAL_NON_VINTAGE',
        native_identity='UNPROVEN', database_written=False)
    return frame, receipt


def preregister_market_risk_v1(*, plan, output_root):
    plan = MarketRiskPricePlanV1.model_validate(plan)
    root, source = _market_risk_root(plan, output_root), market_risk_sources_v1(plan)
    if (root/'preregistered').exists():
        read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    else:
        _publish(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None,
            artifacts={'plan.json': _json_bytes(plan.model_dump(mode='json')), 'recipe.json': _json_bytes(plan.parameters),
                'source_receipt.json': _json_bytes(information_source_receipt_v1(names=NAMES, implementation=market_risk_implementation_sha256_v1())),
                'profile_identity.json': _json_bytes(source[5])})
    _record(plan, root, source[0], 'PREREGISTERED', root/'preregistered/manifest.json')
    return root/'preregistered/plan.json'


def load_market_risk_study_v1(*, plan_path, output_root):
    plan = MarketRiskPricePlanV1.model_validate_json(Path(plan_path).read_text(encoding='utf-8'))
    root = _market_risk_root(plan, output_root)
    if Path(plan_path).resolve() != root/'preregistered/plan.json':
        raise ValueError('market risk requires exact preregistered plan')
    registered = read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    _ledger(plan, root, 'PREREGISTERED', root/'preregistered/manifest.json')
    source = market_risk_sources_v1(plan)
    if json.loads((root/'preregistered/recipe.json').read_text(encoding='utf-8')) != plan.parameters or sha(json.loads((root/'preregistered/profile_identity.json').read_text(encoding='utf-8'))) != sha(source[5]):
        raise ValueError('market risk frozen recipe/profile changed')
    return plan, root, registered, source


def prepare_market_risk_v1(*, plan_path, output_root):
    plan, root, registered, source = load_market_risk_study_v1(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity, _, reusable, _ = source
    if (root/'prepared').exists():
        read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    else:
        rows = pd.read_parquet(reusable/'rows.parquet')
        rankings = pd.read_parquet(frozen/'frozen_rankings.parquet')
        candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)]
        if (len(rows) > 7720 or set(rows[KEY].itertuples(index=False, name=None)) != set(candidates[KEY].itertuples(index=False, name=None))
                or candidate_roster_sha256(candidates) != identity.candidate_roster_sha256):
            raise ValueError('market risk cannot replace/drop original population')
        calendar = pd.DatetimeIndex(pd.to_datetime(json.loads((frozen/'calendar.json').read_text(encoding='utf-8'))))
        benchmark, receipt = load_market_risk_index_v1(dates=calendar[calendar <= candidates[KEY[0]].max()])
        prices = pd.read_parquet(frozen/'raw_daily.parquet', columns=plan.parameters['stock_source_columns'])
        block = market_risk_rows_v1(candidates=candidates, prices=prices, benchmark=benchmark, calendar=calendar)
        merged = rows.merge(block, on=KEY, how='left', validate='one_to_one', indicator=True, sort=False)
        if not merged._merge.eq('both').all() or not merged[KEY].equals(rows[KEY].reset_index(drop=True)):
            raise ValueError('market risk cannot change original key order')
        _publish(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'],
            artifacts={'rows.parquet': _parquet_bytes(merged.drop(columns='_merge')), 'index_close.parquet': _parquet_bytes(benchmark),
                'index_source_receipt.json': _json_bytes(receipt),
                'preparation.json': _json_bytes(dict(candidate_rows=len(rows), market_risk_status=block.market_risk_feature_status.value_counts().to_dict(),
                    raw_stock_rows=len(prices), benchmark_rows=len(benchmark), source_receipt=receipt,
                    fits=0, stock_source_evidence=identity.source_evidence, native_identity='UNPROVEN',
                    sealed_accessed=False, database_written=False))})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    return root/'prepared'


def _market_risk_fit_event(plan, root, name):
    return _fit_event(plan, root, name, model_id='M8', campaign_fit_budget=31)


def train_market_risk_study_v1(*, plan_path, output_root, qe_training_idle):
    return train_information_study_v1(plan_path=plan_path, output_root=output_root, qe_training_idle=qe_training_idle,
        load_study=load_market_risk_study_v1, train_model=train_market_risk_price_v1, fit_event=_market_risk_fit_event)


def load_market_risk_fit_v1(*, plan_path, output_root):
    return load_information_fit_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_market_risk_study_v1, fit_identity=market_risk_fit_identity_v1)


def market_risk_actual_decisions_v1(*, fitted, candidates, inputs, prices, references, identity, arm):
    return information_actual_decisions_v1(fitted=fitted, candidates=candidates, inputs=inputs, prices=prices,
        references=references, identity=identity, arm=arm, information_features=MARKET_RISK_FEATURES,
        information_clock='market_risk_feature_visible_through', nodes=market_risk_nodes_v1)


def evaluate_market_risk_v1(*, plan_path, output_root):
    return evaluate_information_study_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_market_risk_study_v1, load_fit=load_market_risk_fit_v1, decisions=market_risk_actual_decisions_v1)
