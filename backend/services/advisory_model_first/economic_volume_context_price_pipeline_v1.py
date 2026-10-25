"""One immutable M9 daily study, exact readonly volume keys and no QE dispatch."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_entry_labels import KEY, _frame, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, _verify_reference, read_stage
from backend.services.advisory_model_first.economic_market_risk_price_v1 import MarketRiskPricePlanV1
from backend.services.advisory_model_first.economic_moneyflow_price_pipeline_v1 import verify_moneyflow_budget_v1
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import MoneyflowPricePlanV1
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _ledger, _publish, _record, _root, campaign_sources_v2, implementation_sha256_v2
from backend.services.advisory_model_first.economic_price_path_value_v1 import PricePathPlanV1
from backend.services.advisory_model_first.economic_sector_price_pipeline_v1 import _fit_event, evaluate_information_study_v1, information_actual_decisions_v1, information_source_receipt_v1, load_information_fit_v1, sector_implementation_sha256_v1, train_information_study_v1
from backend.services.advisory_model_first.economic_selection_state_pipeline_v1 import SOURCE_FIELDS
from backend.services.advisory_model_first.economic_volume_context_price_v1 import VOLUME_CONTEXT_FEATURES, VolumeContextPricePlanV1, train_volume_context_price_v1, volume_context_fit_identity_v1, volume_context_nodes_v1, volume_context_requests_v1, volume_context_rows_v1
from backend.services.advisory_model_first.entry_price_daily_service import BoundedEntryReadSession, EntryWorkBudget
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

NAMES = ('economic_volume_context_price_v1.py', 'economic_volume_context_price_pipeline_v1.py')


def volume_context_implementation_sha256_v1():
    names = (*NAMES, 'economic_moneyflow_price_pipeline_v1.py', 'economic_price_path_value_v1.py',
        'economic_market_risk_price_v1.py', 'economic_moneyflow_price_v1.py', 'economic_selection_state_pipeline_v1.py', 'entry_price_daily_service.py')
    return sha(dict(algorithm='UTF8_LF_BYTES_V1', common=sector_implementation_sha256_v1(),
        files={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in names}))


def volume_context_sources_v1(plan):
    if plan.implementation_sha256 != volume_context_implementation_sha256_v1():
        raise ValueError('volume context implementation changed')
    predecessor = _verify_reference(plan.predecessor_manifest_ref)
    market = MarketRiskPricePlanV1.model_validate_json((predecessor.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    previous_path = _verify_reference(market.predecessor_manifest_ref)
    previous = PricePathPlanV1.model_validate_json((previous_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    old_path = _verify_reference(previous.predecessor_manifest_ref)
    original = MoneyflowPricePlanV1.model_validate_json((old_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    verify_moneyflow_budget_v1(original, price_path_extension=previous, market_risk_extension=market, volume_context_extension=plan)
    adapter = PriceCampaignPlanV2(**{name: getattr(plan, name) for name in SOURCE_FIELDS},
        model_id='M2', implementation_sha256=implementation_sha256_v2())
    return campaign_sources_v2(adapter)


def _volume_context_root(plan, output_root):
    root = _root(plan, output_root)
    if root.parent != plan.campaign_root.resolve():
        raise ValueError('volume context output cannot reset original budget')
    return root


def load_volume_context_v1(*, candidates, calendar, session_factory=None):
    pairs = volume_context_requests_v1(candidates=candidates, calendar=calendar)
    if not pairs or len(pairs) > 154400:
        raise ValueError('volume context request pair budget differs')
    session = (session_factory or (lambda: BoundedEntryReadSession(EntryWorkBudget(30))))()
    query_at = datetime.now(timezone.utc).isoformat()
    try:
        with session.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''SELECT price.trade_date,price.ts_code AS instrument,price.volume_hand
                    FROM market.kline_daily_raw price JOIN unnest(%s::date[],%s::text[]) request(day,instrument)
                      ON request.day=price.trade_date AND request.instrument=price.ts_code
                    WHERE price.trade_date BETWEEN %s AND %s ORDER BY price.trade_date,price.ts_code LIMIT %s''',
                    ([day.date() for day, _ in pairs], [symbol for _, symbol in pairs], pairs[0][0].date(), pairs[-1][0].date(), len(pairs)+1))
                values = cursor.fetchall()
                names = [item.name for item in cursor.description]
    finally:
        session.close()
    if names != ['trade_date', 'instrument', 'volume_hand'] or len(values) > len(pairs):
        raise ValueError('volume context schema/returned row budget differs')
    frame = _frame(pd.DataFrame(values, columns=names), names[:2], set(names))
    if not set(frame[names[:2]].itertuples(index=False, name=None)).issubset(set(pairs)):
        raise ValueError('volume context returned foreign date/instrument')
    receipt = dict(query_at=query_at, selects=1, requested_pairs=len(pairs), returned_rows=len(frame), requested_keys_sha256=sha(
        [[str(day.date()), symbol] for day, symbol in pairs]), source='market.kline_daily_raw', volume_unit='hands_100_shares',
        first_day=str(pairs[0][0].date()), last_day=str(pairs[-1][0].date()), source_evidence='CURRENT_DB_HISTORICAL_NON_VINTAGE',
        native_identity='UNPROVEN', database_written=False)
    return frame, receipt


def preregister_volume_context_v1(*, plan, output_root):
    plan = VolumeContextPricePlanV1.model_validate(plan)
    root, source = _volume_context_root(plan, output_root), volume_context_sources_v1(plan)
    if (root/'preregistered').exists():
        read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    else:
        _publish(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None,
            artifacts={'plan.json': _json_bytes(plan.model_dump(mode='json')), 'recipe.json': _json_bytes(plan.parameters),
                'source_receipt.json': _json_bytes(information_source_receipt_v1(names=NAMES, implementation=volume_context_implementation_sha256_v1())),
                'profile_identity.json': _json_bytes(source[5])})
    _record(plan, root, source[0], 'PREREGISTERED', root/'preregistered/manifest.json')
    return root/'preregistered/plan.json'


def load_volume_context_study_v1(*, plan_path, output_root):
    plan = VolumeContextPricePlanV1.model_validate_json(Path(plan_path).read_text(encoding='utf-8'))
    root = _volume_context_root(plan, output_root)
    if Path(plan_path).resolve() != root/'preregistered/plan.json':
        raise ValueError('volume context requires exact preregistered plan')
    registered = read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    _ledger(plan, root, 'PREREGISTERED', root/'preregistered/manifest.json')
    source = volume_context_sources_v1(plan)
    if json.loads((root/'preregistered/recipe.json').read_text(encoding='utf-8')) != plan.parameters or sha(json.loads((root/'preregistered/profile_identity.json').read_text(encoding='utf-8'))) != sha(source[5]):
        raise ValueError('volume context frozen recipe/profile changed')
    return plan, root, registered, source


def prepare_volume_context_v1(*, plan_path, output_root):
    plan, root, registered, source = load_volume_context_study_v1(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity, _, reusable, _ = source
    if (root/'prepared').exists():
        read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    else:
        rows = pd.read_parquet(reusable/'rows.parquet')
        rankings = pd.read_parquet(frozen/'frozen_rankings.parquet')
        candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)]
        if (len(rows) > 7720 or set(rows[KEY].itertuples(index=False, name=None)) != set(candidates[KEY].itertuples(index=False, name=None))
                or candidate_roster_sha256(candidates) != identity.candidate_roster_sha256):
            raise ValueError('volume context cannot replace/drop original population')
        calendar = pd.DatetimeIndex(pd.to_datetime(json.loads((frozen/'calendar.json').read_text(encoding='utf-8'))))
        volumes, receipt = load_volume_context_v1(candidates=candidates, calendar=calendar)
        prices = pd.read_parquet(frozen/'raw_daily.parquet', columns=plan.parameters['stock_source_columns'])
        block = volume_context_rows_v1(candidates=candidates, prices=prices, volumes=volumes, calendar=calendar)
        merged = rows.merge(block, on=KEY, how='left', validate='one_to_one', indicator=True, sort=False)
        if not merged._merge.eq('both').all() or not merged[KEY].equals(rows[KEY].reset_index(drop=True)):
            raise ValueError('volume context cannot change original key order')
        _publish(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'],
            artifacts={'rows.parquet': _parquet_bytes(merged.drop(columns='_merge')), 'volume_daily.parquet': _parquet_bytes(volumes),
                'volume_source_receipt.json': _json_bytes(receipt),
                'preparation.json': _json_bytes(dict(candidate_rows=len(rows), volume_context_status=block.volume_context_feature_status.value_counts().to_dict(),
                    raw_stock_rows=len(prices), volume_rows=len(volumes), source_receipt=receipt, fits=0,
                    stock_source_evidence=identity.source_evidence, native_identity='UNPROVEN', sealed_accessed=False, database_written=False))})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    return root/'prepared'


def _volume_context_fit_event(plan, root, name):
    return _fit_event(plan, root, name, model_id='M9', campaign_fit_budget=35)


def train_volume_context_study_v1(*, plan_path, output_root, qe_training_idle):
    return train_information_study_v1(plan_path=plan_path, output_root=output_root, qe_training_idle=qe_training_idle,
        load_study=load_volume_context_study_v1, train_model=train_volume_context_price_v1, fit_event=_volume_context_fit_event)


def load_volume_context_fit_v1(*, plan_path, output_root):
    return load_information_fit_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_volume_context_study_v1, fit_identity=volume_context_fit_identity_v1)


def volume_context_actual_decisions_v1(*, fitted, candidates, inputs, prices, references, identity, arm):
    return information_actual_decisions_v1(fitted=fitted, candidates=candidates, inputs=inputs, prices=prices,
        references=references, identity=identity, arm=arm, information_features=VOLUME_CONTEXT_FEATURES,
        information_clock='volume_context_feature_visible_through', nodes=volume_context_nodes_v1)


def evaluate_volume_context_v1(*, plan_path, output_root):
    return evaluate_information_study_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_volume_context_study_v1, load_fit=load_volume_context_fit_v1, decisions=volume_context_actual_decisions_v1)
