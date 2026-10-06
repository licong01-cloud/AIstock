"""One M11 immutable study using an existing frozen volume snapshot, zero SQL."""
import hashlib
import json
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_breadth_state_price_v1 import BreadthStatePricePlanV1
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, _verify_reference, read_stage
from backend.services.advisory_model_first.economic_market_risk_price_v1 import MarketRiskPricePlanV1
from backend.services.advisory_model_first.economic_moneyflow_price_pipeline_v1 import verify_moneyflow_budget_v1
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import MoneyflowPricePlanV1
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _ledger, _publish, _record, _root, campaign_sources_v2, implementation_sha256_v2
from backend.services.advisory_model_first.economic_price_path_value_v1 import PricePathPlanV1
from backend.services.advisory_model_first.economic_sector_price_pipeline_v1 import _fit_event, evaluate_information_study_v1, information_actual_decisions_v1, information_source_receipt_v1, load_information_fit_v1, sector_implementation_sha256_v1, train_information_study_v1
from backend.services.advisory_model_first.economic_selection_state_pipeline_v1 import SOURCE_FIELDS
from backend.services.advisory_model_first.economic_traded_price_distribution_v1 import ATR_COLUMNS, CLOCK, STATUS, TRADED_PRICE_FEATURES, TradedPriceDistributionPlanV1, traded_price_distribution_rows_v1, traded_price_fit_identity_v1, traded_price_nodes_v1, train_traded_price_v1
from backend.services.advisory_model_first.economic_volume_context_price_v1 import VolumeContextPricePlanV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

NAMES = ('economic_traded_price_distribution_v1.py', 'economic_traded_price_distribution_pipeline_v1.py')


def traded_price_implementation_sha256_v1():
    names = (*NAMES, 'economic_moneyflow_price_pipeline_v1.py', 'economic_breadth_state_price_v1.py',
        'economic_price_path_value_v1.py', 'economic_market_risk_price_v1.py', 'economic_volume_context_price_v1.py',
        'economic_moneyflow_price_v1.py', 'economic_selection_state_pipeline_v1.py', 'entry_price_daily_service.py')
    return sha(dict(algorithm='UTF8_LF_BYTES_V1', common=sector_implementation_sha256_v1(),
        files={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in names}))


def _volume_snapshot(plan, volume):
    path = _verify_reference(plan.volume_snapshot_manifest_ref)
    root = plan.campaign_root.resolve()/volume.experiment_id
    if path != root/'prepared/manifest.json':
        raise ValueError('traded price volume snapshot differs from original M9')
    registered = read_stage(root/'preregistered', stage='preregistered', plan_sha256=volume.plan_sha256, parent_sha256=None)
    read_stage(root/'prepared', stage='prepared', plan_sha256=volume.plan_sha256, parent_sha256=registered['stage_sha256'])
    if not (root/'prepared/volume_daily.parquet').is_file() or not (root/'prepared/volume_source_receipt.json').is_file():
        raise ValueError('traded price frozen volume artifacts are missing')
    return root/'prepared'


def traded_price_sources_v1(plan):
    if plan.implementation_sha256 != traded_price_implementation_sha256_v1():
        raise ValueError('traded price implementation changed')
    predecessor = _verify_reference(plan.predecessor_manifest_ref)
    breadth = BreadthStatePricePlanV1.model_validate_json((predecessor.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    volume_path = _verify_reference(breadth.predecessor_manifest_ref)
    volume = VolumeContextPricePlanV1.model_validate_json((volume_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    market_path = _verify_reference(volume.predecessor_manifest_ref)
    market = MarketRiskPricePlanV1.model_validate_json((market_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    previous_path = _verify_reference(market.predecessor_manifest_ref)
    previous = PricePathPlanV1.model_validate_json((previous_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    old_path = _verify_reference(previous.predecessor_manifest_ref)
    original = MoneyflowPricePlanV1.model_validate_json((old_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    verify_moneyflow_budget_v1(original, price_path_extension=previous, market_risk_extension=market,
        volume_context_extension=volume, breadth_state_extension=breadth, traded_price_extension=plan)
    snapshot = _volume_snapshot(plan, volume)
    adapter = PriceCampaignPlanV2(**{name: getattr(plan, name) for name in SOURCE_FIELDS},
        model_id='M2', implementation_sha256=implementation_sha256_v2())
    return (*campaign_sources_v2(adapter), snapshot)


def _traded_price_root(plan, output_root):
    root = _root(plan, output_root)
    if root.parent != plan.campaign_root.resolve():
        raise ValueError('traded price output cannot reset original budget')
    return root


def preregister_traded_price_v1(*, plan, output_root):
    declared = plan.model_dump() if isinstance(plan, TradedPriceDistributionPlanV1) else plan
    plan = TradedPriceDistributionPlanV1.model_validate(declared)
    root, source = _traded_price_root(plan, output_root), traded_price_sources_v1(plan)
    if (root/'preregistered').exists():
        read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    else:
        _publish(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None,
            artifacts={'plan.json': _json_bytes(plan.model_dump(mode='json')), 'recipe.json': _json_bytes(plan.parameters),
                'source_receipt.json': _json_bytes(information_source_receipt_v1(names=NAMES, implementation=traded_price_implementation_sha256_v1())),
                'profile_identity.json': _json_bytes(source[5])})
    _record(plan, root, source[0], 'PREREGISTERED', root/'preregistered/manifest.json')
    return root/'preregistered/plan.json'


def load_traded_price_study_v1(*, plan_path, output_root):
    plan = TradedPriceDistributionPlanV1.model_validate_json(Path(plan_path).read_text(encoding='utf-8'))
    root = _traded_price_root(plan, output_root)
    if Path(plan_path).resolve() != root/'preregistered/plan.json':
        raise ValueError('traded price requires exact preregistered plan')
    registered = read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    _ledger(plan, root, 'PREREGISTERED', root/'preregistered/manifest.json')
    source = traded_price_sources_v1(plan)
    if json.loads((root/'preregistered/recipe.json').read_text(encoding='utf-8')) != plan.parameters or sha(json.loads((root/'preregistered/profile_identity.json').read_text(encoding='utf-8'))) != sha(source[5]):
        raise ValueError('traded price frozen recipe/profile changed')
    return plan, root, registered, source


def prepare_traded_price_v1(*, plan_path, output_root):
    plan, root, registered, source = load_traded_price_study_v1(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity, feature_root, reusable, _, snapshot = source
    if (root/'prepared').exists():
        read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    else:
        rows = pd.read_parquet(reusable/'rows.parquet')
        rankings = pd.read_parquet(frozen/'frozen_rankings.parquet')
        candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)]
        if (len(rows) > 7720 or set(rows[KEY].itertuples(index=False, name=None)) != set(candidates[KEY].itertuples(index=False, name=None))
                or candidate_roster_sha256(candidates) != identity.candidate_roster_sha256):
            raise ValueError('traded price cannot replace/drop original population')
        calendar = pd.DatetimeIndex(pd.to_datetime(json.loads((frozen/'calendar.json').read_text(encoding='utf-8'))))
        atr = pd.read_parquet(feature_root/'features.parquet', columns=list(ATR_COLUMNS))
        if set(atr[KEY].itertuples(index=False, name=None)) != set(candidates[KEY].itertuples(index=False, name=None)):
            raise ValueError('traded price ATR source changed original keys')
        prices = pd.read_parquet(frozen/'raw_daily.parquet', columns=plan.parameters['stock_source_columns'])
        volumes = pd.read_parquet(snapshot/'volume_daily.parquet', columns=['trade_date', 'instrument', 'volume_hand'])
        block = traded_price_distribution_rows_v1(candidates=candidates, prices=prices, volumes=volumes, atr=atr, calendar=calendar)
        volume_receipt = json.loads((snapshot/'volume_source_receipt.json').read_text(encoding='utf-8'))
        receipt = dict(selects=0, source='EXISTING_FROZEN_M9_PREPARED', source_manifest_sha256=plan.volume_snapshot_manifest_ref.sha256,
            volume_source_receipt=volume_receipt, source_evidence='RECOVERED_LIMITED_NON_VINTAGE', native_identity='UNPROVEN',
            database_accessed=False, database_written=False)
        merged = rows.merge(block, on=KEY, how='left', validate='one_to_one', indicator=True, sort=False)
        if not merged._merge.eq('both').all() or not merged[KEY].equals(rows[KEY].reset_index(drop=True)):
            raise ValueError('traded price cannot change original key order')
        _publish(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'],
            artifacts={'rows.parquet': _parquet_bytes(merged.drop(columns='_merge')), 'traded_price_source_receipt.json': _json_bytes(receipt),
                'preparation.json': _json_bytes(dict(candidate_rows=len(rows), traded_price_status=block[STATUS].value_counts().to_dict(),
                    volume_rows=len(volumes), source_receipt=receipt, fits=0,
                    stock_source_evidence=identity.source_evidence, native_identity='UNPROVEN', sealed_accessed=False, database_written=False))})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    return root/'prepared'


def _traded_price_fit_event(plan, root, name):
    return _fit_event(plan, root, name, model_id='M11', campaign_fit_budget=43)


def train_traded_price_study_v1(*, plan_path, output_root, qe_training_idle):
    return train_information_study_v1(plan_path=plan_path, output_root=output_root, qe_training_idle=qe_training_idle,
        load_study=load_traded_price_study_v1, train_model=train_traded_price_v1, fit_event=_traded_price_fit_event)


def load_traded_price_fit_v1(*, plan_path, output_root):
    return load_information_fit_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_traded_price_study_v1, fit_identity=traded_price_fit_identity_v1)


def traded_price_actual_decisions_v1(*, fitted, candidates, inputs, prices, references, identity, arm):
    return information_actual_decisions_v1(fitted=fitted, candidates=candidates, inputs=inputs, prices=prices,
        references=references, identity=identity, arm=arm, information_features=TRADED_PRICE_FEATURES,
        information_clock=CLOCK, nodes=traded_price_nodes_v1)


def evaluate_traded_price_v1(*, plan_path, output_root):
    return evaluate_information_study_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_traded_price_study_v1, load_fit=load_traded_price_fit_v1, decisions=traded_price_actual_decisions_v1)
