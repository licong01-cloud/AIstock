"""One M19 frozen sector/flow union study, zero SQL."""
import hashlib
import json
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, _verify_reference, read_stage
from backend.services.advisory_model_first.economic_market_risk_price_v1 import MarketRiskPricePlanV1
from backend.services.advisory_model_first.economic_volume_context_price_v1 import VolumeContextPricePlanV1
from backend.services.advisory_model_first.economic_moneyflow_price_pipeline_v1 import verify_moneyflow_budget_v1
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import MoneyflowPricePlanV1
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _ledger, _publish, _record, _root, campaign_sources_v2, implementation_sha256_v2
from backend.services.advisory_model_first.economic_price_path_value_v1 import PricePathPlanV1
from backend.services.advisory_model_first.economic_sector_price_pipeline_v1 import _fit_event, evaluate_information_study_v1, information_actual_decisions_v1, information_source_receipt_v1, load_information_fit_v1, sector_implementation_sha256_v1, train_information_study_v1
from backend.services.advisory_model_first.economic_selection_state_pipeline_v1 import SOURCE_FIELDS, _same_sources
from backend.services.advisory_model_first.economic_candidate_cohort_v1 import CandidateCohortPlanV1
from backend.services.advisory_model_first.economic_flow_path_v1 import FlowPathPlanV1
from backend.services.advisory_model_first.economic_sector_moneyflow_v1 import CLOCK, STATUS, JOINT_FEATURES, MONEYFLOW_FEATURES, SOURCE_CLOCK, SOURCE_STATUS, SectorMoneyflowPlanV1, train_sector_moneyflow_v1, sector_moneyflow_fit_identity_v1, sector_moneyflow_nodes_v1, sector_moneyflow_rows_v1
from backend.services.advisory_model_first.economic_asymmetric_risk_v1 import AsymmetricRiskPlanV1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPricePlanV1
from backend.services.advisory_model_first.economic_breadth_state_price_v1 import BreadthStatePricePlanV1
from backend.services.advisory_model_first.economic_traded_price_distribution_v1 import TradedPriceDistributionPlanV1
from backend.services.advisory_model_first.economic_limit_state_v1 import LimitStatePlanV1
from backend.services.advisory_model_first.economic_valuation_context_v1 import ValuationContextPlanV1
from backend.services.advisory_model_first.economic_free_float_turnover_v1 import FreeFloatTurnoverPlanV1
from backend.services.advisory_model_first.economic_session_path_v1 import SessionPathPlanV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

NAMES = ('economic_sector_moneyflow_v1.py', 'economic_sector_moneyflow_pipeline_v1.py')


def sector_moneyflow_implementation_sha256_v1():
    names = (*NAMES, 'economic_asymmetric_risk_v1.py', 'economic_flow_path_v1.py', 'economic_candidate_cohort_v1.py', 'economic_limit_state_v1.py', 'economic_valuation_context_v1.py', 'economic_free_float_turnover_v1.py', 'economic_session_path_v1.py', 'economic_moneyflow_price_pipeline_v1.py', 'economic_traded_price_distribution_v1.py',
        'economic_breadth_state_price_v1.py', 'economic_price_path_value_v1.py',
        'economic_market_risk_price_v1.py', 'economic_volume_context_price_v1.py', 'economic_moneyflow_price_v1.py', 'economic_selection_state_pipeline_v1.py', 'entry_price_daily_service.py')
    return sha(dict(algorithm='UTF8_LF_BYTES_V1', common=sector_implementation_sha256_v1(),
        files={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in names}))


def sector_moneyflow_sources_v1(plan):
    if plan.implementation_sha256 != sector_moneyflow_implementation_sha256_v1():
        raise ValueError('sector moneyflow implementation changed')
    predecessor = _verify_reference(plan.predecessor_manifest_ref)
    asymmetric = AsymmetricRiskPlanV1.model_validate_json((predecessor.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    flow_path = _verify_reference(asymmetric.predecessor_manifest_ref)
    flow = FlowPathPlanV1.model_validate_json((flow_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    cohort_path = _verify_reference(flow.predecessor_manifest_ref)
    cohort = CandidateCohortPlanV1.model_validate_json((cohort_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    limit_path = _verify_reference(cohort.predecessor_manifest_ref)
    limit = LimitStatePlanV1.model_validate_json((limit_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    valuation_path = _verify_reference(limit.predecessor_manifest_ref)
    valuation = ValuationContextPlanV1.model_validate_json((valuation_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    free_path = _verify_reference(valuation.predecessor_manifest_ref)
    free = FreeFloatTurnoverPlanV1.model_validate_json((free_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    session_path = _verify_reference(free.predecessor_manifest_ref)
    session = SessionPathPlanV1.model_validate_json((session_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    traded_path = _verify_reference(session.predecessor_manifest_ref)
    traded = TradedPriceDistributionPlanV1.model_validate_json((traded_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    breadth_path = _verify_reference(traded.predecessor_manifest_ref)
    breadth = BreadthStatePricePlanV1.model_validate_json((breadth_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    volume_path = _verify_reference(breadth.predecessor_manifest_ref)
    volume = VolumeContextPricePlanV1.model_validate_json((volume_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    market_path = _verify_reference(volume.predecessor_manifest_ref)
    market = MarketRiskPricePlanV1.model_validate_json((market_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    previous_path = _verify_reference(market.predecessor_manifest_ref)
    previous = PricePathPlanV1.model_validate_json((previous_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    old_path = _verify_reference(previous.predecessor_manifest_ref)
    original = MoneyflowPricePlanV1.model_validate_json((old_path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    verify_moneyflow_budget_v1(original, price_path_extension=previous, market_risk_extension=market, volume_context_extension=volume, breadth_state_extension=breadth, traded_price_extension=traded, session_path_extension=session, free_float_extension=free, valuation_extension=valuation, limit_state_extension=limit, candidate_cohort_extension=cohort, flow_path_extension=flow, asymmetric_risk_extension=asymmetric, sector_moneyflow_extension=plan)
    for ref, cls in ((plan.sector_prepared_manifest_ref, SectorPricePlanV1), (plan.moneyflow_prepared_manifest_ref, MoneyflowPricePlanV1)):
        _verify_snapshot(plan, ref, cls)
    adapter = PriceCampaignPlanV2(**{name: getattr(plan, name) for name in SOURCE_FIELDS},
        model_id='M2', implementation_sha256=implementation_sha256_v2())
    return campaign_sources_v2(adapter)


def _verify_snapshot(plan, ref, cls):
    path = _verify_reference(ref)
    frozen = cls.model_validate_json((path.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    if (path != plan.campaign_root.resolve()/frozen.experiment_id/'prepared/manifest.json'
            or not _same_sources(frozen, plan)
            or frozen.parameters['policy_sha256'] != plan.parameters['policy_sha256']
            or frozen.parameters['cost_sha256'] != plan.parameters['cost_sha256']):
        raise ValueError('sector moneyflow snapshot original source/profile/policy differs')
    registered = read_stage(path.parent.parent/'preregistered', stage='preregistered', plan_sha256=frozen.plan_sha256, parent_sha256=None)
    _ledger(frozen, path.parent.parent, 'PREREGISTERED', path.parent.parent/'preregistered/manifest.json')
    read_stage(path.parent, stage='prepared', plan_sha256=frozen.plan_sha256, parent_sha256=registered['stage_sha256'])
    _ledger(frozen, path.parent.parent, 'PREPARED', path)
    return path.parent


def _sector_moneyflow_root(plan, output_root):
    root = _root(plan, output_root)
    if root.parent != plan.campaign_root.resolve():
        raise ValueError('sector moneyflow output cannot reset original budget')
    return root


def preregister_sector_moneyflow_v1(*, plan, output_root):
    declared = plan.model_dump() if isinstance(plan, SectorMoneyflowPlanV1) else plan
    plan = SectorMoneyflowPlanV1.model_validate(declared)
    receipt = information_source_receipt_v1(names=NAMES, implementation=sector_moneyflow_implementation_sha256_v1())
    root, source = _sector_moneyflow_root(plan, output_root), sector_moneyflow_sources_v1(plan)
    if (root/'preregistered').exists():
        read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    else:
        _publish(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None,
            artifacts={'plan.json': _json_bytes(plan.model_dump(mode='json')), 'recipe.json': _json_bytes(plan.parameters),
                'source_receipt.json': _json_bytes(receipt),
                'profile_identity.json': _json_bytes(source[5])})
    _record(plan, root, source[0], 'PREREGISTERED', root/'preregistered/manifest.json')
    return root/'preregistered/plan.json'


def load_sector_moneyflow_study_v1(*, plan_path, output_root):
    plan = SectorMoneyflowPlanV1.model_validate_json(Path(plan_path).read_text(encoding='utf-8'))
    root = _sector_moneyflow_root(plan, output_root)
    if Path(plan_path).resolve() != root/'preregistered/plan.json':
        raise ValueError('sector moneyflow requires exact preregistered plan')
    registered = read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    _ledger(plan, root, 'PREREGISTERED', root/'preregistered/manifest.json')
    source = sector_moneyflow_sources_v1(plan)
    if json.loads((root/'preregistered/recipe.json').read_text(encoding='utf-8')) != plan.parameters or sha(json.loads((root/'preregistered/profile_identity.json').read_text(encoding='utf-8'))) != sha(source[5]):
        raise ValueError('sector moneyflow frozen recipe/profile changed')
    return plan, root, registered, source


def prepare_sector_moneyflow_v1(*, plan_path, output_root):
    plan, root, registered, source = load_sector_moneyflow_study_v1(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity, _, reusable, _ = source
    if (root/'prepared').exists():
        read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    else:
        rows = pd.read_parquet(Path(plan.sector_prepared_manifest_ref.artifact_uri).parent/'rows.parquet')
        flow = pd.read_parquet(Path(plan.moneyflow_prepared_manifest_ref.artifact_uri).parent/'rows.parquet',
            columns=[*KEY, *MONEYFLOW_FEATURES, SOURCE_STATUS[1], SOURCE_CLOCK[1]])
        rankings = pd.read_parquet(frozen/'frozen_rankings.parquet')
        candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)]
        if candidate_roster_sha256(candidates) != identity.candidate_roster_sha256:
            raise ValueError('sector moneyflow cannot replace original population')
        calendar = pd.DatetimeIndex(pd.to_datetime(json.loads((frozen/'calendar.json').read_text(encoding='utf-8'))))
        merged = sector_moneyflow_rows_v1(sector_rows=rows, moneyflow_rows=flow, candidates=candidates, calendar=calendar)
        receipt = dict(selects=0, source=plan.parameters['source'],
            sector_manifest_sha256=plan.sector_prepared_manifest_ref.sha256,
            moneyflow_manifest_sha256=plan.moneyflow_prepared_manifest_ref.sha256,
            source_evidence='RECOVERED_LIMITED_NON_VINTAGE', native_identity='UNPROVEN',
            database_accessed=False, database_written=False)
        _publish(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'],
            artifacts={'rows.parquet': _parquet_bytes(merged),
                'sector_moneyflow_source_receipt.json': _json_bytes(receipt),
                'preparation.json': _json_bytes(dict(candidate_rows=len(rows), sector_moneyflow_status=merged[STATUS].value_counts().to_dict(),
                    source_receipt=receipt, fits=0, stock_source_evidence=identity.source_evidence,
                    native_identity='UNPROVEN', sealed_accessed=False, database_written=False))})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    return root/'prepared'


def _sector_moneyflow_fit_event(plan, root, name):
    return _fit_event(plan, root, name, model_id='M19', campaign_fit_budget=75)


def train_sector_moneyflow_study_v1(*, plan_path, output_root, qe_training_idle):
    return train_information_study_v1(plan_path=plan_path, output_root=output_root, qe_training_idle=qe_training_idle,
        load_study=load_sector_moneyflow_study_v1, train_model=train_sector_moneyflow_v1, fit_event=_sector_moneyflow_fit_event)


def load_sector_moneyflow_fit_v1(*, plan_path, output_root):
    return load_information_fit_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_sector_moneyflow_study_v1, fit_identity=sector_moneyflow_fit_identity_v1)


def sector_moneyflow_actual_decisions_v1(*, fitted, candidates, inputs, prices, references, identity, arm):
    return information_actual_decisions_v1(fitted=fitted, candidates=candidates, inputs=inputs, prices=prices,
        references=references, identity=identity, arm=arm, information_features=JOINT_FEATURES,
        information_clock=CLOCK, information_status=STATUS, nodes=sector_moneyflow_nodes_v1)


def evaluate_sector_moneyflow_v1(*, plan_path, output_root):
    return evaluate_information_study_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_sector_moneyflow_study_v1, load_fit=load_sector_moneyflow_fit_v1, decisions=sector_moneyflow_actual_decisions_v1)
