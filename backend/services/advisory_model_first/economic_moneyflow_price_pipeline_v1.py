"""One M6 immutable study over existing Advisory labels and a frozen flow snapshot."""
import hashlib
import json
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, _verify_reference, read_stage
from backend.services.advisory_model_first.economic_moneyflow_price_source_v1 import load_moneyflow_source_v1
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import MONEYFLOW_FEATURES, MoneyflowPricePlanV1, moneyflow_fit_identity_v1, moneyflow_nodes_v1, moneyflow_rows_v1, train_moneyflow_price_v1
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _ledger, _publish, _record, _root, campaign_sources_v2, implementation_sha256_v2
from backend.services.advisory_model_first.economic_sector_price_pipeline_v1 import _fit_event, evaluate_information_study_v1, information_actual_decisions_v1, information_source_receipt_v1, load_information_fit_v1, sector_implementation_sha256_v1, train_information_study_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPricePlanV1
from backend.services.advisory_model_first.economic_selection_state_pipeline_v1 import SOURCE_FIELDS, _same_sources
from backend.services.advisory_model_first.economic_selection_state_price_v1 import SelectionStatePricePlanV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

NAMES = ('economic_moneyflow_price_v1.py', 'economic_moneyflow_price_source_v1.py', 'economic_moneyflow_price_pipeline_v1.py')


def moneyflow_implementation_sha256_v1():
    repo = Path(__file__).resolve().parents[3]
    dependencies = ('backend/data_service/moneyflow_contract.py',
                    'backend/services/advisory_model_first/entry_price_daily_service.py',
                    'backend/services/advisory_model_first/economic_selection_state_pipeline_v1.py')
    return sha(dict(algorithm='UTF8_LF_BYTES_V1', common=sector_implementation_sha256_v1(),
        files={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in NAMES},
        readonly_dependencies={name: hashlib.sha256((repo/name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in dependencies}))


def verify_moneyflow_budget_v1(plan, *, price_path_extension=None, market_risk_extension=None, volume_context_extension=None, breadth_state_extension=None):
    anchor = _verify_reference(plan.budget_anchor_ref)
    root = plan.campaign_root.resolve()
    original = PriceCampaignPlanV2.model_validate_json((anchor.parent/'plan.json').read_text(encoding='utf-8'))
    predecessor = _verify_reference(plan.predecessor_manifest_ref)
    old = SelectionStatePricePlanV1.model_validate_json((predecessor.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    if (original.model_id != 'M2' or anchor.parent.parent != root/original.experiment_id
            or predecessor != root/old.experiment_id/'evaluated/manifest.json'
            or not _same_sources(original, plan) or not _same_sources(old, plan)
            or old.budget_anchor_ref.model_dump(exclude={'artifact_uri'}) != plan.budget_anchor_ref.model_dump(exclude={'artifact_uri'})
            or Path(old.budget_anchor_ref.artifact_uri).resolve() != anchor
            or old.parameters['policy_sha256'] != plan.parameters['policy_sha256']
            or old.parameters['cost_sha256'] != plan.parameters['cost_sha256']):
        raise ValueError('moneyflow original anchor/predecessor source/root/policy differs')
    journal = root/'campaign_fit_journal.jsonl'
    events = [json.loads(line) for line in journal.read_text(encoding='utf-8').splitlines()]
    if market_risk_extension is not None and price_path_extension is None:
        raise ValueError('market risk extension requires original explicit M7 extension')
    if volume_context_extension is not None and market_risk_extension is None:
        raise ValueError('volume context extension requires original explicit M8 extension')
    if breadth_state_extension is not None and volume_context_extension is None:
        raise ValueError('breadth state extension requires original explicit M9 extension')
    if price_path_extension is not None:
        from backend.services.advisory_model_first.economic_price_path_value_v1 import PricePathPlanV1
        extension = PricePathPlanV1.model_validate(price_path_extension)
        extension_predecessor = _verify_reference(extension.predecessor_manifest_ref)
        if (extension.campaign_root.resolve() != root or not _same_sources(extension, plan)
                or extension.budget_anchor_ref != plan.budget_anchor_ref
                or extension_predecessor != root/plan.experiment_id/'evaluated/manifest.json'
                or extension.parameters['policy_sha256'] != plan.parameters['policy_sha256']
                or extension.parameters['cost_sha256'] != plan.parameters['cost_sha256']):
            raise ValueError('price path extension original root/source/predecessor differs')
    if market_risk_extension is not None:
        from backend.services.advisory_model_first.economic_market_risk_price_v1 import MarketRiskPricePlanV1
        market_extension = MarketRiskPricePlanV1.model_validate(market_risk_extension)
        market_predecessor = _verify_reference(market_extension.predecessor_manifest_ref)
        if (market_extension.campaign_root.resolve() != root or not _same_sources(market_extension, extension)
                or market_extension.budget_anchor_ref != extension.budget_anchor_ref
                or market_predecessor != root/extension.experiment_id/'evaluated/manifest.json'
                or market_extension.parameters['policy_sha256'] != extension.parameters['policy_sha256']
                or market_extension.parameters['cost_sha256'] != extension.parameters['cost_sha256']):
            raise ValueError('market risk extension original root/source/predecessor differs')
    if volume_context_extension is not None:
        from backend.services.advisory_model_first.economic_volume_context_price_v1 import VolumeContextPricePlanV1
        volume_extension = VolumeContextPricePlanV1.model_validate(volume_context_extension)
        volume_predecessor = _verify_reference(volume_extension.predecessor_manifest_ref)
        if (volume_extension.campaign_root.resolve() != root or not _same_sources(volume_extension, market_extension)
                or volume_extension.budget_anchor_ref != market_extension.budget_anchor_ref
                or volume_predecessor != root/market_extension.experiment_id/'evaluated/manifest.json'
                or volume_extension.parameters['policy_sha256'] != market_extension.parameters['policy_sha256']
                or volume_extension.parameters['cost_sha256'] != market_extension.parameters['cost_sha256']):
            raise ValueError('volume context extension original root/source/predecessor differs')
    if breadth_state_extension is not None:
        from backend.services.advisory_model_first.economic_breadth_state_price_v1 import BreadthStatePricePlanV1
        declared = breadth_state_extension.model_dump() if isinstance(breadth_state_extension, BreadthStatePricePlanV1) else breadth_state_extension
        breadth_extension = BreadthStatePricePlanV1.model_validate(declared)
        breadth_predecessor = _verify_reference(breadth_extension.predecessor_manifest_ref)
        if (breadth_extension.campaign_root.resolve() != root or not _same_sources(breadth_extension, volume_extension)
                or breadth_extension.budget_anchor_ref != volume_extension.budget_anchor_ref
                or breadth_predecessor != root/volume_extension.experiment_id/'evaluated/manifest.json'
                or breadth_extension.parameters['policy_sha256'] != volume_extension.parameters['policy_sha256']
                or breadth_extension.parameters['cost_sha256'] != volume_extension.parameters['cost_sha256']):
            raise ValueError('breadth state extension original root/source/predecessor differs')
    expected = {'M2': (4, PriceCampaignPlanV2), 'M3': (5, PriceCampaignPlanV2), 'M4': (2, PriceCampaignPlanV2),
                'M1': (4, SectorPricePlanV1), 'M5': (4, SelectionStatePricePlanV1)}
    if any(event.get('state') != 'STARTED' or event.get('kind') not in ('PHYSICAL_FIT', 'INDEX_BUILD')
           or event.get('model_id') not in (*expected, 'M6', *(('M7',) if price_path_extension is not None else ()),
               *(('M8',) if market_risk_extension is not None else ()), *(('M9',) if volume_context_extension is not None else ()),
               *(('M10',) if breadth_state_extension is not None else ())) for event in events):
        raise ValueError('moneyflow cumulative journal has foreign state/model/kind')
    for model, (count, cls) in expected.items():
        found = [event for event in events if event['kind'] == 'PHYSICAL_FIT' and event['model_id'] == model]
        if len(found) != count or len({event['experiment_id'] for event in found}) != 1 or len({event['head'] for event in found}) != count:
            raise ValueError('moneyflow original cumulative fit journal missing/contradictory')
        study_root = root/found[0]['experiment_id']
        previous = cls.model_validate_json((study_root/'preregistered/plan.json').read_text(encoding='utf-8'))
        if (previous.model_id != model or study_root.name != previous.experiment_id or not _same_sources(previous, plan)
                or (model == 'M2' and previous.experiment_id != original.experiment_id)
                or (model == 'M5' and previous.experiment_id != old.experiment_id)
                or any(event['campaign_id'] != previous.campaign_id for event in found)):
            raise ValueError('moneyflow original journal study identity differs')
        registered = read_stage(study_root/'preregistered', stage='preregistered', plan_sha256=previous.plan_sha256, parent_sha256=None)
        _ledger(previous, study_root, 'PREREGISTERED', study_root/'preregistered/manifest.json')
        if model == 'M5':
            parent = registered['stage_sha256']
            for stage in ('prepared', 'trained', 'evaluated'):
                parent = read_stage(study_root/stage, stage=stage, plan_sha256=previous.plan_sha256, parent_sha256=parent)['stage_sha256']
            _ledger(previous, study_root, 'EVALUATED', predecessor)
    indices = [event for event in events if event['kind'] == 'INDEX_BUILD']
    m4 = next(event for event in events if event['kind'] == 'PHYSICAL_FIT' and event['model_id'] == 'M4')
    if len(indices) != 1 or any(indices[0].get(key) != m4.get(key) for key in ('model_id', 'campaign_id', 'experiment_id')):
        raise ValueError('moneyflow original index journal differs')
    added = [event for event in events if event['model_id'] == 'M6']
    heads = {'matched_mean', 'matched_path', 'candidate_mean', 'candidate_path'}
    if len(added) > 4 or len({event['head'] for event in added}) != len(added) or any(
        event['kind'] != 'PHYSICAL_FIT' or event['campaign_id'] != plan.campaign_id or event['experiment_id'] != plan.experiment_id or event['head'] not in heads for event in added):
        raise ValueError('moneyflow own cumulative budget/journal differs')
    if price_path_extension is not None:
        if len(added) != 4:
            raise ValueError('price path extension needs actual four completed M6 fits')
        previous_root = root/plan.experiment_id
        registered = read_stage(previous_root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
        _ledger(plan, previous_root, 'PREREGISTERED', previous_root/'preregistered/manifest.json')
        frozen = MoneyflowPricePlanV1.model_validate_json((previous_root/'preregistered/plan.json').read_text(encoding='utf-8'))
        if frozen != plan:
            raise ValueError('price path extension cannot substitute original M6 plan')
        parent = registered['stage_sha256']
        for stage in ('prepared', 'trained', 'evaluated'):
            parent = read_stage(previous_root/stage, stage=stage, plan_sha256=plan.plan_sha256, parent_sha256=parent)['stage_sha256']
        _ledger(plan, previous_root, 'EVALUATED', extension_predecessor)
        latest = [event for event in events if event['model_id'] == 'M7']
        if len(latest) > 4 or len({event['head'] for event in latest}) != len(latest) or any(
            event['kind'] != 'PHYSICAL_FIT' or event['campaign_id'] != extension.campaign_id
            or event['experiment_id'] != extension.experiment_id or event['head'] not in heads for event in latest):
            raise ValueError('price path extension own cumulative budget/journal differs')
    if market_risk_extension is not None:
        if len(latest) != 4:
            raise ValueError('market risk extension needs actual four completed M7 fits')
        previous_root = root/extension.experiment_id
        registered = read_stage(previous_root/'preregistered', stage='preregistered', plan_sha256=extension.plan_sha256, parent_sha256=None)
        _ledger(extension, previous_root, 'PREREGISTERED', previous_root/'preregistered/manifest.json')
        frozen = PricePathPlanV1.model_validate_json((previous_root/'preregistered/plan.json').read_text(encoding='utf-8'))
        if frozen != extension:
            raise ValueError('market risk extension cannot substitute original M7 plan')
        parent = registered['stage_sha256']
        for stage in ('prepared', 'trained', 'evaluated'):
            parent = read_stage(previous_root/stage, stage=stage, plan_sha256=extension.plan_sha256, parent_sha256=parent)['stage_sha256']
        _ledger(extension, previous_root, 'EVALUATED', market_predecessor)
        newest = [event for event in events if event['model_id'] == 'M8']
        if len(newest) > 4 or len({event['head'] for event in newest}) != len(newest) or any(
            event['kind'] != 'PHYSICAL_FIT' or event['campaign_id'] != market_extension.campaign_id
            or event['experiment_id'] != market_extension.experiment_id or event['head'] not in heads for event in newest):
            raise ValueError('market risk extension own cumulative budget/journal differs')
    if volume_context_extension is not None:
        if len(newest) != 4:
            raise ValueError('volume context extension needs actual four completed M8 fits')
        previous_root = root/market_extension.experiment_id
        registered = read_stage(previous_root/'preregistered', stage='preregistered', plan_sha256=market_extension.plan_sha256, parent_sha256=None)
        _ledger(market_extension, previous_root, 'PREREGISTERED', previous_root/'preregistered/manifest.json')
        frozen = MarketRiskPricePlanV1.model_validate_json((previous_root/'preregistered/plan.json').read_text(encoding='utf-8'))
        if frozen != market_extension:
            raise ValueError('volume context extension cannot substitute original M8 plan')
        parent = registered['stage_sha256']
        for stage in ('prepared', 'trained', 'evaluated'):
            parent = read_stage(previous_root/stage, stage=stage, plan_sha256=market_extension.plan_sha256, parent_sha256=parent)['stage_sha256']
        _ledger(market_extension, previous_root, 'EVALUATED', volume_predecessor)
        final = [event for event in events if event['model_id'] == 'M9']
        if len(final) > 4 or len({event['head'] for event in final}) != len(final) or any(
            event['kind'] != 'PHYSICAL_FIT' or event['campaign_id'] != volume_extension.campaign_id
            or event['experiment_id'] != volume_extension.experiment_id or event['head'] not in heads for event in final):
            raise ValueError('volume context extension own cumulative budget/journal differs')
    if breadth_state_extension is not None:
        if len(final) != 4:
            raise ValueError('breadth state extension needs actual four completed M9 fits')
        previous_root = root/volume_extension.experiment_id
        registered = read_stage(previous_root/'preregistered', stage='preregistered', plan_sha256=volume_extension.plan_sha256, parent_sha256=None)
        _ledger(volume_extension, previous_root, 'PREREGISTERED', previous_root/'preregistered/manifest.json')
        frozen = VolumeContextPricePlanV1.model_validate_json((previous_root/'preregistered/plan.json').read_text(encoding='utf-8'))
        if frozen != volume_extension:
            raise ValueError('breadth state extension cannot substitute original M9 plan')
        parent = registered['stage_sha256']
        for stage in ('prepared', 'trained', 'evaluated'):
            parent = read_stage(previous_root/stage, stage=stage, plan_sha256=volume_extension.plan_sha256, parent_sha256=parent)['stage_sha256']
        _ledger(volume_extension, previous_root, 'EVALUATED', breadth_predecessor)
        latest_breadth = [event for event in events if event['model_id'] == 'M10']
        if len(latest_breadth) > 4 or len({event['head'] for event in latest_breadth}) != len(latest_breadth) or any(
            event['kind'] != 'PHYSICAL_FIT' or event['campaign_id'] != breadth_extension.campaign_id
            or event['experiment_id'] != breadth_extension.experiment_id or event['head'] not in heads for event in latest_breadth):
            raise ValueError('breadth state extension own cumulative budget/journal differs')
    return root


def moneyflow_sources_v1(plan):
    if plan.implementation_sha256 != moneyflow_implementation_sha256_v1():
        raise ValueError('moneyflow implementation changed')
    verify_moneyflow_budget_v1(plan)
    adapter = PriceCampaignPlanV2(**{name: getattr(plan, name) for name in SOURCE_FIELDS},
        model_id='M2', implementation_sha256=implementation_sha256_v2())
    return campaign_sources_v2(adapter)


def _moneyflow_root(plan, output_root):
    root = _root(plan, output_root)
    if root.parent != plan.campaign_root.resolve():
        raise ValueError('moneyflow output cannot reset original budget')
    return root


def preregister_moneyflow_v1(*, plan, output_root):
    plan = MoneyflowPricePlanV1.model_validate(plan)
    root = _moneyflow_root(plan, output_root)
    source = moneyflow_sources_v1(plan)
    if (root/'preregistered').exists():
        read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    else:
        _publish(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None,
            artifacts={'plan.json': _json_bytes(plan.model_dump(mode='json')), 'recipe.json': _json_bytes(plan.parameters),
                'source_receipt.json': _json_bytes(information_source_receipt_v1(names=NAMES, implementation=moneyflow_implementation_sha256_v1())),
                'profile_identity.json': _json_bytes(source[5])})
    _record(plan, root, source[0], 'PREREGISTERED', root/'preregistered/manifest.json')
    return root/'preregistered/plan.json'


def load_moneyflow_study_v1(*, plan_path, output_root):
    plan = MoneyflowPricePlanV1.model_validate_json(Path(plan_path).read_text(encoding='utf-8'))
    root = _moneyflow_root(plan, output_root)
    if Path(plan_path).resolve() != root/'preregistered/plan.json':
        raise ValueError('moneyflow requires exact preregistered plan')
    registered = read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    _ledger(plan, root, 'PREREGISTERED', root/'preregistered/manifest.json')
    source = moneyflow_sources_v1(plan)
    if json.loads((root/'preregistered/recipe.json').read_text(encoding='utf-8')) != plan.parameters or sha(json.loads((root/'preregistered/profile_identity.json').read_text(encoding='utf-8'))) != sha(source[5]):
        raise ValueError('moneyflow frozen recipe/profile changed')
    return plan, root, registered, source


def prepare_moneyflow_v1(*, plan_path, output_root):
    plan, root, registered, source = load_moneyflow_study_v1(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity, _, reusable, _ = source
    if (root/'prepared').exists():
        read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    else:
        rows = pd.read_parquet(reusable/'rows.parquet')
        rankings = pd.read_parquet(frozen/'frozen_rankings.parquet')
        candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)]
        if (len(rows) > 7720 or set(rows[KEY].itertuples(index=False, name=None)) != set(candidates[KEY].itertuples(index=False, name=None))
                or candidate_roster_sha256(candidates) != identity.candidate_roster_sha256):
            raise ValueError('moneyflow cannot replace/drop original population')
        batch = load_moneyflow_source_v1(candidates=candidates)
        original_days = pd.DatetimeIndex(pd.to_datetime(json.loads((frozen/'calendar.json').read_text(encoding='utf-8'))))
        if not original_days[(original_days >= candidates[KEY[0]].min()) & (original_days <= candidates[KEY[1]].max())].equals(
                batch.calendar[(batch.calendar >= candidates[KEY[0]].min()) & (batch.calendar <= candidates[KEY[1]].max())]):
            raise ValueError('moneyflow public and original candidate calendars differ')
        block = moneyflow_rows_v1(candidates=candidates, amounts=batch.amounts, calendar=batch.calendar)
        merged = rows.merge(block, on=KEY, how='left', validate='one_to_one', indicator=True, sort=False)
        if not merged._merge.eq('both').all() or not merged[KEY].equals(rows[KEY].reset_index(drop=True)):
            raise ValueError('moneyflow cannot change original key order')
        _publish(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'],
            artifacts={'rows.parquet': _parquet_bytes(merged.drop(columns='_merge')), 'amounts_cny.parquet': _parquet_bytes(batch.amounts),
                'source_calendar.json': _json_bytes([str(day.date()) for day in batch.calendar]),
                'preparation.json': _json_bytes(dict(candidate_rows=len(rows), moneyflow_status=block.moneyflow_feature_status.value_counts().to_dict(),
                    source_receipt=batch.receipt, fits=0, source_evidence='CURRENT_DB_HISTORICAL_NON_VINTAGE',
                    native_identity='UNPROVEN', sealed_accessed=False, database_written=False))})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    return root/'prepared'


def _moneyflow_fit_event(plan, root, name):
    return _fit_event(plan, root, name, model_id='M6', campaign_fit_budget=23)


def train_moneyflow_study_v1(*, plan_path, output_root, qe_training_idle):
    return train_information_study_v1(plan_path=plan_path, output_root=output_root, qe_training_idle=qe_training_idle,
        load_study=load_moneyflow_study_v1, train_model=train_moneyflow_price_v1, fit_event=_moneyflow_fit_event)


def load_moneyflow_fit_v1(*, plan_path, output_root):
    return load_information_fit_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_moneyflow_study_v1, fit_identity=moneyflow_fit_identity_v1)


def moneyflow_actual_decisions_v1(*, fitted, candidates, inputs, prices, references, identity, arm):
    return information_actual_decisions_v1(fitted=fitted, candidates=candidates, inputs=inputs, prices=prices,
        references=references, identity=identity, arm=arm, information_features=MONEYFLOW_FEATURES,
        information_clock='moneyflow_feature_visible_through', nodes=moneyflow_nodes_v1)


def evaluate_moneyflow_v1(*, plan_path, output_root):
    return evaluate_information_study_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_moneyflow_study_v1, load_fit=load_moneyflow_fit_v1, decisions=moneyflow_actual_decisions_v1)
