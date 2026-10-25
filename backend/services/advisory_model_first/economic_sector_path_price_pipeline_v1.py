"""One frozen PIT sector path price study, zero SQL and no old model re-run."""
import hashlib
import json
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, _verify_reference, read_stage
from backend.services.advisory_model_first.economic_sector_path_price_v1 import CLOCK, INFORMATION_FEATURES, STATUS, SectorPathPricePlanV1, sector_path_price_fit_identity_v1, sector_path_price_nodes_v1, sector_path_price_rows_v1, load_sector_path_quotes_v1, train_sector_path_price_v1
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _ledger, _publish, _record, _root, campaign_sources_v2, implementation_sha256_v2
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPricePlanV1, sector_fit_identity_v1
from backend.services.advisory_model_first.economic_parent_normalized_trajectory_v1 import ParentNormalizedTrajectoryPlanV1, parent_normalized_trajectory_fit_identity_v1
from backend.services.advisory_model_first.economic_sector_moneyflow_pipeline_v1 import _verify_snapshot
from backend.services.advisory_model_first.economic_sector_price_pipeline_v1 import _fit_event, evaluate_information_study_v1, information_actual_decisions_v1, information_source_receipt_v1, load_information_fit_v1, sector_implementation_sha256_v1, train_information_study_v1
from backend.services.advisory_model_first.economic_selection_state_pipeline_v1 import SOURCE_FIELDS, _same_sources
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

NAMES = ('economic_sector_path_price_v1.py', 'economic_sector_path_price_pipeline_v1.py')


def sector_path_price_implementation_sha256_v1():
    names = (*NAMES, 'economic_parent_normalized_trajectory_v1.py', 'economic_sector_moneyflow_v1.py', 'economic_sector_moneyflow_pipeline_v1.py', 'economic_selection_state_pipeline_v1.py', 'economic_volume_context_price_v1.py')
    return sha(dict(algorithm='UTF8_LF_BYTES_V1', common=sector_implementation_sha256_v1(),
        files={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in names}))


def verify_sector_path_price_budget_v1(plan):
    path = _verify_reference(plan.predecessor_manifest_ref)
    _verify_reference(plan.budget_anchor_ref)
    prior_root = path.parent.parent
    prior = ParentNormalizedTrajectoryPlanV1.model_validate_json((prior_root/'preregistered/plan.json').read_text(encoding='utf-8'))
    root = plan.campaign_root.resolve()
    if (path.resolve() != root/prior.experiment_id/'evaluated/manifest.json'
            or not _same_sources(prior, plan) or prior.budget_anchor_ref != plan.budget_anchor_ref
            or any(prior.parameters[name] != plan.parameters[name] for name in ('policy_sha256', 'cost_sha256'))):
        raise ValueError('sector path actual M24 root/source/policy differs')
    parent = None
    for stage in ('preregistered', 'prepared', 'trained', 'evaluated'):
        parent = read_stage(prior_root/stage, stage=stage, plan_sha256=prior.plan_sha256, parent_sha256=parent)['stage_sha256']
        _ledger(prior, prior_root, stage.upper(), prior_root/stage/'manifest.json')
    body = json.loads((prior_root/'trained/metadata.json').read_text(encoding='utf-8'))
    support = ValueAnchorGapSupportV1(tuple(tuple(pair) for pair in body['support']['intervals_bps']))
    if (body['parameters'] != prior.parameters or body['diagnostics']['fitted_head_count'] != 4
            or body['diagnostics']['index_build_count'] != 0
            or parent_normalized_trajectory_fit_identity_v1(body['recipe'], body['models'], support) != body['model_sha256']):
        raise ValueError('sector path actual M24 completion differs')
    # The immutable byte prefix includes all 95 real fits plus the one old index.
    # Do not compare an old producer's implementation hash to this new source.
    journal = root/'campaign_fit_journal.jsonl'
    if not journal.is_file() or journal.stat().st_size > 512000:
        raise ValueError('sector path original fit journal missing/budget differs')
    lines = journal.read_bytes().splitlines(keepends=True)
    if len(lines) < 96 or hashlib.sha256(b''.join(lines[:96])).hexdigest() != plan.prior_fit_journal_sha256:
        raise ValueError('sector path original 95-fit journal prefix changed')
    prefix = [json.loads(line) for line in lines[:96]]
    expected = {f'M{i}': 5 if i == 3 else 2 if i == 4 else 4 for i in range(1, 25)}
    if any(e.get('state') != 'STARTED' or e.get('kind') not in ('PHYSICAL_FIT', 'INDEX_BUILD') or e.get('model_id') not in expected for e in prefix):
        raise ValueError('sector path original journal state/model differs')
    for model, count in expected.items():
        events = [e for e in prefix if e['model_id'] == model and e['kind'] == 'PHYSICAL_FIT']
        if (len(events) != count or len({e.get('head') for e in events}) != count
                or len({(e.get('campaign_id'), e.get('experiment_id')) for e in events}) != 1):
            raise ValueError('sector path original physical-fit counts/identity differ')
        if model == 'M24' and any((e['campaign_id'], e['experiment_id']) != (prior.campaign_id, prior.experiment_id) for e in events):
            raise ValueError('sector path predecessor journal identity differs')
    indices = [e for e in prefix if e['kind'] == 'INDEX_BUILD']
    m4 = next(e for e in prefix if e['model_id'] == 'M4' and e['kind'] == 'PHYSICAL_FIT')
    if len(indices) != 1 or any(indices[0].get(name) != m4.get(name) for name in ('model_id', 'campaign_id', 'experiment_id')):
        raise ValueError('sector path old index identity differs')
    added = [json.loads(line) for line in lines[96:]]
    heads = {'matched_mean', 'matched_path', 'candidate_mean', 'candidate_path'}
    if (len(added) > 4 or len({e.get('head') for e in added}) != len(added)
            or any(e.get('state') != 'STARTED' or e.get('kind') != 'PHYSICAL_FIT'
                or (e.get('model_id'), e.get('campaign_id'), e.get('experiment_id')) != (plan.model_id, plan.campaign_id, plan.experiment_id)
                or e.get('head') not in heads for e in added)):
        raise ValueError('sector path cumulative99/foreign or duplicate fit differs')
    return prior


def sector_path_price_sources_v1(plan):
    if plan.implementation_sha256 != sector_path_price_implementation_sha256_v1():
        raise ValueError('sector path implementation changed')
    verify_sector_path_price_budget_v1(plan)
    path = _verify_reference(plan.sector_prepared_manifest_ref)
    original_root = path.parent.parent
    original = SectorPricePlanV1.model_validate_json((original_root/'preregistered/plan.json').read_text(encoding='utf-8'))
    _verify_snapshot(plan, plan.sector_prepared_manifest_ref, SectorPricePlanV1)
    if original.crosswalk_ref != plan.crosswalk_ref:
        raise ValueError('sector path original M1 crosswalk differs')
    parent = None
    for stage in ('preregistered', 'prepared', 'trained', 'evaluated'):
        parent = read_stage(original_root/stage, stage=stage, plan_sha256=original.plan_sha256, parent_sha256=parent)['stage_sha256']
        _ledger(original, original_root, stage.upper(), original_root/stage/'manifest.json')
    body = json.loads((original_root/'trained/metadata.json').read_text(encoding='utf-8'))
    support = ValueAnchorGapSupportV1(tuple(tuple(pair) for pair in body['support']['intervals_bps']))
    if (body['parameters'] != original.parameters or body['diagnostics']['fitted_head_count'] != 4
            or body['diagnostics']['index_build_count'] != 0
            or sector_fit_identity_v1(body['recipe'], body['models'], support) != body['model_sha256']):
        raise ValueError('sector path original M1 completion differs')
    adapter = PriceCampaignPlanV2(**{name: getattr(plan, name) for name in SOURCE_FIELDS},
        model_id='M2', implementation_sha256=implementation_sha256_v2())
    return campaign_sources_v2(adapter)

def _raw_root(plan, output_root):
    root = _root(plan, output_root)
    if root.parent != plan.campaign_root.resolve():
        raise ValueError('sector path output cannot reset original budget')
    return root


def preregister_sector_path_price_v1(*, plan, output_root):
    plan = SectorPathPricePlanV1.model_validate(plan.model_dump() if isinstance(plan, SectorPathPricePlanV1) else plan)
    receipt = information_source_receipt_v1(names=NAMES, implementation=sector_path_price_implementation_sha256_v1())
    root, source = _raw_root(plan, output_root), sector_path_price_sources_v1(plan)
    if (root/'preregistered').exists():
        read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    else:
        _publish(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None,
            artifacts={'plan.json': _json_bytes(plan.model_dump(mode='json')), 'recipe.json': _json_bytes(plan.parameters),
                'source_receipt.json': _json_bytes(receipt), 'profile_identity.json': _json_bytes(source[5])})
    _record(plan, root, source[0], 'PREREGISTERED', root/'preregistered/manifest.json')
    return root/'preregistered/plan.json'


def load_sector_path_price_study_v1(*, plan_path, output_root):
    plan = SectorPathPricePlanV1.model_validate_json(Path(plan_path).read_text(encoding='utf-8'))
    root = _raw_root(plan, output_root)
    if Path(plan_path).resolve() != root/'preregistered/plan.json':
        raise ValueError('sector path requires exact preregistered plan')
    registered = read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    _ledger(plan, root, 'PREREGISTERED', root/'preregistered/manifest.json')
    source = sector_path_price_sources_v1(plan)
    if (json.loads((root/'preregistered/recipe.json').read_text(encoding='utf-8')) != plan.parameters
            or sha(json.loads((root/'preregistered/profile_identity.json').read_text(encoding='utf-8'))) != sha(source[5])):
        raise ValueError('sector path frozen recipe/profile changed')
    return plan, root, registered, source


def prepare_sector_path_price_v1(*, plan_path, output_root):
    plan, root, registered, source = load_sector_path_price_study_v1(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity = source[:3]
    if (root/'prepared').exists():
        read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    else:
        fields = [*KEY, 'is_candidate_decision', 'selection_effective_rank']
        rankings = pd.read_parquet(frozen/'frozen_rankings.parquet', columns=fields)
        if len(rankings) > plan.parameters['maximum_ranking_rows']:
            raise ValueError('sector path frozen ranking budget differs')
        candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)]
        if candidate_roster_sha256(candidates) != identity.candidate_roster_sha256:
            raise ValueError('sector path cannot replace original candidates')
        calendar = pd.DatetimeIndex(pd.to_datetime(json.loads((frozen/'calendar.json').read_text(encoding='utf-8'))))
        base = pd.read_parquet(Path(plan.sector_prepared_manifest_ref.artifact_uri).parent/'rows.parquet')
        quotes, crosswalk, summary = load_sector_path_quotes_v1(plan=plan, base_rows=base, calendar=calendar)
        rows = sector_path_price_rows_v1(base_rows=base, quotes=quotes, candidates=candidates,
            calendar=calendar, crosswalk=crosswalk)
        receipt = {**summary, 'sector_manifest_sha256': plan.sector_prepared_manifest_ref.sha256,
            'source': plan.parameters['source'], 'path_window_sessions': 21}
        _publish(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'],
            artifacts={'rows.parquet': _parquet_bytes(rows), 'sector_path_price_source_receipt.json': _json_bytes(receipt),
                'preparation.json': _json_bytes(dict(candidate_rows=len(rows), decisions=rows[KEY[0]].nunique(),
                    sector_path_price_status=rows[STATUS].value_counts().to_dict(), source_receipt=receipt,
                    fits=0, stock_source_evidence=identity.source_evidence, native_identity='UNPROVEN',
                    sealed_accessed=False, database_accessed=False, database_written=False))})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    return root/'prepared'

def _raw_fit_event(plan, root, name):
    return _fit_event(plan, root, name, model_id='M25', campaign_fit_budget=99)


def train_sector_path_price_study_v1(*, plan_path, output_root, qe_training_idle):
    return train_information_study_v1(plan_path=plan_path, output_root=output_root, qe_training_idle=qe_training_idle,
        load_study=load_sector_path_price_study_v1, train_model=train_sector_path_price_v1, fit_event=_raw_fit_event)


def load_sector_path_price_fit_v1(*, plan_path, output_root):
    return load_information_fit_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_sector_path_price_study_v1, fit_identity=sector_path_price_fit_identity_v1)


def sector_path_price_actual_decisions_v1(*, fitted, candidates, inputs, prices, references, identity, arm):
    return information_actual_decisions_v1(fitted=fitted, candidates=candidates, inputs=inputs, prices=prices,
        references=references, identity=identity, arm=arm, information_features=INFORMATION_FEATURES,
        information_clock=CLOCK, information_status=STATUS, nodes=sector_path_price_nodes_v1)


def evaluate_sector_path_price_v1(*, plan_path, output_root):
    return evaluate_information_study_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_sector_path_price_study_v1, load_fit=load_sector_path_price_fit_v1, decisions=sector_path_price_actual_decisions_v1)
