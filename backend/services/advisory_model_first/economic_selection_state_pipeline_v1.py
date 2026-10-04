"""Thin M5 consumer of frozen rankings and the shared price-value kernel."""
import hashlib
import json
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, _verify_reference, read_stage
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _ledger, _publish, _record, _root, campaign_sources_v2, implementation_sha256_v2
from backend.services.advisory_model_first.economic_sector_price_pipeline_v1 import _fit_event, evaluate_information_study_v1, information_actual_decisions_v1, information_source_receipt_v1, load_information_fit_v1, sector_implementation_sha256_v1, train_information_study_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPricePlanV1
from backend.services.advisory_model_first.economic_selection_state_price_v1 import STATE_FEATURES, SelectionStatePricePlanV1, selection_state_fit_identity_v1, selection_state_nodes_v1, selection_state_rows_v1, train_selection_state_price_v1
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

NAMES = ('economic_selection_state_price_v1.py', 'economic_selection_state_pipeline_v1.py')
SOURCE_FIELDS = ('parent_plan_ref', 'parent_prepared_manifest_ref', 'feature_manifest_ref', 'reusable_prepared_manifest_ref',
                 'profile_path', 'profile_sha256', 'universe_selection')


def selection_state_implementation_sha256_v1():
    return sha(dict(algorithm='UTF8_LF_BYTES_V1', common=sector_implementation_sha256_v1(),
        files={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in NAMES}))


def _same_sources(left, right):
    for name in SOURCE_FIELDS:
        a, b = getattr(left, name), getattr(right, name)
        if isinstance(a, EvidenceReferenceV1) and isinstance(b, EvidenceReferenceV1):
            if (a.model_dump(exclude={'artifact_uri'}) != b.model_dump(exclude={'artifact_uri'})
                    or Path(a.artifact_uri).resolve() != Path(b.artifact_uri).resolve()):
                return False
        elif name == 'profile_path':
            if Path(a).resolve() != Path(b).resolve():
                return False
        elif a != b:
            return False
    return True


def verify_selection_budget_anchor_v1(plan):
    anchor = _verify_reference(plan.budget_anchor_ref)
    original_root = anchor.parent.parent
    original = PriceCampaignPlanV2.model_validate_json((anchor.parent/'plan.json').read_text(encoding='utf-8'))
    if (original.model_id != 'M2' or original_root.name != original.experiment_id
            or original_root.parent != plan.campaign_root.resolve() or not _same_sources(original, plan)
            or original.parameters['policy_sha256'] != plan.parameters['policy_sha256']
            or original.parameters['cost_sha256'] != plan.parameters['cost_sha256']):
        raise ValueError('selection original budget anchor source/root/policy differs')
    read_stage(anchor.parent, stage='preregistered', plan_sha256=original.plan_sha256, parent_sha256=None)
    _ledger(original, original_root, 'PREREGISTERED', anchor)
    events = [json.loads(line) for line in (original_root.parent/'campaign_fit_journal.jsonl').read_text(encoding='utf-8').splitlines()]
    if any(event.get('state') != 'STARTED' or event.get('model_id') not in ('M1', 'M2', 'M3', 'M4', 'M5')
            or (event.get('model_id') == 'M5' and (event.get('experiment_id') != plan.experiment_id or event.get('campaign_id') != plan.campaign_id))
            for event in events):
        raise ValueError('selection cumulative journal contains foreign identity')
    for model, count in (('M2', 4), ('M3', 5), ('M4', 2)):
        found = [event for event in events if event.get('kind') == 'PHYSICAL_FIT' and event.get('model_id') == model]
        if (len(found) != count or any(event.get('campaign_id') != original.campaign_id or event.get('state') != 'STARTED' for event in found)
                or len({event['experiment_id'] for event in found}) != 1
                or (model == 'M2' and any(event['experiment_id'] != original.experiment_id for event in found))
                or len({(event['experiment_id'], event['head']) for event in found}) != count):
            raise ValueError('selection original cumulative fit journal missing/contradictory')
        prior_root = original_root.parent/found[0]['experiment_id']
        prior = PriceCampaignPlanV2.model_validate_json((prior_root/'preregistered/plan.json').read_text(encoding='utf-8'))
        if prior.model_id != model or prior_root.name != prior.experiment_id or not _same_sources(prior, plan):
            raise ValueError('selection original journal study identity differs')
        read_stage(prior_root/'preregistered', stage='preregistered', plan_sha256=prior.plan_sha256, parent_sha256=None)
        _ledger(prior, prior_root, 'PREREGISTERED', prior_root/'preregistered/manifest.json')
    indices = [event for event in events if event.get('kind') == 'INDEX_BUILD']
    if (len(indices) != 1 or indices[0].get('model_id') != 'M4' or indices[0].get('campaign_id') != original.campaign_id
            or indices[0].get('state') != 'STARTED' or any(event.get('kind') not in ('PHYSICAL_FIT', 'INDEX_BUILD') for event in events)):
        raise ValueError('selection original index journal missing/contradictory')
    return original_root.parent, events


def verify_selection_predecessor_v1(plan, campaign_root, events):
    records = AdvisoryResearchTrialRegistryV1(campaign_root/'trial_registry.jsonl').read()
    prior = [record for record in records if record.attempt_id == 'advisory_price_r2_20261004_M1' and record.research_stage == 'EVALUATED']
    if len(prior) != 1 or len(prior[0].evidence_refs) != 1:
        raise ValueError('selection requires one terminal M1 before formal study')
    manifest_path = _verify_reference(prior[0].evidence_refs[0])
    old_root = manifest_path.parent.parent
    old = SectorPricePlanV1.model_validate_json((old_root/'preregistered/plan.json').read_text(encoding='utf-8'))
    if (old_root.parent != campaign_root or old_root.name != old.experiment_id or prior[0].experiment_id != old.experiment_id
            or not _same_sources(old, plan) or manifest_path != old_root/'evaluated/manifest.json'):
        raise ValueError('selection M1 predecessor source/root differs')
    registered = read_stage(old_root/'preregistered', stage='preregistered', plan_sha256=old.plan_sha256, parent_sha256=None)
    prepared = read_stage(old_root/'prepared', stage='prepared', plan_sha256=old.plan_sha256, parent_sha256=registered['stage_sha256'])
    trained = read_stage(old_root/'trained', stage='trained', plan_sha256=old.plan_sha256, parent_sha256=prepared['stage_sha256'])
    read_stage(manifest_path.parent, stage='evaluated', plan_sha256=old.plan_sha256, parent_sha256=trained['stage_sha256'])
    report = json.loads((manifest_path.parent/'evaluation.json').read_text(encoding='utf-8'))
    if (report.get('plan_sha256') != old.plan_sha256 or report.get('decision_use') != 'NAVIGATION_ONLY'
            or report.get('deployable') is not False or report.get('sealed_accessed') is not False
            or report.get('navigation') not in ('CONSIDER_CONFIRMATION_DESIGN_ONLY',
                'STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION', 'BLOCKED_EXECUTION_OR_MARK_UNPROVEN')
            or sum(event.get('kind') == 'PHYSICAL_FIT' and event.get('model_id') == 'M1' and event.get('experiment_id') == old.experiment_id for event in events) != 4
            or any(event.get('model_id') == 'M1' and event.get('experiment_id') != old.experiment_id for event in events)):
        raise ValueError('selection M1 terminal identity or cumulative fit accounting differs')
    return old.experiment_id


def selection_state_sources_v1(plan):
    if plan.implementation_sha256 != selection_state_implementation_sha256_v1():
        raise ValueError('selection implementation changed')
    campaign_root, events = verify_selection_budget_anchor_v1(plan)
    verify_selection_predecessor_v1(plan, campaign_root, events)
    adapter = PriceCampaignPlanV2(**{name: getattr(plan, name) for name in SOURCE_FIELDS},
        model_id='M2', implementation_sha256=implementation_sha256_v2())
    return campaign_sources_v2(adapter)


def _selection_root(plan, output_root):
    root = _root(plan, output_root)
    if root.parent != plan.campaign_root.resolve():
        raise ValueError('selection output root cannot reset original budget')
    return root


def preregister_selection_state_v1(*, plan, output_root):
    plan = SelectionStatePricePlanV1.model_validate(plan)
    root = _selection_root(plan, output_root)
    source = selection_state_sources_v1(plan)
    if (root/'preregistered').exists():
        read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    else:
        _publish(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None,
            artifacts={'plan.json': _json_bytes(plan.model_dump(mode='json')), 'recipe.json': _json_bytes(plan.parameters),
                'source_receipt.json': _json_bytes(information_source_receipt_v1(names=NAMES, implementation=selection_state_implementation_sha256_v1())),
                'profile_identity.json': _json_bytes(source[5])})
    _record(plan, root, source[0], 'PREREGISTERED', root/'preregistered/manifest.json')
    return root/'preregistered/plan.json'


def load_selection_state_v1(*, plan_path, output_root):
    plan = SelectionStatePricePlanV1.model_validate_json(Path(plan_path).read_text(encoding='utf-8'))
    root = _selection_root(plan, output_root)
    if Path(plan_path).resolve() != root/'preregistered/plan.json':
        raise ValueError('selection requires exact preregistered plan')
    registered = read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    _ledger(plan, root, 'PREREGISTERED', root/'preregistered/manifest.json')
    source = selection_state_sources_v1(plan)
    if (json.loads((root/'preregistered/recipe.json').read_text(encoding='utf-8')) != plan.parameters
            or sha(json.loads((root/'preregistered/profile_identity.json').read_text(encoding='utf-8'))) != sha(source[5])):
        raise ValueError('selection frozen recipe/profile changed')
    return plan, root, registered, source


def prepare_selection_state_v1(*, plan_path, output_root):
    plan, root, registered, source = load_selection_state_v1(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity, _, reusable, _ = source
    if (root/'prepared').exists():
        read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    else:
        rows = pd.read_parquet(reusable/'rows.parquet')
        rankings = pd.read_parquet(frozen/'frozen_rankings.parquet')
        candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)]
        if (len(rows) > 7720 or set(rows[KEY].itertuples(index=False, name=None)) != set(candidates[KEY].itertuples(index=False, name=None))
                or candidate_roster_sha256(candidates) != identity.candidate_roster_sha256):
            raise ValueError('selection original population differs')
        block = selection_state_rows_v1(candidates=candidates, rankings=rankings,
            calendar=json.loads((frozen/'calendar.json').read_text(encoding='utf-8')))
        merged = rows.merge(block, on=KEY, how='outer', validate='one_to_one', indicator=True)
        if not merged._merge.eq('both').all():
            raise ValueError('selection cannot replace/drop original keys')
        _publish(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'],
            artifacts={'rows.parquet': _parquet_bytes(merged.drop(columns='_merge')), 'preparation.json': _json_bytes(dict(
                candidate_rows=len(rows), state_status=block.state_feature_status.value_counts().to_dict(), fits=0,
                source_evidence=identity.source_evidence, native_identity='UNPROVEN', sealed_accessed=False, database_written=False))})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    return root/'prepared'


def _selection_fit_event(plan, root, name):
    return _fit_event(plan, root, name, model_id='M5', campaign_fit_budget=19)


def train_selection_state_study_v1(*, plan_path, output_root, qe_training_idle):
    return train_information_study_v1(plan_path=plan_path, output_root=output_root, qe_training_idle=qe_training_idle,
        load_study=load_selection_state_v1, train_model=train_selection_state_price_v1, fit_event=_selection_fit_event)


def load_selection_state_fit_v1(*, plan_path, output_root):
    return load_information_fit_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_selection_state_v1, fit_identity=selection_state_fit_identity_v1)


def selection_state_actual_decisions_v1(*, fitted, candidates, inputs, prices, references, identity, arm):
    return information_actual_decisions_v1(fitted=fitted, candidates=candidates, inputs=inputs, prices=prices,
        references=references, identity=identity, arm=arm, information_features=STATE_FEATURES,
        information_clock='state_feature_visible_through', nodes=selection_state_nodes_v1)


def evaluate_selection_state_v1(*, plan_path, output_root):
    return evaluate_information_study_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_selection_state_v1, load_fit=load_selection_state_fit_v1, decisions=selection_state_actual_decisions_v1)
