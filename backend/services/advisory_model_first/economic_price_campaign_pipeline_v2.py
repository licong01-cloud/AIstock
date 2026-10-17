"""Minimal immutable consumer-only campaign using existing Advisory stages."""
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

import pandas as pd

from backend.services.advisory_model_first.economic_context_consumer_v1 import load_economic_context_source_v1
from backend.services.advisory_model_first.economic_context_value_contracts_v1 import ContextValuePlanV1
from backend.services.advisory_model_first.economic_context_value_pipeline_v1 import context_implementation_sha256_v1, context_observations_v1
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, _verify_reference, file_sha256, publish_stage, read_stage
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_price_campaign_models_v2 import PriceCampaignFitV2, fit_identity_v2, train_campaign_v2
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, ValueAnchorGapSupportV1, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.economic_value_anchor_pipeline_v1 import _value_prices, value_anchor_implementation_sha256_v1, value_anchor_sources_v1
from backend.services.advisory_model_first.economic_value_anchor_training_v1 import ValueAnchorStudyPlanV1, assemble_value_anchor_rows_v1
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

NAMES = tuple(f'economic_price_campaign_{name}_v2.py' for name in ('contracts', 'models', 'inference', 'pipeline', 'evaluation'))


def implementation_sha256_v2():
    files = {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in NAMES}
    # Also binds borrowed observation, bootstrap/navigation, source and shadow implementations.
    return sha(dict(algorithm='UTF8_LF_BYTES_V1', files=files, source_and_shadow=context_implementation_sha256_v1()))


def _root(plan, output_root):
    declared = Path(output_root)
    root = declared.resolve()
    if not declared.is_absolute() or root != declared.absolute() or root.drive.upper() == 'C:':
        raise ValueError('campaign needs original absolute non-C output root')
    return root/plan.experiment_id


def _publish(**kwargs):
    import psutil
    if sum(len(body) for body in kwargs['artifacts'].values()) > 512*1024**2 or psutil.Process().memory_info().rss > 2*1024**3:
        raise ValueError('campaign publication exceeds RSS/artifact budget')
    return publish_stage(**kwargs)


def campaign_sources_v2(plan):
    if plan.implementation_sha256 != implementation_sha256_v2():
        raise ValueError('campaign implementation changed')
    refs = {name: {**getattr(plan, name).model_dump(mode='json'), 'role': role} for name, role in
        (('parent_plan_ref', 'value_parent_plan'), ('parent_prepared_manifest_ref', 'value_prices_snapshot'), ('feature_manifest_ref', 'value_d_snapshot'))}
    # This authorizes the already-consumed window BEFORE parsing outcome arrays.
    parent, frozen, identity, features = value_anchor_sources_v1(ValueAnchorStudyPlanV1(**refs, implementation_sha256=value_anchor_implementation_sha256_v1()))
    profile = load_economic_context_source_v1(profile_path=plan.profile_path, profile_sha256=plan.profile_sha256,
        universe_selection=plan.universe_selection)
    reused = _verify_reference(plan.reusable_prepared_manifest_ref)
    old_root = reused.parent.parent
    old_plan = ContextValuePlanV1.model_validate_json((old_root/'preregistered/plan.json').read_text(encoding='utf-8'))
    if (reused != old_root/'prepared/manifest.json' or old_root.name != old_plan.experiment_id
            or old_plan.implementation_sha256 != context_implementation_sha256_v1()
            or old_plan.profile_path != plan.profile_path or old_plan.profile_sha256 != plan.profile_sha256
            or old_plan.universe_selection != plan.universe_selection
            or any(getattr(old_plan, name).sha256 != getattr(plan, name).sha256
                   or getattr(old_plan, name).artifact_uri != getattr(plan, name).artifact_uri
                for name in ('parent_plan_ref', 'parent_prepared_manifest_ref', 'feature_manifest_ref'))
            or old_plan.parameters['policy_sha256'] != plan.parameters['policy_sha256']
            or old_plan.parameters['cost_sha256'] != plan.parameters['cost_sha256']):
        raise ValueError('campaign reusable labels source/policy/profile differs')
    registered = read_stage(old_root/'preregistered', stage='preregistered', plan_sha256=old_plan.plan_sha256, parent_sha256=None)
    read_stage(reused.parent, stage='prepared', plan_sha256=old_plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    profile.verify_unchanged()
    return parent, frozen, identity, features, reused.parent, profile.identity


def _record(plan, root, parent, stage, evidence, *, generated=0, evaluated=0):
    config = parent.configuration
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id=plan.campaign_id+'_'+plan.model_id,
        research_stage=stage, study_type=plan.study_type, hypothesis_family_id='economic_entry_price_value',
        parent_lineage=(parent.experiment_id, plan.campaign_id), unique_variable=plan.parameters['hypothesis'],
        objective_contract=plan.objective_contract, dataset_identity=parent.dataset_identity, schema_identity=plan.schema_version,
        policy_identity=sha(dict(parent_source_policy=parent.policy_identity, value_policy=value_anchor_policy_sha256_v1(), cost=COST.policy_sha256)),
        planned_trial_count=1, generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(window_id='P0C_DEVELOPMENT_V1', dataset_identity=parent.dataset_identity,
            start_date=config.train_start, end_date=config.label_cutoff),),
        result_class='CONTROL_READY' if stage == 'PREREGISTERED' else 'EXPLORATORY', decision_use='NAVIGATION_ONLY',
        evidence_refs=(evidence_reference_for_file(evidence, role='campaign_'+stage.lower()),))
    return AdvisoryResearchTrialRegistryV1(root.parent/'trial_registry.jsonl').append_batch((record,))


def _ledger(plan, root, stage, evidence):
    found = [row for row in AdvisoryResearchTrialRegistryV1(root.parent/'trial_registry.jsonl').read()
        if row.experiment_id == plan.experiment_id and row.research_stage == stage]
    if len(found) != 1 or len(found[0].evidence_refs) != 1 or found[0].evidence_refs[0].sha256 != file_sha256(evidence):
        raise ValueError('campaign stage and registry disagree')


def _source_receipt():
    repo = Path(__file__).resolve().parents[3]
    head = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=repo, check=True, capture_output=True).stdout.decode().strip()
    if subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', 'backend/services/advisory_model_first', 'backend/services/advisory_list_transition.py'], cwd=repo).returncode:
        raise ValueError('campaign source dependencies are not committed')
    blobs = {}
    for name in NAMES:
        relative = 'backend/services/advisory_model_first/'+name
        body = subprocess.run(['git', 'show', f'{head}:{relative}'], cwd=repo, check=True, capture_output=True).stdout
        if body.replace(b'\r\n', b'\n') != (repo/relative).read_bytes().replace(b'\r\n', b'\n'):
            raise ValueError('campaign source/Git blob differ')
        blobs[relative] = hashlib.sha256(body).hexdigest()
    return dict(head=head, git_blob_content_sha256=blobs, implementation_sha256=implementation_sha256_v2())


def preregister_campaign_v2(*, plan, output_root):
    plan = PriceCampaignPlanV2.model_validate(plan)
    source = campaign_sources_v2(plan)
    root = _root(plan, output_root)
    if (root/'preregistered').exists():
        read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
        _record(plan, root, source[0], 'PREREGISTERED', root/'preregistered/manifest.json')
        return root/'preregistered/plan.json'
    path = _publish(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None,
        artifacts={'plan.json': _json_bytes(plan.model_dump(mode='json')), 'recipe.json': _json_bytes(plan.parameters),
            'source_receipt.json': _json_bytes(_source_receipt()), 'profile_identity.json': _json_bytes(source[-1])})
    _record(plan, root, source[0], 'PREREGISTERED', path/'manifest.json')
    return path/'plan.json'


def load_campaign_v2(*, plan_path, output_root):
    plan = PriceCampaignPlanV2.model_validate_json(Path(plan_path).read_text(encoding='utf-8'))
    root = _root(plan, output_root)
    if Path(plan_path).resolve() != root/'preregistered/plan.json':
        raise ValueError('campaign requires its exact registered plan path')
    registered = read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    _ledger(plan, root, 'PREREGISTERED', root/'preregistered/manifest.json')
    source = campaign_sources_v2(plan)
    if json.loads((root/'preregistered/recipe.json').read_text(encoding='utf-8')) != plan.parameters:
        raise ValueError('campaign preregistered recipe differs')
    if sha(json.loads((root/'preregistered/profile_identity.json').read_text(encoding='utf-8'))) != sha(source[-1]):
        raise ValueError('campaign profile projection changed since registration')
    return plan, root, registered, source


def prepare_campaign_v2(*, plan_path, output_root):
    plan, root, registered, source = load_campaign_v2(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity, feature, reusable, _ = source
    if (root/'prepared').exists():
        read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
        _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
        return root/'prepared'
    rankings = pd.read_parquet(frozen/'frozen_rankings.parquet')
    candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)]
    prices = _value_prices(frozen, parent.configuration)
    if len(candidates) > 7720 or len(prices) > 500000 or candidate_roster_sha256(candidates) != identity.candidate_roster_sha256:
        raise ValueError('campaign population/source budget differs')
    labels = pd.read_parquet(reusable/'labels.parquet')
    if (not labels.value_scenario.eq('VALUE_REVIEW_5_V1').all()
            or not labels.parent_input_identity_sha256.eq(identity.identity_sha256).all()
            or not labels.native_identity.eq('UNPROVEN').all()):
        raise ValueError('campaign label policy/parent/evidence differs')
    inputs = pd.read_parquet(feature/'features.parquet', columns=[*KEY, *D_FEATURES, 'feature_visible_through', 'daily_input_sha256'])
    observations = context_observations_v1(candidates, prices, pd.read_parquet(frozen/'references.parquet'))
    rows = assemble_value_anchor_rows_v1(inputs=inputs, labels=labels, observations=observations, configuration=parent.configuration)
    path = _publish(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'],
        artifacts={'rows.parquet': _parquet_bytes(rows), 'labels.parquet': _parquet_bytes(labels),
            'preparation.json': _json_bytes(dict(candidate_rows=len(rows), decisions=rows[KEY[0]].nunique(),
                model_fits=0, reused_labels_manifest_sha256=plan.reusable_prepared_manifest_ref.sha256,
                source_evidence=identity.source_evidence, evidence_limitations=identity.evidence_limitations,
                native_identity='UNPROVEN', sealed_accessed=False, database_written=False))})
    _record(plan, root, parent, 'PREPARED', path/'manifest.json')
    return path


def _append_event(path, event):
    with path.open('ab') as handle:
        handle.write(_json_bytes(event).replace(b'\n', b'')+b'\n')
        handle.flush()
        os.fsync(handle.fileno())


def train_campaign_study_v2(*, plan_path, output_root, qe_training_idle):
    if qe_training_idle is not True:
        raise ValueError('campaign fit cannot overlap QE training/unknown state')
    plan, root, registered, source = load_campaign_v2(plan_path=plan_path, output_root=output_root)
    parent = source[0]
    prepared = read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    _ledger(plan, root, 'PREPARED', root/'prepared/manifest.json')
    with _exclusive_file_lock(root/'fit.lock'):
        if (root/'trained').exists():
            read_stage(root/'trained', stage='trained', plan_sha256=plan.plan_sha256, parent_sha256=prepared['stage_sha256'])
            _record(plan, root, parent, 'TRAINED', root/'trained/manifest.json', generated=1)
            return root/'trained'
        marker = root/'fit_attempt.json'
        if marker.exists():
            raise ValueError('campaign partial fit exists; no implicit refit')
        with marker.open('xb') as handle:
            handle.write(_json_bytes(dict(plan_sha256=plan.plan_sha256, model_id=plan.model_id,
                physical_fit_budget=plan.parameters['physical_fit_budget'], completion='UNPROVEN')))
            handle.flush()
            os.fsync(handle.fileno())
        _record(plan, root, parent, 'FIT_STARTED', marker)
        started, count, indices = time.monotonic(), 0, 0

        def before_fit(name):
            import psutil
            nonlocal count, indices
            index = name == 'local_index'
            if time.monotonic()-started > 1800 or psutil.Process().memory_info().rss > 2*1024**3:
                raise ValueError('campaign fit exceeds resource budget')
            if (not index and count >= plan.parameters['physical_fit_budget']) or (index and (plan.model_id != 'M4' or indices)):
                raise ValueError('campaign estimator/index budget exceeded')
            event = dict(campaign_id=plan.campaign_id, experiment_id=plan.experiment_id, model_id=plan.model_id,
                head=name, kind='INDEX_BUILD' if index else 'PHYSICAL_FIT', state='STARTED', time=datetime.now(timezone.utc).isoformat())
            journal = root.parent/'campaign_fit_journal.jsonl'
            with _exclusive_file_lock(root.parent/'campaign_fit.lock'):
                previous = [json.loads(line) for line in journal.read_text(encoding='utf-8').splitlines()] if journal.exists() else []
                if sum(row['kind'] == 'PHYSICAL_FIT' for row in previous) >= 11 and not index:
                    raise ValueError('campaign cumulative physical fit budget exhausted')
                _append_event(journal, event)
                _append_event(root/'fit_journal.jsonl', event)
            count += int(not index)
            indices += int(index)

        fitted = train_campaign_v2(rows=pd.read_parquet(root/'prepared/rows.parquet'), configuration=parent.configuration,
            model_id=plan.model_id, before_fit=before_fit)
        if count != plan.parameters['physical_fit_budget'] or indices != plan.parameters['index_build_count'] or time.monotonic()-started > 1800:
            raise ValueError('campaign completion count/time differs; no publication')
        metadata = dict(model_id=fitted.model_id, recipe=fitted.recipe, models=fitted.models,
            support=asdict(fitted.support), diagnostics=fitted.diagnostics, model_sha256=fitted.model_sha256, parameters=plan.parameters)
        path = _publish(study_root=root, stage='trained', plan_sha256=plan.plan_sha256, parent_sha256=prepared['stage_sha256'],
            artifacts={'metadata.json': _json_bytes(metadata)})
        _record(plan, root, parent, 'TRAINED', path/'manifest.json', generated=1)
        return path


def load_campaign_fit_v2(*, plan_path, output_root):
    plan, root, registered, _ = load_campaign_v2(plan_path=plan_path, output_root=output_root)
    prepared = read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    read_stage(root/'trained', stage='trained', plan_sha256=plan.plan_sha256, parent_sha256=prepared['stage_sha256'])
    _ledger(plan, root, 'TRAINED', root/'trained/manifest.json')
    body = json.loads((root/'trained/metadata.json').read_text(encoding='utf-8'))
    support = ValueAnchorGapSupportV1(tuple(tuple(pair) for pair in body['support']['intervals_bps']))
    if (body['parameters'] != plan.parameters or body['model_id'] != plan.model_id
            or fit_identity_v2(body['model_id'], body['recipe'], body['models'], support) != body['model_sha256']):
        raise ValueError('campaign fitted metadata/parameters differ')
    return PriceCampaignFitV2(body['model_id'], body['recipe'], body['models'], support, body['diagnostics'], body['model_sha256'])
