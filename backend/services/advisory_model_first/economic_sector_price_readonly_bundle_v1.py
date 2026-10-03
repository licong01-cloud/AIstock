"""Read published M1 JSON weights without reopening training data or fitting."""

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import re
from types import MappingProxyType

import numpy as np

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryStudyPlanV1
from backend.services.advisory_model_first.economic_price_campaign_models_v2 import predict_json_v2
from backend.services.advisory_model_first.economic_sector_price_pipeline_v1 import sector_implementation_sha256_v1
from backend.services.advisory_model_first.economic_sector_price_source_v1 import SECTOR_FEATURES
from backend.services.advisory_model_first.economic_sector_price_value_v1 import (
    SectorPriceFitV1, SectorPricePlanV1, sector_fit_identity_v1,
)
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, ValueAnchorGapSupportV1
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1
from backend.services.advisory_model_first.research_control_contracts import AdvisoryResearchTrialRecordV1, EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

JSON_LIMIT = 2 * 1024**2
TOTAL_LIMIT = 16 * 1024**2
STAGES = ('preregistered', 'prepared', 'trained', 'evaluated')
GATES = {'interventions', 'mdd', 'model_takes', 'net_increment', 'tail'}


def _path(value):
    path = Path(value)
    if not path.is_absolute() or path.drive.upper() == 'C:' or path.resolve() != path.absolute() or not path.is_file():
        raise ValueError('sector reader needs an existing absolute non-C nonredirected file')
    return path


def _read_file(path, maximum):
    path = _path(path)
    before = path.stat()
    if not 0 < before.st_size <= maximum:
        raise ValueError('sector reader file exceeds its byte budget')
    with path.open('rb') as handle:
        body = handle.read(maximum + 1)
    after = _path(path).stat()
    fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns')
    if not 0 < len(body) <= maximum or any(getattr(before, key) != getattr(after, key) for key in fields):
        raise ValueError('sector reader file changed during bounded consumption')
    return body


def _object(body):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('sector reader JSON has duplicate fields')
            result[key] = value
        return result

    def invalid(value):
        raise ValueError('sector reader JSON has nonfinite constants')

    def number(value):
        result = float(value)
        if not math.isfinite(result):
            invalid(value)
        return result

    value = json.loads(body.decode('utf-8'), object_pairs_hook=unique, parse_constant=invalid, parse_float=number)
    if not isinstance(value, dict):
        raise ValueError('sector reader JSON must be an object')
    return value


class _ReadSet:
    def __init__(self):
        self.pins = {}
        self.total = 0

    def read(self, path, descriptor=None, *, registry=False):
        path = _path(path)
        body = _read_file(path, TOTAL_LIMIT if registry else JSON_LIMIT)
        digest = hashlib.sha256(body).hexdigest()
        pin = (digest, len(body))
        if descriptor is not None and (descriptor.get('sha256'), descriptor.get('size_bytes')) != pin:
            raise ValueError('sector reader consumed bytes disagree with the pinned descriptor')
        if path in self.pins and self.pins[path] != pin:
            raise ValueError('sector reader consumed contradictory bytes for one path')
        if path not in self.pins and not registry:
            self.total += len(body)
            if self.total > TOTAL_LIMIT:
                raise ValueError('sector reader aggregate JSON budget exceeded')
        self.pins[path] = pin
        return body

    def json(self, path, descriptor=None):
        return _object(self.read(path, descriptor))

    def reference(self, reference, role):
        ref = EvidenceReferenceV1.model_validate(reference)
        if ref.role != role:
            raise ValueError('sector reader external reference role differs')
        return _path(ref.artifact_uri), self.json(ref.artifact_uri, ref.model_dump())


def _stage(reads, root, stage, plan_hash, parent_hash, manifest=None):
    body = reads.json(root / stage / 'manifest.json') if manifest is None else manifest
    if (set(body) != {'schema_version', 'stage', 'plan_sha256', 'parent_sha256', 'files', 'stage_sha256'}
            or body['schema_version'] != 'economic_entry_stage_v1' or body['stage'] != stage
            or body['plan_sha256'] != plan_hash or body['parent_sha256'] != parent_hash
            or sha({key: value for key, value in body.items() if key != 'stage_sha256'}) != body['stage_sha256']):
        raise ValueError('sector reader stage identity or parent chain differs')
    files = body['files']
    if not isinstance(files, dict) or not 1 <= len(files) <= 20:
        raise ValueError('sector reader manifest artifact count differs')
    for name, descriptor in files.items():
        if (not re.fullmatch(r'[a-z0-9_]+\.(json|jsonl|parquet|txt)', name) or name == 'manifest.json'
                or not isinstance(descriptor, dict) or type(descriptor.get('size_bytes')) is not int
                or not 0 < descriptor['size_bytes'] <= 2 * 1024**3
                or not isinstance(descriptor.get('sha256'), str)
                or not re.fullmatch(r'[0-9a-f]{64}', descriptor['sha256'])):
            raise ValueError('sector reader manifest descriptor is unsafe')
    return body


def _registration(reads, root, plan, parent, stages):
    lines = reads.read(root.parent / 'trial_registry.jsonl', registry=True).splitlines()
    if not 1 <= len(lines) <= 1000 or any(not line.strip() for line in lines):
        raise ValueError('sector reader registry row budget or JSONL differs')
    records = tuple(AdvisoryResearchTrialRecordV1.model_validate(_object(line)) for line in lines)
    AdvisoryResearchTrialRegistryV1._validate_record_set(records)
    policy = sha(dict(parent_source_policy=parent.policy_identity,
                      value_policy=plan.parameters['policy_sha256'], cost=COST.policy_sha256))
    for stage in stages:
        found = [row for row in records if row.experiment_id == plan.experiment_id and row.research_stage == stage.upper()]
        if len(found) != 1:
            raise ValueError('sector reader original stage registration is missing or duplicated')
        row = found[0]
        expected_counts = (1, 1) if stage == 'evaluated' else (1, 0) if stage == 'trained' else (0, 0)
        if (row.attempt_id != plan.campaign_id + '_M1' or row.study_type.value != plan.study_type
                or row.hypothesis_family_id != 'economic_entry_price_value'
                or row.parent_lineage != (parent.experiment_id, plan.campaign_id)
                or row.unique_variable != plan.parameters['hypothesis']
                or row.objective_contract.value != plan.objective_contract or row.dataset_identity != parent.dataset_identity
                or row.schema_identity != plan.schema_version or row.policy_identity != policy
                or row.result_class.value != ('CONTROL_READY' if stage == 'preregistered' else 'EXPLORATORY')
                or row.planned_trial_count != 1 or row.selected_trial_count != 0
                or (row.generated_trial_count, row.evaluated_trial_count) != expected_counts
                or row.decision_use.value != 'NAVIGATION_ONLY' or len(row.evidence_refs) != 1
                or len(row.consumed_windows) != 1):
            raise ValueError('sector reader registration model, policy, counts or evidence level differs')
        ref, window = row.evidence_refs[0], row.consumed_windows[0]
        target = root / stage / 'manifest.json'
        if (ref.role != 'campaign_' + stage or _path(ref.artifact_uri) != target
                or (ref.sha256, ref.size_bytes) != reads.pins[target]
                or window.window_id != 'P0C_DEVELOPMENT_V1' or window.dataset_identity != parent.dataset_identity
                or window.start_date != parent.configuration.train_start or window.end_date != parent.configuration.label_cutoff):
            raise ValueError('sector reader registry evidence or consumed window differs')


@dataclass(frozen=True)
class LoadedSectorResearchBundleV1:
    plan: SectorPricePlanV1
    fitted: SectorPriceFitV1
    bundle_sha256: str
    scope: object
    source_pins: tuple
    original_plan_sha256: str
    original_model_sha256: str
    original_scope_sha256: str
    original_diagnostics_sha256: str
    decision_use: str = 'NAVIGATION_ONLY'
    deployable: bool = False
    native_identity: str = 'UNPROVEN'
    source_evidence: str = 'RECOVERED_LIMITED'

    def verify_unchanged(self):
        if (self.plan.plan_sha256 != self.original_plan_sha256
                or sha(dict(self.scope)) != self.original_scope_sha256
                or self.decision_use != 'NAVIGATION_ONLY' or self.deployable is not False
                or self.native_identity != 'UNPROVEN' or self.source_evidence != 'RECOVERED_LIMITED'
                or sha(self.fitted.diagnostics) != self.original_diagnostics_sha256
                or self.plan.implementation_sha256 != sector_implementation_sha256_v1()
                or self.fitted.model_sha256 != self.original_model_sha256
                or sector_fit_identity_v1(self.fitted.recipe, self.fitted.models, self.fitted.support) != self.original_model_sha256):
            raise ValueError('sector reader in-memory model, plan or math identity changed')
        for path, digest, size in self.source_pins:
            body = _read_file(path, TOTAL_LIMIT if path.name == 'trial_registry.jsonl' else JSON_LIMIT)
            if (hashlib.sha256(body).hexdigest(), len(body)) != (digest, size):
                raise ValueError('sector reader original source changed after loading')


def load_sector_research_bundle_v1(*, plan_ref, trained_manifest_ref, evaluated_manifest_ref):
    reads = _ReadSet()
    path, raw_plan = reads.reference(plan_ref, 'sector_readonly_plan')
    plan = SectorPricePlanV1.model_validate(raw_plan)
    root = path.parent.parent
    if path != root / 'preregistered/plan.json' or root.name != plan.experiment_id:
        raise ValueError('sector reader needs the original published plan location')
    if plan.implementation_sha256 != sector_implementation_sha256_v1():
        raise ValueError('sector reader frozen logical math implementation differs')
    trained_path, raw_trained = reads.reference(trained_manifest_ref, 'sector_readonly_trained')
    evaluated_path, raw_evaluated = reads.reference(evaluated_manifest_ref, 'sector_readonly_evaluated')
    if trained_path != root / 'trained/manifest.json' or evaluated_path != root / 'evaluated/manifest.json':
        raise ValueError('sector reader external model references are from another study')
    stages, previous = {}, None
    for stage in STAGES:
        manifest = raw_trained if stage == 'trained' else raw_evaluated if stage == 'evaluated' else None
        stages[stage] = _stage(reads, root, stage, plan.plan_sha256, previous, manifest)
        previous = stages[stage]['stage_sha256']
    registered = stages['preregistered']['files']
    if (set(registered) != {'plan.json', 'recipe.json', 'source_receipt.json', 'profile_identity.json'}
            or reads.json(path, registered['plan.json']) != raw_plan
            or reads.json(root / 'preregistered/recipe.json', registered['recipe.json']) != plan.parameters):
        raise ValueError('sector reader original registration recipe or plan differs')
    receipt = reads.json(root / 'preregistered/source_receipt.json', registered['source_receipt.json'])
    profile = reads.json(root / 'preregistered/profile_identity.json', registered['profile_identity.json'])
    if receipt.get('implementation_sha256') != plan.implementation_sha256 or profile.get('profile_sha256') != plan.profile_sha256:
        raise ValueError('sector reader original source/profile declaration differs')
    preparation = reads.json(root / 'prepared/preparation.json', stages['prepared']['files']['preparation.json'])
    if (preparation.get('native_identity') != 'UNPROVEN' or preparation.get('source_evidence') != 'RECOVERED_LIMITED'
            or preparation.get('sealed_accessed') is not False or preparation.get('database_written') is not False
            or type(preparation.get('fits')) is not int or preparation['fits'] != 0
            or type(preparation.get('candidate_rows')) is not int
            or not 0 < preparation['candidate_rows'] <= plan.parameters['maximum_rows']):
        raise ValueError('sector reader cannot upgrade the original source qualification')
    _, raw_parent = reads.reference(plan.parent_plan_ref, 'campaign_parent_plan')
    parent = EconomicEntryStudyPlanV1.model_validate(raw_parent)
    _, dataset = reads.reference(parent.dataset_manifest_ref, 'economic_original_policy_dataset_manifest')
    if (dataset.get('policy_dataset_bundle_id') != parent.dataset_identity
            or any(not isinstance(dataset.get(key), str) or not dataset[key] for key in ('package_id', 'program_id'))
            or not isinstance(dataset.get('manifest_sha256'), str)
            or not re.fullmatch(r'[0-9a-f]{64}', dataset['manifest_sha256'])):
        raise ValueError('sector reader parent dataset declaration differs')
    _registration(reads, root, plan, parent, stages)
    if set(stages['trained']['files']) != {'metadata.json'}:
        raise ValueError('sector reader trained stage is not the original single metadata artifact')
    metadata = reads.json(root / 'trained/metadata.json', stages['trained']['files']['metadata.json'])
    if set(metadata) != {'recipe', 'models', 'support', 'diagnostics', 'model_sha256', 'parameters'}:
        raise ValueError('sector reader fitted metadata schema differs')
    recipe, models, diagnostic = (metadata[name] for name in ('recipe', 'models', 'diagnostics'))
    if (metadata['parameters'] != plan.parameters
            or set(recipe) != {'d_features', 'sector_features', 'common_supervision_sha256'}
            or recipe['d_features'] != list(D_FEATURES) or recipe['sector_features'] != list(SECTOR_FEATURES)
            or not re.fullmatch(r'[0-9a-f]{64}', recipe['common_supervision_sha256'])
            or set(models) != {'candidate_mean', 'candidate_path', 'matched_mean', 'matched_path'}
            or set(metadata['support']) != {'intervals_bps'}):
        raise ValueError('sector reader frozen recipe, head set or support differs')
    support = ValueAnchorGapSupportV1(tuple(tuple(pair) for pair in metadata['support']['intervals_bps']))
    if not support.intervals_bps or len(support.intervals_bps) > 200:
        raise ValueError('sector reader support is empty or exceeds its budget')
    for name, model in models.items():
        width = 16 if name.startswith('candidate_') else 13
        if (set(model) != {'kind', 'features', 'initial', 'learning_rate', 'trees'} or model['kind'] != 'gbdt'
                or type(model['features']) is not int or model['features'] != width
                or type(model['initial']) is not float
                or len(model['trees']) != plan.parameters['gbdt']['n_estimators']
                or type(model['learning_rate']) is not float or model['learning_rate'] != plan.parameters['gbdt']['learning_rate']):
            raise ValueError('sector reader frozen GBDT head structure differs')
        for tree in model['trees']:
            if (not isinstance(tree, dict) or set(tree) != {'left', 'right', 'feature', 'threshold', 'value'}
                    or any(not isinstance(tree[key], list) or any(type(value) is bool for value in tree[key]) for key in tree)):
                raise ValueError('sector reader tree arrays are not numeric model coordinates')
        predict_json_v2(model, np.zeros((1, width)))
    if (any(type(diagnostic.get(key)) is not int or diagnostic[key] != value for key, value in
            (('fitted_head_count', 4), ('index_build_count', 0), ('candidate_count', 1)))
            or diagnostic.get('test_used_for_training_or_calibration') is not False
            or diagnostic.get('deployable') is not False or diagnostic.get('decision_use') != 'NAVIGATION_ONLY'
            or type(diagnostic.get('train_rows')) is not int
            or not plan.parameters['minimum_train_rows'] <= diagnostic['train_rows'] <= preparation['candidate_rows']
            or type(diagnostic.get('train_days')) is not int or diagnostic['train_days'] < plan.parameters['minimum_train_days']
            or sector_fit_identity_v1(recipe, models, support) != metadata['model_sha256']):
        raise ValueError('sector reader fit identity or original evidence level differs')
    report = reads.json(root / 'evaluated/evaluation.json', stages['evaluated']['files']['evaluation.json'])
    gates = report.get('gates')
    if (report.get('plan_sha256') != plan.plan_sha256 or report.get('model_id') != 'M1'
            or report.get('navigation') != 'CONSIDER_CONFIRMATION_DESIGN_ONLY'
            or report.get('economic_effectiveness') != 'NOT_CONFIRMED' or report.get('decision_use') != 'NAVIGATION_ONLY'
            or any(report.get(key) is not False for key in ('deployable', 'sealed_accessed', 'database_written', 'real_fill_proven'))
            or not isinstance(gates, dict) or set(gates) != GATES or any(value is not True for value in gates.values())
            or report.get('source_evidence') != preparation['source_evidence']):
        raise ValueError('sector reader cannot reinterpret the original navigation or economic evidence')
    scope = {key: dataset[key] for key in ('package_id', 'program_id', 'manifest_sha256')}
    scope.update(dataset_identity=parent.dataset_identity, parent_policy_identity=parent.policy_identity,
                 policy_identity=plan.parameters['policy_sha256'], universe_selection=plan.model_dump()['universe_selection'],
                 parent_model_information_end='UNPROVEN')
    fitted = SectorPriceFitV1(recipe, models, support, diagnostic, metadata['model_sha256'])
    pins = tuple((path, digest, size) for path, (digest, size) in reads.pins.items())
    digest = sha(dict(plan_sha256=plan.plan_sha256, model_sha256=fitted.model_sha256,
                      stages={name: item['stage_sha256'] for name, item in stages.items()}, scope=scope))
    loaded = LoadedSectorResearchBundleV1(plan, fitted, digest, MappingProxyType(scope), pins,
                                        plan.plan_sha256, fitted.model_sha256, sha(scope), sha(diagnostic))
    loaded.verify_unchanged()
    return loaded
