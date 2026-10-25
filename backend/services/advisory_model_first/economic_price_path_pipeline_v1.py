"""M7 thin immutable consumer of the original frozen daily-price snapshot."""
import hashlib
import json
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, _verify_reference, read_stage
from backend.services.advisory_model_first.economic_moneyflow_price_pipeline_v1 import verify_moneyflow_budget_v1
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import MoneyflowPricePlanV1
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _ledger, _publish, _record, _root, campaign_sources_v2, implementation_sha256_v2
from backend.services.advisory_model_first.economic_price_path_value_v1 import PRICE_PATH_FEATURES, PricePathPlanV1, price_path_fit_identity_v1, price_path_nodes_v1, price_path_rows_v1, train_price_path_v1
from backend.services.advisory_model_first.economic_sector_price_pipeline_v1 import _fit_event, evaluate_information_study_v1, information_actual_decisions_v1, information_source_receipt_v1, load_information_fit_v1, sector_implementation_sha256_v1, train_information_study_v1
from backend.services.advisory_model_first.economic_selection_state_pipeline_v1 import SOURCE_FIELDS
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

NAMES = ('economic_price_path_value_v1.py', 'economic_price_path_pipeline_v1.py')


def price_path_implementation_sha256_v1():
    names = (*NAMES, 'economic_moneyflow_price_pipeline_v1.py', 'economic_selection_state_pipeline_v1.py')
    return sha(dict(algorithm='UTF8_LF_BYTES_V1', common=sector_implementation_sha256_v1(),
        files={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in names}))


def price_path_sources_v1(plan):
    if plan.implementation_sha256 != price_path_implementation_sha256_v1():
        raise ValueError('price path implementation changed')
    predecessor = _verify_reference(plan.predecessor_manifest_ref)
    previous = MoneyflowPricePlanV1.model_validate_json((predecessor.parent.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    verify_moneyflow_budget_v1(previous, price_path_extension=plan)
    adapter = PriceCampaignPlanV2(**{name: getattr(plan, name) for name in SOURCE_FIELDS},
        model_id='M2', implementation_sha256=implementation_sha256_v2())
    return campaign_sources_v2(adapter)


def _price_path_root(plan, output_root):
    root = _root(plan, output_root)
    if root.parent != plan.campaign_root.resolve():
        raise ValueError('price path output cannot reset original budget')
    return root


def preregister_price_path_v1(*, plan, output_root):
    plan = PricePathPlanV1.model_validate(plan)
    root, source = _price_path_root(plan, output_root), price_path_sources_v1(plan)
    if (root/'preregistered').exists():
        read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    else:
        _publish(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None,
            artifacts={'plan.json': _json_bytes(plan.model_dump(mode='json')), 'recipe.json': _json_bytes(plan.parameters),
                'source_receipt.json': _json_bytes(information_source_receipt_v1(names=NAMES, implementation=price_path_implementation_sha256_v1())),
                'profile_identity.json': _json_bytes(source[5])})
    _record(plan, root, source[0], 'PREREGISTERED', root/'preregistered/manifest.json')
    return root/'preregistered/plan.json'


def load_price_path_study_v1(*, plan_path, output_root):
    plan = PricePathPlanV1.model_validate_json(Path(plan_path).read_text(encoding='utf-8'))
    root = _price_path_root(plan, output_root)
    if Path(plan_path).resolve() != root/'preregistered/plan.json':
        raise ValueError('price path requires exact preregistered plan')
    registered = read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    _ledger(plan, root, 'PREREGISTERED', root/'preregistered/manifest.json')
    source = price_path_sources_v1(plan)
    if json.loads((root/'preregistered/recipe.json').read_text(encoding='utf-8')) != plan.parameters or sha(json.loads((root/'preregistered/profile_identity.json').read_text(encoding='utf-8'))) != sha(source[5]):
        raise ValueError('price path frozen recipe/profile changed')
    return plan, root, registered, source


def prepare_price_path_v1(*, plan_path, output_root):
    plan, root, registered, source = load_price_path_study_v1(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity, _, reusable, _ = source
    if (root/'prepared').exists():
        read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    else:
        rows = pd.read_parquet(reusable/'rows.parquet')
        rankings = pd.read_parquet(frozen/'frozen_rankings.parquet')
        candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)]
        if (len(rows) > 7720 or set(rows[KEY].itertuples(index=False, name=None)) != set(candidates[KEY].itertuples(index=False, name=None))
                or candidate_roster_sha256(candidates) != identity.candidate_roster_sha256):
            raise ValueError('price path cannot replace/drop original population')
        prices = pd.read_parquet(frozen/'raw_daily.parquet', columns=plan.parameters['source_columns'])
        calendar = json.loads((frozen/'calendar.json').read_text(encoding='utf-8'))
        block = price_path_rows_v1(candidates=candidates, prices=prices, calendar=calendar)
        merged = rows.merge(block, on=KEY, how='left', validate='one_to_one', indicator=True, sort=False)
        if not merged._merge.eq('both').all() or not merged[KEY].equals(rows[KEY].reset_index(drop=True)):
            raise ValueError('price path cannot change original key order')
        _publish(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'],
            artifacts={'rows.parquet': _parquet_bytes(merged.drop(columns='_merge')),
                'preparation.json': _json_bytes(dict(candidate_rows=len(rows), price_path_status=block.price_path_feature_status.value_counts().to_dict(),
                    raw_source_rows=len(prices), source_manifest_sha256=plan.parent_prepared_manifest_ref.sha256,
                    fits=0, source_evidence=identity.source_evidence, native_identity='UNPROVEN',
                    sealed_accessed=False, database_reads=0, database_written=False))})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    return root/'prepared'


def _price_path_fit_event(plan, root, name):
    return _fit_event(plan, root, name, model_id='M7', campaign_fit_budget=27)


def train_price_path_study_v1(*, plan_path, output_root, qe_training_idle):
    return train_information_study_v1(plan_path=plan_path, output_root=output_root, qe_training_idle=qe_training_idle,
        load_study=load_price_path_study_v1, train_model=train_price_path_v1, fit_event=_price_path_fit_event)


def load_price_path_fit_v1(*, plan_path, output_root):
    return load_information_fit_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_price_path_study_v1, fit_identity=price_path_fit_identity_v1)


def price_path_actual_decisions_v1(*, fitted, candidates, inputs, prices, references, identity, arm):
    return information_actual_decisions_v1(fitted=fitted, candidates=candidates, inputs=inputs, prices=prices,
        references=references, identity=identity, arm=arm, information_features=PRICE_PATH_FEATURES,
        information_clock='price_path_feature_visible_through', nodes=price_path_nodes_v1)


def evaluate_price_path_v1(*, plan_path, output_root):
    return evaluate_information_study_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_price_path_study_v1, load_fit=load_price_path_fit_v1, decisions=price_path_actual_decisions_v1)
