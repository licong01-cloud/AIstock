"""Small immutable sector study consumer; no QE submissions or DB writes."""
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_context_consumer_v1 import load_economic_context_source_v1
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, _frame, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, _verify_reference, read_stage
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import ARMS, PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_price_campaign_evaluation_v2 import evaluate_price_actions_v2
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _append_event, _ledger, _publish, _record, _root, campaign_sources_v2, implementation_sha256_v2
from backend.services.advisory_model_first.economic_sector_price_source_v1 import SECTOR_FEATURES, load_sector_dynamic_rows_v1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1, SectorPricePlanV1, sector_fit_identity_v1, sector_nodes_v1, train_sector_price_v1
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
from backend.services.advisory_model_first.economic_value_anchor_labels_v1 import _positive
from backend.services.advisory_model_first.economic_value_anchor_pipeline_v1 import _value_prices
from backend.services.advisory_model_first.research_control import _exclusive_file_lock
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

NAMES = ('economic_sector_price_value_v1.py', 'economic_sector_price_source_v1.py', 'economic_sector_price_pipeline_v1.py')


def sector_implementation_sha256_v1():
    return sha(dict(algorithm='UTF8_LF_BYTES_V1', common=implementation_sha256_v2(),
        files={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in NAMES}))


def sector_sources_v1(plan):
    if plan.implementation_sha256 != sector_implementation_sha256_v1():
        raise ValueError('sector implementation changed')
    _verify_reference(plan.crosswalk_ref)
    # Read-only adapter authorizes the consumed development window first. It
    # does NOT call an old fit/evaluation or select its models as candidates.
    adapter = PriceCampaignPlanV2(**{name: getattr(plan, name) for name in
        ('parent_plan_ref', 'parent_prepared_manifest_ref', 'feature_manifest_ref', 'reusable_prepared_manifest_ref',
         'profile_path', 'profile_sha256', 'universe_selection')}, model_id='M2', implementation_sha256=implementation_sha256_v2())
    source = campaign_sources_v2(adapter)
    reusable = source[4]
    old_plan = json.loads((reusable.parent/'preregistered/plan.json').read_text(encoding='utf-8'))
    authority = old_plan['authority_root']
    strict = load_economic_context_source_v1(profile_path=plan.profile_path, profile_sha256=plan.profile_sha256,
        universe_selection=plan.universe_selection, authority_root=authority)
    if strict.identity['sector']['taxonomy_version'] != 'SW2021':
        raise ValueError('sector strict historical taxonomy differs')
    strict.verify_unchanged()
    return (*source[:5], strict.identity, authority)


def information_source_receipt_v1(*, names=NAMES, implementation=None):
    repo = Path(__file__).resolve().parents[3]
    head = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=repo, check=True, capture_output=True).stdout.decode().strip()
    if subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', 'backend/services/advisory_model_first', 'backend/services/advisory_list_transition.py'], cwd=repo).returncode:
        raise ValueError('sector source dependencies are uncommitted')
    blobs = {}
    for name in names:
        relative = 'backend/services/advisory_model_first/'+name
        body = subprocess.run(['git', 'show', f'{head}:{relative}'], cwd=repo, check=True, capture_output=True).stdout
        if body.replace(b'\r\n', b'\n') != (repo/relative).read_bytes().replace(b'\r\n', b'\n'):
            raise ValueError('sector source/Git blob differ')
        blobs[relative] = hashlib.sha256(body).hexdigest()
    return dict(head=head, git_blob_content_sha256=blobs,
        implementation_sha256=sector_implementation_sha256_v1() if implementation is None else implementation)


def _receipt():
    return information_source_receipt_v1()


def preregister_sector_price_v1(*, plan, output_root):
    plan = SectorPricePlanV1.model_validate(plan)
    source, root = sector_sources_v1(plan), _root(plan, output_root)
    if (root/'preregistered').exists():
        read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    else:
        _publish(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None,
            artifacts={'plan.json': _json_bytes(plan.model_dump(mode='json')), 'recipe.json': _json_bytes(plan.parameters),
                'source_receipt.json': _json_bytes(_receipt()), 'profile_identity.json': _json_bytes(source[5])})
    _record(plan, root, source[0], 'PREREGISTERED', root/'preregistered/manifest.json')
    return root/'preregistered/plan.json'


def load_sector_price_v1(*, plan_path, output_root):
    plan = SectorPricePlanV1.model_validate_json(Path(plan_path).read_text(encoding='utf-8'))
    root = _root(plan, output_root)
    if Path(plan_path).resolve() != root/'preregistered/plan.json':
        raise ValueError('sector requires exact preregistered plan path')
    registered = read_stage(root/'preregistered', stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    _ledger(plan, root, 'PREREGISTERED', root/'preregistered/manifest.json')
    source = sector_sources_v1(plan)
    if (json.loads((root/'preregistered/recipe.json').read_text(encoding='utf-8')) != plan.parameters
            or sha(json.loads((root/'preregistered/profile_identity.json').read_text(encoding='utf-8'))) != sha(source[5])):
        raise ValueError('sector frozen recipe/source projection changed')
    return plan, root, registered, source


def prepare_sector_price_v1(*, plan_path, output_root):
    plan, root, registered, source = load_sector_price_v1(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity, _, reusable, _, authority = source
    if (root/'prepared').exists():
        read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    else:
        rows = pd.read_parquet(reusable/'rows.parquet')
        rankings = pd.read_parquet(frozen/'frozen_rankings.parquet')
        candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)]
        if (len(rows) > 7720 or not set(rows[KEY].itertuples(index=False, name=None)) == set(candidates[KEY].itertuples(index=False, name=None))
                or candidate_roster_sha256(candidates) != identity.candidate_roster_sha256):
            raise ValueError('sector original population differs')
        sector, summary = load_sector_dynamic_rows_v1(plan=plan, rows=rows,
            calendar=json.loads((frozen/'calendar.json').read_text(encoding='utf-8')), authority_root=authority)
        merged = rows.merge(sector, on=KEY, how='outer', validate='one_to_one', indicator=True)
        if not merged._merge.eq('both').all():
            raise ValueError('sector cannot drop/replace original keys')
        _publish(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'],
            artifacts={'rows.parquet': _parquet_bytes(merged.drop(columns='_merge')),
                'source_summary.json': _json_bytes(summary), 'preparation.json': _json_bytes(dict(candidate_rows=len(rows), fits=0,
                    source_evidence=identity.source_evidence, native_identity='UNPROVEN', sealed_accessed=False, database_written=False))})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    return root/'prepared'


def _fit_event(plan, root, name, *, model_id='M1', campaign_fit_budget=15):
    if (model_id, campaign_fit_budget) not in (('M1', 15), ('M5', 19), ('M6', 23), ('M7', 27), ('M8', 31), ('M9', 35), ('M10', 39), ('M11', 43), ('M12', 47), ('M13', 51), ('M14', 55), ('M15', 59), ('M16', 63), ('M17', 67), ('M18', 71), ('M19', 75), ('M20', 79), ('M21', 83)):
        raise ValueError('fixed information campaign budget differs')
    journal = root.parent/'campaign_fit_journal.jsonl'
    with _exclusive_file_lock(root.parent/'campaign_fit.lock'):
        previous = [json.loads(line) for line in journal.read_text(encoding='utf-8').splitlines()] if journal.exists() else []
        if sum(event['kind'] == 'PHYSICAL_FIT' for event in previous) >= campaign_fit_budget:
            raise ValueError('sector cumulative campaign fit budget exhausted')
        event = dict(campaign_id=plan.campaign_id, experiment_id=plan.experiment_id, model_id=model_id, head=name,
            kind='PHYSICAL_FIT', state='STARTED', time=datetime.now(timezone.utc).isoformat())
        _append_event(journal, event)
        _append_event(root/'fit_journal.jsonl', event)


def train_information_study_v1(*, plan_path, output_root, qe_training_idle, load_study, train_model, fit_event):
    if qe_training_idle is not True:
        raise ValueError('sector fit cannot overlap QE training/unknown state')
    plan, root, registered, source = load_study(plan_path=plan_path, output_root=output_root)
    prepared = read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    _ledger(plan, root, 'PREPARED', root/'prepared/manifest.json')
    with _exclusive_file_lock(root/'fit.lock'):
        if (root/'trained').exists():
            read_stage(root/'trained', stage='trained', plan_sha256=plan.plan_sha256, parent_sha256=prepared['stage_sha256'])
            _record(plan, root, source[0], 'TRAINED', root/'trained/manifest.json', generated=1)
            return root/'trained'
        marker = root/'fit_attempt.json'
        if marker.exists():
            raise ValueError('sector partial fit exists; no implicit refit')
        with marker.open('xb') as handle:
            handle.write(_json_bytes(dict(plan_sha256=plan.plan_sha256, physical_fit_budget=4, completion='UNPROVEN')))
            handle.flush()
            os.fsync(handle.fileno())
        _record(plan, root, source[0], 'FIT_STARTED', marker)
        started, count = time.monotonic(), 0

        def before_fit(name):
            import psutil
            nonlocal count
            if count >= 4 or time.monotonic()-started > 1800 or psutil.Process().memory_info().rss > 2*1024**3:
                raise ValueError('sector fit resource budget exhausted')
            fit_event(plan, root, name)
            count += 1

        fitted = train_model(rows=pd.read_parquet(root/'prepared/rows.parquet'), configuration=source[0].configuration, before_fit=before_fit)
        if count != 4 or time.monotonic()-started > 1800:
            raise ValueError('sector fit count/time differs; no publication')
        _publish(study_root=root, stage='trained', plan_sha256=plan.plan_sha256, parent_sha256=prepared['stage_sha256'],
            artifacts={'metadata.json': _json_bytes(dict(recipe=fitted.recipe, models=fitted.models, support=asdict(fitted.support),
                diagnostics=fitted.diagnostics, model_sha256=fitted.model_sha256, parameters=plan.parameters))})
        _record(plan, root, source[0], 'TRAINED', root/'trained/manifest.json', generated=1)
        return root/'trained'


def load_information_fit_v1(*, plan_path, output_root, load_study, fit_identity):
    plan, root, registered, _ = load_study(plan_path=plan_path, output_root=output_root)
    prepared = read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    read_stage(root/'trained', stage='trained', plan_sha256=plan.plan_sha256, parent_sha256=prepared['stage_sha256'])
    _ledger(plan, root, 'TRAINED', root/'trained/manifest.json')
    body = json.loads((root/'trained/metadata.json').read_text(encoding='utf-8'))
    support = ValueAnchorGapSupportV1(tuple(tuple(pair) for pair in body['support']['intervals_bps']))
    if (body['parameters'] != plan.parameters or fit_identity(body['recipe'], body['models'], support) != body['model_sha256']
            or body['diagnostics']['fitted_head_count'] != 4 or body['diagnostics']['index_build_count'] != 0):
        raise ValueError('sector fitted metadata differs')
    return SectorPriceFitV1(body['recipe'], body['models'], support, body['diagnostics'], body['model_sha256'])


def information_actual_decisions_v1(*, fitted, candidates, inputs, prices, references, identity, arm,
        information_features, information_clock, nodes, information_status=None):
    fields = [*KEY, *D_FEATURES, *information_features, 'feature_visible_through', information_clock, *([information_status] if information_status else [])]
    features = _frame(inputs.loc[:, fields], KEY, set(fields))
    if not all(pd.to_datetime(features[name]).eq(features[KEY[0]]).all() for name in ('feature_visible_through', information_clock)):
        raise ValueError('sector actual query sees future feature')
    roster = _frame(candidates.loc[:, KEY+['selection_effective_rank']], KEY, set(KEY)|{'selection_effective_rank'})
    if (arm not in ARMS or not roster[KEY[0]].lt(roster[KEY[1]]).all()
            or not roster.selection_effective_rank.between(1, 20).all() or roster.selection_effective_rank.mod(1).ne(0).any()
            or roster.duplicated([KEY[0], 'selection_effective_rank']).any()
            or roster.selection_effective_rank.map(lambda v: isinstance(v, (bool, np.bool_))).any()):
        raise ValueError('sector roster rank/clock/arm differs')
    if not set(roster[KEY].itertuples(index=False, name=None)).issubset(set(features[KEY].itertuples(index=False, name=None))):
        raise ValueError('sector query contains foreign candidate')
    quote_fields = ['trade_date', 'instrument', 'raw_open_cny', 'suspended', 'tradability_unknown', 'up_limit', 'down_limit', 'source_sha256', 'price_coordinate_sha256']
    quotes = _frame(prices.loc[:, quote_fields], quote_fields[:2], set(quote_fields))
    ref_fields = [*KEY, 'target_reference_raw_cny', 'reference_visible_through', 'source_sha256']
    refs = _frame(references.loc[:, ref_fields], KEY, set(ref_fields))
    if (not quotes.source_sha256.eq(identity.price_source_sha256).all() or not quotes.price_coordinate_sha256.eq(identity.price_coordinate_sha256).all()
            or not refs.source_sha256.eq(identity.reference_source_sha256).all() or not pd.to_datetime(refs.reference_visible_through).le(refs[KEY[0]]).all()
            or any(not quotes[name].map(lambda v: type(v) is bool).all() for name in ('suspended', 'tradability_unknown'))):
        raise ValueError('sector quote/reference identity or clock differs')
    feature_map, quote_map, ref_map = features.set_index(KEY).to_dict('index'), quotes.set_index(quote_fields[:2]).to_dict('index'), refs.set_index(KEY).to_dict('index')
    output, queries, positions = [], [], []
    for item in roster.to_dict('records'):
        key = tuple(item[name] for name in KEY)
        row = {**item, 'model_action': 'UNAVAILABLE', 'market_admissible': False, 'reason_code': 'MARKET_UNPROVEN',
            'actual_gap_bps': None, 'expected_net_return_bps': None, 'downside_q90_bps': None}
        quote, ref = quote_map.get((key[1], key[2])), ref_map.get(key)
        if quote is not None and ref is not None:
            price, reference, upper, lower = (_positive(quote['raw_open_cny']), _positive(ref['target_reference_raw_cny']), _positive(quote['up_limit']), _positive(quote['down_limit']))
            if None not in (price, upper, lower) and (lower > upper or price < lower-1e-9 or price > upper+1e-9):
                raise ValueError('sector quote contradicts legal limits')
            if (not quote['suspended'] and not quote['tradability_unknown'] and None not in (price, reference, upper, lower)
                    and price < upper and not Decimal(str(price)) % Decimal('.01')):
                gap = float((Decimal(str(price))/Decimal(str(reference))-1)*10000)
                row.update(market_admissible=True, actual_gap_bps=gap, reason_code='UNKNOWN_INPUT_OR_SUPPORT')
                queries.append({**feature_map[key], 'actual_gap_bps': gap})
                positions.append(len(output))
        output.append(row)
    if queries:
        estimated = nodes(fitted=fitted, rows=pd.DataFrame(queries), arm=arm)
        for position, node in zip(positions, estimated.to_dict('records'), strict=True):
            output[position].update(model_action={'ACCEPTABLE': 'TAKE', 'AVOID': 'SKIP'}.get(node['status'], 'UNAVAILABLE'),
                reason_code=node['status'], expected_net_return_bps=node['expected_net_bps'], downside_q90_bps=node['downside_q90_bps'])
    return pd.DataFrame(output)


def evaluate_information_study_v1(*, plan_path, output_root, load_study, load_fit, decisions):
    plan, root, registered, source = load_study(plan_path=plan_path, output_root=output_root)
    parent, frozen, identity = source[:3]
    prepared = read_stage(root/'prepared', stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=registered['stage_sha256'])
    trained = read_stage(root/'trained', stage='trained', plan_sha256=plan.plan_sha256, parent_sha256=prepared['stage_sha256'])
    _ledger(plan, root, 'TRAINED', root/'trained/manifest.json')
    if not (root/'evaluated').exists():
        fitted = load_fit(plan_path=plan_path, output_root=output_root)
        rankings = pd.read_parquet(frozen/'frozen_rankings.parquet')
        rankings = rankings.loc[rankings[KEY[0]].ge(pd.Timestamp(parent.configuration.test_start))]
        candidates = rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)
            & rankings[KEY[0]].le(pd.Timestamp(parent.configuration.test_end))]
        prices = _value_prices(frozen, parent.configuration)
        if candidates.empty or len(prices) > 500000:
            raise ValueError('sector evaluation population/price budget differs')
        inputs, refs = pd.read_parquet(root/'prepared/rows.parquet'), pd.read_parquet(frozen/'references.parquet')
        actions = {arm: decisions(fitted=fitted, candidates=candidates, inputs=inputs, prices=prices,
            references=refs, identity=identity, arm=arm) for arm in ARMS}
        artifacts = evaluate_price_actions_v2(plan=plan, fitted=fitted, rankings=rankings, candidates=candidates,
            prices=prices, identity=identity, calendar=json.loads((frozen/'calendar.json').read_text(encoding='utf-8')), actions=actions)
        _publish(study_root=root, stage='evaluated', plan_sha256=plan.plan_sha256, parent_sha256=trained['stage_sha256'], artifacts=artifacts)
    else:
        read_stage(root/'evaluated', stage='evaluated', plan_sha256=plan.plan_sha256, parent_sha256=trained['stage_sha256'])
    _record(plan, root, parent, 'EVALUATED', root/'evaluated/manifest.json', generated=1, evaluated=1)
    return root/'evaluated'


def train_sector_price_study_v1(*, plan_path, output_root, qe_training_idle):
    return train_information_study_v1(plan_path=plan_path, output_root=output_root, qe_training_idle=qe_training_idle,
        load_study=load_sector_price_v1, train_model=train_sector_price_v1, fit_event=_fit_event)


def load_sector_price_fit_v1(*, plan_path, output_root):
    return load_information_fit_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_sector_price_v1, fit_identity=sector_fit_identity_v1)


def sector_actual_decisions_v1(*, fitted, candidates, inputs, prices, references, identity, arm):
    return information_actual_decisions_v1(fitted=fitted, candidates=candidates, inputs=inputs, prices=prices,
        references=references, identity=identity, arm=arm, information_features=SECTOR_FEATURES,
        information_clock='sector_feature_visible_through', nodes=sector_nodes_v1)


def evaluate_sector_price_v1(*, plan_path, output_root):
    return evaluate_information_study_v1(plan_path=plan_path, output_root=output_root,
        load_study=load_sector_price_v1, load_fit=load_sector_price_fit_v1, decisions=sector_actual_decisions_v1)
