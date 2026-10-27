import hashlib
import os
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_legacy_adoption import adopt_completed_exports
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest
import numpy as np
import pandas as pd
from backend.services.dataset_release import monthly_fact_correction as factor_correction_subject
from backend.services.dataset_release import monthly_index_repair as index_repair_subject
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.index_materializer import _index_csv_frames, _to_context_frame
from backend.services.dataset_release.monthly_unified import MonthlyReleaseConflict, MonthlyReleaseService
from datetime import date, datetime
from zoneinfo import ZoneInfo
from backend.services.dataset_release import monthly_minute_repair as minute_repair_subject
from backend.services.dataset_release import monthly_minute_repair as subject
from backend.services.dataset_release import monthly_prev_close_repair as transaction
from backend.services.dataset_release.canonical_stock_transformer import _minute_session_times
from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS

from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.monthly_build_resume import (
    MonthlyBuildResume, MonthlyBuildResumeError, materialization_attempt, retained_staging_root, _atomic, _signature,
)
from backend.services.dataset_release.monthly_worker import ProducerContext


DAY, CODE = "2026-07-10", "603900.SH"


@pytest.mark.parametrize("mode", ["fill", "prepend", "new"])
def test_quote_fact_patch_retains_finite_values_and_accepts_only_real_fills(mode):
    from backend.services.dataset_release.monthly_quote_fact_correction import validate_missing_only
    old = None if mode == "new" else np.array([3, 7, np.nan], dtype="<f4").tobytes()
    target = np.array([1, 4, np.nan, 7, 9], dtype="<f4").tobytes() if mode == "prepend" else np.array([3, 7, 9], dtype="<f4").tobytes()
    assert validate_missing_only(old, target) == (2 if mode in {"new", "prepend"} else 1)


@pytest.mark.parametrize("target", [[3, 8, 9], [4, 9], [3, 7, float('inf')], [3.5, 7, 9], [3, 7]])
def test_quote_fact_patch_rejects_changed_facts_and_invalid_bounds(target):
    from backend.services.dataset_release.monthly_quote_fact_correction import validate_missing_only
    old = np.array([3, 7, np.nan], dtype="<f4").tobytes()
    with pytest.raises(ValueError):
        validate_missing_only(old, np.array(target, dtype="<f4").tobytes())


def _quote_prepared(tmp_path):
    from backend.services.dataset_release import monthly_quote_fact_correction as quote
    resume, finalized, plan = prepared(tmp_path)
    path = resume.root / f'minute_bin/qlib/features/{CODE.lower()}/close.1min.bin'
    values = np.frombuffer(path.read_bytes(), dtype='<f4').copy()
    values[1:241] = 5
    base = {"sha256": "b" * 64}
    entry = {"relative_path": path.relative_to(resume.root).as_posix(),
        "original_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "target_ref": resume.cas.put_bytes(values.tobytes()).as_dict()}
    factor = resume.cas.put_json({'files':[{'dataset':'daily_pv','sha256':'c'*64}]}).as_dict()
    prepared_ref = resume.cas.put_json({'refs':{'factor_receipt':factor}}).as_dict()
    resume.record['steps']['prepare'] = resume.cas.put_json({'prepare_receipt_ref':prepared_ref}).as_dict()
    authority = {"schema_version": quote.SCHEMA, "base_manifest_ref": base, "files": [entry],
        "daily_pv_sha256":'c'*64, "coverage": {"stock_days": 1}}
    plan["monthly_ready_correction"] = {"base_manifest_ref": base}
    plan[quote.BINDING] = {"operation_id": resume.context.operation_id, "target_cutoff": plan['target_cutoff'],
        "authority_ref": resume.cas.put_json(authority).as_dict()}
    _atomic(resume.path, resume.record)
    return quote, resume, finalized, plan, path


@pytest.mark.parametrize('crash', [False, True])
def test_quote_fact_patch_is_atomic_cow_and_idempotent(tmp_path, monkeypatch, crash):
    quote, resume, finalized, plan, path = _quote_prepared(tmp_path)
    old = tmp_path / 'old-release.bin'
    os.link(path, old)
    original = old.read_bytes()
    if crash:
        replace_file = transaction._replace
        calls = [0]
        def fail_after_one(target, payload):
            calls[0] += 1
            if calls[0] == 2:
                raise OSError('crash after first quote rename')
            return replace_file(target, payload)
        monkeypatch.setattr(transaction, '_replace', fail_after_one)
        with pytest.raises(OSError, match='crash'):
            quote.apply_quote_repairs(resume, finalized=finalized, plan=plan, cas=resume.cas)
        monkeypatch.setattr(transaction, '_replace', replace_file)
        resume.record = quote.recover_quote_repairs(root=resume.root,journal=resume.path,record=resume.record,cas=resume.cas)
    result = quote.apply_quote_repairs(resume, finalized=finalized, plan=plan, cas=resume.cas)
    assert old.read_bytes() == original
    assert np.all(np.fromfile(path,dtype='<f4')[1:241] == 5)
    assert result == quote.apply_quote_repairs(resume,finalized=result,plan=plan,cas=resume.cas)
    inner = resume.cas.get_json(result['finalize_receipt_ref'])
    materialization = resume.cas.get_json(inner['minute_receipt_ref'])
    assert len(quote.correction_pins(materialization,provider=path.parents[2],cas=resume.cas)) == 1


@pytest.mark.parametrize('defect', ['running','published','wrong_operation','changed_target','qfq_basis'])
def test_quote_fact_patch_fails_closed_without_touching_other_files(tmp_path, defect):
    quote, resume, finalized, plan, path = _quote_prepared(tmp_path)
    old = path.read_bytes()
    if defect == 'running':
        resume.root = resume.root.with_name('outside').parent
    elif defect == 'published':
        (resume.root/'qe_dataset_manifest.json').write_text('{}')
    elif defect == 'wrong_operation':
        plan[quote.BINDING]['operation_id'] = 'other'
    elif defect == 'changed_target':
        path.write_bytes(np.array([0,7],dtype='<f4').tobytes())
    else:
        authority = resume.cas.get_json(plan[quote.BINDING]['authority_ref'])
        authority['daily_pv_sha256'] = 'd'*64
        plan[quote.BINDING]['authority_ref'] = resume.cas.put_json(authority).as_dict()
    before = path.read_bytes()
    with pytest.raises(ValueError):
        quote.apply_quote_repairs(resume,finalized=finalized,plan=plan,cas=resume.cas)
    assert path.read_bytes() == before
    assert before == old or defect == 'changed_target'


def test_quote_fact_daily_catalog_exposes_real_history_without_extending_other_stocks(tmp_path):
    from backend.services.dataset_release.monthly_quote_fact_correction import daily_catalog_payload
    cas = CASStore(ControlStore.initialize(tmp_path/'control').root)
    values = np.array([0, 10, np.nan, 12, 13],dtype='<f4').tobytes()
    entries = [{'relative_path':'daily_bin/qlib/features/000001.sz/close.day.bin',
                'target_ref':cas.put_bytes(values).as_dict()}]
    payload = daily_catalog_payload(b'000001.SZ\t2026-09-04\t2026-09-04\n000002.SZ\t2026-09-02\t2026-09-02\n',
        entries,['2026-09-01','2026-09-02','2026-09-03','2026-09-04'],cas)
    assert payload.decode().splitlines() == ['000001.SZ\t2026-09-01\t2026-09-01',
        '000001.SZ\t2026-09-03\t2026-09-04','000002.SZ\t2026-09-02\t2026-09-02']


def _quote_capture_fixture(tmp_path):
    from backend.services.dataset_release import monthly_quote_fact_correction as quote
    repair = tmp_path/'data_repair_staging'
    repair.mkdir()
    source = repair/'source.gz'
    source.write_bytes(b'frozen exact DB capture')
    target = repair/'close.bin'
    target.write_bytes(np.array([0,9],dtype='<f4').tobytes())
    receipt = repair/'receipt.json'
    receipt.write_text(json.dumps({'schema_version':quote.PRODUCER_SCHEMA,'status':'PRIVATE_COMPONENTS_NOT_A_RELEASE',
        'base_manifest_file_sha256':'a'*64,'daily_pv_sha256':'c'*64,'cutoff':'2026-09-30','database_write':False,'profile_write':False,'runtime_action':False,
        'source_captures':[{'path':str(source),'sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'size':source.stat().st_size}],
        'files':[{'relative_path':'daily_bin/qlib/features/000001.sz/close.day.bin','source_path':str(target),
            'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'original_sha256':None}], 'coverage':{'stock_days':1}}))
    inputs = {'receipt_path':str(receipt),'receipt_sha256':hashlib.sha256(receipt.read_bytes()).hexdigest()}
    cas = CASStore(ControlStore.initialize(tmp_path/'control').root)
    return quote, repair, source, target, receipt, inputs, cas


@pytest.mark.parametrize('defect', [None,'source','output','receipt','cutoff','base','path'])
def test_quote_fact_capture_reuses_exact_frozen_facts_and_rejects_drift(tmp_path, defect):
    quote, repair, source, target, receipt, inputs, cas = _quote_capture_fixture(tmp_path)
    if defect in {'source','output','receipt'}:
        {'source':source,'output':target,'receipt':receipt}[defect].write_bytes(b'changed')
    elif defect == 'path':
        value = json.loads(receipt.read_bytes())
        value['files'][0]['relative_path'] = '../active_profile.json'
        receipt.write_text(json.dumps(value))
        inputs['receipt_sha256'] = hashlib.sha256(receipt.read_bytes()).hexdigest()
    arguments = dict(repair_root=repair,base_manifest_ref={'sha256': ('b' if defect=='base' else 'a')*64},
        cutoff='2026-08-31' if defect=='cutoff' else '2026-09-30',cas=cas)
    if defect is not None:
        with pytest.raises(ValueError):
            quote.capture_quote_repairs(inputs,**arguments)
    else:
        result = quote.capture_quote_repairs(inputs,**arguments)
        assert result['database_write'] is False and len(result['files']) == len(result['source_capture_refs']) == 1


def test_quote_fact_binding_and_operator_route_do_not_start_or_activate(tmp_path):
    quote, repair, source, target, receipt, inputs, cas = _quote_capture_fixture(tmp_path)
    root = tmp_path/'operation'
    root.mkdir()
    plan = {'candidate_root':str(tmp_path/'releases/new'),'target_cutoff':'2026-09-30',
        'monthly_ready_correction':{'base_manifest_ref':{'sha256':'a'*64}}}
    state = {'status':'FAILED','current_stage':'BUILD'}
    checkpoints = {'SOURCE':{'canonical_sha256':'b'*64}}
    service = SimpleNamespace(store=SimpleNamespace(operation_root=lambda _:root,read_plan=lambda _:plan,
        read_state=lambda _:state,read_checkpoint=lambda _,s:checkpoints.get(s),replace_plan=lambda _,p:plan.update(p)))
    bound = MonthlyReleaseService.bind_quote_fact_repairs(service,'operation',inputs=inputs,principal='operator',cas=cas)
    assert state == {'status':'FAILED','current_stage':'BUILD'} and set(checkpoints) == {'SOURCE'}
    assert not plan[quote.BINDING]['activation_authorized']
    from backend.routers.monthly_dataset_releases import bind_monthly_quote_fact_repairs, UnifiedMonthlyRepairInputsRequest
    adapter = SimpleNamespace(bind_quote_fact_repairs=lambda op,**kw:MonthlyReleaseService.bind_quote_fact_repairs(service,op,cas=cas,**kw))
    result = bind_monthly_quote_fact_repairs('operation',UnifiedMonthlyRepairInputsRequest(inputs=inputs),
        SimpleNamespace(principal_id='operator'),adapter)
    assert result['data'] == bound
    with pytest.raises(MonthlyReleaseConflict, match='binding changed'):
        MonthlyReleaseService.bind_quote_fact_repairs(service,'operation',inputs={},principal='operator',cas=cas)


@pytest.mark.parametrize('defect', ['running','not_correction','no_source','build_done'])
def test_quote_fact_binding_requires_idle_source_bound_private_build(tmp_path, defect):
    quote, repair, source, target, receipt, inputs, cas = _quote_capture_fixture(tmp_path)
    root = tmp_path/'operation'
    root.mkdir()
    plan = {'candidate_root':str(tmp_path/'releases/new'),'target_cutoff':'2026-09-30',
        'monthly_ready_correction':{'base_manifest_ref':{'sha256':'a'*64}}}
    state, checkpoints = {'status':'FAILED','current_stage':'BUILD'}, {'SOURCE':{'canonical_sha256':'b'*64}}
    if defect == 'running':
        state['status'] = 'BUILDING'
    elif defect == 'not_correction':
        plan.pop('monthly_ready_correction')
    elif defect == 'no_source':
        checkpoints.clear()
    else:
        checkpoints['BUILD'] = {}
    service = SimpleNamespace(store=SimpleNamespace(operation_root=lambda _:root,read_plan=lambda _:plan,
        read_state=lambda _:state,read_checkpoint=lambda _,s:checkpoints.get(s)))
    with pytest.raises(MonthlyReleaseConflict,match='idle retained BUILD'):
        MonthlyReleaseService.bind_quote_fact_repairs(service,'operation',inputs=inputs,principal='operator',cas=cas)
    assert quote.BINDING not in plan


def test_quote_fact_completed_commit_recovery_preserves_later_steps(tmp_path, monkeypatch):
    quote, resume, finalized, plan, path = _quote_prepared(tmp_path)
    unlink = Path.unlink
    def interrupted(path, *args, **kwargs):
        if path == quote._pending(resume.root):
            raise OSError('after quote journal commit')
        return unlink(path,*args,**kwargs)
    monkeypatch.setattr(Path,'unlink',interrupted)
    with pytest.raises(OSError,match='after quote journal commit'):
        quote.apply_quote_repairs(resume,finalized=finalized,plan=plan,cas=resume.cas)
    monkeypatch.setattr(Path,'unlink',unlink)
    resume.record = json.loads(resume.path.read_bytes())
    resume.record['steps']['later'] = resume.cas.put_json({'status':'PASS'}).as_dict()
    _atomic(resume.path,resume.record)
    assert quote.recover_quote_repairs(root=resume.root,journal=resume.path,record=resume.record,cas=resume.cas) == resume.record


def test_quote_fact_inherited_private_successor_keeps_completed_proof(tmp_path):
    quote, resume, finalized, plan, path = _quote_prepared(tmp_path)
    completed = quote.apply_quote_repairs(resume,finalized=finalized,plan=plan,cas=resume.cas)
    binding = dict(plan[quote.BINDING])
    plan['monthly_ready_correction'] = {'base_manifest_ref':{'sha256':'e'*64},'base_plan':{quote.BINDING:binding}}
    resume.record['steps'].pop('quote-facts')
    _atomic(resume.path,resume.record)
    assert quote.apply_quote_repairs(resume,finalized=completed,plan=plan,cas=resume.cas) == completed
    assert resume.record['steps']['quote-facts'] == binding['authority_ref']


def test_quote_fact_creates_missing_feature_without_touching_other_files(tmp_path):
    quote, resume, finalized, plan, path = _quote_prepared(tmp_path)
    # This fixture-owned file is absent in the old export, not in another release.
    path.unlink()
    resume.record['files'].pop(path.relative_to(resume.root).as_posix())
    authority = resume.cas.get_json(plan[quote.BINDING]['authority_ref'])
    authority['files'][0]['original_sha256'] = None
    plan[quote.BINDING]['authority_ref'] = resume.cas.put_json(authority).as_dict()
    _atomic(resume.path,resume.record)
    quote.apply_quote_repairs(resume,finalized=finalized,plan=plan,cas=resume.cas)
    assert np.all(np.fromfile(path,dtype='<f4')[1:241] == 5)


def test_quote_fact_applies_to_regular_incremental_predecessor(tmp_path):
    quote, resume, finalized, plan, path = _quote_prepared(tmp_path)
    plan['predecessor_manifest_ref'] = plan.pop('monthly_ready_correction')['base_manifest_ref']
    quote.apply_quote_repairs(resume, finalized=finalized, plan=plan, cas=resume.cas)
    assert np.all(np.fromfile(path, dtype='<f4')[1:241] == 5)


@pytest.mark.parametrize('order', [('minute_correction','quote_fact_correction'),('quote_fact_correction','minute_correction')])
def test_quote_and_minute_amendments_validate_actual_byte_order(tmp_path, order):
    quote, resume, finalized, plan, path = _quote_prepared(tmp_path)
    relative = path.relative_to(resume.root).as_posix()
    first = {'relative_path':relative,'original_sha256':'a'*64,'target_ref':resume.cas.put_bytes(b'first').as_dict()}
    last = {'relative_path':relative,'original_sha256':first['target_ref']['sha256'],
        'target_ref':resume.cas.put_bytes(b'last').as_dict()}
    materialization = {order[0]:{'files':[first]},order[1]:{'files':[last]}}
    for owner, row in zip(order,(first,last)):
        assert quote.following_correction_ref(materialization,relative=relative,reference=row['target_ref'],owner=owner) == last['target_ref']
    materialization['prev_close_correction'] = {'files':[{**last,'target_ref':first['target_ref']}]}
    with pytest.raises(ValueError,match='branch'):
        quote.following_correction_ref(materialization,relative=relative,reference=first['target_ref'],owner=order[0])


def test_minute_fill_after_catalog_quote_keeps_both_physical_proofs(tmp_path):
    quote, resume, finalized, plan, path = _quote_prepared(tmp_path)
    authority = resume.cas.get_json(plan[quote.BINDING]['authority_ref'])
    target = np.frombuffer(resume.cas.get_bytes(resume.cas.validate_reference_metadata(
        authority['files'][0]['target_ref'])),dtype='<f4').copy()
    target[121:241] = np.nan
    authority['files'][0]['target_ref'] = resume.cas.put_bytes(target.tobytes()).as_dict()
    plan[quote.BINDING]['authority_ref'] = resume.cas.put_json(authority).as_dict()
    finalized = quote.apply_quote_repairs(resume,finalized=finalized,plan=plan,cas=resume.cas)
    finalized = minute_repair_subject.apply_minute_corrections(resume,finalized=finalized,plan=plan,cas=resume.cas)
    materialization = resume.cas.get_json(resume.cas.get_json(finalized['finalize_receipt_ref'])['minute_receipt_ref'])
    provider = path.parents[2]
    quote_pins = quote.correction_pins(materialization,provider=provider,cas=resume.cas)
    minute_pins = minute_repair_subject.correction_pins(materialization,provider=provider,cas=resume.cas)
    assert quote_pins[path]['target_sha256'] == minute_pins[path]['target_sha256']
    assert np.all(np.fromfile(path,dtype='<f4')[1:241]==5)


def test_catalog_delta_reuses_atomic_quote_transaction_and_resume(tmp_path):
    from backend.services.dataset_release.monthly_construction_facts import DAILY_CATALOG_SCHEMA, daily_catalog_repair_plan
    quote, resume, finalized, plan, path = _quote_prepared(tmp_path)
    plan['predecessor_manifest_ref'] = plan.pop('monthly_ready_correction')['base_manifest_ref']
    plan.pop(quote.BINDING)
    code, day = '000585.SZ', '2026-07-10'
    provider = resume.root/'daily_bin/qlib'
    (provider/'calendars').mkdir(parents=True, exist_ok=True)
    (provider/'calendars/day.txt').write_text(day+'\n')
    catalog = provider/'instruments/all.txt'
    catalog.parent.mkdir()
    catalog.write_text('603900.SH\t2026-07-10\t2026-09-01\n')
    materialization = {'dataset':'daily_bin','cutoff':'2026-09-30'}
    _atomic(resume.root/'daily_bin/materialization_receipt.json',materialization)
    inner = resume.cas.get_json(finalized['finalize_receipt_ref'])
    inner['daily_receipt_ref'] = resume.cas.put_json(materialization).as_dict()
    finalized = {'finalize_receipt_ref':resume.cas.put_json(inner).as_dict()}
    resume.record['steps']['finalize-bins'] = resume.cas.put_json(finalized).as_dict()
    for target in (catalog,resume.root/'daily_bin/materialization_receipt.json'):
        resume.record['files'][target.relative_to(resume.root).as_posix()] = list(_signature(target))
    _atomic(resume.path,resume.record)
    factor = resume.root/'factor_bundle/daily_pv.h5'
    factor.parent.mkdir(exist_ok=True)
    frame = pd.DataFrame({'open':[5.], 'high':[5.5], 'low':[4.5], 'close':[5.],
        'volume':[2000.], 'amount':[10000.], 'factor':[.5]},
        index=pd.MultiIndex.from_tuples([(code,pd.Timestamp(day))],names=['instrument','datetime']))
    frame.to_hdf(factor,key='data',format='table',data_columns=True)
    source = {'schema_version':DAILY_CATALOG_SCHEMA,'database_write':False,'stock_days':1,'codes':[code],
        'facts':[{'ts_code':code,'trade_date':day,'pre_close':10.,'up_limit':11.,'down_limit':9.}]}
    inputs = {'historical_daily_catalog_ref':resume.cas.put_json(source).as_dict()}
    prepared = resume.cas.get_json(resume.record['steps']['prepare'])
    computed = daily_catalog_repair_plan(plan,resume=resume,prepare=prepared,build_inputs=inputs,cas=resume.cas)
    authority = resume.cas.get_json(computed[quote.BINDING]['authority_ref'])
    assert len(authority['files']) == 12
    assert authority['base_manifest_ref'] == plan['predecessor_manifest_ref']
    result = quote.apply_quote_repairs(resume,finalized=finalized,prepare=prepared,plan=computed,cas=resume.cas)
    assert np.fromfile(provider/'features/000585.sz/prev_close.day.bin',dtype='<f4').tolist() == [0.,10.]
    assert daily_catalog_repair_plan(plan,resume=resume,prepare=prepared,build_inputs=inputs,cas=resume.cas) == computed
    assert quote.apply_quote_repairs(resume,finalized=result,prepare=prepared,plan=computed,cas=resume.cas) == result

@pytest.mark.parametrize("mode", ["prepend", "append", "absent"])
@pytest.mark.parametrize("crash", [False, True])
def test_expand_real_minute_facts_keeps_published_bytes_and_recovers(tmp_path, monkeypatch, mode, crash):
    resume, finalized, plan = prepared(tmp_path)
    provider = resume.root / "minute_bin/qlib"
    folder = provider / f"features/{CODE.lower()}"
    old = tmp_path / "published"
    old.mkdir()
    originals = {}
    if mode == "append":
        # Earlier real observations are retained; the unrequested intervening
        # timestamp remains NaN rather than being silently filled.
        calendar_path = provider / "calendars/1min.txt"
        calendar_path.write_text("2026-07-09 14:59:00\n2026-07-09 15:00:00\n" + calendar_path.read_text())
    for field in QLIB_STOCK_FIELDS:
        path = folder / f"{field}.1min.bin"
        if mode in {"prepend", "append"}:
            path.write_bytes(np.array([240, 1] if mode == "prepend" else [0, 7], dtype="<f4").tobytes())
            os.link(path, old / path.name)
            originals[path.name] = path.read_bytes()
            resume.record["files"][path.relative_to(resume.root).as_posix()] = _signature(path)
        else:
            path.unlink()
            resume.record["files"].pop(path.relative_to(resume.root).as_posix())
    if mode == "absent":
        folder.rmdir()
    _atomic(resume.path, resume.record)
    if crash:
        original, calls = transaction._replace, []

        def interrupted(path, payload):
            calls.append(path)
            if len(calls) == 4:
                raise OSError("injected create interruption")
            original(path, payload)

        monkeypatch.setattr(transaction, "_replace", interrupted)
        with pytest.raises(OSError, match="injected"):
            subject.apply_minute_corrections(resume, finalized=finalized, cas=resume.cas, plan=plan)
        monkeypatch.setattr(transaction, "_replace", original)
        resume.record = subject.recover_minute_correction(root=resume.root, journal=resume.path,
            record=resume.record, cas=resume.cas)
        finalized = resume.cas.get_json(resume.record["steps"]["finalize-bins"])
    result = subject.apply_minute_corrections(resume, finalized=finalized, cas=resume.cas, plan=plan)
    path = folder / "close.1min.bin"
    expected = ([0, 7, np.nan, *([5] * 240)] if mode == "append" else
                [0, *([5] * 240), *([1] if mode == "prepend" else [])])
    assert np.array_equal(np.fromfile(path, dtype="<f4"), np.array(expected, dtype="<f4"), equal_nan=True)
    for name, raw in originals.items():
        assert (old / name).read_bytes() == raw
    materialization = resume.cas.get_json(resume.cas.get_json(result["finalize_receipt_ref"])["minute_receipt_ref"])
    pins = subject.correction_pins(materialization, provider=provider, cas=resume.cas)
    assert len(pins) == 12 and pins[path]["start_index"] == 0
    assert pins[path]["end_index"] == len(expected) - 2
    assert materialization["minute_correction"]["filled_cells"] == 240 * 12
    assert subject.apply_minute_corrections(resume, finalized=result, cas=resume.cas, plan=plan) == result
    assert json.loads(resume.path.read_bytes())["files"] == resume.record["files"]


def test_unknown_calendar_fails_before_any_mutation(tmp_path):
    resume, finalized, plan = prepared(tmp_path)
    calendar = resume.root / "minute_bin/qlib/calendars/1min.txt"
    calendar.write_text("\n".join(calendar.read_text().splitlines()[1:]) + "\n")
    originals = {p: p.read_bytes() for p in (resume.root / "minute_bin/qlib/features").rglob("*.bin")}
    with pytest.raises(MonthlyBuildResumeError, match="outside frozen calendar"):
        subject.apply_minute_corrections(resume, finalized=finalized, cas=resume.cas, plan=plan)
    assert all(p.read_bytes() == raw for p, raw in originals.items())
    assert not subject._pending(resume.root).exists()


def test_recovery_rejects_unexpected_new_file_instead_of_overwriting(tmp_path, monkeypatch):
    resume, finalized, plan = prepared(tmp_path)
    path = resume.root / f"minute_bin/qlib/features/{CODE.lower()}/close.1min.bin"
    path.unlink()
    original = transaction._replace

    def interrupt(path, payload):
        raise OSError("injected before first replacement")

    monkeypatch.setattr(transaction, "_replace", interrupt)
    with pytest.raises(OSError, match="injected"):
        subject.apply_minute_corrections(resume, finalized=finalized, cas=resume.cas, plan=plan)
    path.write_bytes(b"unrelated writer")
    monkeypatch.setattr(transaction, "_replace", original)
    with pytest.raises(MonthlyBuildResumeError, match="input changed"):
        subject.recover_minute_correction(root=resume.root, journal=resume.path, record=resume.record, cas=resume.cas)
    assert path.read_bytes() == b"unrelated writer"


def facts():
    return [dict(trade_time=f"{DAY} {t}", ts_code=CODE,
                 open_li=10000, high_li=11000, low_li=9000, close_li=10000,
                 volume_hand=1, amount_li=1050500, volume_shares=105,
                 volume_shares_source="tdx_wire_shares", volume_shares_sha256=None)
            for t in _minute_session_times()]


@pytest.mark.parametrize("defect", [None, "missing", "duplicate", "invalid"])
def test_capture_authoritative_database_session_without_supplier_gates(defect):
    rows, calls = facts(), []
    if defect == "missing":
        rows.pop()
    if defect == "duplicate":
        rows[-1] = rows[0]
    if defect == "invalid":
        rows[0]["volume_shares"] = float("nan")
    records = []
    for row in rows:
        row = {**row, "trade_time": datetime.fromisoformat(row["trade_time"]).replace(tzinfo=ZoneInfo("Asia/Shanghai"))}
        records.append(tuple(row[f] for f in minute_repair_subject._FIELDS))
    class Connection:
        def set_session(self, **kw):
            calls.append(kw)
        def cursor(self):
            return self
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def execute(self, sql, params=None):
            calls.append((sql, params))
        def fetchone(self):
            return ("aistock", "snapshot")
        def fetchall(self):
            return records
        def rollback(self):
            calls.append("rollback")
        def close(self):
            calls.append("close")
    inputs = {"stock_dates": [{"ts_code": CODE, "trade_date": DAY}]}
    if defect:
        with pytest.raises((ValueError, RuntimeError)):
            minute_repair_subject.capture_database_minutes(inputs, cutoff=date(2026, 9, 30), connection_factory=Connection)
    else:
        result = minute_repair_subject.capture_database_minutes(inputs, cutoff=date(2026, 9, 30), connection_factory=Connection)
        assert result["row_count"] == 240 and result["stock_dates"] == 1
        assert result["database_write"] is False
        assert result["facts"] == facts()
    assert calls[0] == {"readonly": True, "isolation_level": "REPEATABLE READ"}
    assert calls[-1] == "close"
    assert all(not isinstance(c, tuple) or c[0].startswith("SELECT ") for c in calls)


@pytest.mark.parametrize('calendar_has_auction', [True, False])
def test_capture_follows_frozen_timestamps_not_source_row_count(calendar_has_auction):
    from unittest.mock import MagicMock
    rows = [{**facts()[0], 'trade_time': f'{DAY} 09:30:00'}, *facts()]
    records = [tuple((datetime.fromisoformat(row[f]).replace(tzinfo=ZoneInfo('Asia/Shanghai'))
                     if f == 'trade_time' else row[f]) for f in minute_repair_subject._FIELDS) for row in rows]
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = ('aistock', 'same_source_snapshot')
    cursor.fetchall.return_value = records
    stamps = [r['trade_time'] for r in (rows if calendar_has_auction else rows[1:])]
    result = minute_repair_subject.capture_database_minutes(
        {'stock_dates': [{'ts_code': CODE, 'trade_date': DAY}]}, cutoff=date(2026,9,30),
        frozen_sessions={DAY: stamps}, connection=connection)
    assert result['row_count'] == (241 if calendar_has_auction else 240)
    assert result['non_calendar_source_rows_ignored'] == (0 if calendar_has_auction else 1)
    assert [r['trade_time'] for r in result['facts']] == stamps
    assert "ts_code=ANY(%s::bpchar[])" in cursor.execute.call_args_list[1].args[0]
    assert "adjust_type='none'" in cursor.execute.call_args_list[1].args[0]
    connection.set_session.assert_not_called()  # Does not replace the exported SOURCE snapshot.
    connection.rollback.assert_not_called()
    connection.close.assert_not_called()


def prepared(tmp_path):
    artifact_root = tmp_path / "cas"
    ControlStore.initialize(artifact_root)
    cas = CASStore(artifact_root)
    root = tmp_path / ".staging" / "retained"
    root.mkdir(parents=True)
    minute = root / "minute_bin/qlib"
    daily = root / "daily_bin/qlib"
    for provider, freq, calendar in (
        (minute, "1min", [r["trade_time"] for r in facts()] + ["2026-09-01 09:31:00"]),
        (daily, "day", [DAY, "2026-09-01"]),
    ):
        (provider / "calendars").mkdir(parents=True)
        (provider / f"calendars/{freq}.txt").write_text("\n".join(calendar) + "\n")
        (provider / f"features/{CODE.lower()}").mkdir(parents=True)
    for field, value in (("factor", .5), ("up_limit_price", 11), ("down_limit_price", 9), ("prev_close", 10)):
        np.array([0, value, value], dtype="<f4").tofile(daily / f"features/{CODE.lower()}/{field}.day.bin")
    for field in QLIB_STOCK_FIELDS:
        np.array([0, *([np.nan] * 240), 1], dtype="<f4").tofile(minute / f"features/{CODE.lower()}/{field}.1min.bin")
    materialization = {"dataset": "minute_bin", "cutoff": "2026-09-30"}
    _atomic(root / "minute_bin/materialization_receipt.json", materialization)
    ref = cas.put_json(materialization).as_dict()
    finalized = {"finalize_receipt_ref": cas.put_json({"minute_receipt_ref": ref}).as_dict()}
    original_steps = {"finalize-bins": cas.put_json(finalized).as_dict()}
    resume = SimpleNamespace(root=root, cas=cas, path=root.parent / "resume.json",
        context=SimpleNamespace(operation_id="dmr_test", checkpoint=lambda: None),
        record={"binding_digest": "bound", "steps": original_steps,
                "files": {p.relative_to(root).as_posix(): _signature(p) for p in root.rglob("*") if p.is_file()}})
    _atomic(resume.path, resume.record)
    authority = {"schema_version": minute_repair_subject.SCHEMA, "facts": facts(), "row_count": 240, "stock_dates": 1}
    plan = {"target_cutoff": "2026-09-30", "predecessor": {"dataset_manifest_sha256": "a" * 64}}
    plan[minute_repair_subject.BINDING] = {"operation_id": "dmr_test", "target_cutoff": plan["target_cutoff"],
        "predecessor_manifest_sha256": "a" * 64, "authority_ref": cas.put_json(authority).as_dict()}
    return resume, finalized, plan


@pytest.mark.parametrize("crash", [False, True])
@pytest.mark.parametrize("packaging_started", [False, True])
def test_bounded_fill_is_cow_recoverable_and_idempotent(tmp_path, monkeypatch, crash, packaging_started):
    resume, finalized, plan = prepared(tmp_path)
    resume.record["finalization_started"] = packaging_started
    _atomic(resume.path, resume.record)
    provider = resume.root / "minute_bin/qlib"
    path = provider / f"features/{CODE.lower()}/close.1min.bin"
    old_release = tmp_path / "old-release.bin"
    os.link(path, old_release)
    old = old_release.read_bytes()
    if crash:
        from backend.services.dataset_release import monthly_prev_close_repair as transaction
        original = transaction._replace
        count = [0]
        def interrupt(path, payload):
            count[0] += 1
            if count[0] == 3:
                raise OSError("injected interruption")
            original(path, payload)
        monkeypatch.setattr(transaction, "_replace", interrupt)
        with pytest.raises(OSError, match="injected"):
            minute_repair_subject.apply_minute_corrections(resume, finalized=finalized, cas=resume.cas, plan=plan)
        monkeypatch.setattr(transaction, "_replace", original)
        resume.record = minute_repair_subject.recover_minute_correction(root=resume.root, journal=resume.path,
                                                          record=resume.record, cas=resume.cas)
        finalized = resume.cas.get_json(resume.record["steps"]["finalize-bins"])
    result = minute_repair_subject.apply_minute_corrections(resume, finalized=finalized, cas=resume.cas, plan=plan)
    values = np.fromfile(path, dtype="<f4")
    assert np.array_equal(values, np.array([0, *([5] * 240), 1], dtype="<f4"))
    volume = np.fromfile(path.with_name("volume.1min.bin"), dtype="<f4")
    assert (volume[1:241] == 210).all()
    assert old_release.read_bytes() == old  # Shared published inode never written.
    inner = resume.cas.get_json(result["finalize_receipt_ref"])
    materialization = resume.cas.get_json(inner["minute_receipt_ref"])
    assert materialization["minute_correction"]["filled_cells"] == 240 * 12
    assert len(minute_repair_subject.correction_pins(materialization, provider=provider, cas=resume.cas)) == 12
    signatures = dict(resume.record["files"])
    assert minute_repair_subject.apply_minute_corrections(resume, finalized=result, cas=resume.cas, plan=plan) == result
    assert resume.record["files"] == signatures
    assert not minute_repair_subject._pending(resume.root).exists()
    # Every file pin survives a completely new reader.
    assert json.loads(resume.path.read_bytes())["files"] == signatures


def test_correction_cannot_overwrite_finite_quotes_or_touch_published_root(tmp_path):
    resume, finalized, plan = prepared(tmp_path)
    path = resume.root / f"minute_bin/qlib/features/{CODE.lower()}/open.1min.bin"
    values = np.fromfile(path, dtype="<f4")
    values[1] = 999
    values.tofile(path)
    before = path.read_bytes()
    with pytest.raises(MonthlyBuildResumeError, match="overwrite a valid fact"):
        minute_repair_subject.apply_minute_corrections(resume, finalized=finalized, cas=resume.cas, plan=plan)
    assert path.read_bytes() == before and not minute_repair_subject._pending(resume.root).exists()
    resume.root = Path(tmp_path / "published")
    with pytest.raises(MonthlyBuildResumeError, match="private retained staging"):
        minute_repair_subject.apply_minute_corrections(resume, finalized=finalized, cas=resume.cas, plan=plan)


def _fixture(tmp_path):
    context = ProducerContext(stage="BUILD", operation_id="dmr_" + "1" * 32, attempt=2,
        request={}, prior_receipts={}, plan={"candidate_root": str(tmp_path / "release"),
        "release_id": "september", "target_cutoff": "2026-09-30", "predecessor": {"manifest": "a" * 64}})
    compiled = SimpleNamespace(source_bundle_sha256="b" * 64, physical_plan={"action_plan_digest": "c" * 64})
    root = tmp_path / ".staging" / f".release.{context.operation_id}.attempt-2.building"
    (root / "daily_bin").mkdir(parents=True)
    (root / "daily_bin/close.bin").write_bytes(b"accurate")
    cas = CASStore(ControlStore.initialize(tmp_path / "control").root)
    journal = MonthlyBuildResume(context=context, compiled=compiled, release_root=tmp_path, staging_root=root, cas=cas)
    receipt = cas.put_json({"status": "PASS", "real_file": "daily_bin/close.bin"})
    journal.save("prepare", receipt, roots=("daily_bin",))
    return context, compiled, root, cas, journal


def test_sealed_build_receipt_is_bounded_by_complete_reference_not_control_json_cap(tmp_path):
    from backend.services.dataset_release.monthly_build_resume import load_sealed_build_json
    from backend.services.dataset_release.cas_store import CASStoreError, CASCorruptionError

    cas = CASStore(ControlStore.initialize(tmp_path / "control").root)
    payload = {"verified_output_files": {"inventory": "x" * (17 * 1024 * 1024)}}
    reference = cas.put_json(payload)
    assert load_sealed_build_json(cas, reference.as_dict()) == payload
    with pytest.raises(CASStoreError, match="size is incomplete"):
        load_sealed_build_json(cas, reference.sha256)
    with pytest.raises(CASStoreError, match="non-canonical"):
        load_sealed_build_json(cas, {**reference.as_dict(), "relative_path": "other.json"})
    with pytest.raises(CASCorruptionError, match="size mismatch"):
        load_sealed_build_json(cas, {**reference.as_dict(), "size": reference.size - 1})
    path = cas.root / reference.relative_path
    raw = path.read_bytes()
    path.write_bytes(raw[:-1])
    with pytest.raises(CASCorruptionError, match="size mismatch"):
        load_sealed_build_json(cas, reference)
    path.write_bytes(raw.replace(b"x", b"y", 1))
    with pytest.raises(CASCorruptionError, match="readback mismatch"):
        load_sealed_build_json(cas, reference)


def test_completed_large_export_receipt_resumes_without_rewriting_price_files(tmp_path):
    context, compiled, root, cas, journal = _fixture(tmp_path)
    payload = {"verified_output_files": {"inventory": "x" * (17 * 1024 * 1024)}}
    journal.save("qlib_dump_minute", cas.put_json(payload), roots=("daily_bin",))
    path = root / "daily_bin/close.bin"
    before = path.stat()
    assert journal.get("qlib_dump_minute") == payload
    assert path.stat().st_mtime_ns == before.st_mtime_ns
    assert path.read_bytes() == b"accurate"


def test_fresh_resume_keeps_exact_bytes_and_reuses_original_staging(tmp_path):
    context, compiled, root, cas, journal = _fixture(tmp_path)
    before = (root / "daily_bin/close.bin").stat()
    retry = replace(context, attempt=3)
    selected = retained_staging_root(retry, compiled, tmp_path, tmp_path / "new-staging")
    assert selected == root
    reopened = MonthlyBuildResume(context=retry, compiled=compiled, release_root=tmp_path, staging_root=selected, cas=cas)
    assert reopened.get("prepare")["status"] == "PASS"
    assert (root / "daily_bin/close.bin").stat().st_mtime_ns == before.st_mtime_ns
    reopened.write_lease("d" * 64)
    assert materialization_attempt(root, operation_id=context.operation_id, execution_attempt=3,
        candidate_name="release", release_digest="d" * 64) == 2


def test_interrupted_history_commit_keeps_price_exports_and_finishes_without_writers(tmp_path, monkeypatch):
    import copy
    from backend.services.dataset_release.monthly_industry_history import recover_history_commit
    from backend.services.dataset_release.monthly_build_resume import _atomic, _signature
    context, compiled, root, cas, journal = _fixture(tmp_path)
    factor = root / "factor_bundle"
    factor.mkdir()
    names = ("sector_data.h5", "static_factors.parquet")
    for name in names:
        (factor / name).write_bytes(b"original")
    journal.save("prepare", cas.put_json({"status": "PASS"}), roots=("factor_bundle",))
    scratch = root.parent / f".{root.name}.industry-history-{'e' * 64}"
    scratch.mkdir()
    proposed, files = copy.deepcopy(journal.record), {}
    proposed["steps"]["industry-history"] = cas.put_json({"projection_sha256": "e" * 64}).as_dict()
    for name in names:
        path = scratch / name
        path.write_bytes(b"repaired")
        files[name] = {"scratch_relative_path": path.relative_to(root.parent).as_posix(),
                       "target_signature": _signature(path), "original_signature": _signature(factor / name)}
        proposed["files"][f"factor_bundle/{name}"] = _signature(path)
    pending = root.parent / f".{root.name}.industry-history-commit.json"
    intent = {"binding_digest": journal.record["binding_digest"], "projection_sha256": "e" * 64,
              "record": proposed, "files": files}
    _atomic(pending, intent)
    original_rename = Path.rename
    def interrupted(path, target):
        if path.parent == scratch and path.name == "static_factors.parquet":
            raise OSError("interrupted after preserving the original")
        return original_rename(path, target)
    monkeypatch.setattr(Path, "rename", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        recover_history_commit(root=root, journal=journal.path, record=journal.record, expected_projection="e" * 64)
    monkeypatch.setattr(Path, "rename", original_rename)
    recovered = recover_history_commit(root=root, journal=journal.path, record=journal.record, expected_projection="e" * 64)
    assert recovered == proposed
    assert not pending.exists()
    assert (root / "daily_bin/close.bin").read_bytes() == b"accurate"
    for name in names:
        assert (factor / name).read_bytes() == b"repaired"
        assert (scratch / (name + ".before")).read_bytes() == b"original"
    assert recover_history_commit(root=root, journal=journal.path, record=recovered, expected_projection="e" * 64) == proposed

    # A later authorized quote/prev_close amendment has its own file pins and
    # completion receipt. Reopening an already committed history transaction
    # must not replay its obsolete writer provenance over that newer journal.
    later = copy.deepcopy(recovered)
    price = root / "daily_bin/close.bin"
    price.write_bytes(b"later verified correction")
    later["files"]["daily_bin/close.bin"] = _signature(price)
    later["steps"]["finalize-bins"] = cas.put_json({"later_correction": True}).as_dict()
    _atomic(journal.path, later)
    before = journal.path.read_bytes(), journal.path.stat().st_mtime_ns
    _atomic(pending, intent)  # Legacy workers left a completed intent behind.
    assert recover_history_commit(root=root, journal=journal.path, record=later, expected_projection="e" * 64) == later
    assert not pending.exists()
    assert (journal.path.read_bytes(), journal.path.stat().st_mtime_ns) == before
    assert price.read_bytes() == b"later verified correction"

    forged = copy.deepcopy(later)
    forged["steps"]["industry-history"] = cas.put_json({"different_projection": True}).as_dict()
    _atomic(pending, intent)
    with pytest.raises(MonthlyBuildResumeError, match="industry commit"):
        recover_history_commit(root=root, journal=journal.path, record=forged, expected_projection="e" * 64)
    forged = copy.deepcopy(later)
    forged["files"]["factor_bundle/sector_data.h5"] = [0, 0, "f" * 64]
    with pytest.raises(MonthlyBuildResumeError, match="completed industry"):
        recover_history_commit(root=root, journal=journal.path, record=forged, expected_projection="e" * 64)
    (factor / "sector_data.h5").write_bytes(b"tampered")
    with pytest.raises(MonthlyBuildResumeError, match="completed industry"):
        recover_history_commit(root=root, journal=journal.path, record=later, expected_projection="e" * 64)
    assert journal.path.read_bytes() == before[0]


@pytest.mark.parametrize("fault", [None, "resume", "retire_crash", "missing_cas", "authority", "receipt", "signature", "tamper", "backup"])
def test_completed_industry_intent_preserves_pinned_later_factor_outputs(tmp_path, monkeypatch, fault):
    import copy
    from backend.services.dataset_release.monthly_industry_history import recover_history_commit
    context, compiled, root, cas, journal = _fixture(tmp_path)
    factor = root / "factor_bundle"
    factor.mkdir()
    scratch = root.parent / f".{root.name}.industry-history-{'e' * 64}"
    scratch.mkdir()
    proposed, files = copy.deepcopy(journal.record), {}
    proposed["steps"]["industry-history"] = cas.put_json({"projection_sha256": "e" * 64}).as_dict()
    pins, rows = [], []
    for name in ("sector_data.h5", "static_factors.parquet"):
        target, original = factor / name, scratch / (name + ".before")
        original.write_bytes(b"original")
        target.write_bytes(b"industry corrected")
        relative = "factor_bundle/" + name
        files[name] = {"scratch_relative_path": (scratch / name).relative_to(root.parent).as_posix(),
                       "original_signature": _signature(original), "target_signature": _signature(target)}
        proposed["files"][relative] = _signature(target)
        replacement = scratch / (name + ".factor")
        replacement.write_bytes(b"later validated factor output")
        os.replace(replacement, target)
        dataset = name.split(".")[0]
        digest = hashlib.sha256(target.read_bytes()).hexdigest()
        rows.append({"dataset": dataset, "relative_path": relative, "sha256": digest, "size": target.stat().st_size})
        pins.append({"dataset": dataset, "sha256": digest, "size": target.stat().st_size,
                     "target_signature": _signature(target)})
    authority = cas.put_json({"schema_version": factor_correction_subject.SCHEMA, "target_cutoff": "2026-09-30", "files": rows}).as_dict()
    latest = copy.deepcopy(proposed)
    latest["steps"]["fact-correction"] = authority
    for pin in pins:
        relative = "factor_bundle/" + factor_correction_subject._name(pin["dataset"])
        latest["files"][relative] = pin["target_signature"]
        pin["fact_correction_ref"] = authority
    if fault == "authority":
        latest["steps"]["fact-correction"] = cas.put_json({"schema_version": "unknown", "files": rows}).as_dict()
    if fault == "receipt":
        pins[0]["sha256"] = "f" * 64
    if fault == "signature":
        latest["files"]["factor_bundle/sector_data.h5"] = [0, 0, 0, 0]
    factor_ref = cas.put_json({"files": pins, "fact_correction_ref": authority}).as_dict()
    nested = cas.put_json({"refs": {"factor_receipt": factor_ref}}).as_dict()
    latest["steps"]["prepare"] = cas.put_json({"prepare_receipt_ref": nested}).as_dict()
    _atomic(journal.path, latest)
    pending = root.parent / f".{root.name}.industry-history-commit.json"
    _atomic(pending, {"binding_digest": latest["binding_digest"], "projection_sha256": "e" * 64,
                      "record": proposed, "files": files})
    if fault == "tamper":
        (factor / "sector_data.h5").write_bytes(b"unexplained change")
    if fault == "backup":
        (scratch / "sector_data.h5.before").write_bytes(b"lost original")
    before = journal.path.read_bytes(), {p.name: p.read_bytes() for p in factor.iterdir()}
    def recover():
        return recover_history_commit(root=root, journal=journal.path, record=latest,
            expected_projection="e" * 64, cas=None if fault == "missing_cas" else cas)
    if fault == "retire_crash":
        original_unlink = Path.unlink
        def fail_retirement(path, *args, **kwargs):
            if path == pending:
                raise OSError("interrupted intent retirement")
            return original_unlink(path, *args, **kwargs)
        monkeypatch.setattr(Path, "unlink", fail_retirement)
        with pytest.raises(OSError, match="intent retirement"):
            recover()
        assert pending.exists()
        monkeypatch.setattr(Path, "unlink", original_unlink)
        fault = None
    if fault == "resume":
        from backend.services.dataset_release import monthly_industry_history as history
        monkeypatch.setattr(history, "load_frozen_history", lambda *a, **kw: SimpleNamespace(sha256="e" * 64))
        compiled.physical_plan["build_inputs"] = {}
        retry = MonthlyBuildResume(context=replace(context, attempt=3), compiled=compiled,
            release_root=tmp_path, staging_root=root, cas=cas)
        assert retry.record == latest
        assert not pending.exists()
    elif fault:
        with pytest.raises(MonthlyBuildResumeError):
            recover()
        assert pending.exists()
    else:
        assert recover() == latest
        assert not pending.exists()
        assert recover() == latest
    assert (journal.path.read_bytes(), {p.name: p.read_bytes() for p in factor.iterdir()}) == before
    assert (root / "daily_bin/close.bin").read_bytes() == b"accurate"


def test_fresh_process_recovers_completed_export_without_any_writer(tmp_path):
    repo = str(Path(__file__).resolve().parents[3])
    script = """
import sys
sys.path.insert(0, sys.argv[1])
from pathlib import Path
from dataclasses import replace
from backend.tests.dataset_release.test_monthly_build_resume import _fixture
from backend.services.dataset_release.monthly_build_resume import MonthlyBuildResume, retained_staging_root
root = Path(sys.argv[2])
if sys.argv[3] == 'write':
    _fixture(root)
else:
    from types import SimpleNamespace
    from backend.services.dataset_release.monthly_worker import ProducerContext
    from backend.services.dataset_release.cas_store import CASStore
    context = ProducerContext(stage='BUILD', operation_id='dmr_' + '1' * 32, attempt=3,
        request={}, prior_receipts={}, plan={'candidate_root': str(root / 'release'),
        'release_id': 'september', 'target_cutoff': '2026-09-30', 'predecessor': {'manifest': 'a' * 64}})
    compiled = SimpleNamespace(source_bundle_sha256='b' * 64, physical_plan={'action_plan_digest': 'c' * 64})
    selected = retained_staging_root(context, compiled, root, root / 'never-created')
    journal = MonthlyBuildResume(context=context, compiled=compiled, release_root=root,
        staging_root=selected, cas=CASStore(root / 'control'))
    assert journal.get('prepare')['status'] == 'PASS'
    assert (selected / 'daily_bin/close.bin').read_bytes() == b'accurate'
    assert not (root / 'never-created').exists()
    print('FRESH_PROCESS_RESUME_PASS')
"""
    first = subprocess.run([sys.executable, "-I", "-c", script, repo, str(tmp_path), "write"],
        capture_output=True, text=True, timeout=30)
    assert first.returncode == 0, first.stderr
    exported = next((tmp_path / ".staging").glob("*.building")) / "daily_bin/close.bin"
    before = exported.stat()
    second = subprocess.run([sys.executable, "-I", "-c", script, repo, str(tmp_path), "read"],
        capture_output=True, text=True, timeout=30)
    assert second.returncode == 0, second.stderr
    assert "FRESH_PROCESS_RESUME_PASS" in second.stdout
    assert exported.stat().st_mtime_ns == before.st_mtime_ns


@pytest.mark.parametrize("fault", ["source", "actions", "predecessor", "target", "operation"])
def test_changed_data_binding_does_not_adopt_or_remove_old_exports(tmp_path, fault):
    context, compiled, root, _, _ = _fixture(tmp_path)
    retry = replace(context, attempt=3)
    if fault == "source":
        compiled = SimpleNamespace(source_bundle_sha256="e" * 64, physical_plan=compiled.physical_plan)
    elif fault == "actions":
        compiled = SimpleNamespace(source_bundle_sha256=compiled.source_bundle_sha256, physical_plan={"action_plan_digest": "e" * 64})
    elif fault == "predecessor":
        retry = replace(retry, plan={**retry.plan, "predecessor": {"manifest": "e" * 64}})
    elif fault == "target":
        retry = replace(retry, plan={**retry.plan, "candidate_root": str(tmp_path / "other")})
    else:
        retry = replace(retry, operation_id="dmr_" + "2" * 32)
    requested = tmp_path / "new-staging"
    assert retained_staging_root(retry, compiled, tmp_path, requested) == requested
    assert (root / "daily_bin/close.bin").read_bytes() == b"accurate"


@pytest.mark.parametrize("fault", ["changed", "missing", "escape"])
def test_unverified_data_is_not_reused_and_never_silently_reexported(tmp_path, fault):
    context, compiled, root, cas, journal = _fixture(tmp_path)
    data = root / "daily_bin/close.bin"
    if fault == "changed":
        data.write_bytes(b"wrong!!!")
    elif fault == "missing":
        data.rename(root / "preserved.bin")
    else:
        record = json.loads(journal.path.read_text(encoding="utf-8"))
        record["files"]["../outside.bin"] = [0, 0, 8, 0]
        journal.path.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises((MonthlyBuildResumeError, FileNotFoundError)):
        MonthlyBuildResume(context=replace(context, attempt=3), compiled=compiled,
            release_root=tmp_path, staging_root=root, cas=cas)


def test_incomplete_writer_is_preserved_outside_publishable_candidate(tmp_path):
    context, compiled, root, cas, _ = _fixture(tmp_path)
    (root / "minute_bin").mkdir()
    (root / "minute_bin/partial.bin").write_bytes(b"keep-all-bytes")
    retry = replace(context, attempt=3)
    journal = MonthlyBuildResume(context=retry, compiled=compiled, release_root=tmp_path, staging_root=root, cas=cas)
    journal.preserve_incomplete_writer("minute_bin")
    preserved = root.parent / f".unfinished.{context.operation_id}.minute_bin.before-3/partial.bin"
    assert preserved.read_bytes() == b"keep-all-bytes"
    assert not (root / "minute_bin").exists()
    assert (root / "daily_bin/close.bin").read_bytes() == b"accurate"


def test_old_worker_exports_without_journal_are_never_silently_reexported(tmp_path):
    context, compiled, root, _, journal = _fixture(tmp_path)
    journal.path.unlink()
    (root / "daily_bin/materialization_receipt.json").write_text("{}", encoding="utf-8")
    with pytest.raises(MonthlyBuildResumeError, match="re-export forbidden"):
        retained_staging_root(replace(context, attempt=3), compiled, tmp_path, tmp_path / "new")
    assert (root / "daily_bin/close.bin").read_bytes() == b"accurate"
    assert not (tmp_path / "new").exists()


def test_partial_finalizer_retry_preserves_packaging_and_export_bytes(tmp_path):
    context, compiled, root, cas, journal = _fixture(tmp_path)
    journal.begin_finalization()
    (root / "reports").mkdir()
    (root / "reports/completed.json").write_bytes(b"accurate-packaging")
    daily = root / "daily_bin/close.bin"
    before = daily.stat()
    retry = MonthlyBuildResume(context=replace(context, attempt=3), compiled=compiled,
        release_root=tmp_path, staging_root=root, cas=cas)
    retry.begin_finalization()
    preserved = root.parent / f".unfinished.{context.operation_id}.packaging.before-3/reports/completed.json"
    assert preserved.read_bytes() == b"accurate-packaging"
    assert not (root / "reports").exists()
    assert daily.read_bytes() == b"accurate"
    assert daily.stat().st_mtime_ns == before.st_mtime_ns


@pytest.mark.parametrize("field,value", [("execution_attempt", 2), ("operation_id", "dmr_" + "2" * 32),
    ("release_digest", "e" * 64), ("materialization_attempt", 4)])
def test_materialization_lease_never_weakens_current_execution_fence(tmp_path, field, value):
    context, compiled, root, cas, _ = _fixture(tmp_path)
    journal = MonthlyBuildResume(context=replace(context, attempt=3), compiled=compiled, release_root=tmp_path, staging_root=root, cas=cas)
    journal.write_lease("d" * 64)
    path = root / ".monthly-materialization-lease.json"
    lease = json.loads(path.read_text(encoding="utf-8"))
    lease[field] = value
    path.write_text(json.dumps(lease), encoding="utf-8")
    with pytest.raises(MonthlyBuildResumeError, match="identity"):
        materialization_attempt(root, operation_id=context.operation_id, execution_attempt=3,
            candidate_name="release", release_digest="d" * 64)


def _legacy(tmp_path):
    context = ProducerContext(stage="BUILD", operation_id="dmr_" + "a" * 32, attempt=3,
        request={}, prior_receipts={}, plan={"candidate_root": str(tmp_path / "release"),
        "release_id": "september", "target_cutoff": "2026-09-30",
        "predecessor": {"dataset_manifest_sha256": "a" * 64}})
    compiled = SimpleNamespace(source_bundle_sha256="b" * 64,
        physical_plan={"action_plan_digest": "c" * 64, "release_digest": "d" * 64})
    cas = CASStore(ControlStore.initialize(tmp_path / "control").root)
    relative = f".staging/.release.{context.operation_id}.attempt-2.building"
    root = tmp_path / relative

    def file(relative, data=b"actual-original-bytes"):
        p = root / relative
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        s = p.stat()
        return {"relative_path": relative, "sha256": hashlib.sha256(data).hexdigest(),
            "size": s.st_size, "target_signature": [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns]}

    identity = {"cutoff": "2026-09-30", "source_bundle_sha256": "b" * 64,
        "predecessor_manifest_sha256": "a" * 64, "status": "PASS"}
    operations, refs = [], {}
    for dataset, frequency in (("daily_bin", "day"), ("minute_bin", "1min")):
        cal = file(f"{dataset}/qlib/calendars/{frequency}.txt")
        instruments = file(f"{dataset}/qlib/instruments/all.txt")
        feature = file(f"{dataset}/qlib/features/000001.sz/close.{frequency}.bin")
        native = {"target_root": str(root / dataset / "qlib"), "frequency": frequency,
            "baseline_root": str(tmp_path / "baseline/components" / dataset),
            "calendar_signature": cal["target_signature"], "instruments_signature": instruments["target_signature"],
            "calendar_sha256": cal["sha256"], "instruments_sha256": instruments["sha256"],
            "feature_file_count": 1, "feature_receipts": [{"instrument": "000001.sz", "feature": "close",
                "target_size": feature["size"], "target_sha256": feature["sha256"],
                "target_signature": feature["target_signature"]}]}
        receipt = {**identity, "schema_version": "aistock_monthly_native_qlib_materialization_v1",
            "dataset": dataset, "native_writer": native}
        raw = json.dumps(receipt, sort_keys=True).encode()
        file(f"{dataset}/materialization_receipt.json", raw)
        refs[dataset] = cas.put_bytes(raw).as_dict()
        prepared = cas.put_json({**identity, "dataset": dataset,
            "schema_version": "aistock_monthly_legacy_qlib_operation_v1"})
        operations.append({"operation_id": dataset.removesuffix("_bin"), "dataset": dataset,
            "mode": "inherited_month", "preparation_ref": prepared.as_dict()})
    files = []
    for dataset in ("daily_pv", "daily_basic", "moneyflow", "bak_basic", "cyq_perf", "sector_data", "margin_detail", "static_factors"):
        name = dataset + (".parquet" if dataset == "static_factors" else ".h5")
        info = file("factor_bundle/" + name)
        info.pop("relative_path")
        files.append({"dataset": dataset, **info})
    factor_ref = cas.put_json({**identity, "schema_version": "aistock_monthly_native_factor_materialization_v1",
        "files": files, "metadata_files": []})
    index_file = file("index_context/index_daily.h5")
    index_file["relative_path"] = "index_daily.h5"
    index = {**identity, "schema_version": "aistock_monthly_native_index_materialization_v1", "files": [index_file]}
    index_raw = json.dumps(index).encode()
    file("index_context/index_materialization_receipt.json", index_raw)
    index_ref = cas.put_bytes(index_raw)
    prepare_inner = cas.put_json({"schema_version": "dataset_release_build_prepare_v1", "cutoff": "2026-09-30",
        "refs": {"factor_receipt": factor_ref.as_dict(), "index_materialization_receipt_file": index_ref.as_dict()}})
    finalize_inner = cas.put_json({"schema_version": "dataset_release_build_finalize_v1", "cutoff": "2026-09-30",
        "daily_materialization_receipt_file_ref": refs["daily_bin"],
        "minute_materialization_receipt_file_ref": refs["minute_bin"]})
    stage = {"schema_version": "dataset_release_build_stage_result_v1", "status": "PASS",
        "run_id": context.operation_id, "attempt_id": context.operation_id + "-attempt-2", "attempt_fence": 2,
        "staging_relative_path": relative, "release_id": "september", "release_digest": "d" * 64}
    prepare = cas.put_json({**stage, "stage": "prepare", "prepare_receipt_ref": prepare_inner.as_dict(), "qlib_dump_operations": operations})
    finalize = cas.put_json({**stage, "stage": "finalize-bins", "finalize_receipt_ref": finalize_inner.as_dict()})
    return context, compiled, root, cas, prepare, finalize


def test_dry_run_then_adopt_original_receipts_without_writing_exports(tmp_path):
    context, compiled, root, cas, prepare, finalize = _legacy(tmp_path)
    data = root / "minute_bin/qlib/features/000001.sz/close.1min.bin"
    before = data.stat()
    kwargs = dict(context=context, compiled=compiled, release_root=tmp_path, cas=cas,
        prepare_ref=prepare, finalize_ref=finalize)
    dry = adopt_completed_exports(**kwargs, apply=False)
    assert dry["journal_written"] is False
    result = adopt_completed_exports(**kwargs, apply=True)
    assert result["materialization_attempt"] == 2
    assert result["exports_written"] is False
    assert data.stat() == before
    assert retained_staging_root(context, compiled, tmp_path, tmp_path / "never") == root
    journal = MonthlyBuildResume(context=context, compiled=compiled, release_root=tmp_path, staging_root=root, cas=cas)
    assert journal.get("prepare")["attempt_fence"] == 2
    assert journal.get("finalize-bins")["attempt_fence"] == 2
    assert journal.get("qlib_dump_minute")["status"] == "PASS"
    with pytest.raises(FileExistsError):
        adopt_completed_exports(**kwargs, apply=True)


@pytest.mark.parametrize("fault", ["source", "predecessor", "attempt", "file", "receipt", "missing", "extra"])
def test_wrong_receipt_or_changed_export_cannot_be_adopted(tmp_path, fault):
    context, compiled, root, cas, prepare, finalize = _legacy(tmp_path)
    if fault == "source":
        compiled.source_bundle_sha256 = "e" * 64
    elif fault == "predecessor":
        context = replace(context, plan={**context.plan, "predecessor": {"dataset_manifest_sha256": "e" * 64}})
    elif fault == "attempt":
        context = replace(context, attempt=1)
    elif fault == "file":
        (root / "factor_bundle/sector_data.h5").write_bytes(b"different")
    elif fault == "receipt":
        (root / "daily_bin/materialization_receipt.json").write_bytes(b"{}")
    elif fault == "missing":
        (root / "index_context/index_daily.h5").unlink()
    else:
        (root / "minute_bin/qlib/features/000001.sz/extra.1min.bin").write_bytes(b"not-authorized")
    with pytest.raises((ValueError, OSError)):
        adopt_completed_exports(context=context, compiled=compiled, release_root=tmp_path, cas=cas,
            prepare_ref=prepare, finalize_ref=finalize, apply=True)
    assert not list((tmp_path / ".staging").glob("*.json"))


@pytest.mark.parametrize("hardlinked", [True, False])
def test_inherited_feature_requires_pinned_baseline_and_exact_unchanged_file_identity(tmp_path, hardlinked):
    context, compiled, root, cas, prepare, finalize = _legacy(tmp_path)
    baseline = tmp_path / "baseline"
    old = baseline / "components/daily_bin/features/000004.sz/close.day.bin"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"old-delisted-stock-kept")
    target = root / "daily_bin/qlib/features/000004.sz/close.day.bin"
    target.parent.mkdir()
    if hardlinked:
        os.link(old, target)
    else:
        target.write_bytes(old.read_bytes())
    manifest = {"release_id": "old", "cutoff_trade_date": "2026-08-31",
        "components": {"day_provider_catalog": {"path": "components/daily_bin/instruments/all.txt"}}}
    manifest_hash = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    manifest["dataset_manifest_sha256"] = manifest_hash
    raw = canonical_json_bytes(manifest) + b"\n"
    (baseline / "qe_dataset_manifest.json").write_bytes(raw)
    # The original materialization pins must still agree with the baseline
    # identity. Test fixture replaces only its controller binding, so keep
    # materialization identity checks explicit by rewriting fixture receipts.
    for ref in (prepare, finalize):
        stage = cas.get_json_bounded(ref, max_bytes=1024*1024)
        inner_key = "prepare_receipt_ref" if stage["stage"] == "prepare" else "finalize_receipt_ref"
        inner = cas.get_json_bounded(stage[inner_key], max_bytes=1024*1024)
        if stage["stage"] == "prepare":
            fact = cas.get_json_bounded(inner["refs"]["factor_receipt"], max_bytes=1024*1024)
            fact["predecessor_manifest_sha256"] = manifest_hash
            inner["refs"]["factor_receipt"] = cas.put_json(fact).as_dict()
            idx = cas.get_json_bounded(inner["refs"]["index_materialization_receipt_file"], max_bytes=1024*1024)
            idx["predecessor_manifest_sha256"] = manifest_hash
            idx_raw = canonical_json_bytes(idx)
            (root / "index_context/index_materialization_receipt.json").write_bytes(idx_raw)
            inner["refs"]["index_materialization_receipt_file"] = cas.put_bytes(idx_raw).as_dict()
            for op in stage["qlib_dump_operations"]:
                value = cas.get_json_bounded(op["preparation_ref"], max_bytes=1024*1024)
                value["predecessor_manifest_sha256"] = manifest_hash
                op["preparation_ref"] = cas.put_json(value).as_dict()
        else:
            for dataset in ("daily", "minute"):
                key = dataset + "_materialization_receipt_file_ref"
                value = cas.get_json_bounded(inner[key], max_bytes=1024*1024)
                value["predecessor_manifest_sha256"] = manifest_hash
                data = canonical_json_bytes(value)
                (root / (dataset + "_bin/materialization_receipt.json")).write_bytes(data)
                inner[key] = cas.put_bytes(data).as_dict()
        stage[inner_key] = cas.put_json(inner).as_dict()
        if stage["stage"] == "prepare":
            prepare = cas.put_json(stage)
        else:
            finalize = cas.put_json(stage)
    context = replace(context, plan={**context.plan, "predecessor": {"candidate_root": str(baseline),
        "dataset_manifest_sha256": manifest_hash, "cutoff": "2026-08-31", "release_id": "old"},
        "predecessor_manifest_ref": {"sha256": hashlib.sha256(raw).hexdigest()}})
    kwargs = dict(context=context, compiled=compiled, release_root=tmp_path, cas=cas,
        prepare_ref=prepare, finalize_ref=finalize, apply=False)
    if hardlinked:
        assert adopt_completed_exports(**kwargs)["inherited_unchanged_feature_file_count"] == 1
    else:
        with pytest.raises(ValueError, match="unproved inherited"):
            adopt_completed_exports(**kwargs)


def test_failed_journal_flush_keeps_exports_and_does_not_publish_partial_journal(tmp_path, monkeypatch):
    context, compiled, root, cas, prepare, finalize = _legacy(tmp_path)
    def fail_flush(_fd):
        raise OSError("simulated disk flush failure")
    monkeypatch.setattr("backend.services.dataset_release.monthly_legacy_adoption.os.fsync", fail_flush)
    with pytest.raises(OSError, match="simulated disk"):
        adopt_completed_exports(context=context, compiled=compiled, release_root=tmp_path, cas=cas,
            prepare_ref=prepare, finalize_ref=finalize, apply=True)
    assert (root / "daily_bin/qlib/features/000001.sz/close.day.bin").read_bytes() == b"actual-original-bytes"
    assert not list((tmp_path / ".staging").glob("*.json"))
    assert not list((tmp_path / ".staging").glob("*.partial"))


# Validated factual component corrections share this existing CI resume target.


def _factor_fixture(tmp_path):
    repair = tmp_path / "data_repair_staging"
    repair.mkdir()
    source = repair / "moneyflow.h5"
    frame = pd.DataFrame({"x": [1.0, 2.0]}, index=pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2026-08-31"), "300114.SZ"),
         (pd.Timestamp("2026-09-30"), "302132.SZ")], names=["datetime", "instrument"]))
    frame.to_hdf(source, key="data", format="table", data_columns=["datetime", "instrument"])
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    receipt = repair / "receipt.json"
    receipt.write_text(json.dumps({"schema_version": "aistock_auxiliary_fact_component_repair_v1",
        "status": "PRIVATE_COMPONENTS_NOT_A_RELEASE", "database_write": False,
        "profile_write": False, "runtime_action": False,
        "components": {"moneyflow": {"target_path": str(source), "target_sha256": digest,
            "existing_finite_values_unchanged": True}}}))
    static = repair / "static_factors.parquet"
    frame.to_parquet(static)
    static_receipt = repair / "static.json"
    static_receipt.write_text(json.dumps({
        "schema_version": "aistock_private_full_fact_static_repair_v1",
        "status": "PRIVATE_COMPONENT_NOT_A_RELEASE", "database_write": False,
        "profile_write": False, "runtime_action": False,
        "input_sha256": {"moneyflow": digest}, "target_path": str(static),
        "target_sha256": hashlib.sha256(static.read_bytes()).hexdigest()}))
    inputs = {"components": [{"dataset": "moneyflow", "receipt_path": str(receipt),
        "receipt_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest()},
        {"dataset": "static_factors", "receipt_path": str(static_receipt),
         "receipt_sha256": hashlib.sha256(static_receipt.read_bytes()).hexdigest()}]}
    return repair, source, inputs


def test_factor_correction_capture_reuses_existing_receipt_and_does_not_reaudit_history(tmp_path):
    repair, source, inputs = _factor_fixture(tmp_path)
    result = factor_correction_subject.capture_factor_repairs(inputs, repair_root=repair, cutoff="2026-09-30")
    assert result["files"][0]["source_path"] == str(source)
    assert result["files"][0]["dataset"] == "moneyflow"
    assert result["database_write"] is False
    assert result["historical_business_audit_performed"] is False


@pytest.mark.parametrize("defect", ["receipt_drift", "outside", "duplicate", "wrong_dataset", "traversal", "dependency"])
def test_factor_correction_capture_rejects_wrong_or_ambiguous_input(tmp_path, defect):
    repair, source, inputs = _factor_fixture(tmp_path)
    if defect == "receipt_drift":
        Path(inputs["components"][0]["receipt_path"]).write_text("{}")
    elif defect == "outside":
        inputs["components"][0]["receipt_path"] = str(tmp_path / "unregistered.json")
    elif defect == "duplicate":
        inputs["components"] *= 2
    elif defect == "wrong_dataset":
        inputs["components"][0]["dataset"] = "model"
    elif defect == "traversal":
        inputs["components"][0]["receipt_path"] = str(repair / ".." / "unregistered.json")
    else:
        row = inputs["components"][1]
        path = Path(row["receipt_path"])
        data = json.loads(path.read_bytes())
        data["input_sha256"]["moneyflow"] = "0" * 64
        path.write_text(json.dumps(data))
        row["receipt_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError):
        factor_correction_subject.capture_factor_repairs(inputs, repair_root=repair, cutoff="2026-09-30")


def _factor_prepared(tmp_path):
    repair, source, inputs = _factor_fixture(tmp_path)
    authority = factor_correction_subject.capture_factor_repairs(inputs, repair_root=repair, cutoff="2026-09-30")
    root = tmp_path / "releases/.staging/correction.building"
    (root / "factor_bundle").mkdir(parents=True)
    target = root / "factor_bundle/moneyflow.h5"
    old_frame = pd.DataFrame({"x": [2.0]}, index=pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2026-09-30"), "302132.SZ")], names=["datetime", "instrument"]))
    old_frame.to_hdf(target, key="data", format="table", data_columns=["datetime", "instrument"])
    published = tmp_path / "published.h5"
    os.link(target, published)
    static = root / "factor_bundle/static_factors.parquet"
    old_frame.to_parquet(static)
    cas = CASStore(ControlStore.initialize(tmp_path / "control").root)
    flow_proof = authority["files"][0]["validation_payload"]["components"]["moneyflow"]
    flow_proof["baseline_sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
    alias = {"status": "PASS", "moneyflow_sha256": flow_proof["baseline_sha256"],
        "expected": 1, "resolved": 1, "unknown": 0, "nonfinite": 0, "mismatched": 0}
    alias_path = root / "factor_bundle/moneyflow_alias_coverage_v1.json"
    alias_path.write_text(json.dumps(alias))
    factor = {"cutoff": "2026-09-30", "files": [{"dataset": "moneyflow",
        "sha256": hashlib.sha256(target.read_bytes()).hexdigest(), "size": target.stat().st_size,
        "target_signature": list(_signature(target)), "inherited_rows": 0, "month_rows": 1},
        {"dataset": "static_factors", "sha256": hashlib.sha256(static.read_bytes()).hexdigest(),
         "size": static.stat().st_size, "target_signature": list(_signature(static))}],
        "alias_coverage": alias, "metadata_files": [{"relative_path": alias_path.name,
            "sha256": hashlib.sha256(alias_path.read_bytes()).hexdigest(),
            "size": alias_path.stat().st_size, "target_signature": list(_signature(alias_path))}]}
    nested = {"refs": {"factor_receipt": cas.put_json(factor).as_dict()}}
    prepare = {"prepare_receipt_ref": cas.put_json(nested).as_dict()}
    record = {"binding_digest": "b" * 64, "steps": {"prepare": cas.put_json(prepare).as_dict()},
        "files": {"factor_bundle/moneyflow.h5": list(_signature(target)),
                  "factor_bundle/moneyflow_alias_coverage_v1.json": list(_signature(alias_path)),
                  "factor_bundle/static_factors.parquet": list(_signature(static))}}
    journal = tmp_path / "journal.json"
    _atomic(journal, record)
    resume = SimpleNamespace(root=root, path=journal, record=record,
        context=SimpleNamespace(operation_id="operation", checkpoint=lambda: None))
    binding = {"authority_ref": cas.put_json(authority).as_dict(), "operation_id": "operation",
        "target_cutoff": "2026-09-30"}
    plan = {"target_cutoff": "2026-09-30", factor_correction_subject.BINDING: binding}
    return resume, prepare, plan, cas, source, target, published


def test_factor_correction_apply_copy_on_write_preserves_old_release_and_is_idempotent(tmp_path):
    resume, prepare, plan, cas, source, target, old = _factor_prepared(tmp_path)
    before = old.read_bytes()
    result = factor_correction_subject.apply_factor_repairs(resume, prepare=prepare, plan=plan, cas=cas)
    assert old.read_bytes() == before and target.read_bytes() == source.read_bytes()
    nested = cas.get_json(result["prepare_receipt_ref"])
    factor = cas.get_json(nested["refs"]["factor_receipt"])
    pin = factor["files"][0]
    assert pin["inherited_rows"] == 1 and pin["month_rows"] == 1
    assert pin["target_signature"] == list(_signature(target))
    assert factor["alias_coverage"]["expected"] == factor["alias_coverage"]["resolved"] == 1
    assert factor["alias_coverage"]["moneyflow_sha256"] == pin["sha256"]
    assert factor_correction_subject.apply_factor_repairs(resume, prepare=result, plan=plan, cas=cas) == result


def test_factor_correction_source_drift_never_replaces_completed_output(tmp_path):
    resume, prepare, plan, cas, source, target, old = _factor_prepared(tmp_path)
    before = target.read_bytes()
    with source.open("ab") as writer:
        writer.write(b"unexpected")
    with pytest.raises(ValueError, match="changed|differs"):
        factor_correction_subject.apply_factor_repairs(resume, prepare=prepare, plan=plan, cas=cas)
    assert target.read_bytes() == before and old.read_bytes() == before


def test_factor_correction_interrupted_replace_recovers_without_reexport(tmp_path, monkeypatch):
    resume, prepare, plan, cas, source, target, old = _factor_prepared(tmp_path)
    before = old.read_bytes()
    replace = factor_correction_subject.os.replace

    def interrupted(a, b):
        if Path(b) == target:
            raise OSError("interrupted")
        return replace(a, b)

    monkeypatch.setattr(factor_correction_subject.os, "replace", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        factor_correction_subject.apply_factor_repairs(resume, prepare=prepare, plan=plan, cas=cas)
    monkeypatch.setattr(factor_correction_subject.os, "replace", replace)
    resume.record = factor_correction_subject.recover_factor_repairs(root=resume.root, journal=resume.path,
        record=resume.record, cas=cas)
    assert target.read_bytes() == source.read_bytes() and old.read_bytes() == before
    result = cas.get_json(resume.record["steps"]["prepare"])
    assert factor_correction_subject.apply_factor_repairs(resume, prepare=result, plan=plan, cas=cas) == result


def _factor_service_fixture(tmp_path):
    repair, source, inputs = _factor_fixture(tmp_path)
    root = tmp_path / "operation"
    root.mkdir()
    plan = {"candidate_root": str(tmp_path / "releases/correction"), "target_cutoff": "2026-09-30",
            "monthly_ready_correction": {"base_manifest_ref": {"sha256": "a" * 64}}}
    state = {"status": "FAILED", "current_stage": "BUILD"}
    checkpoints = {"SOURCE": {"canonical_sha256": "b" * 64}}
    store = SimpleNamespace(operation_root=lambda _: root, read_plan=lambda _: plan,
        read_state=lambda _: state, read_checkpoint=lambda _, stage: checkpoints.get(stage),
        replace_plan=lambda _, value: plan.update(value))
    return SimpleNamespace(store=store), inputs, plan, state, checkpoints, CASStore(
        ControlStore.initialize(tmp_path / "control").root)


def test_factor_correction_formal_binding_is_idempotent_and_does_not_activate_or_resume(tmp_path):
    service, inputs, plan, state, checkpoints, cas = _factor_service_fixture(tmp_path)
    result = MonthlyReleaseService.bind_factor_repairs(service, "operation", inputs=inputs, principal="operator", cas=cas)
    assert result["status"] == "BOUND" and state == {"status": "FAILED", "current_stage": "BUILD"}
    assert set(checkpoints) == {"SOURCE"}
    assert plan[factor_correction_subject.BINDING]["activation_authorized"] is False
    assert result == MonthlyReleaseService.bind_factor_repairs(
        service, "operation", inputs=inputs, principal="operator", cas=cas)
    with pytest.raises(MonthlyReleaseConflict, match="binding changed"):
        MonthlyReleaseService.bind_factor_repairs(service, "operation", inputs={}, principal="operator", cas=cas)


@pytest.mark.parametrize("defect", ["running", "not_correction", "no_source", "build_done"])
def test_factor_correction_formal_binding_requires_idle_private_correction(tmp_path, defect):
    service, inputs, plan, state, checkpoints, cas = _factor_service_fixture(tmp_path)
    if defect == "running":
        state["status"] = "RUNNING"
    elif defect == "not_correction":
        plan.pop("monthly_ready_correction")
    elif defect == "no_source":
        checkpoints.clear()
    else:
        checkpoints["BUILD"] = {}
    with pytest.raises(MonthlyReleaseConflict, match="idle retained BUILD"):
        MonthlyReleaseService.bind_factor_repairs(service, "operation", inputs=inputs, principal="operator", cas=cas)


def test_factor_correction_factor_binding_route_uses_existing_operator_and_service_contract(tmp_path):
    from backend.routers.monthly_dataset_releases import (
        UnifiedMonthlyRepairInputsRequest, bind_monthly_factor_repairs,
    )
    service, inputs, plan, state, checkpoints, cas = _factor_service_fixture(tmp_path)
    adapter = SimpleNamespace(bind_factor_repairs=lambda operation_id, **kwargs:
        MonthlyReleaseService.bind_factor_repairs(service, operation_id, cas=cas, **kwargs))
    result = bind_monthly_factor_repairs("operation", UnifiedMonthlyRepairInputsRequest(inputs=inputs),
        SimpleNamespace(principal_id="operator"), adapter)
    assert result["data"]["status"] == "BOUND"


def test_factor_correction_completed_commit_recovery_keeps_later_receipts(tmp_path, monkeypatch):
    resume, prepare, plan, cas, source, target, old = _factor_prepared(tmp_path)
    unlink = Path.unlink

    def interrupted(path, *args, **kwargs):
        if path == factor_correction_subject._pending(resume.root):
            raise OSError("after journal commit")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", interrupted)
    with pytest.raises(OSError, match="after journal commit"):
        factor_correction_subject.apply_factor_repairs(resume, prepare=prepare, plan=plan, cas=cas)
    monkeypatch.setattr(Path, "unlink", unlink)
    resume.record = json.loads(resume.path.read_bytes())
    resume.record["steps"]["later-validation"] = cas.put_json({"status": "PASS"}).as_dict()
    _atomic(resume.path, resume.record)
    recovered = factor_correction_subject.recover_factor_repairs(root=resume.root, journal=resume.path, record=resume.record, cas=cas)
    assert recovered == resume.record and "later-validation" in recovered["steps"]


def test_factor_correction_runner_applies_factor_repairs_after_industry_without_reexport(tmp_path, monkeypatch):
    from backend.services.dataset_release import monthly_mature_build_runner as runner_module
    from backend.services.dataset_release import monthly_industry_history
    from backend.tests.dataset_release.test_monthly_mature_build_runner import _native_month_fixture
    runner, context, staging, compiled, operation, _, _ = _native_month_fixture(tmp_path)
    order = []

    def stage(invocation):
        result = {"schema_version": "dataset_release_build_stage_result_v1", "stage": invocation.stage, "status": "PASS"}
        if invocation.stage == "prepare":
            result["qlib_dump_operations"] = [operation]
        return result

    def applied(resume, *, prepare, **kwargs):
        order.append("factor")
        return prepare

    def industry(resume, *, prepare, **kwargs):
        order.append("industry")
        return prepare

    monkeypatch.setattr(runner_module, "run_build_stage", stage)
    monkeypatch.setattr(factor_correction_subject, "apply_factor_repairs", applied)
    monkeypatch.setattr(monthly_industry_history, "apply_retained_history", industry)
    runner.execute(context=context, staging_root=staging, compiled=compiled)
    assert order == ["industry", "factor"]


def test_factor_correction_next_ready_correction_retains_completed_factor_proof(tmp_path):
    from backend.services.dataset_release.monthly_ready_correction import seed_completed_exports
    from backend.services.dataset_release.monthly_build_resume import MonthlyBuildResume
    from backend.tests.dataset_release.test_monthly_mature_build_runner import _context, _compiled
    resume, prepare, plan, cas, source, target, old = _factor_prepared(tmp_path)
    prepared_result = factor_correction_subject.apply_factor_repairs(resume, prepare=prepare, plan=plan, cas=cas)
    base = resume.root
    (base / "reports").mkdir()
    compiled = _compiled()
    context = _context()
    evidence = base / "reports/monthly_candidate_build_evidence.json"
    evidence.write_text(json.dumps({"operation_id": context.operation_id,
        "source_bundle_sha256": compiled.source_bundle_sha256, "stage_refs": {
            "prepare": cas.put_json(prepared_result).as_dict(), "finalize_bins": cas.put_json({"status": "PASS"}).as_dict()}}))
    manifest = base / "qe_dataset_manifest.json"
    manifest.write_text(json.dumps({"components": {"evidence": {"path": "reports/monthly_candidate_build_evidence.json",
        "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(), "size": evidence.stat().st_size}}}))
    context = replace(context, attempt=3, plan={**context.plan, "candidate_root": str(tmp_path / "successor"),
        "monthly_ready_correction": {"base_plan": {"candidate_root": str(base)},
            "base_manifest_ref": {"sha256": hashlib.sha256(manifest.read_bytes()).hexdigest()}}})
    staging = tmp_path / ".staging" / f".successor.{context.operation_id}.attempt-3.building"
    assert seed_completed_exports(context=context, staging_root=staging, compiled=compiled, cas=cas)
    seeded = MonthlyBuildResume(context=context, compiled=compiled, staging_root=staging, release_root=tmp_path, cas=cas)
    assert seeded.record["steps"]["fact-correction"] == plan[factor_correction_subject.BINDING]["authority_ref"]


def _index_fixture(tmp_path):
    repair = tmp_path / 'data_repair_staging'
    out = repair / 'index-history'
    out.mkdir(parents=True)
    baseline = tmp_path / 'backtest_dataset_candidates/base'
    baseline.mkdir(parents=True)
    manifest = baseline / 'qe_dataset_manifest.json'
    manifest.write_text('{}')
    rows = [dict(ts_code=d.daily_code,trade_date=day,open=10,high=12,low=9,close=11,
                 pre_close=10,pct_chg=10,vol=5,amount=50)
            for d in DOMESTIC_INDEX_DEFINITIONS for day in ['2026-08-31','2026-09-30']]
    source = out / 'source.json'
    source.write_text(json.dumps(dict(schema_version='aistock_frozen_index_facts_v1',
        source_dataset='market.index_daily', rows=rows, captured_at='2026-10-20T00:00:00+00:00')))
    context = _to_context_frame(rows)
    context.to_parquet(out/'index_context.parquet')
    raw = pd.DataFrame([dict(ts_code=r['ts_code'],trade_date=pd.Timestamp(r['trade_date']),
        **{k:float(r[k]) for k in ['open','high','low','close','amount']},volume=float(r['vol'])*100) for r in rows])
    raw.to_hdf(out/'index_daily.h5',key='data',format='fixed')
    (baseline/'index_context').mkdir()
    for name in ['index_daily.h5','index_context.parquet']:
        (baseline/'index_context'/name).write_bytes((out/name).read_bytes())
    inventory = []
    for code,frame in _index_csv_frames(context).items():
        for field in frame.columns[2:]:
            path = out/'index_features'/code.lower()/(field+'.day.bin')
            path.parent.mkdir(parents=True,exist_ok=True)
            original = np.array([0,np.nan,float(frame[field].iloc[-1])],dtype='<f4').tobytes()
            target = np.r_[np.float32(0),frame[field].to_numpy(dtype='<f4')].astype('<f4').tobytes()
            path.write_bytes(target)
            inventory.append(dict(code=code,feature=field,target_path=str(path),
                original_sha256=hashlib.sha256(original).hexdigest(),corrected_sha256=_index_sha(path)))
    receipt = dict(schema_version=index_repair_subject.PRODUCER_SCHEMA,baseline_candidate=str(baseline),
        source_sha256=_index_sha(source),raw_h5_sha256=_index_sha(out/'index_daily.h5'),
        context_parquet_sha256=_index_sha(out/'index_context.parquet'),inventory=inventory,
        status='PRIVATE_COMPONENT_NOT_PUBLISHED', database_write=False,profile_write=False,runtime_action=False,
        full_source_calendar_coverage_verified=True,no_quote_interpolation=True,
        raw_h5_amount_unit='thousand_cny',normalized_and_bin_amount_unit='cny',source_rows=len(rows))
    receipt_path = out/'receipt.json'
    receipt_path.write_text(json.dumps(receipt))
    inputs = dict(receipt_path=str(receipt_path),receipt_sha256=_index_sha(receipt_path))
    cas = CASStore(ControlStore.initialize(tmp_path/'control').root)
    base_ref = dict(sha256=_index_sha(manifest))
    return repair, out, inputs, cas, base_ref


def _index_sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_index_adoption_capture_freezes_existing_outputs_and_source_units(tmp_path):
    repair,out,inputs,cas,base = _index_fixture(tmp_path)
    result = index_repair_subject.capture_index_repairs(inputs,repair_root=repair,base_manifest_ref=base,cutoff='2026-09-30',cas=cas)
    assert len(result['files']) == 146
    assert result['source_rows'] == 24 and result['database_write'] is False
    assert result['base_manifest_ref'] == base
    assert cas.get_json(result['source_ref'])['source_dataset'] == 'market.index_daily'


@pytest.mark.parametrize('defect',['source','receipt','output','baseline','wrong_index','duplicate','unit'])
def test_index_adoption_capture_rejects_drift_and_cross_component_data(tmp_path,defect):
    repair,out,inputs,cas,base = _index_fixture(tmp_path)
    receipt = json.loads((out/'receipt.json').read_bytes())
    if defect == 'source':
        (out/'source.json').write_text('{}')
    elif defect == 'output':
        (out/'index_context.parquet').write_bytes(b'bad')
    elif defect == 'baseline':
        base['sha256'] = '0'*64
    elif defect == 'wrong_index':
        receipt['inventory'][0]['code'] = '600519.SH'
    elif defect == 'duplicate':
        receipt['inventory'].append(receipt['inventory'][0])
    elif defect == 'unit':
        receipt['raw_h5_amount_unit'] = 'cny'
    else:
        inputs['receipt_sha256'] = '0'*64
    if defect in {'wrong_index','duplicate','unit'}:
        (out/'receipt.json').write_text(json.dumps(receipt))
        inputs['receipt_sha256'] = _index_sha(out/'receipt.json')
    with pytest.raises((ValueError,KeyError)):
        index_repair_subject.capture_index_repairs(inputs,repair_root=repair,base_manifest_ref=base,cutoff='2026-09-30',cas=cas)


def _index_prepared(tmp_path):
    repair,out,inputs,cas,base = _index_fixture(tmp_path)
    authority = index_repair_subject.capture_index_repairs(inputs,repair_root=repair,base_manifest_ref=base,cutoff='2026-09-30',cas=cas)
    root = tmp_path/'releases/.staging/correction.building'
    root.mkdir(parents=True)
    files = {}
    features = []
    for row in authority['files']:
        path = root/row['relative_path']
        path.parent.mkdir(parents=True,exist_ok=True)
        if row['relative_path'].endswith('.bin'):
            values = np.frombuffer(cas.get_bytes(cas.validate_reference_metadata(row['target_ref'])),dtype='<f4').copy()
            values[1] = np.nan
            path.write_bytes(values.tobytes())
            features.append(dict(instrument=row['code'],feature=row['feature'],target_sha256=_index_sha(path),
                target_size=path.stat().st_size,target_signature=list(_signature(path)),start_index=0,end_index=1))
        else:
            path.write_bytes(b'original')
        row['original_sha256'] = _index_sha(path)
        files[row['relative_path']] = list(_signature(path))
    daily = dict(native_writer=dict(feature_receipts=features),cutoff='2026-09-30')
    native = root/'daily_bin/materialization_receipt.json'
    native.write_text(json.dumps(daily))
    files['daily_bin/materialization_receipt.json'] = list(_signature(native))
    index = dict(month_rows=12,rows=12,files=[dict(relative_path=r['relative_path'].removeprefix('index_context/'))
        for r in authority['files'] if r['relative_path'].startswith('index_context/')])
    index_path = root/'index_context/index_materialization_receipt.json'
    index_path.write_text(json.dumps(index))
    files['index_context/index_materialization_receipt.json'] = list(_signature(index_path))
    prep = dict(prepare_receipt_ref=cas.put_json(dict(refs=dict(index_receipt=cas.put_json(index).as_dict()))).as_dict())
    final = dict(finalize_receipt_ref=cas.put_json(dict(daily_receipt_ref=cas.put_json(daily).as_dict())).as_dict())
    record = dict(binding_digest='b'*64,files=files,steps=dict(prepare=cas.put_json(prep).as_dict(),
        **{'finalize-bins':cas.put_json(final).as_dict()}))
    journal = tmp_path/'journal.json'
    _atomic(journal,record)
    resume = SimpleNamespace(root=root,path=journal,record=record,
        context=SimpleNamespace(operation_id='operation',checkpoint=lambda:None))
    binding = dict(operation_id='operation',target_cutoff='2026-09-30',authority_ref=cas.put_json(authority).as_dict())
    plan = dict(target_cutoff='2026-09-30',monthly_ready_correction=dict(base_manifest_ref=base),
        **{index_repair_subject.BINDING:binding})
    return resume,prep,final,plan,cas,authority


def test_index_adoption_apply_copy_on_write_updates_real_pins_and_is_idempotent(tmp_path):
    resume,prep,final,plan,cas,authority = _index_prepared(tmp_path)
    target = resume.root/authority['files'][0]['relative_path']
    old = tmp_path/'published.bin'
    os.link(target,old)
    before = old.read_bytes()
    result_p,result_f = index_repair_subject.apply_index_repairs(resume,prepare=prep,finalized=final,plan=plan,cas=cas)
    assert old.read_bytes() == before and target.read_bytes() != before
    index = cas.get_json(cas.get_json(result_p['prepare_receipt_ref'])['refs']['index_receipt'])
    assert index['normalized_parquet_scope'] == 'frozen_history_and_month_delta'
    daily = cas.get_json(cas.get_json(result_f['finalize_receipt_ref'])['daily_receipt_ref'])
    assert daily['index_fact_correction']['authority_ref'] == plan[index_repair_subject.BINDING]['authority_ref']
    assert daily['native_writer']['feature_receipts'][0]['target_signature'] == list(_signature(target))
    assert index_repair_subject.apply_index_repairs(resume,prepare=result_p,finalized=result_f,plan=plan,cas=cas) == (result_p,result_f)


def test_index_adoption_apply_rejects_changed_output_before_any_replacement(tmp_path):
    resume,prep,final,plan,cas,authority = _index_prepared(tmp_path)
    target = resume.root/authority['files'][-1]['relative_path']
    target.write_bytes(b'concurrent')
    first = resume.root/authority['files'][0]['relative_path']
    before = first.read_bytes()
    with pytest.raises(ValueError,match='changed|provenance|original'):
        index_repair_subject.apply_index_repairs(resume,prepare=prep,finalized=final,plan=plan,cas=cas)
    assert first.read_bytes() == before


def test_index_adoption_commit_interruption_recovers_without_export_or_old_release_write(tmp_path,monkeypatch):
    resume,prep,final,plan,cas,authority = _index_prepared(tmp_path)
    replace = index_repair_subject.os.replace
    count = 0
    def interrupted(path,payload):
        nonlocal count
        count += 1
        if count == 2:
            raise OSError('interruption')
        return replace(path,payload)
    monkeypatch.setattr(index_repair_subject.os,'replace',interrupted)
    with pytest.raises(OSError,match='interruption'):
        index_repair_subject.apply_index_repairs(resume,prepare=prep,finalized=final,plan=plan,cas=cas)
    monkeypatch.setattr(index_repair_subject.os,'replace',replace)
    resume.record = index_repair_subject.recover_index_repairs(root=resume.root,journal=resume.path,record=resume.record,cas=cas)
    p = cas.get_json(resume.record['steps']['prepare'])
    f = cas.get_json(resume.record['steps']['finalize-bins'])
    assert index_repair_subject.apply_index_repairs(resume,prepare=p,finalized=f,plan=plan,cas=cas) == (p,f)


@pytest.mark.parametrize('field,new,accepted',[
    ('amount',50000,True),('amount',50,True),('amount',5000,False),('close',50000,False)])
def test_index_adoption_only_diagnosed_amount_unit_conversion_can_change_finite_values(field,new,accepted):
    old = np.array([0,50],dtype='<f4').tobytes()
    target = np.array([0,new],dtype='<f4').tobytes()
    if accepted:
        index_repair_subject.validate_feature_change(old,target,field)
    else:
        with pytest.raises(ValueError,match='finite fact'):
            index_repair_subject.validate_feature_change(old,target,field)


def test_index_adoption_binding_uses_existing_operator_route_without_resume_or_activation(tmp_path):
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseService, MonthlyReleaseConflict
    from backend.routers.monthly_dataset_releases import bind_monthly_index_fact_repairs,UnifiedMonthlyRepairInputsRequest
    repair,out,inputs,cas,base = _index_fixture(tmp_path)
    root = tmp_path/'operation'
    root.mkdir()
    plan = dict(candidate_root=str(tmp_path/'backtest_dataset_candidates/correction'),target_cutoff='2026-09-30',
                monthly_ready_correction=dict(base_manifest_ref=base))
    state = dict(status='FAILED',current_stage='BUILD')
    checkpoints = {'SOURCE':{'canonical_sha256':'b'*64}}
    service = SimpleNamespace(store=SimpleNamespace(operation_root=lambda _:root,read_plan=lambda _:plan,
        read_state=lambda _:state,read_checkpoint=lambda _,s:checkpoints.get(s),replace_plan=lambda _,p:plan.update(p)))
    adapter = SimpleNamespace(bind_index_fact_repairs=lambda op,**kwargs:
        MonthlyReleaseService.bind_index_fact_repairs(service,op,cas=cas,**kwargs))
    bound = bind_monthly_index_fact_repairs('operation',UnifiedMonthlyRepairInputsRequest(inputs=inputs),
        SimpleNamespace(principal_id='operator'),adapter)
    assert bound['data']['status'] == 'BOUND'
    assert plan[index_repair_subject.BINDING]['activation_authorized'] is False
    assert state['status'] == 'FAILED' and set(checkpoints) == {'SOURCE'}
    assert bound == bind_monthly_index_fact_repairs('operation',UnifiedMonthlyRepairInputsRequest(inputs=inputs),
        SimpleNamespace(principal_id='operator'),adapter)
    with pytest.raises(MonthlyReleaseConflict,match='binding changed'):
        MonthlyReleaseService.bind_index_fact_repairs(service,'operation',inputs={},principal='operator',cas=cas)


@pytest.mark.parametrize('defect',['running','published','wrong_operation','cutoff','authority'])
def test_index_adoption_apply_never_writes_outside_the_same_private_operation(tmp_path,defect):
    resume,prep,final,plan,cas,authority = _index_prepared(tmp_path)
    if defect == 'running':
        resume.path = None
    elif defect == 'published':
        (resume.root/'qe_dataset_manifest.json').write_text('{}')
    elif defect == 'wrong_operation':
        plan[index_repair_subject.BINDING]['operation_id'] = 'other'
    elif defect == 'cutoff':
        plan['target_cutoff'] = '2026-10-30'
    else:
        plan['monthly_ready_correction']['base_manifest_ref'] = {'sha256':'c'*64}
    first = resume.root/authority['files'][0]['relative_path']
    before = first.read_bytes()
    with pytest.raises(ValueError):
        index_repair_subject.apply_index_repairs(resume,prepare=prep,finalized=final,plan=plan,cas=cas)
    assert first.read_bytes() == before


def test_index_adoption_committed_transaction_cleanup_keeps_later_steps(tmp_path,monkeypatch):
    resume,prep,final,plan,cas,authority = _index_prepared(tmp_path)
    unlink = index_repair_subject.Path.unlink
    def interrupted(path,*args,**kwargs):
        if path == index_repair_subject._pending(resume.root):
            raise OSError('after commit')
        return unlink(path,*args,**kwargs)
    monkeypatch.setattr(index_repair_subject.Path,'unlink',interrupted)
    with pytest.raises(OSError,match='after commit'):
        index_repair_subject.apply_index_repairs(resume,prepare=prep,finalized=final,plan=plan,cas=cas)
    monkeypatch.setattr(index_repair_subject.Path,'unlink',unlink)
    resume.record = json.loads(resume.path.read_bytes())
    resume.record['steps']['later'] = cas.put_json({'status':'PASS'}).as_dict()
    result = index_repair_subject.recover_index_repairs(root=resume.root,journal=resume.path,record=resume.record,cas=cas)
    assert result == resume.record and 'later' in result['steps']


@pytest.mark.parametrize('defect',['running','no_source','not_correction','build_done'])
def test_index_adoption_binding_cannot_mutate_a_running_or_published_operation(tmp_path,defect):
    from backend.services.dataset_release.monthly_unified import MonthlyReleaseService,MonthlyReleaseConflict
    root = tmp_path/'operation'
    root.mkdir()
    plan = dict(monthly_ready_correction={})
    state = dict(status='FAILED',current_stage='BUILD')
    checkpoints = {'SOURCE':{}}
    if defect == 'running':
        state['status'] = 'RUNNING'
    elif defect == 'no_source':
        checkpoints.clear()
    elif defect == 'not_correction':
        plan.clear()
    else:
        checkpoints['BUILD'] = {}
    service = SimpleNamespace(store=SimpleNamespace(operation_root=lambda _:root,
        read_plan=lambda _:plan,read_state=lambda _:state,read_checkpoint=lambda _,s:checkpoints.get(s)))
    with pytest.raises(MonthlyReleaseConflict,match='idle retained BUILD'):
        MonthlyReleaseService.bind_index_fact_repairs(service,'operation',inputs={},principal='operator')


def test_index_adoption_runner_adopts_index_after_existing_factor_and_quote_repairs(tmp_path,monkeypatch):
    from backend.services.dataset_release import monthly_mature_build_runner as runner_module
    from backend.tests.dataset_release.test_monthly_mature_build_runner import _native_month_fixture
    runner,context,staging,compiled,operation,_,_ = _native_month_fixture(tmp_path)
    invoked = []
    def stage(invocation):
        result = {'schema_version':'dataset_release_build_stage_result_v1','stage':invocation.stage,'status':'PASS'}
        if invocation.stage == 'prepare':
            result['qlib_dump_operations'] = [operation]
        return result
    def applied(resume,*,prepare,finalized,**kwargs):
        invoked.append('index')
        return prepare,finalized
    monkeypatch.setattr(runner_module,'run_build_stage',stage)
    monkeypatch.setattr(index_repair_subject,'apply_index_repairs',applied)
    runner.execute(context=context,staging_root=staging,compiled=compiled)
    assert invoked == ['index']
