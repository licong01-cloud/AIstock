"""Direct measurement, frozen D reader, identity and immutable zero-fit contracts."""
import hashlib
import json
import os
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.data_service.frozen_dataset import stamp
from backend.services.advisory_model_first import minute_recovery_inputs_v1 as inp
from backend.services.advisory_model_first import minute_recovery_v1 as core
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.volume_conditioned_pressure_top10_5td_v1 import minute_features as old

DAY = pd.Timestamp("2024-07-04")


def measure(values, slots=None):
    return core.recovery_features(core.core_clock(DAY) if slots is None else slots, values, day=DAY)


def test_event_censoring_ties_no_drawdown_and_lunch():
    p = np.full(240, 100.)
    none = measure(p)
    assert none["recovery_status"] == "NO_DRAWDOWN" and none["recovery_hold_fraction"] == 1
    p[118:121] = 90
    p[150:152] = 90  # Equal drawdown: the earlier trough is selected.
    r = measure(p)
    assert r["recovery_status"] == "RECOVERED"
    assert r["recovery_delay_fraction"] == 3/239  # No lunch wall-time added.
    assert r["trough_clock_fraction"] == 118/239
    assert r["recovery_hold_fraction"] == 117/121
    p[121:] = 95
    r = measure(p)
    assert r["recovery_status"] == "UNRECOVERED"
    assert r["recovery_delay_fraction"] == 121/239 and r["unrecovered_fraction"] == .5
    p[:] = 100
    p[-1] = 90
    r = measure(p)
    assert r["is_unrecovered"] == 1 and r["recovery_delay_fraction"] == 0
    assert r["recovery_hold_fraction"] == 0 and r["unrecovered_fraction"] == 1


def test_optional_anchors_missing_slots_and_missing_closes():
    slots = core.core_clock(DAY)
    p = np.linspace(100, 101, 240)
    anchor = pd.DatetimeIndex([DAY + pd.Timedelta(hours=9, minutes=30), DAY + pd.Timedelta(hours=13)])
    extended = slots.union(anchor)
    values = pd.Series(p, index=slots).reindex(extended).to_numpy()
    assert measure(values, extended) == measure(p)
    for s, v in ((slots.delete(4), np.delete(p, 4)), (slots, np.where(np.arange(240) == 4, np.nan, p))):
        r = measure(v, s)
        assert r["recovery_status"] == "UNKNOWN_INCOMPLETE_CLOSE_PATH"
        assert all(np.isnan(r[n]) for n in (*core.CONTROLS, *core.NEW))
        assert r["recovery_core_known"] == 239


@pytest.mark.parametrize("kind", ["inf", "zero", "negative", "duplicate", "reverse", "future", "lunch", "seconds", "timezone", "shape"])
def test_invalid_known_values_and_clock_raise(kind):
    s, p = core.core_clock(DAY), np.ones(240)
    if kind in ("inf", "zero", "negative"):
        p[2] = {"inf": np.inf, "zero": 0., "negative": -1.}[kind]
    elif kind == "duplicate":
        s = s[:1].append(s[:-1])
    elif kind == "reverse":
        s = s[::-1]
    elif kind == "future":
        s = s + pd.Timedelta(days=1)
    elif kind == "lunch":
        s = s[:120].append(pd.DatetimeIndex([DAY + pd.Timedelta(hours=12)])).append(s[121:])
    elif kind == "seconds":
        s = s + pd.Timedelta(seconds=1)
    elif kind == "timezone":
        s = s.tz_localize("Asia/Shanghai")
    else:
        p = p.reshape(240, 1)
    with pytest.raises(ValueError):
        measure(p, s)


def test_constructive_counterexample_and_frozen_column_contract():
    s = pd.DatetimeIndex([DAY + pd.Timedelta(hours=9, minutes=30)]).append(core.core_clock(DAY))
    a, b = np.full(241, 100.), np.full(241, 100.)
    a[18:24] = a[27:29] = 99
    b[18:20] = b[23:29] = 99
    def arrays(p):
        return {**{n: p.copy() for n in inp.FIELDS[:4]}, "volume": np.ones(241)}
    aa, bb = old(s, arrays(a), day=DAY), old(s, arrays(b), day=DAY)
    for n in (*core.OLD, *core.CONDITIONAL):
        np.testing.assert_allclose(aa[n], bb[n], rtol=1e-12, atol=1e-10, equal_nan=True)
    assert measure(a, s)["recovery_delay_fraction"] == 6/239
    assert measure(b, s)["recovery_delay_fraction"] == 2/239
    assert len(core.MATCHED) == 55 and len(set(core.FEATURES)) == 60


def population():
    f = pd.DataFrame([dict(package_id=inp.PACKAGE, manifest_sha256=inp.MANIFEST, run_id=inp.RUN,
                          decision_as_of_trade_date=DAY, target_trade_date=DAY+pd.Timedelta(days=1),
                          instrument=f"{n:06d}.SZ", selection_effective_rank=n, candidate_group_size=50,
                          source_id=inp.SOURCE_ID, parent_score=np.float32(n),
                          **{field: float(n) for field in inp.INPUT_FEATURES}) for n in range(1, 51)])
    return f[list(inp.ORIGINAL)]


@pytest.mark.parametrize("kind", ["duplicate", "rank", "date", "target", "identity", "instrument", "inf"])
def test_population_fail_closed(kind, monkeypatch):
    monkeypatch.setattr(inp, "END", str(DAY.date()))
    f = population()
    inp.validate_population(f, [DAY, DAY+pd.Timedelta(days=1)])
    if kind == "duplicate":
        f.loc[1] = f.loc[0]
    elif kind == "rank":
        f.loc[1, "selection_effective_rank"] = 1
    elif kind == "date":
        f[inp.DAY] += pd.Timedelta(days=1)
    elif kind == "target":
        f[inp.TARGET] += pd.Timedelta(days=1)
    elif kind == "identity":
        f.loc[0, "manifest_sha256"] = "f" * 64
    elif kind == "instrument":
        f.loc[0, "instrument"] = "../escape"
    else:
        f.loc[0, "ret_1"] = np.inf
    with pytest.raises(ValueError):
        inp.validate_population(f, [DAY, DAY+pd.Timedelta(days=1)])


def dataset(root, symbols=("000001.SZ",)):
    root.mkdir()
    slots = core.core_clock(DAY).append(core.core_clock(DAY+pd.Timedelta(days=1)))
    calendar = root / "calendar.txt"
    calendar.write_text("\n".join(map(str, slots)), encoding="utf-8")
    meta = root / "meta.json"
    meta.write_text("{}", encoding="utf-8")
    files = dict(minute_calendar=calendar, minute_meta_export=meta, minute_provider_catalog=meta)
    lease = {}
    for symbol in symbols:
        folder = root / "features" / symbol.lower()
        folder.mkdir(parents=True)
        for field in inp.FIELDS:
            # Future T contains poison; a D-only reader must never decode it.
            p = folder / (field + ".1min.bin")
            values = np.r_[0., np.ones(240)*100, np.full(240, np.inf)].astype("<f4")
            p.write_bytes(values.tobytes())
            lease[p.relative_to(root).as_posix()] = stamp(p)
    return SimpleNamespace(root=root, _bin_lease=lease, calendar=[DAY, DAY+pd.Timedelta(days=1)],
                           file=lambda name: files[name], assert_unchanged=lambda: None, close=lambda: None)


def test_reader_bounded_D_slices_retry_hashes_and_unknown(tmp_path):
    ds = dataset(tmp_path / "data")
    f = population().iloc[:2].copy()  # Second symbol is absent in the frozen lease.
    b = inp.Budget()
    m, index = inp.read_minutes(f, ds, b)
    assert b.bytes == 5*(4+240*4)
    assert m.recovery_status.tolist() == ["NO_DRAWDOWN", "UNKNOWN_INCOMPLETE_CLOSE_PATH"]
    joined, daily = inp.attach(f, m)
    assert joined[list(inp.ORIGINAL)].equals(f)
    assert joined.model_support.tolist() == [True, False] and not daily.complete_support_D.any()
    empty, retry = inp.read_minutes(f, ds, inp.Budget(), compute=False)
    assert empty.empty and retry == index
    path = ds.root / index[0]["path"]
    path.write_bytes(path.read_bytes()[:-4])
    with pytest.raises(ValueError, match="lease"):
        inp.read_minutes(f, ds, inp.Budget())


def test_bad_header_budget_and_source_drift(tmp_path, monkeypatch):
    ds = dataset(tmp_path / "data")
    path = ds.root / "features/000001.sz/open.1min.bin"
    path.write_bytes(np.array([.5], dtype="<f4").tobytes()+path.read_bytes()[4:])
    ds._bin_lease[path.relative_to(ds.root).as_posix()] = stamp(path)
    with pytest.raises(ValueError, match="header"):
        inp.read_minutes(population().iloc[:1], ds, inp.Budget())
    snapshot = {str(path): (stamp(path), hashlib.sha256(path.read_bytes()).hexdigest())}
    path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="changed"):
        inp.assert_sources(snapshot)
    monkeypatch.setattr(inp, "MAX_BYTES", 1)
    with pytest.raises(ValueError, match="budget"):
        inp.Budget().check(2)


def test_prepare_real_publication_retry_and_drift_contract(tmp_path, monkeypatch):
    monkeypatch.setattr(inp, "END", str(DAY.date()))
    source = tmp_path / "source"
    source.mkdir()
    frame = population()
    ds = dataset(tmp_path / "data", tuple(frame.instrument))
    monkeypatch.setattr(inp, "load_source", lambda p: ({"binding": {}}, frame, {}))
    monkeypatch.setattr(inp, "FrozenDataset", lambda *a, **kw: ds)
    monkeypatch.setattr(inp, "implementation_identity", lambda: {"files": {"f": "a"*64}})
    target = tmp_path / "study"
    first = inp.prepare(source_study=source, output_root=target)
    original_bytes = (target / "prepared/inputs.parquet").read_bytes()
    monkeypatch.setattr(core, "minute_features", lambda *a, **kw: pytest.fail("exact retry recomputed features"))
    second = inp.prepare(source_study=source, output_root=target)
    assert second["status"] == "EXACT_ARTIFACT_RETRY_VERIFIED"
    assert first["stage_sha256"] == second["stage_sha256"]
    assert (target / "prepared/inputs.parquet").read_bytes() == original_bytes
    assert not (target / "trained").exists() and not (target / "evaluated").exists()
    receipt = json.loads((target / "prepared/receipt.json").read_bytes())
    assert receipt["fit_count"] == receipt["financial_labels_decoded"] == 0
    path = ds.root / "features/000001.sz/close.1min.bin"
    before = path.stat()
    path.write_bytes(path.read_bytes()[:4]+np.array([99.], dtype="<f4").tobytes()+path.read_bytes()[8:])
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    with pytest.raises(ValueError, match="consumed minute content"):
        inp.prepare(source_study=source, output_root=target)
    monkeypatch.setattr(inp, "implementation_identity", lambda: {"files": {"f": "b"*64}})
    with pytest.raises(AdvisoryModelFirstError):
        inp.prepare(source_study=source, output_root=target)
    with pytest.raises(ValueError, match="separate"):
        inp.prepare(source_study=source, output_root=source / "overwrite")


def test_static_closure_changes_with_imported_implementation(tmp_path):
    (tmp_path / "backend").mkdir()
    entry = tmp_path / "entry.py"
    entry.write_text("from backend.dep import value\n", encoding="utf-8")
    dep = tmp_path / "backend/dep.py"
    dep.write_text("value = 1\n", encoding="utf-8")
    a = inp.implementation_identity(tmp_path, [str(entry)])
    dep.write_text("value = 2\n", encoding="utf-8")
    b = inp.implementation_identity(tmp_path, [str(entry)])
    assert a != b and set(a["files"]) == {"entry.py", "backend/dep.py"}


def test_load_source_checks_chain_without_decoding_finance(tmp_path, monkeypatch):
    source = tmp_path / "source"
    plan = {"binding": {"binding_hash": "binding"}}
    plan_hash = inp.sha(plan)
    registered = inp.publish_stage(study_root=source, stage="preregistered", plan_sha256=plan_hash,
                                   parent_sha256=None, artifacts={"plan.json": inp._json_bytes(plan)})
    parent = json.loads((registered / "manifest.json").read_bytes())["stage_sha256"]
    raw = inp.parquet_bytes(population())
    prepared = inp.publish_stage(study_root=source, stage="prepared", plan_sha256=plan_hash,
                                 parent_sha256=parent, artifacts={"inputs.parquet": raw,
                                 "train_finance.parquet": b"MUST NEVER DECODE THIS POISON FINANCE"})
    stage = json.loads((prepared / "manifest.json").read_bytes())["stage_sha256"]
    for name, value in dict(SOURCE_PLAN=plan_hash, SOURCE_REGISTERED=parent, SOURCE_PREPARED=stage,
                            SOURCE_INPUT=hashlib.sha256(raw).hexdigest(), BINDING="binding",
                            MAX_ROWS=50, MAX_DAYS=1).items():
        monkeypatch.setattr(inp, name, value)
    _, frame, _ = inp.load_source(source)
    assert frame.equals(population())
    (source / "prepared/inputs.parquet").write_bytes(raw+b"drift")
    with pytest.raises(ValueError, match="input identity"):
        inp.load_source(source)


def test_source_changes_mid_read_are_rejected(tmp_path):
    ds = dataset(tmp_path / "data")
    path = ds.root / "features/000001.sz/open.1min.bin"
    budget = inp.Budget()
    original_check = budget.check
    changed = []
    def check(additional=0):
        original_check(additional)
        if budget.bytes >= 4820 and not changed:
            path.write_bytes(path.read_bytes()+b"four")
            changed.append(True)
    budget.check = check
    with pytest.raises(ValueError, match="source changed"):
        inp.read_minutes(population().iloc[:1], ds, budget)


@pytest.mark.parametrize("kind", ["time", "rss", "empty_existing", "C_drive"])
def test_resource_and_output_rejections(tmp_path, kind):
    budget = inp.Budget()
    if kind == "time":
        budget.started -= inp.MAX_SECONDS+1
    elif kind == "rss":
        budget.process = SimpleNamespace(memory_info=lambda: SimpleNamespace(rss=inp.MAX_RSS+1))
    else:
        target = tmp_path / "existing"
        target.mkdir()
        if kind == "C_drive":
            target = "C:/unapproved-output"  # Rejected as C on Windows / relative on POSIX.
        with pytest.raises(ValueError):
            inp.prepare(source_study=tmp_path / "source", output_root=target)
        return
    with pytest.raises(ValueError, match="budget"):
        budget.check()


def test_cli_explicit_forwarding_and_existing_ci_route(tmp_path, monkeypatch, capsys):
    from scripts import ci_change_classifier
    route = ci_change_classifier.classify_changed_files(
        ["scripts/advisory_minute_recovery.py"], repo_root=Path(__file__).resolve().parents[3])
    assert route["backend_sessions"] == ["advisory_modeling_backend"]
    assert not route["unmapped_code_files"] and not route["dev_db_required"]
    calls = []
    def prepare(**kwargs):
        calls.append(kwargs)
        return {"status": "TEST_FORWARDING_ONLY"}
    monkeypatch.setattr(inp, "prepare", prepare)
    monkeypatch.setattr(sys, "argv", ["advisory_minute_recovery.py", "--source-study", "explicit-source",
                                     "--output-root", str(tmp_path / "target")])
    for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        monkeypatch.setenv(name, "1")
    runpy.run_path(str(Path(__file__).resolve().parents[3] / "scripts/advisory_minute_recovery.py"), run_name="__main__")
    assert len(calls) == 1 and calls[0]["source_study"] == "explicit-source"
    assert calls[0]["output_root"] == str(tmp_path / "target")
    assert "TEST_FORWARDING_ONLY" in capsys.readouterr().out
