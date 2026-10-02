"""Append missing observed daily_basic history to a shared immutable successor.

Only SELECTs run in one repeatable-read transaction. Existing values, PIT
population, prices, moneyflow, and static factors are never rewritten.
"""
from __future__ import annotations

import copy
from collections import defaultdict
import hashlib
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from .adj_factor_candidate_repair import _assert_plain_directory, _assert_plain_path_chain
from .canonical import canonical_json_bytes
from .factor_materializer import FACTOR_H5_SCHEMAS, FACTOR_SOURCE_SCHEMAS
from .release_successor import _manifest_identity, _require_manifest_identity, _validate_manifest_components
from .source_fact_history import SOURCE_HISTORY_CONTRACT, filter_source_fact_history
from .streaming_artifacts import sha256_file

FACTOR = "components/factor_h5_static_candidate_v2"


def consumer_causal_windows(cutoff: str) -> dict[str, tuple[str, str]]:
    """Approved consumers of this frozen model/release, not inferred model windows.

    Product source history extends to the candidate cutoff. Executor and QE
    coefficient boundaries remain their approved fixed dates; this producer
    cannot change them or authorize new NA exceptions.
    """
    end = date.fromisoformat(cutoff)
    if end < date(2026, 8, 31) or end.isoformat() != cutoff:
        raise ValueError("cutoff cannot cover the approved consumers")
    return {
        "source": ("2020-07-30", "2025-04-30"),
        "train": ("2022-01-01", "2024-06-30"),
        "validation": ("2024-07-01", "2025-03-31"),
        "product_history": ("2020-07-30", cutoff),
        "qe_coefficients": ("2024-07-01", "2026-08-28"),
    }


def causal_audit_status(audit: dict) -> str:
    """No partial/window-only PASS can label a shared candidate READY."""
    if set(audit) != set(consumer_causal_windows("2026-08-31")):
        raise ValueError("required consumer causal windows are incomplete")
    counts = [item["after"]["unresolved_count"] for item in audit.values()]
    if any(type(count) is not int or count < 0 for count in counts):
        raise ValueError("causal gap counts are invalid")
    for item in audit.values():
        after = item["after"]
        expected, resolved = after["expected_keys"], after["strict_prior_resolved"]
        explained = after.get("explained_warmup", 0)
        not_applicable = after.get("not_applicable", 0)
        if (type(expected) is not int or expected <= 0 or type(resolved) is not int
                or type(explained) is not int or explained < 0
                or type(not_applicable) is not int or not_applicable < 0
                or resolved < 0 or resolved + explained + not_applicable + after["unresolved_count"] != expected):
            raise ValueError("causal audit denominator does not close")
    return "BLOCKED" if any(counts) else "CANDIDATE_READY"


def write_exclusive(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(value) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())


def merge_missing_facts(old: pd.DataFrame, source: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if old.index.has_duplicates or source.index.has_duplicates:
        raise ValueError("duplicate daily_basic facts")
    if tuple(old.columns) != tuple(source.columns) or old.dtypes.to_dict() != source.dtypes.to_dict():
        raise ValueError("daily_basic schema/dtype drift")
    added = source.loc[~source.index.isin(old.index)].copy()
    merged = pd.concat([old, added]).sort_index()
    pd.testing.assert_frame_equal(merged.loc[old.index], old, check_exact=True)
    pd.testing.assert_frame_equal(merged.loc[added.index], added, check_exact=True)
    return merged, added


def causal_coverage(spans: list[tuple[str, str, str]], calendar: list[str],
                    facts: dict[str, list[tuple[int, bool]]], *, start: str, end: str,
                    history_start: str | None = None,
                    approved_warmup_keys: frozenset[tuple[str, str]] = frozenset(),
                    not_applicable_keys: frozenset[tuple[str, str]] = frozenset()) -> dict:
    days = pd.DatetimeIndex([day for day in calendar if start <= day <= end]).asi8
    history_ns = pd.Timestamp(history_start).value if history_start is not None else None
    warmup_dates: dict[str, list[int]] = defaultdict(list)
    excluded_dates: dict[str, list[int]] = defaultdict(list)
    for symbol, day in approved_warmup_keys:
        warmup_dates[symbol].append(pd.Timestamp(day).value)
    for symbol, day in not_applicable_keys:
        excluded_dates[symbol].append(pd.Timestamp(day).value)
    expected = resolved = explained = not_applicable = 0
    unresolved = []
    for symbol, begin, finish in spans:
        required = days[(days >= pd.Timestamp(begin).value) & (days <= pd.Timestamp(finish).value)]
        observations = sorted(row for row in facts.get(symbol, []) if history_ns is None or row[0] >= history_ns)
        prior = np.asarray([row[0] for row in observations], dtype=np.int64)
        valid = np.asarray([row[1] for row in observations], dtype=bool)
        positions = np.searchsorted(prior, required, side="left") - 1
        covered = np.zeros(len(required), dtype=bool)
        has_prior = positions >= 0
        covered[has_prior] = valid[positions[has_prior]]
        exempt = np.isin(required, excluded_dates.get(symbol, ()))
        warmup = ~has_prior & ~exempt & (np.isin(required, warmup_dates.get(symbol, ()))
                                         | (required == history_ns if history_ns is not None else False))
        expected += len(required)
        resolved += int((covered & ~exempt).sum())
        explained += int(warmup.sum())
        not_applicable += int(exempt.sum())
        for day in required[~covered & ~exempt & ~warmup]:
            unresolved.append({"symbol": symbol, "trade_date": pd.Timestamp(day).date().isoformat()})
    return {"expected_keys": expected, "strict_prior_resolved": resolved,
            "explained_warmup": explained, "not_applicable": not_applicable,
            "unresolved_count": len(unresolved), "unresolved": unresolved}


def _collect_circ_mv_facts(
    frame: pd.DataFrame, target: dict[str, list[tuple[int, bool]]], *, window_start: str,
) -> None:
    # Preserve invalid observations: the latest record invalidates earlier cap,
    # just as the official file consumer does. This is not forward filling.
    cap = frame["db_circ_mv"].to_numpy()
    dates = pd.DatetimeIndex(frame.index.get_level_values("datetime"))
    selected = dates >= pd.Timestamp(window_start)
    observed = pd.DataFrame(
        {"valid": np.isfinite(cap) & (cap > 0)}, index=frame.index,
    ).loc[selected]
    for symbol, group in observed.groupby(level="instrument", sort=False):
        source_dates = pd.DatetimeIndex(group.index.get_level_values("datetime")).asi8
        target.setdefault(symbol, []).extend(zip(source_dates.tolist(), group.valid.tolist(), strict=True))


def _pin(root: Path, relative: str, **extra: Any) -> dict:
    path = root / relative
    return {"path": relative, "sha256": sha256_file(path), "size": path.stat().st_size, **extra}


def repair_daily_basic_history(
    *, baseline_root: Path, candidate_root: Path, expected_manifest: str,
    generation: str, revision: str, connection_factory: Callable[[], Any],
    diagnostic: dict, progress: Callable[[dict], None] = lambda value: None,
    resume_cloned: bool = False,
) -> dict:
    baseline = _assert_plain_directory(baseline_root, label="baseline")
    _assert_plain_path_chain(candidate_root, label="successor")
    candidate = candidate_root.absolute()
    if candidate.parent != baseline.parent or candidate == baseline:
        raise ValueError("successor must be a new sibling on the baseline volume")
    if candidate.exists() and not resume_cloned:
        raise FileExistsError(candidate)
    manifest_path = baseline / "qe_dataset_manifest.json"
    before_manifest_sha = sha256_file(manifest_path)
    manifest = _require_manifest_identity(manifest_path, expected_identity=expected_manifest,
                                          expected_file_sha256=before_manifest_sha)
    _validate_manifest_components(baseline, manifest["components"])
    old_inventory_pin = manifest["components"]["factor_content_manifest"]
    inventory = json.loads((baseline / old_inventory_pin["path"]).read_text(encoding="utf-8"))
    for item in inventory["files"]:
        path = baseline / item["path"]
        _assert_plain_path_chain(path, label="factor inventory file")
        if sha256_file(path) != item["sha256"] or path.stat().st_size != item["size"]:
            raise ValueError(f"factor inventory drift: {item['path']}")
    spans = [tuple(line.split()) for line in (baseline / "stock_pools/stock_universe.txt").read_text().splitlines() if line.strip()]
    codes = sorted({row[0] for row in spans})
    calendar = (baseline / manifest["components"]["day_calendar"]["path"]).read_text().splitlines()
    cutoff = manifest["cutoff_trade_date"]
    required_windows = consumer_causal_windows(cutoff)
    suspend_path = baseline / "components/suspend_d_daily_candidate_v2/suspend_d.parquet"
    if not any(pin["path"] == suspend_path.relative_to(baseline).as_posix()
               for pin in manifest["components"].values()):
        raise ValueError("frozen suspension authority is not pinned")
    suspension = pd.read_parquet(suspend_path)
    full_day = suspension[(suspension.suspend_type == "S")
                          & suspension.suspend_timing.fillna("").isin(["", "09:30-09:30"])]
    not_applicable_keys = frozenset((str(row.ts_code), pd.Timestamp(row.trade_date).date().isoformat())
                                   for row in full_day.itertuples())
    # Exact user-approved warmup, not a general resumption/provider exemption.
    causal_options = {"history_start": "2020-07-30",
                      "approved_warmup_keys": frozenset({("000792.SZ", "2021-08-10")}),
                      "not_applicable_keys": not_applicable_keys}
    if diagnostic["manifest_identity"] != expected_manifest:
        raise ValueError("diagnostic manifest differs")
    new_inventory_path = "reports/factor_component_pin_inventory_daily_basic_history.json"
    receipt_path = "reports/daily_basic_source_history_repair.json"
    mutable = {f"{FACTOR}/daily_basic.h5", f"{FACTOR}/meta.json", "qe_dataset_manifest.json",
               "direct_monthly_state.json", old_inventory_pin["path"]}
    # Exclusive namespace; unchanged files are hardlinked, changed files never are.
    if candidate.exists():
        # Only resume the pure clone stage; never reuse/overwrite a partially
        # written H5 or receipt. Every existing file must be the exact original.
        _assert_plain_directory(candidate, label="pending clone")
        for target in candidate.rglob("*"):
            _assert_plain_path_chain(target, label="pending clone entry")
            if target.is_file():
                relative = target.relative_to(candidate).as_posix()
                source = baseline / relative
                if relative in mutable or not source.is_file() or not os.path.samefile(source, target):
                    raise ValueError(f"not a pure baseline clone: {relative}")
    else:
        candidate.mkdir()
    reused_stats = {}
    for directory, directories, filenames in os.walk(baseline, followlinks=False):
        directory_path = Path(directory)
        for name in directories:
            _assert_plain_path_chain(directory_path / name, label="baseline directory")
        for name in filenames:
            source = directory_path / name
            relative = source.relative_to(baseline).as_posix()
            _assert_plain_path_chain(source, label="baseline file")
            if relative in mutable:
                continue
            if not source.is_file():
                raise ValueError(f"not a regular file: {relative}")
            target = candidate / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                if not os.path.samefile(source, target):
                    raise ValueError(f"clone file identity differs: {relative}")
            else:
                os.link(source, target)
            info = source.stat()
            reused_stats[relative] = (info.st_size, info.st_mtime_ns)
    progress({"stage": "clone_complete", "reused_files": len(reused_stats)})
    new_h5 = candidate / FACTOR / "daily_basic.h5"
    new_h5.parent.mkdir(parents=True, exist_ok=True)
    source_facts: dict[str, list[tuple[int, bool]]] = {}
    old_facts: dict[str, list[tuple[int, bool]]] = {}
    new_facts: dict[str, list[tuple[int, bool]]] = {}
    source_digest = hashlib.sha256()
    added_digest = hashlib.sha256()
    output_digest = hashlib.sha256()
    months = []
    source_columns = tuple(FACTOR_SOURCE_SCHEMAS["daily_basic"])
    output_columns = tuple(FACTOR_H5_SCHEMAS["daily_basic"])
    connection = connection_factory()
    try:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with connection.cursor() as cursor:
            cursor.execute("SELECT txid_current_snapshot()::text")
            snapshot_identity = cursor.fetchone()[0]
        with pd.HDFStore(baseline / FACTOR / "daily_basic.h5", mode="r") as old_store:
            baseline_rows = int(old_store.get_storer("data").nrows)
            copied_rows = added_rows = 0
            for month in pd.period_range("2018-08", cutoff[:7], freq="M"):
                start = month.start_time.date()
                end = min(month.end_time.date(), date.fromisoformat(cutoff))
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"SELECT trade_date,ts_code,{','.join(source_columns)} FROM market.daily_basic "
                        "WHERE ts_code=ANY(%s) AND trade_date BETWEEN %s AND %s ORDER BY trade_date,ts_code",
                        (codes, start, end),
                    )
                    rows = cursor.fetchall()
                raw = pd.DataFrame.from_records(rows, columns=["datetime", "instrument", *output_columns])
                raw["datetime"] = pd.to_datetime(raw["datetime"])
                raw = raw.set_index(["datetime", "instrument"])
                source = raw.astype({col: "float32" for col in output_columns})
                source = filter_source_fact_history(source, codes=codes, start=start, end=end)
                source_digest.update(pd.util.hash_pandas_object(raw, index=True).to_numpy().tobytes())
                old = old_store.select("data", where=[f"datetime >= '{start.isoformat()}'", f"datetime <= '{end.isoformat()}'"])
                merged, added = merge_missing_facts(old, source)
                if not merged.empty:
                    merged.to_hdf(new_h5, key="data", format="table", append=new_h5.exists(),
                                  data_columns=["instrument", "datetime"], index=False,
                                  min_itemsize={"instrument": 16})
                _collect_circ_mv_facts(source, source_facts, window_start="2020-07-30")
                _collect_circ_mv_facts(old, old_facts, window_start="2020-07-30")
                _collect_circ_mv_facts(merged, new_facts, window_start="2020-07-30")
                added_digest.update(pd.util.hash_pandas_object(added, index=True).to_numpy().tobytes())
                output_digest.update(pd.util.hash_pandas_object(merged, index=True).to_numpy().tobytes())
                copied_rows += len(old)
                added_rows += len(added)
                summary = {"month": str(month), "source_rows": len(source), "old_rows": len(old), "added_rows": len(added)}
                months.append(summary)
                progress(summary)
            if copied_rows != baseline_rows:
                raise ValueError("repair omitted existing historical rows")
        # Read every newly written chunk back, including exact old/new values.
        with pd.HDFStore(new_h5, mode="a") as store:
            store.create_table_index("data", columns=["instrument", "datetime"], optlevel=6)
            if int(store.get_storer("data").nrows) != copied_rows + added_rows:
                raise ValueError("new H5 row count drift")
        readback_digest = hashlib.sha256()
        with pd.HDFStore(new_h5, mode="r") as store:
            for frame in store.select("data", chunksize=100_000):
                readback_digest.update(pd.util.hash_pandas_object(frame, index=True).to_numpy().tobytes())
        if output_digest.hexdigest() != readback_digest.hexdigest():
            raise ValueError("new H5 exact value readback failed")
        audit = {}
        for name, (start, end) in required_windows.items():
            audit[name] = {"before": causal_coverage(spans, calendar, old_facts, start=start, end=end, **causal_options),
                           "after": causal_coverage(spans, calendar, new_facts, start=start, end=end, **causal_options),
                           "source": causal_coverage(spans, calendar, source_facts, start=start, end=end, **causal_options)}
            for section in audit[name].values():
                section["window_start"] = start
                section["window_end"] = end
        targeted = []
        for key in diagnostic["keys"]:
            symbol, entry = key["symbol"], key["pit_entry_date"]
            prior = sorted(row for row in new_facts.get(symbol, []) if row[0] < pd.Timestamp(entry).value)
            latest = prior[-1] if prior else None
            targeted.append({"symbol": symbol, "pit_entry_date": entry,
                             "resolved": latest is not None and latest[1],
                             "prior_fact_date": pd.Timestamp(latest[0]).date().isoformat() if latest else None})
        unresolved = [key for key in targeted if not key["resolved"]]
        source_unexplained = sum(len(
            {(key['symbol'], key['trade_date']) for key in audit[name]['after']['unresolved']}
            - {(key['symbol'], key['trade_date']) for key in audit[name]['source']['unresolved']}
        ) for name in audit)
        if unresolved or source_unexplained:
            raise ValueError("causal source facts remain unexplained")
    finally:
        connection.rollback()
        connection.close()
    for relative, expected_stat in reused_stats.items():
        source, target = baseline / relative, candidate / relative
        info = source.stat()
        if (info.st_size, info.st_mtime_ns) != expected_stat or not os.path.samefile(source, target):
            raise ValueError(f"reused content drift: {relative}")
    if sha256_file(manifest_path) != before_manifest_sha:
        raise ValueError("baseline manifest changed")
    meta = json.loads((baseline / FACTOR / "meta.json").read_text(encoding="utf-8"))
    meta["rows_by_file"]["daily_basic.h5"] = copied_rows + added_rows
    meta["daily_basic_source_history"] = {"contract": SOURCE_HISTORY_CONTRACT, "receipt": receipt_path,
                                         "added_rows": added_rows, "pit_population_unchanged": True}
    write_exclusive(candidate / FACTOR / "meta.json", meta)
    receipt = {
        "schema_version": "qe_daily_basic_source_history_repair_v1", "generation": generation,
        "predecessor_manifest_sha256": expected_manifest, "predecessor_file_sha256": before_manifest_sha,
        "source_dataset": "market.daily_basic", "snapshot_identity": snapshot_identity,
        "source_start": "2018-08-01", "source_end": cutoff,
        "causal_fact_window": {"start": "2020-07-30", "end": "2025-04-30"},
        "consumer_causal_windows": required_windows,
        "approved_causal_warmup_keys": [{"symbol": "000792.SZ", "trade_date": "2021-08-10"}],
        "causal_history_start": "2020-07-30",
        "suspension_authority_path": suspend_path.relative_to(baseline).as_posix(),
        "source_hash_contract": "pandas_hash_pandas_object_ordered_dataframe_v1",
        "pandas_version": pd.__version__, "source_rows_digest": source_digest.hexdigest(),
        "added_rows_digest": added_digest.hexdigest(), "months": months,
        "output_value_digest": output_digest.hexdigest(), "readback_value_digest": readback_digest.hexdigest(),
        "before_rows": copied_rows, "added_rows": added_rows, "after_rows": copied_rows + added_rows,
        "targeted_keys": targeted, "targeted_unresolved": len(unresolved), "coverage": audit,
        "reused_file_count": len(reused_stats), "reused_files_identity_digest": hashlib.sha256(canonical_json_bytes(reused_stats)).hexdigest(),
        "existing_values_preserved": True, "static_population_unchanged": True,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "database_read": True, "database_write": False, "active_profile_write": False,
        "runtime_action": False, "training": False, "network_api_access": False,
    }
    write_exclusive(candidate / receipt_path, receipt)
    new_inventory = copy.deepcopy(inventory)
    new_inventory["candidate_root"] = str(candidate)
    new_inventory["files"] = [_pin(candidate, item["path"]) for item in inventory["files"]]
    write_exclusive(candidate / new_inventory_path, new_inventory)
    updated = copy.deepcopy(manifest)
    updated["revision"] = revision
    updated["components"]["factor_content_manifest"] = _pin(candidate, new_inventory_path,
                                                               schema_version=inventory["schema_version"], file_count=inventory["file_count"])
    updated["components"]["factor_meta"] = _pin(candidate, f"{FACTOR}/meta.json", schema_version=meta["schema_version"])
    updated["components"]["daily_basic_h5"] = _pin(candidate, f"{FACTOR}/daily_basic.h5", schema_version=SOURCE_HISTORY_CONTRACT)
    updated["components"]["daily_basic_source_history_repair"] = _pin(candidate, receipt_path, schema_version=receipt["schema_version"])
    updated["source_contract"]["daily_basic_source_history"] = {"contract": SOURCE_HISTORY_CONTRACT, "predecessor": expected_manifest,
                                                                  "strict_prior_facts": True, "existing_values_preserved": True}
    content_identity = hashlib.sha256(canonical_json_bytes(updated["components"])).hexdigest()
    updated["deployment_content_sha256"] = content_identity
    updated["deployment_snapshot_id"] = f"{updated['release_id']}_{content_identity[:16]}"
    updated["availability_status"] = causal_audit_status(audit)
    updated["dataset_manifest_sha256"] = _manifest_identity(updated)
    write_exclusive(candidate / "qe_dataset_manifest.json", updated)
    _validate_manifest_components(candidate, updated["components"])
    state = json.loads((baseline / "direct_monthly_state.json").read_text(encoding="utf-8"))
    state.update(generation=generation, revision=revision, candidate_root=str(candidate), baseline_root=str(baseline),
                 status=updated["availability_status"], production_activation=False, production_writes=0)
    state["components"]["factor_h5_static"] = {"action": "APPEND_OBSERVED_SOURCE_HISTORY", "status": "PASS", "added_rows": added_rows,
                                               "receipt_sha256": sha256_file(candidate / receipt_path)}
    state["manifest"] = {"path": "qe_dataset_manifest.json", "file_sha256": sha256_file(candidate / "qe_dataset_manifest.json"),
                         "dataset_manifest_sha256": updated["dataset_manifest_sha256"], "deployment_snapshot_id": updated["deployment_snapshot_id"]}
    write_exclusive(candidate / "direct_monthly_state.json", state)
    return {"candidate_root": str(candidate), "generation": generation, "revision": revision,
            "manifest": state["manifest"], "daily_basic": updated["components"]["daily_basic_h5"],
            "receipt": updated["components"]["daily_basic_source_history_repair"], "added_rows": added_rows,
            "targeted_resolved": len(targeted) - len(unresolved), "targeted_unresolved": len(unresolved),
            "coverage": {name: {part: {k:v for k,v in item.items() if k != 'unresolved'} for part,item in value.items()}
                         for name,value in audit.items()}, "status": updated["availability_status"]}
