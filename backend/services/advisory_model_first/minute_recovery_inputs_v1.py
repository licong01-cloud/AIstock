"""Bounded frozen-file preparation for the single minute-recovery F1. Zero fit."""
from __future__ import annotations

import ast
from contextlib import ExitStack, closing
import hashlib
import io
import json
from pathlib import Path
import platform
import re
import time

import numpy as np
import pandas as pd
import psutil
import pyarrow
import pyarrow.parquet as pq
from threadpoolctl import threadpool_limits

from backend.data_service.frozen_dataset import FrozenDataset, stamp
from backend.services.advisory_model_first import minute_recovery_v1 as core
from backend.services.advisory_model_first.economic_entry_pipeline import (
    _json_bytes, file_sha256, publish_stage, read_stage,
)
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY, INPUT_FEATURES
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

PACKAGE = "pkg_cc7eccb6202e4816b9d7c7d6b748bd37"
MANIFEST = "93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6"
RUN = "qe_20261007_141342_0221_L4"
SOURCE_ID = "qe_20261007_141342_0221"
BINDING = "589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c"
SOURCE_PLAN = "d5753325ae465d6b2417abbc396ef8e4bbe268f4a741bc0dcff6b85205b7bc28"
SOURCE_REGISTERED = "aee9ab9a38b31af07f77933b887223c4aa31f960459b0a4a830bd993c3c5ccec"
SOURCE_PREPARED = "b9e4881cc599d670a969fef148a96aa1653cf065c35755308ac37ed9309b6c62"
SOURCE_INPUT = "c8d6ed3a1bc59aa365a15b0abcefb5b99cc19f84726337744ca048fa34f7a08b"
START, END = "2024-07-04", "2026-08-28"
DAY, TARGET = "decision_as_of_trade_date", "target_trade_date"
KEY = ["package_id", DAY, TARGET, "instrument"]
ORIGINAL = (*IDENTITY, "source_id", "parent_score", *INPUT_FEATURES)
FIELDS = ("open", "high", "low", "close", "volume")
MAX_ROWS, MAX_DAYS = 26200, 524
MAX_BYTES, MAX_RSS, MAX_SECONDS = 200 * 1024**2, 2 * 1024**3, 1800
CONTRACT = dict(schema=core.VERSION, package_id=PACKAGE, manifest_sha256=MANIFEST,
                binding_sha256=BINDING, source_plan_sha256=SOURCE_PLAN,
                source_prepared_sha256=SOURCE_PREPARED, source_inputs_sha256=SOURCE_INPUT,
                start=START, end=END, rows=MAX_ROWS, days=MAX_DAYS, max_fit_count=0,
                original_columns=ORIGINAL, matched=core.MATCHED, candidate=core.FEATURES,
                max_bytes=MAX_BYTES, max_rss=MAX_RSS, max_seconds=MAX_SECONDS, max_threads=2,
                provenance="NON_VINTAGE_RECONSTRUCTED_EXPLORATION",
                measurement_spec_sha256="c054ea17d5103e20a909f09b31c54ec2aa89c6855c11b1c4c4d5dc02b6b06b89")


class Budget:
    def __init__(self):
        self.started, self.bytes, self.peak_rss = time.monotonic(), 0, 0
        self.process = psutil.Process()

    def check(self, additional=0):
        self.bytes += additional
        self.peak_rss = max(self.peak_rss, self.process.memory_info().rss)
        if (self.bytes > MAX_BYTES or self.peak_rss > MAX_RSS
                or time.monotonic() - self.started > MAX_SECONDS):
            raise ValueError("minute recovery resource budget exceeded")

    def receipt(self):
        self.check()
        return dict(minute_bytes_read=self.bytes, peak_sampled_rss_bytes=self.peak_rss,
                    elapsed_seconds=time.monotonic() - self.started, max_numeric_threads=2)


def implementation_identity(root=None, entries=None):
    """Hash the static local import closure, including package initializers.

    No module is executed by this traversal. External package versions are bound too.
    """
    root = Path(root) if root is not None else Path(__file__).resolve().parents[3]
    entries = entries or ["scripts/advisory_minute_recovery.py", __file__]
    pending = [root / p for p in entries]
    files = {}
    while pending:
        path = pending.pop().resolve()
        if not path.is_relative_to(root.resolve()) or not path.is_file():
            raise ValueError("implementation closure path missing or outside repository")
        relative = path.relative_to(root.resolve()).as_posix()
        if relative in files:
            continue
        raw = path.read_bytes()
        files[relative] = hashlib.sha256(raw).hexdigest()
        parts = Path(relative).with_suffix("").parts
        package = list(parts[:-1])
        for parent in path.parents:
            if parent == root.resolve():
                break
            init = parent / "__init__.py"
            if init.is_file():
                pending.append(init)
        for node in ast.walk(ast.parse(raw, filename=relative)):
            modules = []
            if isinstance(node, ast.Import):
                modules = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                prefix = package[:len(package)-node.level+1] if node.level else []
                module = ".".join([*prefix, *([node.module] if node.module else [])])
                modules = [module, *(module + "." + a.name for a in node.names)]
            for module in modules:
                if not module.startswith("backend.") and module != "backend":
                    continue
                base = root.joinpath(*module.split("."))
                for candidate in (base.with_suffix(".py"), base / "__init__.py"):
                    if candidate.is_file():
                        pending.append(candidate)
    return dict(files=dict(sorted(files.items())), python=platform.python_version(),
                numpy=np.__version__, pandas=pd.__version__, pyarrow=pyarrow.__version__)


def load_source(source):
    source = Path(source)
    if not source.is_absolute() or source.resolve() != source:
        raise ValueError("source study must be an explicit resolved directory")
    snapshot = {str(p): (stamp(p), file_sha256(p)) for p in (
        source / "preregistered/manifest.json", source / "preregistered/plan.json",
        source / "prepared/manifest.json", source / "prepared/inputs.parquet")}
    registered = read_stage(source / "preregistered", stage="preregistered",
                            plan_sha256=SOURCE_PLAN, parent_sha256=None)
    if registered["stage_sha256"] != SOURCE_REGISTERED:
        raise ValueError("source preregistration identity differs")
    plan = json.loads((source / "preregistered/plan.json").read_bytes())
    manifest_path = source / "prepared/manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    # Verify metadata and only the declared input parquet, never finance artifacts.
    payload = {k: v for k, v in manifest.items() if k != "stage_sha256"}
    if (sha(plan) != SOURCE_PLAN or plan["binding"]["binding_hash"] != BINDING
            or manifest.get("schema_version") != "economic_entry_stage_v1"
            or manifest.get("stage") != "prepared" or manifest.get("plan_sha256") != SOURCE_PLAN
            or manifest.get("parent_sha256") != SOURCE_REGISTERED
            or manifest.get("stage_sha256") != SOURCE_PREPARED or sha(payload) != SOURCE_PREPARED):
        raise ValueError("source plan/prepared chain differs")
    path = source / "prepared/inputs.parquet"
    descriptor = manifest["files"]["inputs.parquet"]
    if (path.resolve() != path or manifest_path.resolve() != manifest_path
            or descriptor["sha256"] != SOURCE_INPUT or path.stat().st_size != descriptor["size_bytes"]
            or file_sha256(path) != SOURCE_INPUT):
        raise ValueError("original input identity differs")
    if pq.read_schema(path).names != list(ORIGINAL):
        raise ValueError("original input schema differs; no extra financial columns allowed")
    frame = pq.read_table(path, columns=list(ORIGINAL), filters=[("package_id", "=", PACKAGE)]).to_pandas()
    if len(frame) != MAX_ROWS or frame[DAY].nunique() != MAX_DAYS:
        raise ValueError("original population size differs")
    assert_sources(snapshot)
    return plan, frame.reset_index(drop=True), snapshot


def validate_population(frame, calendar):
    if (list(frame.columns) != list(ORIGINAL) or frame.empty or len(frame) > MAX_ROWS
            or frame[list(IDENTITY) + ["source_id"]].isna().any().any() or frame.duplicated(KEY).any()):
        raise ValueError("original population schema/identity differs")
    for column, value in (("package_id", PACKAGE), ("manifest_sha256", MANIFEST),
                          ("run_id", RUN), ("source_id", SOURCE_ID)):
        if not frame[column].eq(value).all():
            raise ValueError("foreign population identity")
    if not frame.instrument.map(lambda x: bool(re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", x))).all():
        raise ValueError("unsafe instrument")
    if np.isinf(frame[["parent_score", *INPUT_FEATURES]].to_numpy(float)).any():
        raise ValueError("invalid known original score/feature")
    calendar = pd.DatetimeIndex(calendar)
    expected = calendar[(calendar >= START) & (calendar <= END)]
    actual = pd.DatetimeIndex(sorted(frame[DAY].unique()))
    if not actual.equals(expected):
        raise ValueError("missing or foreign original D")
    for d, group in frame.groupby(DAY, sort=False):
        i = calendar.get_loc(d)
        if (len(group) != 50 or sorted(group.selection_effective_rank) != list(range(1, 51))
                or not group.candidate_group_size.eq(50).all()
                or i + 1 >= len(calendar) or not group[TARGET].eq(calendar[i+1]).all()):
            raise ValueError("original rank/slots or next-session T differs")


def assert_sources(snapshot):
    for name, (before, digest) in snapshot.items():
        p = Path(name)
        if stamp(p) != before or file_sha256(p) != digest:
            raise ValueError("source input changed during preparation")


def assert_minute_sources(dataset, index):
    for item in index:
        path = dataset.root / item["path"]
        changed = path.exists() if item["stat"] is None else stamp(path) != tuple(item["stat"])
        if changed:
            raise ValueError("minute source changed during preparation")
    dataset.assert_unchanged()


def read_minutes(frame, dataset, budget, *, compute=True, progress=lambda event: None):
    """Open five files per symbol, seek only declared D spans, retain every slot."""
    calendar = pd.DatetimeIndex(pd.to_datetime(dataset.file("minute_calendar").read_text(encoding="utf-8").splitlines()))
    if calendar.tz is not None or calendar.hasnans or calendar.has_duplicates or not calendar.is_monotonic_increasing:
        raise ValueError("invalid frozen minute calendar")
    normalized = calendar.normalize()
    starts = np.r_[0, np.flatnonzero(normalized[1:].to_numpy() != normalized[:-1].to_numpy()) + 1]
    positions = {normalized[a]: (int(a), int(b)) for a, b in zip(starts, np.r_[starts[1:], len(calendar)], strict=True)}
    minute_root = dataset.file("minute_meta_export").parent
    dataset.file("minute_provider_catalog")
    results, index = [], []
    for number, (symbol, group) in enumerate(frame.groupby("instrument", sort=True), 1):
        budget.check()
        with ExitStack() as stack:
            headers = {}
            for field in FIELDS:
                path = minute_root / "features" / symbol.lower() / (field + ".1min.bin")
                if path.resolve() != path or not path.is_relative_to(dataset.root):
                    raise ValueError("minute source escapes frozen root")
                rel = path.relative_to(dataset.root).as_posix()
                expected = dataset._bin_lease.get(rel)
                info = dict(path=rel, stat=list(expected) if expected is not None else None,
                            segments=0, payload_bytes=0)
                digest = hashlib.sha256()
                if not path.exists() and expected is None:
                    headers[field] = (None, 0, 0, info, digest)
                    continue
                if expected is None or stamp(path) != expected or expected[0] < 4 or expected[0] % 4:
                    raise ValueError("minute source lease differs")
                stream = stack.enter_context(path.open("rb"))
                budget.check(4)
                raw = stream.read(4)
                if len(raw) != 4:
                    raise ValueError("truncated minute header")
                offset = float(np.frombuffer(raw, dtype="<f4")[0])
                length = expected[0] // 4 - 1
                if not np.isfinite(offset) or offset < 0 or int(offset) != offset or offset + length > len(calendar):
                    raise ValueError("invalid minute header offset")
                info["header_sha256"] = hashlib.sha256(raw).hexdigest()
                headers[field] = (stream, int(offset), length, info, digest)
            days = sorted(group[DAY].unique())
            for chunk in range(0, len(days), 20):
                for d in days[chunk:chunk+20]:
                    d = pd.Timestamp(d)
                    lo, hi = positions.get(d, (0, 0))
                    slots = calendar[lo:hi]
                    if len(slots) > 300:
                        raise ValueError("excess minute slots")
                    arrays = {}
                    for field, (stream, offset, length, info, digest) in headers.items():
                        values = np.full(len(slots), np.nan)
                        a, b = max(lo, offset), min(hi, offset + length)
                        raw = b""
                        if stream is not None and b > a:
                            budget.check(4 * (b-a))
                            stream.seek(4 * (1 + a-offset))
                            raw = stream.read(4 * (b-a))
                            if len(raw) != 4 * (b-a):
                                raise ValueError("truncated minute D slice")
                            if compute:
                                values[a-lo:b-lo] = np.frombuffer(raw, dtype="<f4")
                        digest.update(_json_bytes([str(d.date()), lo, hi, a, b]))
                        digest.update(raw)
                        info["segments"] += 1
                        info["payload_bytes"] += len(raw)
                        arrays[field] = values
                    if compute:
                        results.append({"instrument": symbol, DAY: d, **core.minute_features(slots, arrays, day=d)})
                    budget.check()
            for _, _, _, info, digest in headers.values():
                info["consumed_slices_sha256"] = digest.hexdigest()
                index.append(info)
        if number % 200 == 0:
            progress(dict(phase="FEATURES" if compute else "RETRY_VERIFY", symbols=number,
                          minute_bytes_read=budget.bytes))
    assert_minute_sources(dataset, index)
    return pd.DataFrame(results), index


def attach(frame, minute):
    joined = frame.merge(minute, on=[DAY, "instrument"], how="left", sort=False,
                         validate="one_to_one", indicator=True)
    if not joined._merge.eq("both").all() or not joined[list(ORIGINAL)].equals(frame):
        raise ValueError("feature attachment changed original population/order/dtypes")
    joined = joined.drop(columns="_merge")
    joined["model_support"] = (~joined.recovery_status.str.startswith("UNKNOWN")
                               & np.isfinite(joined[list(INPUT_FEATURES)].to_numpy(float)).any(axis=1))
    joined["path_known"] = ~joined.recovery_status.str.startswith("UNKNOWN")
    daily = joined.groupby(DAY, as_index=False, sort=True).agg(
        original_slots=("instrument", "size"), path_known_slots=("path_known", "sum"),
        model_support_slots=("model_support", "sum"))
    daily["complete_path_D"] = daily.path_known_slots.eq(50)
    daily["complete_support_D"] = daily.model_support_slots.eq(50)
    return joined, daily


def parquet_bytes(frame):
    stream = io.BytesIO()
    frame.to_parquet(stream, index=False)
    return stream.getvalue()


def prepare(*, source_study, output_root, progress=lambda event: None):
    """Publish/reuse a feature-only stage; no train/evaluate option exists."""
    budget = Budget()
    source, target = Path(source_study), Path(output_root)
    if (not target.is_absolute() or target.drive.upper() == "C:" or target.resolve() != target
            or target.is_relative_to(source.resolve()) or source.resolve().is_relative_to(target)):
        raise ValueError("output requires an explicit separate non-C directory")
    if target.exists() and not (target / "preregistered/manifest.json").is_file():
        raise ValueError("refuse existing unregistered output root")
    plan, frame, snapshot = load_source(source)
    implementation = implementation_identity()
    protocol = dict(contract=CONTRACT, source_study=str(source), implementation=implementation)
    plan_hash = sha(protocol)
    with threadpool_limits(limits=2), closing(FrozenDataset(plan["binding"], consumer_scope="ADVISORY",
                                                           run_id="MINUTE_RECOVERY_P4")) as dataset:
        if target.is_relative_to(dataset.root):
            raise ValueError("output cannot overwrite frozen data")
        validate_population(frame, dataset.calendar)
        registered = publish_stage(study_root=target, stage="preregistered", plan_sha256=plan_hash,
                                   parent_sha256=None, artifacts={"plan.json": _json_bytes(protocol)})
        parent = read_stage(registered, stage="preregistered", plan_sha256=plan_hash,
                            parent_sha256=None)["stage_sha256"]
        prepared = target / "prepared"
        retry = prepared.exists()
        if retry:
            manifest = read_stage(prepared, stage="prepared", plan_sha256=plan_hash, parent_sha256=parent)
        minute, source_index = read_minutes(frame, dataset, budget, compute=not retry, progress=progress)
        assert_sources(snapshot)
        if implementation_identity() != implementation:
            raise ValueError("implementation changed during preparation")
        if retry:
            if json.loads((prepared / "source_index.json").read_bytes()) != source_index:
                raise ValueError("consumed minute content changed on exact retry")
            read_stage(prepared, stage="prepared", plan_sha256=plan_hash, parent_sha256=parent)
            assert_minute_sources(dataset, source_index)
            return dict(status="EXACT_ARTIFACT_RETRY_VERIFIED", stage_sha256=manifest["stage_sha256"],
                        fit_count=0, output_root=str(target), **budget.receipt())
        inputs, daily = attach(frame, minute)
        receipt = dict(status="FEATURE_INPUTS_PREPARED_NOT_ALPHA", fit_count=0, financial_labels_decoded=0,
                       rows=len(inputs), days=len(daily), package_id=PACKAGE, binding_sha256=BINDING,
                       source_input_sha256=SOURCE_INPUT, original_columns_preserved=True,
                       statuses={str(k): int(v) for k, v in inputs.recovery_status.value_counts().items()},
                       path_known_slots=int(inputs.path_known.sum()), model_support_slots=int(inputs.model_support.sum()),
                       complete_path_days=int(daily.complete_path_D.sum()),
                       complete_support_days=int(daily.complete_support_D.sum()),
                       feature_missing_counts={n: int(inputs[n].isna().sum()) for n in core.FEATURES},
                       source_files=len(source_index), consumed_slices_sha256=sha(source_index),
                       historical_pit_proven=False, economic_confirmation=False, source_unchanged=True,
                       **budget.receipt())
        artifacts = {"inputs.parquet": parquet_bytes(inputs), "daily_support.parquet": parquet_bytes(daily),
                     "receipt.json": _json_bytes(receipt), "source_index.json": _json_bytes(source_index)}
        if sum(map(len, artifacts.values())) > MAX_BYTES:
            raise ValueError("prepared output exceeds resource budget")
        assert_sources(snapshot)
        assert_minute_sources(dataset, source_index)
        budget.check()
        publish_stage(study_root=target, stage="prepared", plan_sha256=plan_hash,
                      parent_sha256=parent, artifacts=artifacts)
        manifest = read_stage(prepared, stage="prepared", plan_sha256=plan_hash, parent_sha256=parent)
        return dict(status=receipt["status"], stage_sha256=manifest["stage_sha256"],
                    fit_count=0, output_root=str(target), **budget.receipt())
