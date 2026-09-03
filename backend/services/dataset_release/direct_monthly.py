"""Thin orchestration for direct, candidate-only monthly dataset builds.

This module intentionally owns no source-freeze, content-hash, resource
admission, or production activation behavior.  Component builders stream from
their authoritative sources into one new candidate root and report completion
through a small resumable state document.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
from typing import Any, Callable, Mapping, Sequence

from .errors import DatasetReleaseError


DIRECT_MONTHLY_SCHEMA = "qe_direct_monthly_candidate_v1"
DIRECT_MONTHLY_STATE_SCHEMA = "qe_direct_monthly_state_v1"
DIRECT_COMPONENTS = ("daily_bin", "minute_bin", "factor_h5_static", "index_context")
DIRECT_TERMINAL_STATUS = "CANDIDATE_READY"
DIRECT_START_DATE = date(2018, 8, 1)
DIRECT_MINUTE_START_DATE = date(2024, 1, 2)
DIRECT_UNIVERSE_KEY = "aistock_equity_pit_canonical_v2"
DIRECT_FACTOR_COMPONENT_DIR = "factor_h5_static_candidate_v2"
DIRECT_FACTOR_SCHEMA = "qe_direct_factor_h5_static_v2"
DIRECT_SECTOR_AUTHORITY = "classification_pit_to_published_l2_v2"
DIRECT_CANDIDATE_PARENT = Path("X:/AIstock_dataset_candidates/backtest_dataset_candidates")
DIRECT_INDEX_CODES = (
    "000001.SH",
    "000016.SH",
    "000300.SH",
    "000688.SH",
    "000852.SH",
    "000905.SH",
    "000985.CSI",
    "932000.CSI",
    "399001.SZ",
    "399006.SZ",
    "399102.SZ",
    "399107.SZ",
)
_CANDIDATE_NAME = re.compile(r"[0-9]{8}-qe_hmm_full_v2-direct-[0-9]{8}-candidate\Z")


class DirectMonthlyError(DatasetReleaseError):
    """A direct monthly contract or component execution failed."""

    code = "DIRECT_MONTHLY_CANDIDATE_FAILED"


@dataclass(frozen=True, slots=True)
class DirectMonthlyLayout:
    candidate_root: Path
    candidate_parent: Path
    cutoff: date
    baseline_root: Path | None

    @classmethod
    def create(
        cls,
        *,
        candidate_root: Path,
        candidate_parent: Path,
        baseline_root: Path | None,
        cutoff: date,
    ) -> "DirectMonthlyLayout":
        parent = candidate_parent.expanduser().resolve(strict=True)
        candidate = candidate_root.expanduser().resolve(strict=False)
        baseline = baseline_root.expanduser().resolve(strict=True) if baseline_root is not None else None
        if candidate.parent != parent or _CANDIDATE_NAME.fullmatch(candidate.name) is None:
            raise DirectMonthlyError("direct candidate must be a canonical direct child of candidate_root")
        if baseline is not None and (baseline.parent != parent or baseline == candidate or not baseline.is_dir()):
            raise DirectMonthlyError("baseline candidate must be a different direct child of candidate_root")
        if candidate.exists() and not candidate.is_dir():
            raise DirectMonthlyError("direct candidate path exists and is not a directory")
        return cls(
            candidate_root=candidate,
            candidate_parent=parent,
            cutoff=cutoff,
            baseline_root=baseline,
        )

    @property
    def state_path(self) -> Path:
        return self.candidate_root / "direct_monthly_state.json"

    @property
    def components_root(self) -> Path:
        return self.candidate_root / "components"

    @property
    def work_root(self) -> Path:
        return self.candidate_root / "work"

    @property
    def reports_root(self) -> Path:
        return self.candidate_root / "reports"

    @property
    def factor_root(self) -> Path:
        return self.components_root / DIRECT_FACTOR_COMPONENT_DIR

    @property
    def industry_authority_root(self) -> Path:
        return (
            self.candidate_parent
            / ".industry_pit_authority"
            / "qe_hmm_full_v2"
            / self.cutoff.isoformat()
            / "full"
        )


@dataclass(frozen=True, slots=True)
class DirectComponentPlan:
    component: str
    action: str
    reason: str

    def as_dict(self) -> dict[str, str]:
        return {"component": self.component, "action": self.action, "reason": self.reason}


def component_plan(*, july_minute_repaired: bool = True) -> tuple[DirectComponentPlan, ...]:
    """Return the fixed v2 plan without consulting source fingerprints."""

    return (
        DirectComponentPlan(
            "daily_bin",
            "COMPONENT_REBUILD",
            "canonical_v2_pool_and_target_cutoff",
        ),
        DirectComponentPlan(
            "minute_bin",
            "COMPONENT_REBUILD",
            "july_repair_plus_august_tail" if july_minute_repaired else "canonical_v2_pool_and_target_cutoff",
        ),
        DirectComponentPlan(
            "factor_h5_static",
            "COMPONENT_REBUILD",
            "canonical_v2_pool_sector_authority_and_target_cutoff",
        ),
        DirectComponentPlan(
            "index_context",
            "COMPONENT_REBUILD",
            "exact_12_index_contract_and_target_cutoff",
        ),
    )


def initial_state(layout: DirectMonthlyLayout) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "schema_version": DIRECT_MONTHLY_STATE_SCHEMA,
        "status": "PLANNING_DIRECT",
        "profile": "qe_hmm_full_v2",
        "cutoff": layout.cutoff.isoformat(),
        "candidate_root": str(layout.candidate_root),
        "baseline_root": str(layout.baseline_root) if layout.baseline_root is not None else None,
        "components": {
            item.component: {
                "action": item.action,
                "reason": item.reason,
                "status": "PENDING",
            }
            for item in component_plan()
        },
        "source_freeze": False,
        "full_history_content_hash": False,
        "prepublish_source_recheck": False,
        "resource_admission": False,
        "production_writes": 0,
        "production_pointer_changes": 0,
        "created_at": now,
        "updated_at": now,
    }


def read_state(layout: DirectMonthlyLayout) -> dict[str, Any] | None:
    if not layout.state_path.exists():
        return None
    try:
        value = json.loads(layout.state_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DirectMonthlyError("direct monthly state is unreadable") from exc
    _validate_state(layout, value)
    return value


def write_state(layout: DirectMonthlyLayout, value: Mapping[str, Any]) -> None:
    payload = dict(value)
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    _validate_state(layout, payload)
    layout.candidate_root.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".direct_monthly_state.",
        suffix=".tmp",
        dir=layout.candidate_root,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, layout.state_path)
    finally:
        temporary.unlink(missing_ok=True)


ComponentHandler = Callable[[DirectMonthlyLayout], Mapping[str, Any]]


class DirectMonthlyRunner:
    """Execute each component exactly once and resume only completed stages."""

    def __init__(
        self,
        handlers: Mapping[str, ComponentHandler],
        *,
        validator: Callable[[DirectMonthlyLayout], Mapping[str, Any]] = lambda layout: validate_direct_candidate(layout),
    ) -> None:
        if set(handlers) != set(DIRECT_COMPONENTS):
            raise DirectMonthlyError("direct monthly handlers must cover all four components exactly")
        self.handlers = dict(handlers)
        self.validator = validator

    def run(
        self,
        layout: DirectMonthlyLayout,
        *,
        adopt_known_direct_work: bool = False,
    ) -> Mapping[str, Any]:
        state = read_state(layout)
        if state is None:
            if layout.candidate_root.exists() and any(layout.candidate_root.iterdir()):
                if not adopt_known_direct_work:
                    raise DirectMonthlyError("non-empty direct candidate lacks its state document")
                _validate_adoptable_direct_work(layout)
            state = initial_state(layout)
            write_state(layout, state)
        if state["status"] == DIRECT_TERMINAL_STATUS:
            return state

        # A failed validation may be resumed after a component contract fix.
        # Re-open only the component whose small completion metadata no longer
        # satisfies the current contract; already-complete daily/minute/index
        # outputs remain untouched.
        for component in DIRECT_COMPONENTS:
            record = state["components"][component]
            if record["status"] == "PASS" and not _component_output_complete(layout, component):
                record["status"] = "PENDING"
                record.pop("receipt", None)

        layout.components_root.mkdir(parents=True, exist_ok=True)
        layout.work_root.mkdir(parents=True, exist_ok=True)
        layout.reports_root.mkdir(parents=True, exist_ok=True)
        state["status"] = "BUILDING"
        write_state(layout, state)

        for component in DIRECT_COMPONENTS:
            record = state["components"][component]
            if record["status"] == "PASS":
                continue
            record["status"] = "RUNNING"
            write_state(layout, state)
            try:
                receipt = dict(self.handlers[component](layout))
            except Exception:
                record["status"] = "FAILED"
                state["status"] = "FAILED"
                write_state(layout, state)
                raise
            if receipt.get("status") != "PASS" or receipt.get("component") != component:
                record["status"] = "FAILED"
                state["status"] = "FAILED"
                write_state(layout, state)
                raise DirectMonthlyError(f"direct component receipt is invalid: {component}")
            record["status"] = "PASS"
            record["receipt"] = receipt
            write_state(layout, state)

        state["status"] = "VALIDATING"
        write_state(layout, state)
        try:
            validation = dict(self.validator(layout))
        except Exception:
            state["status"] = "FAILED"
            write_state(layout, state)
            raise
        if validation.get("status") != "PASS":
            state["status"] = "FAILED"
            write_state(layout, state)
            raise DirectMonthlyError("direct candidate validation receipt is invalid")
        state["validation"] = validation
        state["status"] = DIRECT_TERMINAL_STATUS
        write_state(layout, state)
        persisted = read_state(layout)
        if persisted is None:  # pragma: no cover - atomic writer contract
            raise DirectMonthlyError("direct monthly terminal state disappeared")
        return persisted


def _validate_state(layout: DirectMonthlyLayout, value: Any) -> None:
    if not isinstance(value, Mapping):
        raise DirectMonthlyError("direct monthly state must be an object")
    components = value.get("components")
    if (
        value.get("schema_version") != DIRECT_MONTHLY_STATE_SCHEMA
        or value.get("profile") != "qe_hmm_full_v2"
        or value.get("cutoff") != layout.cutoff.isoformat()
        or value.get("candidate_root") != str(layout.candidate_root)
        or value.get("baseline_root")
        != (str(layout.baseline_root) if layout.baseline_root is not None else None)
        or not isinstance(components, Mapping)
        or set(components) != set(DIRECT_COMPONENTS)
        or value.get("source_freeze") is not False
        or value.get("full_history_content_hash") is not False
        or value.get("prepublish_source_recheck") is not False
        or value.get("resource_admission") is not False
        or value.get("production_writes") != 0
        or value.get("production_pointer_changes") != 0
    ):
        raise DirectMonthlyError("direct monthly state contract differs")
    for component in DIRECT_COMPONENTS:
        record = components[component]
        if not isinstance(record, Mapping) or record.get("status") not in {
            "PENDING",
            "RUNNING",
            "PASS",
            "FAILED",
        }:
            raise DirectMonthlyError(f"direct monthly component state differs: {component}")


def compact_status(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": DIRECT_MONTHLY_SCHEMA,
        "status": value["status"],
        "profile": value["profile"],
        "cutoff": value["cutoff"],
        "candidate_root": value["candidate_root"],
        "components": {
            name: value["components"][name]["status"] for name in DIRECT_COMPONENTS
        },
        "source_freeze": False,
        "full_history_content_hash": False,
        "production_writes": 0,
        "production_pointer_changes": 0,
    }


def production_handlers(*, project_root: Path) -> Mapping[str, ComponentHandler]:
    """Bind the thin runner to the existing authoritative component writers."""

    root = project_root.expanduser().resolve(strict=True)
    return {
        "daily_bin": lambda layout: _run_qlib_component(
            layout,
            project_root=root,
            component="daily_bin",
            dataset="stock_daily",
            start=DIRECT_START_DATE,
        ),
        "minute_bin": lambda layout: _run_qlib_component(
            layout,
            project_root=root,
            component="minute_bin",
            dataset="stock_minute",
            start=DIRECT_MINUTE_START_DATE,
        ),
        "factor_h5_static": build_factor_h5_static_component,
        "index_context": build_index_context_component,
    }


def discover_latest_validated_baseline(candidate_parent: Path, *, cutoff: date) -> Path | None:
    """Return optional provenance context; a new monthly build never requires it."""

    parent = candidate_parent.expanduser().resolve(strict=True)
    choices: list[tuple[date, Path]] = []
    for child in parent.iterdir():
        if not child.is_dir() or child.is_symlink() or child.name.startswith("."):
            continue
        factor_meta = child / "components" / "factor_h5_static_candidate" / "meta.json"
        run_state = child / "run_state.json"
        if not factor_meta.is_file() or not run_state.is_file():
            continue
        try:
            meta = json.loads(factor_meta.read_text(encoding="utf-8"))
            state = json.loads(run_state.read_text(encoding="utf-8"))
            end = date.fromisoformat(str(meta.get("end")))
        except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
            continue
        if end < cutoff and state.get("status") == "final_validation_validated":
            choices.append((end, child.resolve(strict=True)))
    if not choices:
        return None
    choices.sort(key=lambda item: (item[0], item[1].name))
    return choices[-1][1]


def default_candidate_path(candidate_parent: Path, *, cutoff: date, observed_on: date) -> Path:
    return candidate_parent / (
        f"{cutoff.strftime('%Y%m%d')}-qe_hmm_full_v2-direct-"
        f"{observed_on.strftime('%Y%m%d')}-candidate"
    )


def discover_latest_existing_direct_candidate(
    candidate_parent: Path,
    *,
    cutoff: date,
) -> Path | None:
    candidates: list[tuple[datetime, Path]] = []
    pattern = f"{cutoff.strftime('%Y%m%d')}-qe_hmm_full_v2-direct-*-candidate"
    for candidate in candidate_parent.glob(pattern):
        state_path = candidate / "direct_monthly_state.json"
        if not state_path.is_file():
            continue
        try:
            raw = json.loads(state_path.read_text(encoding="utf-8"))
            layout = DirectMonthlyLayout.create(
                candidate_parent=candidate_parent,
                candidate_root=candidate,
                baseline_root=(
                    Path(str(raw["baseline_root"])) if raw.get("baseline_root") is not None else None
                ),
                cutoff=cutoff,
            )
            state = read_state(layout)
            updated_at = datetime.fromisoformat(str(state["updated_at"])) if state else None
        except (KeyError, OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError, DirectMonthlyError):
            continue
        if updated_at is not None:
            candidates.append((updated_at, candidate))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1].name))
    return candidates[-1][1]


def _run_qlib_component(
    layout: DirectMonthlyLayout,
    *,
    project_root: Path,
    component: str,
    dataset: str,
    start: date,
) -> Mapping[str, Any]:
    snapshot_id = f"{component}_candidate"
    output = layout.components_root / snapshot_id
    calendar_name = "1min.txt" if dataset == "stock_minute" else "day.txt"
    calendar = output / "calendars" / calendar_name
    if _calendar_reaches_cutoff(calendar, layout.cutoff):
        return {
            "status": "PASS",
            "component": component,
            "action": "REUSE_COMPLETED_DIRECT_OUTPUT",
            "path": str(output),
            "cutoff": layout.cutoff.isoformat(),
        }
    command = [
        sys.executable,
        str(project_root / "scripts" / "qlib_authoritative_bin_export.py"),
        "--dataset",
        dataset,
        "--stage",
        "all",
        "--snapshot-id",
        snapshot_id,
        "--start",
        start.isoformat(),
        "--end",
        layout.cutoff.isoformat(),
        "--basis-start",
        start.isoformat(),
        "--basis-end",
        layout.cutoff.isoformat(),
        "--csv-root",
        str(layout.work_root),
        "--bin-root",
        str(layout.components_root),
        "--reports-dir",
        str(layout.reports_root),
        "--stock-universe-mode",
        "pit_spans",
        "--universe-key",
        DIRECT_UNIVERSE_KEY,
    ]
    if dataset == "stock_daily":
        command.append("--no-validate-values")
    if dataset in {"stock_daily", "stock_minute"}:
        command.append("--resume-csv")
    if dataset == "stock_minute":
        command.append("--skip-validation")
    log_root = layout.candidate_root / "logs"
    log_root.mkdir(parents=True, exist_ok=True)
    with (log_root / f"{component}.stdout.log").open("a", encoding="utf-8") as stdout, (
        log_root / f"{component}.stderr.log"
    ).open("a", encoding="utf-8") as stderr:
        completed = subprocess.run(
            command,
            cwd=project_root,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            check=False,
        )
    if completed.returncode != 0 or not _calendar_reaches_cutoff(calendar, layout.cutoff):
        raise DirectMonthlyError(f"direct {component} exporter failed with code {completed.returncode}")
    return {
        "status": "PASS",
        "component": component,
        "action": "COMPONENT_REBUILD",
        "path": str(output),
        "cutoff": layout.cutoff.isoformat(),
        "source_freeze": False,
        "full_history_content_hash": False,
    }


def build_index_context_component(layout: DirectMonthlyLayout) -> Mapping[str, Any]:
    import pandas as pd

    from backend.qlib_exporter.db_reader import DBReader

    output = layout.components_root / "index_context"
    meta_path = output / "meta.json"
    if _meta_reaches_cutoff(meta_path, layout.cutoff):
        return {
            "status": "PASS",
            "component": "index_context",
            "action": "REUSE_COMPLETED_DIRECT_OUTPUT",
            "path": str(output),
            "cutoff": layout.cutoff.isoformat(),
        }
    if output.exists() and any(output.iterdir()):
        raise DirectMonthlyError("partial index_context output requires explicit inspection")
    output.mkdir(parents=True, exist_ok=True)
    reader = DBReader()
    frames = []
    counts: dict[str, int] = {}
    for code in DIRECT_INDEX_CODES:
        frame = reader.load_index_daily(code, DIRECT_START_DATE, layout.cutoff)
        if frame.empty:
            raise DirectMonthlyError(f"required index has no rows: {code}")
        frames.append(frame)
        counts[code] = int(len(frame))
    combined = pd.concat(frames).sort_index()
    combined.to_hdf(output / "index_daily.h5", key="data", mode="w", format="fixed")
    _write_json_new(
        meta_path,
        {
            "schema_version": "qe_direct_index_context_v1",
            "start": DIRECT_START_DATE.isoformat(),
            "end": layout.cutoff.isoformat(),
            "codes": list(DIRECT_INDEX_CODES),
            "benchmark": "000300.SH",
            "rows_by_code": counts,
            "source_freeze": False,
            "full_history_content_hash": False,
        },
    )
    return {
        "status": "PASS",
        "component": "index_context",
        "action": "COMPONENT_REBUILD",
        "path": str(output),
        "cutoff": layout.cutoff.isoformat(),
        "codes": len(counts),
    }


def build_factor_h5_static_component(layout: DirectMonthlyLayout) -> Mapping[str, Any]:
    """Stream a full PIT-v2 factor/static rebuild in bounded date chunks."""

    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    from backend.data_service import qe_data_service as qe_data
    from backend.qlib_exporter.authoritative_bin_exporter import resolve_stock_universe_from_pit_spans
    from backend.services.dataset_release.static_schema import STATIC_ORDERED_COLUMNS
    from backend.services.industry_code_map import UNKNOWN_L2_CODE_ID

    output = layout.factor_root
    meta_path = output / "meta.json"
    if _component_output_complete(layout, "factor_h5_static"):
        return {
            "status": "PASS",
            "component": "factor_h5_static",
            "action": "REUSE_COMPLETED_DIRECT_OUTPUT",
            "path": str(output),
            "cutoff": layout.cutoff.isoformat(),
        }
    if output.exists() and any(output.iterdir()):
        raise DirectMonthlyError("partial factor_h5_static output requires explicit inspection")
    output.mkdir(parents=True, exist_ok=True)
    codes = resolve_stock_universe_from_pit_spans(
        start=DIRECT_START_DATE,
        end=layout.cutoff,
        universe_key=DIRECT_UNIVERSE_KEY,
        exchanges=("sh", "sz"),
    )
    spans = _load_pit_spans(DIRECT_UNIVERSE_KEY, DIRECT_START_DATE, layout.cutoff)
    industry_intervals = _read_classification_intervals(layout)
    l2_projection, l2_code_map = _load_sw_l2_projection()
    h5_names = {
        "daily_pv.h5": "daily_pv",
        "daily_basic.h5": "daily_basic",
        "moneyflow.h5": "moneyflow",
        "bak_basic.h5": "bak_basic",
        "cyq_perf.h5": "cyq_perf",
        "sector_data.h5": "sector_data",
        "margin_detail.h5": "margin_detail",
    }
    parquet_path = output / "static_factors.parquet"
    parquet_writer: pq.ParquetWriter | None = None
    rows_by_file = {name: 0 for name in h5_names}
    static_rows = 0
    observed_ranges: dict[str, tuple[date, date]] = {}
    try:
        for chunk_start, chunk_end in _date_chunks(DIRECT_START_DATE, layout.cutoff, months=3):
            lookback_start = max(DIRECT_START_DATE, chunk_start - pd.Timedelta(days=45).to_pytimedelta())
            daily = qe_data.load_daily_pv(codes, lookback_start, chunk_end)
            daily_basic = qe_data.load_daily_basic(codes, lookback_start, chunk_end)
            moneyflow = qe_data.load_moneyflow(codes, lookback_start, chunk_end)
            bak_basic = qe_data.load_bak_basic(codes, lookback_start, chunk_end)
            cyq_perf = qe_data.load_cyq_perf(codes, lookback_start, chunk_end)
            margin = qe_data.load_margin_detail(codes, lookback_start, chunk_end)
            sector_chunk = _build_sector_frame_from_classification(
                daily,
                moneyflow,
                intervals_by_symbol=industry_intervals,
                l2_projection=l2_projection,
                l2_code_map=l2_code_map,
                start=lookback_start,
                end=chunk_end,
            )
            raw_frames = {
                "daily_pv.h5": daily,
                "daily_basic.h5": daily_basic,
                "moneyflow.h5": moneyflow,
                "bak_basic.h5": bak_basic,
                "cyq_perf.h5": cyq_perf,
                "sector_data.h5": sector_chunk,
                "margin_detail.h5": margin,
            }
            derived_mf = qe_data.compute_moneyflow_derived_factors(moneyflow, daily)
            derived_db = qe_data.compute_daily_basic_precomputed_factors(daily_basic)
            price = pd.DataFrame(index=daily.index)
            price["PriceStrength_10D"] = daily["close"].groupby(level="instrument").pct_change(10)
            static_frames = [
                frame.sort_index()
                for frame in (
                    daily_basic,
                    moneyflow,
                    bak_basic,
                    cyq_perf,
                    sector_chunk,
                    margin,
                    derived_mf,
                    derived_db,
                    price,
                )
                if frame is not None and not frame.empty
            ]
            if not static_frames:
                raise DirectMonthlyError(f"factor source rows are empty for {chunk_start}~{chunk_end}")
            static = static_frames[0]
            for frame in static_frames[1:]:
                overlap = static.columns.intersection(frame.columns)
                static = static.join(frame.drop(columns=list(overlap)), how="left")
            for column in STATIC_ORDERED_COLUMNS:
                if column not in static.columns:
                    static[column] = np.nan
            static = static.loc[:, list(STATIC_ORDERED_COLUMNS)]
            for column in static.columns:
                numeric = pd.to_numeric(static[column], errors="coerce")
                static[column] = (
                    numeric.fillna(UNKNOWN_L2_CODE_ID).astype("int16")
                    if column == "l2_code_id"
                    else numeric.astype("float32")
                )
            static = _filter_frame_to_pit(static, spans, chunk_start, chunk_end)
            if static.empty:
                raise DirectMonthlyError(f"PIT factor denominator is empty for {chunk_start}~{chunk_end}")
            for name, frame in raw_frames.items():
                bounded = _filter_frame_to_pit(frame, spans, chunk_start, chunk_end)
                if bounded.empty and name in {"daily_pv.h5", "daily_basic.h5", "moneyflow.h5"}:
                    raise DirectMonthlyError(f"required factor file is empty for {name}:{chunk_start}~{chunk_end}")
                if not bounded.empty:
                    if name == "daily_pv.h5":
                        _update_observed_ranges(observed_ranges, bounded)
                    bounded.to_hdf(
                        output / name,
                        key="data",
                        mode="a",
                        format="table",
                        append=(output / name).exists(),
                    )
                    rows_by_file[name] += int(len(bounded))
            table = pa.Table.from_pandas(static, preserve_index=True)
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(parquet_path, table.schema, compression="snappy")
            elif table.schema != parquet_writer.schema:
                table = table.cast(parquet_writer.schema)
            parquet_writer.write_table(table, row_group_size=100_000)
            static_rows += int(len(static))
            qe_data.clear_data_cache()
    finally:
        if parquet_writer is not None:
            parquet_writer.close()
    _write_instruments_from_spans(
        output / "instruments" / "all.txt",
        spans,
        observed_ranges=observed_ranges,
    )
    _write_json_new(
        output / "static_factors_schema.json",
        {
            "schema_version": "qe_static_factors_121_v1",
            "columns": list(STATIC_ORDERED_COLUMNS),
            "column_count": len(STATIC_ORDERED_COLUMNS),
            "default_numeric_dtype": "float32",
            "l2_code_id_dtype": "int16",
            "l2_code_id_missing": -1,
        },
    )
    (output / "static_factors_schema.csv").write_text(
        "position,column,dtype\n"
        + "".join(
            f"{position},{column},{'int16' if column == 'l2_code_id' else 'float32'}\n"
            for position, column in enumerate(STATIC_ORDERED_COLUMNS)
        ),
        encoding="utf-8",
    )
    _write_json_new(
        meta_path,
        {
            "schema_version": DIRECT_FACTOR_SCHEMA,
            "start": DIRECT_START_DATE.isoformat(),
            "end": layout.cutoff.isoformat(),
            "universe_key": DIRECT_UNIVERSE_KEY,
            "instruments": len(codes),
            "rows_by_file": rows_by_file,
            "static_rows": static_rows,
            "static_columns": len(STATIC_ORDERED_COLUMNS),
            "sector_authority": DIRECT_SECTOR_AUTHORITY,
            "source_freeze": False,
            "full_history_content_hash": False,
        },
    )
    return {
        "status": "PASS",
        "component": "factor_h5_static",
        "action": "COMPONENT_REBUILD",
        "path": str(output),
        "cutoff": layout.cutoff.isoformat(),
        "static_rows": static_rows,
        "instruments": len(codes),
    }


def _load_pit_spans(universe_key: str, start: date, end: date):
    import pandas as pd

    from backend.db.pg_pool import get_conn

    with get_conn() as connection:
        spans = pd.read_sql(
            """
            SELECT ts_code, eligible_start, eligible_end
            FROM market.stock_universe_pit_spans
            WHERE universe_key = %(universe_key)s
              AND eligible_start <= %(end)s
              AND eligible_end >= %(start)s
            ORDER BY ts_code, eligible_start, eligible_end
            """,
            connection,
            params={"universe_key": universe_key, "start": start, "end": end},
        )
    if spans.empty:
        raise DirectMonthlyError("canonical PIT spans are empty")
    spans["ts_code"] = spans["ts_code"].astype(str).str.upper()
    spans["eligible_start"] = pd.to_datetime(spans["eligible_start"]).dt.normalize()
    spans["eligible_end"] = pd.to_datetime(spans["eligible_end"]).dt.normalize()
    if spans.duplicated(["ts_code", "eligible_start", "eligible_end"]).any():
        raise DirectMonthlyError("canonical PIT spans contain duplicate intervals")
    previous_end = spans.groupby("ts_code")["eligible_end"].shift(1)
    if (spans["eligible_start"] <= previous_end).fillna(False).any():
        raise DirectMonthlyError("canonical PIT spans overlap")
    return spans


def _filter_frame_to_pit(frame, spans, start: date, end: date):
    import pandas as pd

    if frame is None or frame.empty:
        return pd.DataFrame(columns=getattr(frame, "columns", None))
    if not isinstance(frame.index, pd.MultiIndex) or set(frame.index.names) != {"datetime", "instrument"}:
        raise DirectMonthlyError("factor frame must use datetime/instrument MultiIndex")
    value = frame.reset_index()
    value["instrument"] = value["instrument"].astype(str).str.upper()
    value["datetime"] = pd.to_datetime(value["datetime"]).dt.normalize()
    merged = value.merge(spans, left_on="instrument", right_on="ts_code", how="inner")
    lower = pd.Timestamp(start)
    upper = pd.Timestamp(end)
    merged = merged.loc[
        (merged["datetime"] >= lower)
        & (merged["datetime"] <= upper)
        & (merged["datetime"] >= merged["eligible_start"])
        & (merged["datetime"] <= merged["eligible_end"])
    ]
    columns = list(frame.columns)
    if merged.empty:
        return pd.DataFrame(columns=columns, index=pd.MultiIndex.from_arrays([[], []], names=["datetime", "instrument"]))
    if merged.duplicated(["datetime", "instrument"]).any():
        raise DirectMonthlyError("PIT join produced duplicate factor keys")
    return merged.set_index(["datetime", "instrument"])[columns].sort_index()


def _slice_frame(frame, start: date, end: date):
    import pandas as pd

    if frame is None or frame.empty:
        return frame
    dates = pd.to_datetime(frame.index.get_level_values("datetime")).normalize()
    return frame.loc[(dates >= pd.Timestamp(start)) & (dates <= pd.Timestamp(end))]


@dataclass(frozen=True, slots=True)
class _ClassificationInterval:
    start: date
    end_exclusive: date
    l2_code: str


def _canonical_index_code(value: object) -> str:
    code = str(value or "").strip().upper()
    return code if "." in code else f"{code}.SI"


def _read_classification_intervals(
    layout: DirectMonthlyLayout,
) -> Mapping[str, tuple[_ClassificationInterval, ...]]:
    """Read the compact stock-classification authority without P3A row ledgers.

    Published sector values are projected from a stock's classification PIT.
    Official index membership is a separate authority used only by index-
    constituent research; it is not a prerequisite for stock sector features.
    """

    root = layout.industry_authority_root
    path = root / "classification_candidate.jsonl"
    if not root.is_dir() or root.is_symlink() or not path.is_file() or path.is_symlink():
        raise DirectMonthlyError("classification PIT candidate is unavailable at its deterministic path")
    intervals: dict[str, list[_ClassificationInterval]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise DirectMonthlyError(
                    f"classification PIT row is invalid at line {line_number}"
                ) from exc
            identity = row.get("identity")
            if row.get("unavailable_reason") is not None or not isinstance(identity, Mapping):
                continue
            symbol = str(row.get("canonical_symbol") or "").strip().upper()
            l2_code = str(identity.get("l2_code") or "").strip()
            causal_start = row.get("causal_use_from")
            eligible_start = row.get("eligible_from")
            eligible_end = row.get("eligible_to_exclusive")
            if not symbol or not l2_code or not causal_start or not eligible_start or not eligible_end:
                continue
            try:
                start = max(date.fromisoformat(str(causal_start)), date.fromisoformat(str(eligible_start)))
                end_values = [date.fromisoformat(str(eligible_end))]
                if row.get("causal_use_to_exclusive"):
                    end_values.append(date.fromisoformat(str(row["causal_use_to_exclusive"])))
                end_exclusive = min(end_values)
            except ValueError as exc:
                raise DirectMonthlyError(
                    f"classification PIT dates are invalid at line {line_number}"
                ) from exc
            if end_exclusive <= start:
                continue
            intervals.setdefault(symbol, []).append(
                _ClassificationInterval(start=start, end_exclusive=end_exclusive, l2_code=l2_code)
            )
    if not intervals:
        raise DirectMonthlyError("classification PIT candidate has no usable intervals")

    normalized: dict[str, tuple[_ClassificationInterval, ...]] = {}
    for symbol, values in intervals.items():
        ordered = sorted(set(values), key=lambda item: (item.start, item.end_exclusive, item.l2_code))
        merged: list[_ClassificationInterval] = []
        for item in ordered:
            if merged and item.start < merged[-1].end_exclusive:
                previous = merged[-1]
                if item.l2_code != previous.l2_code:
                    raise DirectMonthlyError(f"classification PIT intervals overlap for {symbol}")
                merged[-1] = _ClassificationInterval(
                    start=previous.start,
                    end_exclusive=max(previous.end_exclusive, item.end_exclusive),
                    l2_code=previous.l2_code,
                )
                continue
            merged.append(item)
        normalized[symbol] = tuple(merged)
    return normalized


def _load_sw_l2_projection() -> tuple[Mapping[str, str], Mapping[str, int]]:
    import pandas as pd

    from backend.db.pg_pool import get_conn
    from backend.services.industry_code_map import load_sw_l2_code_map

    with get_conn() as connection:
        rows = pd.read_sql(
            """
            SELECT industry_code,index_code
            FROM market.sw_index_classify
            WHERE level='L2'
              AND industry_code IS NOT NULL
              AND index_code IS NOT NULL
            ORDER BY industry_code,index_code
            """,
            connection,
        )
        raw_code_map = load_sw_l2_code_map(connection)
    projection: dict[str, str] = {}
    for row in rows.itertuples(index=False):
        industry_code = str(row.industry_code).strip()
        index_code = _canonical_index_code(row.index_code)
        previous = projection.get(industry_code)
        if previous is not None and previous != index_code:
            raise DirectMonthlyError(f"SW L2 taxonomy maps to multiple published indices: {industry_code}")
        projection[industry_code] = index_code
    if not projection:
        raise DirectMonthlyError("SW L2 taxonomy projection is empty")
    code_map = {_canonical_index_code(code): value for code, value in raw_code_map.items()}
    missing_ids = sorted(set(projection.values()).difference(code_map))
    if missing_ids:
        raise DirectMonthlyError(f"SW L2 published codes lack stable ids: {missing_ids[:5]}")
    return projection, code_map


def _classify_panel_index(
    index,
    *,
    intervals_by_symbol: Mapping[str, Sequence[_ClassificationInterval]],
    l2_projection: Mapping[str, str],
):
    import numpy as np
    import pandas as pd

    if not isinstance(index, pd.MultiIndex) or index.nlevels != 2:
        raise DirectMonthlyError("sector projection requires a datetime/instrument MultiIndex")
    keys = index.to_frame(index=False)
    keys.columns = ["datetime", "instrument"]
    keys["datetime"] = pd.to_datetime(keys["datetime"]).dt.normalize()
    keys["instrument"] = keys["instrument"].astype(str).str.upper()
    classification = np.full(len(keys), None, dtype=object)
    published = np.full(len(keys), None, dtype=object)
    for symbol, positions in keys.groupby("instrument", sort=False).groups.items():
        absolute_positions = np.asarray(positions, dtype=np.int64)
        dates = keys.loc[absolute_positions, "datetime"]
        matched = np.zeros(len(positions), dtype=np.int8)
        for interval in intervals_by_symbol.get(symbol, ()):
            mask = (dates >= pd.Timestamp(interval.start)) & (
                dates < pd.Timestamp(interval.end_exclusive)
            )
            if not bool(mask.any()):
                continue
            local = np.flatnonzero(mask.to_numpy())
            matched[local] += 1
            absolute = absolute_positions[local]
            classification[absolute] = interval.l2_code
            published[absolute] = l2_projection.get(interval.l2_code)
        if bool((matched > 1).any()):
            raise DirectMonthlyError(f"classification PIT resolves more than once for {symbol}")
    result = pd.DataFrame(
        {
            "classification_l2_code": classification,
            "index_l2_code": published,
        },
        index=index,
    )
    return result.dropna(subset=["classification_l2_code", "index_l2_code"])


def _load_sw_daily_for_projection(index_codes: Sequence[str], start: date, end: date):
    import pandas as pd

    from backend.db.pg_pool import get_conn

    columns = ("open", "high", "low", "close", "pct_change", "vol", "amount", "pe", "pb", "total_mv")
    with get_conn() as connection:
        frame = pd.read_sql(
            f"""
            SELECT trade_date,ts_code,{','.join(columns)}
            FROM market.sw_daily
            WHERE trade_date BETWEEN %s AND %s
              AND ts_code = ANY(%s)
            ORDER BY trade_date,ts_code
            """,
            connection,
            params=(start, end, list(sorted(set(index_codes)))),
        )
    if frame.empty:
        return pd.DataFrame(columns=["datetime", "index_l2_code", *columns])
    frame["datetime"] = pd.to_datetime(frame.pop("trade_date")).dt.normalize()
    frame["index_l2_code"] = frame.pop("ts_code").map(_canonical_index_code)
    if frame.duplicated(["datetime", "index_l2_code"]).any():
        raise DirectMonthlyError("published SW L2 daily facts contain duplicate keys")
    return frame


def _build_sector_frame_from_classification(
    daily,
    moneyflow,
    *,
    intervals_by_symbol: Mapping[str, Sequence[_ClassificationInterval]],
    l2_projection: Mapping[str, str],
    l2_code_map: Mapping[str, int],
    start: date,
    end: date,
):
    import numpy as np
    import pandas as pd

    if daily is None or daily.empty:
        return pd.DataFrame()
    assignments = _classify_panel_index(
        daily.index,
        intervals_by_symbol=intervals_by_symbol,
        l2_projection=l2_projection,
    )
    if assignments.empty:
        return pd.DataFrame()
    published = _load_sw_daily_for_projection(
        assignments["index_l2_code"].dropna().astype(str).unique().tolist(),
        start,
        end,
    )
    sw_fields = {
        "open": "sw2_open",
        "high": "sw2_high",
        "low": "sw2_low",
        "close": "sw2_close",
        "pct_change": "sw2_pct_change",
        "vol": "sw2_vol",
        "amount": "sw2_amount",
        "pe": "sw2_pe",
        "pb": "sw2_pb",
        "total_mv": "sw2_total_mv",
    }
    published = published.rename(columns=sw_fields)

    flow_fields = {
        "mf_sm_buy_amt": "sw2_mf_buy_sm_amt",
        "mf_sm_sell_amt": "sw2_mf_sell_sm_amt",
        "mf_md_buy_amt": "sw2_mf_buy_md_amt",
        "mf_md_sell_amt": "sw2_mf_sell_md_amt",
        "mf_lg_buy_amt": "sw2_mf_buy_lg_amt",
        "mf_lg_sell_amt": "sw2_mf_sell_lg_amt",
        "mf_elg_buy_amt": "sw2_mf_buy_elg_amt",
        "mf_elg_sell_amt": "sw2_mf_sell_elg_amt",
        "mf_net_amt": "sw2_mf_net_amt",
        "mf_elg_buy_vol": "sw2_mf_buy_elg_vol",
        "mf_elg_sell_vol": "sw2_mf_sell_elg_vol",
        "mf_net_vol": "sw2_mf_net_vol",
    }
    flow_aggregate = pd.DataFrame(columns=["datetime", "index_l2_code", *flow_fields.values()])
    if moneyflow is not None and not moneyflow.empty:
        # Moneyflow is a sparse subset of the daily stock-date panel. Reuse the
        # classification result instead of resolving every interval twice.
        flow_assignment = assignments.reindex(moneyflow.index).dropna(
            subset=["classification_l2_code", "index_l2_code"]
        )
        if not flow_assignment.empty:
            available = [column for column in flow_fields if column in moneyflow.columns]
            if available:
                flow = moneyflow.loc[flow_assignment.index, available].join(
                    flow_assignment[["index_l2_code"]]
                )
                flow = flow.reset_index()
                grouped = (
                    flow.groupby(["datetime", "index_l2_code"], as_index=False)[available]
                    .sum(min_count=1)
                    .rename(columns=flow_fields)
                )
                flow_aggregate = grouped

    rows = assignments.reset_index()
    rows["datetime"] = pd.to_datetime(rows["datetime"]).dt.normalize()
    if published.empty:
        for column in sw_fields.values():
            rows[column] = np.nan
    else:
        rows = rows.merge(
            published,
            on=["datetime", "index_l2_code"],
            how="left",
            validate="many_to_one",
        )
    if flow_aggregate.empty:
        for column in flow_fields.values():
            rows[column] = np.nan
    else:
        rows = rows.merge(
            flow_aggregate,
            on=["datetime", "index_l2_code"],
            how="left",
            validate="many_to_one",
        )
    rows["l2_code_id"] = (
        rows["index_l2_code"].map(l2_code_map).fillna(-1).astype("int16")
    )
    value_columns = [*sw_fields.values(), *flow_fields.values()]
    for column in value_columns:
        if column not in rows:
            rows[column] = np.nan
        rows[column] = pd.to_numeric(rows[column], errors="coerce").astype("float32")
    result = rows.set_index(["datetime", "instrument"])[["l2_code_id", *value_columns]].sort_index()
    if result.index.duplicated().any():
        raise DirectMonthlyError("classification sector projection produced duplicate stock-date keys")
    return result


def _date_chunks(start: date, end: date, *, months: int):
    import pandas as pd

    current = pd.Timestamp(start)
    terminal = pd.Timestamp(end)
    while current <= terminal:
        next_start = current + pd.DateOffset(months=months)
        chunk_end = min(terminal, next_start - pd.Timedelta(days=1))
        yield current.date(), chunk_end.date()
        current = chunk_end + pd.Timedelta(days=1)


def _update_observed_ranges(target: dict[str, tuple[date, date]], frame) -> None:
    import pandas as pd

    value = frame.reset_index()[["datetime", "instrument"]]
    value["datetime"] = pd.to_datetime(value["datetime"]).dt.date
    for instrument, group in value.groupby("instrument", sort=False):
        observed = (min(group["datetime"]), max(group["datetime"]))
        previous = target.get(str(instrument))
        target[str(instrument)] = (
            min(previous[0], observed[0]) if previous else observed[0],
            max(previous[1], observed[1]) if previous else observed[1],
        )


def _write_instruments_from_spans(
    path: Path,
    spans,
    *,
    observed_ranges: Mapping[str, tuple[date, date]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for row in spans.itertuples(index=False):
        observed = observed_ranges.get(str(row.ts_code))
        if observed is None:
            continue
        effective_start = max(row.eligible_start.date(), observed[0])
        effective_end = min(row.eligible_end.date(), observed[1])
        if effective_start <= effective_end:
            lines.append(f"{row.ts_code}\t{effective_start.isoformat()}\t{effective_end.isoformat()}")
    if not lines:
        raise DirectMonthlyError("PIT instruments output would be empty")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json_new(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise DirectMonthlyError(f"direct metadata already exists: {path}")
    path.write_text(
        json.dumps(dict(value), ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _validate_adoptable_direct_work(layout: DirectMonthlyLayout) -> None:
    allowed_top = {"components", "logs", "reports", "work"}
    children = {child.name for child in layout.candidate_root.iterdir()}
    if not children or not children.issubset(allowed_top):
        raise DirectMonthlyError("existing direct work contains an unknown path and cannot be adopted")
    for child in layout.candidate_root.rglob("*"):
        if child.is_symlink():
            raise DirectMonthlyError("existing direct work contains a symlink and cannot be adopted")
    components = layout.candidate_root / "components"
    if components.exists():
        allowed_components = {
            "daily_bin_candidate",
            "minute_bin_candidate",
            "factor_h5_static_candidate",
            DIRECT_FACTOR_COMPONENT_DIR,
            "index_context",
        }
        if not {child.name for child in components.iterdir()}.issubset(allowed_components):
            raise DirectMonthlyError("existing direct work contains an unknown component")


def _meta_reaches_cutoff(path: Path, cutoff: date) -> bool:
    if not path.is_file():
        return False
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    return value.get("end") == cutoff.isoformat()


def _component_output_complete(layout: DirectMonthlyLayout, component: str) -> bool:
    """Invalidate only a component whose output contract actually changed.

    Daily, minute and index PASS records remain resumable.  BUG-1336 changes
    the factor/sector schema and intentionally moves it to a new sibling
    directory, so an old factor PASS must be rebuilt without touching the
    already exported Qlib bins.
    """

    if component != "factor_h5_static":
        return True
    meta = layout.factor_root / "meta.json"
    if not _meta_reaches_cutoff(meta, layout.cutoff):
        return False
    try:
        value = json.loads(meta.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    return (
        value.get("schema_version") == DIRECT_FACTOR_SCHEMA
        and value.get("sector_authority") == DIRECT_SECTOR_AUTHORITY
    )


def _calendar_reaches_cutoff(path: Path, cutoff: date) -> bool:
    if not path.is_file():
        return False
    try:
        lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except (OSError, UnicodeDecodeError):
        return False
    return bool(lines) and lines[-1][:10] == cutoff.isoformat()


def validate_direct_candidate(layout: DirectMonthlyLayout) -> Mapping[str, Any]:
    factor = layout.factor_root
    index = layout.components_root / "index_context"
    checks = {
        "daily_bin": _calendar_reaches_cutoff(
            layout.components_root / "daily_bin_candidate" / "calendars" / "day.txt",
            layout.cutoff,
        ),
        "minute_bin": _calendar_reaches_cutoff(
            layout.components_root / "minute_bin_candidate" / "calendars" / "1min.txt",
            layout.cutoff,
        ),
        "factor_h5_static": _component_output_complete(layout, "factor_h5_static")
        and all(
            (factor / name).is_file() and (factor / name).stat().st_size > 0
            for name in (
                "daily_pv.h5",
                "daily_basic.h5",
                "moneyflow.h5",
                "bak_basic.h5",
                "cyq_perf.h5",
                "sector_data.h5",
                "margin_detail.h5",
                "static_factors.parquet",
                "static_factors_schema.json",
                "static_factors_schema.csv",
            )
        ),
        "index_context": _meta_reaches_cutoff(index / "meta.json", layout.cutoff)
        and (index / "index_daily.h5").is_file()
        and (index / "index_daily.h5").stat().st_size > 0,
    }
    if not all(checks.values()):
        raise DirectMonthlyError(f"direct candidate structural validation failed: {checks}")
    return {
        "status": "PASS",
        "cutoff": layout.cutoff.isoformat(),
        "checks": checks,
        "full_history_content_hash": False,
        "production_writes": 0,
        "production_pointer_changes": 0,
    }


def validate_direct_candidate_with_smoke(
    layout: DirectMonthlyLayout,
    *,
    project_root: Path,
) -> Mapping[str, Any]:
    """Run one QE/static and one minute execution smoke plus HMM input readback."""

    import pandas as pd

    structural = validate_direct_candidate(layout)
    reports = layout.reports_root / "consumer_smoke"
    reports.mkdir(parents=True, exist_ok=True)
    daily = layout.components_root / "daily_bin_candidate"
    minute = layout.components_root / "minute_bin_candidate"
    factor = layout.factor_root
    qe_output = reports / "qe_multi_dataset"
    minute_output = reports / "minute_nested_executor.json"
    commands = (
        [
            "python",
            _windows_to_wsl(project_root / "scripts" / "qlib_multi_dataset_smoke_backtest.py"),
            "--provider-uri",
            _windows_to_wsl(daily),
            "--snapshot-dir",
            _windows_to_wsl(factor),
            "--output-dir",
            _windows_to_wsl(qe_output),
            "--feature-start",
            "2025-04-01",
            "--train-start",
            "2025-07-01",
            "--train-end",
            "2026-06-30",
            "--test-start",
            "2026-07-01",
            "--test-end",
            layout.cutoff.isoformat(),
            "--num-stocks",
            "20",
            "--contract-smoke-only",
            "--require-nonempty-source",
            "sector_data",
        ],
        [
            "python",
            _windows_to_wsl(project_root / "scripts" / "qlib_authoritative_smoke_backtest.py"),
            "--minute-provider-uri",
            _windows_to_wsl(minute),
            "--day-provider-uri",
            _windows_to_wsl(daily),
            "--start",
            layout.cutoff.replace(day=1).isoformat(),
            "--end",
            layout.cutoff.isoformat(),
            "--num-stocks",
            "10",
            "--output",
            _windows_to_wsl(minute_output),
            "--contract-smoke-only",
        ],
    )
    log_path = reports / "wsl_smoke.log"
    with log_path.open("a", encoding="utf-8") as log:
        for command in commands:
            inner = " && ".join(
                (
                    "source /home/lc999/miniconda3/etc/profile.d/conda.sh",
                    "conda activate rdagent-gpu",
                    f"cd {shlex.quote(_windows_to_wsl(project_root))}",
                    f"PYTHONPATH={shlex.quote(_windows_to_wsl(project_root))} "
                    + " ".join(shlex.quote(value) for value in command),
                )
            )
            completed = subprocess.run(
                ["wsl", "-d", "Ubuntu", "bash", "-lc", inner],
                cwd=project_root,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                check=False,
            )
            if completed.returncode != 0:
                raise DirectMonthlyError(
                    f"direct candidate WSL smoke failed with code {completed.returncode}"
                )
    index_frame = pd.read_hdf(layout.components_root / "index_context" / "index_daily.h5", key="data")
    if index_frame.empty or not isinstance(index_frame.index, pd.MultiIndex):
        raise DirectMonthlyError("HMM index input smoke found an empty or invalid index frame")
    instruments = index_frame.index.get_level_values("instrument").astype(str)
    dates = pd.to_datetime(index_frame.index.get_level_values("datetime")).date
    if "000300.SH" not in set(instruments) or max(dates) != layout.cutoff:
        raise DirectMonthlyError("HMM benchmark input does not reach the candidate cutoff")
    return {
        **structural,
        "qe_multi_dataset_smoke": "PASS",
        "minute_nested_executor_smoke": "PASS",
        "hmm_index_input_smoke": "PASS",
        "smoke_reports": str(reports),
    }


def _windows_to_wsl(path: Path) -> str:
    text = str(path.resolve(strict=False)).replace("\\", "/")
    if len(text) >= 3 and text[1:3] == ":/":
        return f"/mnt/{text[0].lower()}/{text[3:]}"
    return text


__all__ = [
    "DIRECT_COMPONENTS",
    "DIRECT_MONTHLY_SCHEMA",
    "DIRECT_TERMINAL_STATUS",
    "DirectComponentPlan",
    "DirectMonthlyError",
    "DirectMonthlyLayout",
    "DirectMonthlyRunner",
    "compact_status",
    "component_plan",
    "initial_state",
    "read_state",
    "write_state",
]
