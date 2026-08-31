from __future__ import annotations

import copy
import hashlib
import json
import shutil
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path, PureWindowsPath
from types import SimpleNamespace

import pandas as pd
import pytest

import backend.services.dataset_release.build_stage as build_stage
import backend.services.dataset_release.candidate_validator as candidate_validator_module
from backend.services.dataset_release import component_manifest_producer as component_producer
import backend.tests.dataset_release.test_candidate_validator as candidate_fixtures
from backend.services.dataset_release.build_stage import (
    BuildStageInvocation,
    run_build_stage,
)
from backend.services.dataset_release.canonical_stock_transformer import (
    build_qfq_denominator_authority,
)
from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.canonical_lineage import (
    CANONICAL_LINEAGE_CAPABILITY,
    CANONICAL_LINEAGE_SCHEMA,
    lineage_bucket,
    lineage_event_key,
    planned_lineage_paths,
)
from backend.services.dataset_release.candidate_validator import CandidateValidator
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.component_artifact_manifest import (
    load_component_artifact_manifest,
)
from backend.services.dataset_release.component_manifest_producer import (
    produce_component_artifact_manifest,
)
from backend.services.dataset_release.contracts import Component, ComponentAction
from backend.services.dataset_release.decision import DECISION_SCHEMA_VERSION
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.index_materializer import (
    DOMESTIC_INDEX_DEFINITIONS,
)
from backend.services.dataset_release.factor_materializer import (
    FACTOR_H5_DATASETS,
    FACTOR_SOURCE_SCHEMAS,
    STATIC_DATASET,
)
from backend.data_service.moneyflow_contract import (
    TUSHARE_MONEYFLOW_AMOUNT_COLUMNS,
    TUSHARE_MONEYFLOW_VOLUME_COLUMNS,
)
from backend.services.dataset_release.publisher import artifact_tree_digest
from backend.services.dataset_release.pit import DatasetPitBinding, freeze_pit_snapshot
from backend.services.dataset_release.profile import load_dataset_profile
from backend.services.dataset_release.streaming_artifacts import sha256_file
from backend.services.canonical_equity_pit import CANONICAL_PIT_RULE_VERSION, CANONICAL_PIT_UNIVERSE_KEY
from backend.tests.dataset_release.test_candidate_validator import (
    DATES,
    STOCKS,
    _candidate,
    _daily_bin,
    _minute_bin,
    _pit,
    _spec,
)


def _source_partition(component: Component) -> list[dict]:
    dataset = {
        Component.DAILY_BIN: "kline_daily_raw",
        Component.MINUTE_BIN: "kline_minute_raw",
        Component.FACTOR_H5_STATIC: "daily_basic",
        Component.DOMESTIC_INDEX_CONTEXT: "index_daily_merged",
    }[component]
    return [
        {
            "identity": f"{dataset}:2026-07-01_2026-07-31",
            "dataset": dataset,
            "partition_key": "2026-07-01_2026-07-31",
            "row_count": 1,
            "content_digest": hashlib.sha256(f"{dataset}:content".encode()).hexdigest(),
            "schema_digest": hashlib.sha256(f"{dataset}:schema".encode()).hexdigest(),
            "source_table_schema_digest": hashlib.sha256(f"{dataset}:table".encode()).hexdigest(),
            "source_code_membership_digest": None,
            "min_key": None,
            "max_key": None,
            "monthly_content_leaves": [],
        }
    ]


def test_v2_candidate_manifest_embeds_readable_canonical_pit_binding() -> None:
    profile = load_dataset_profile(Path(__file__).resolve().parents[3] / "configs/datasets/qe_backtest_monthly_v2.yaml")
    pit = freeze_pit_snapshot(
        [
            {
                "ts_code": "000001.SZ",
                "eligible_start": date(2026, 7, 30),
                "eligible_end": date(2026, 7, 31),
                "entry_reason": "listed",
                "exit_reason": "scope_end",
            }
        ],
        universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        scope_start=date(2026, 7, 30),
        cutoff=date(2026, 7, 31),
        state_identity="canonical-state",
        source_fingerprint_sha256="a" * 64,
        parameter_hash="b" * 64,
        state_start=date(2018, 8, 1),
        state_end=date(2026, 7, 31),
    )
    component_ref = SimpleNamespace(as_dict=lambda: {"sha256": "c" * 64, "size_bytes": 1})
    invocation = SimpleNamespace(
        profile=profile,
        release_id="qe-pit-v2-20260731",
        release_digest="d" * 64,
        build_inputs={
            "scope": "full",
            "source_snapshot": {
                "raw_source_content_root": "e" * 64,
                "pit_snapshot_digest": pit.spans_sha256,
            },
            "artifact_ready_content_root": "f" * 64,
        },
    )

    manifest = build_stage._build_candidate_manifest(
        invocation=invocation,
        pit=pit,
        artifact_root="1" * 64,
        component_manifest={"manifest_root": "2" * 64},
        component_ref=component_ref,
    )
    binding = DatasetPitBinding.from_release_manifest(manifest)

    assert binding.release_id == invocation.release_id
    assert binding.cutoff == date(2026, 7, 31)
    assert binding.scope == "full"
    assert binding.rolling_cutoff_spans_sha256 == pit.spans_sha256
    assert manifest["source_content_root"] == "e" * 64


def _frozen(evidence, *, action: ComponentAction) -> dict:
    replace_targets = evidence.all_file_paths if action is ComponentAction.SELECTIVE_REBUILD else ()
    return {
        "source_release_id": "baseline-release",
        "source_release_digest": "1" * 64,
        "source_attestation_key": "2" * 64,
        "artifact_id": evidence.component_identity,
        "component_partition_key": "all",
        "manifest_root": evidence.filesystem_tree_merkle,
        "file_identity": evidence.file_identity,
        "reuse_mode": action.value.casefold(),
        "mutation_set": list(replace_targets),
        "compatibility_reason": "fixture exact compatible component",
        "replace_existing_targets": list(replace_targets),
        "create_new_targets": [],
        "invalidation_scopes": (
            [
                {
                    "kind": "historical_source_revision",
                    "source_partition": "index_daily_merged:2026-07-01_2026-07-31",
                    "months": ["2026-07"],
                }
            ]
            if action is ComponentAction.SELECTIVE_REBUILD
            else []
        ),
        "component_root_relative_path": evidence.component_root_relative_path,
    }


def _bounded_frozen(evidence, *, replace_targets, create_targets, scopes) -> dict:
    value = _frozen(evidence, action=ComponentAction.INCREMENTAL)
    value.update(
        {
            "reuse_mode": "incremental",
            "mutation_set": sorted({*replace_targets, *create_targets}),
            "replace_existing_targets": sorted(set(replace_targets)),
            "create_new_targets": sorted(set(create_targets)),
            "invalidation_scopes": list(scopes),
        }
    )
    return value


def _baseline_build_input(
    manifest,
    manifest_ref,
    artifact_root: str,
    actions: list[dict],
) -> dict:
    return {
        "release_id": "baseline-release",
        "release_digest": "1" * 64,
        "candidate_registration_id": "fixture-registration",
        "candidate_identity": manifest.candidate_identity,
        "artifact_root": artifact_root,
        "profile": manifest.profile,
        "scope": manifest.scope,
        "cutoff": manifest.cutoff.isoformat(),
        "semantic_profile_digest": manifest.semantic_profile_digest,
        "producer_fingerprint": manifest.producer_fingerprint,
        "artifact_fingerprint": manifest.artifact_fingerprint,
        "validation_fingerprint": manifest.validation_fingerprint,
        "source_content_root": manifest.source_content_root,
        "artifact_ready_content_root": manifest.artifact_ready_content_root,
        "pit_snapshot_digest": manifest.pit_snapshot_digest,
        "allowlisted_root_id": "fixture-root",
        "volume_serial": "fixture-volume",
        "root_relative_path": "baseline-release",
        "source_manifest_ref": None,
        "component_artifact_manifest_ref": manifest_ref.as_dict(),
        "attestation_key": "2" * 64,
        "reuse_evidence": [dict(item) for item in actions if item.get("frozen_reuse") is not None],
    }


def _synthetic_batched_dump_receipt(manifest: dict) -> dict:
    completed = []
    for phase in manifest["phases"]:
        for batch in phase["batches"]:
            files = batch["files"]
            completed.append(
                {
                    "sequence": len(completed),
                    "phase_id": phase["phase_id"],
                    "phase_kind": phase["kind"],
                    "ordinal": batch["ordinal"],
                    "role": batch["role"],
                    "mode": batch["mode"],
                    "instrument_authority_restored": (
                        phase["kind"] in {"override", "full"} or batch["role"] == "index"
                    ),
                    "recovered_from_inflight": False,
                    "codes": len(files),
                    "code_list": [item["code"] for item in files],
                    "rows": sum(item["rows"] for item in files),
                    "calendar_rows": 1,
                    "calendar_end": "2026-08-31",
                }
            )
    return {
        "schema_version": "dataset_release_qlib_batched_dump_receipt_v1",
        "manifest_identity": manifest["manifest_identity"],
        "status": "PASS",
        "dataset": manifest["dataset"],
        "completed_batches": completed,
        "completed_batch_count": len(completed),
        "peak_codes_per_batch": max(item["codes"] for item in completed),
        "peak_rows_per_batch": max(item["rows"] for item in completed),
        "all_market_frames_retained": 0,
        "upstream_silent_code_failures": 0,
    }


@pytest.mark.parametrize("tamper", ["missing_batch", "changed_code", "silent_failure"])
def test_batched_dump_finalize_receipt_rejects_incomplete_or_forged_proof(
    tamper: str,
) -> None:
    manifest = {
        "schema_version": "dataset_release_qlib_batched_dump_manifest_v1",
        "dataset": "daily_bin",
        "freq": "day",
        "fields": list(build_stage.QLIB_STOCK_FIELDS),
        "max_codes_per_batch": 20,
        "per_batch_timeout_seconds": 30,
        "resource_checkpoint_identity": {
            "attempt_id": "attempt-a",
            "fence": 1,
            "execution_id": "build-dump-daily",
        },
        "phases": [
            {
                "phase_id": "tail",
                "kind": "tail",
                "batches": [
                    {
                        "ordinal": 0,
                        "role": "stock",
                        "mode": "dump_update",
                        "relative_path": "tail/batch-0000-stock",
                        "files": [
                            {
                                "code": "000001.SZ",
                                "role": "stock",
                                "relative_path": "000001.sz.csv",
                                "rows": 1,
                                "sha256": "a" * 64,
                                "start": "2026-08-31",
                                "end": "2026-08-31",
                            }
                        ],
                    }
                ],
            }
        ],
        "expected_total_code_writes": 1,
        "expected_total_rows": 1,
    }
    manifest["manifest_identity"] = hashlib.sha256(
        json.dumps(
            manifest,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    receipt = _synthetic_batched_dump_receipt(manifest)
    if tamper == "missing_batch":
        receipt["completed_batches"] = []
    elif tamper == "changed_code":
        receipt["completed_batches"][0]["code_list"] = ["000002.SZ"]
    else:
        receipt["upstream_silent_code_failures"] = 1

    with pytest.raises(
        build_stage.CandidateBuildStageError,
        match="bounded Qlib",
    ):
        build_stage._validate_batched_dump_receipt(
            manifest,
            receipt,
            expected_dataset="daily_bin",
            expected_manifest_identity=manifest["manifest_identity"],
        )


class _BuildSource:
    def __init__(self, qfq) -> None:
        self.qfq_authority = qfq
        self.artifact_ready_content_root = "a" * 64
        self.minute_overlay_summary = {
            "source_policy": "tdx_then_tushare_missing_keys_conflict_fail_v1",
            "database_rows": 900,
            "overlay_rows": 60,
            "synthesized_suspend_rows": 0,
            "tdx_rows": 50,
            "tushare_rows": 10,
            "overlap_rows_verified": 0,
            "missing_keys": 0,
            "duplicate_keys": 0,
            "overlap_mismatch_cells": 0,
            "provider_concurrency": 1,
            "database_writes": 0,
            "production_writes": 0,
        }
        self.qfq_source_summary = {
            "source_precedence": "db_then_tushare_missing_keys_conflict_fail_v1",
            "overlap_mismatch_cells": 0,
            "provider_fill_rows": 0,
        }
        self.factor_overlay_summary = {
            "source_precedence": "database_then_provider_missing_keys_conflict_fail_v1",
            "overlap_mismatch_cells": 0,
            "provider_override_rows": 0,
            "provider_fill_rows": 0,
        }

    def trading_days(self):
        return DATES

    def index_rows(self):
        for definition in DOMESTIC_INDEX_DEFINITIONS:
            for ordinal, day in enumerate(DATES):
                yield {
                    "ts_code": definition.daily_code,
                    "trade_date": day,
                    "open": 100.0 + ordinal,
                    "high": 101.0 + ordinal,
                    "low": 99.0 + ordinal,
                    "close": 100.5 + ordinal,
                    "pre_close": 99.5 + ordinal,
                    # Only return changes. Daily Qlib index fields remain reusable.
                    "pct_chg": 2.0,
                    "vol": 10.0 + ordinal,
                    "amount": 20.0 + ordinal,
                }

    def ordered_partitions(self, _component, _dataset, **_kwargs):
        return ()

    def source_partition_evidence(self, component):
        return _source_partition(component)

    def factor_partition_plan(self):
        return (
            {
                "partition_key": "part-1",
                "start": DATES[-1],
                "end": DATES[-1],
                "source_partition_key": "fixture-tail",
            },
        )

    def iter_factor_frames(
        self,
        dataset,
        _partition_key,
        *,
        start,
        end,
        max_rows,
        instruments=(),
    ):
        rows = _factor_source_rows(dataset, start=start, end=end)
        if instruments:
            requested = {str(value).upper() for value in instruments}
            rows = [row for row in rows if str(row["ts_code"]).upper() in requested]
        frame = pd.DataFrame.from_records(rows)
        for offset in range(0, len(frame), max_rows):
            yield frame.iloc[offset : offset + max_rows].copy()


def _factor_source_rows(dataset: str, *, start, end) -> list[dict]:
    rows: list[dict] = []
    for ordinal, day in enumerate(DATES, start=1):
        if not start <= day <= end:
            continue
        for stock_no, code in enumerate(STOCKS):
            base = ordinal * 100 + stock_no
            common = {"trade_date": day, "ts_code": code}
            if dataset == "daily_raw":
                rows.append(
                    {
                        **common,
                        "open_li": 10_000 + base,
                        "high_li": 10_100 + base,
                        "low_li": 9_900 + base,
                        "close_li": 10_050 + base,
                        "volume_hand": 100 + base,
                        "amount_li": 1_000_000 + base,
                    }
                )
            elif dataset == "adj_factor":
                rows.append({**common, "adj_factor": 1.0})
            elif dataset == "daily_basic":
                value = {
                    field: float(ordinal + position + 1)
                    for position, field in enumerate(FACTOR_SOURCE_SCHEMAS["daily_basic"])
                }
                rows.append({**common, **value})
            elif dataset == "moneyflow":
                value = {
                    field: float(ordinal + position + 1)
                    for position, field in enumerate(TUSHARE_MONEYFLOW_VOLUME_COLUMNS)
                }
                value.update(
                    {
                        field: float(ordinal + position + 101)
                        for position, field in enumerate(TUSHARE_MONEYFLOW_AMOUNT_COLUMNS)
                    }
                )
                rows.append({**common, **value})
            elif dataset in {"bak_basic", "cyq_perf", "margin_detail"}:
                rows.append(
                    {
                        **common,
                        **{
                            field: float(ordinal + position + 1)
                            for position, field in enumerate(FACTOR_SOURCE_SCHEMAS[dataset])
                        },
                    }
                )
            elif dataset == "sector_data":
                rows.append(
                    {
                        **common,
                        **{
                            field: (
                                (1 if stock_no == 0 else -1) if field == "l2_code_id" else float(ordinal + position + 1)
                            )
                            for position, field in enumerate(FACTOR_SOURCE_SCHEMAS[dataset])
                        },
                    }
                )
    return rows


def _canonical_daily_rows(factor_root: Path):
    frame = pd.read_hdf(factor_root / "daily_pv.h5", key="data")
    rows = frame.reset_index()
    rows.columns = ["datetime", "instrument", *rows.columns[2:]]
    for _ordinal, row in rows.sort_values(["instrument", "datetime"]).iterrows():
        timestamp = row["datetime"]
        code = row["instrument"]
        close = float(row["close"])
        yield {
            "datetime": timestamp.to_pydatetime(),
            "instrument": str(code),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": close,
            "volume": float(row["volume"]),
            "amount": float(row["amount"]),
            "factor": float(row["factor"]),
            "up_limit_price": close * 1.1,
            "down_limit_price": close * 0.9,
            "prev_close": close if timestamp.date() == DATES[0] else close - 1,
            "limit_up": 0.0,
            "limit_down": 0.0,
        }


def _daily_tail_rows():
    day = DATES[-1]
    ordinal = len(DATES)
    for stock_no, code in enumerate(STOCKS):
        base = ordinal * 100 + stock_no
        close = (10_050 + base) / 1000.0
        yield {
            "datetime": datetime.combine(day, datetime.min.time()),
            "instrument": code,
            "open": (10_000 + base) / 1000.0,
            "high": (10_100 + base) / 1000.0,
            "low": (9_900 + base) / 1000.0,
            "close": close,
            "volume": (100 + base) * 100.0,
            "amount": (1_000_000 + base) / 1000.0,
            "factor": 1.0,
            "up_limit_price": close * 1.1,
            "down_limit_price": close * 0.9,
            "prev_close": float(stock_no + 4),
            "limit_up": 0.0,
            "limit_down": 0.0,
        }


def _minute_tail_rows():
    timestamps = candidate_fixtures._minute_calendar()[-240:]
    for code in STOCKS:
        for local, timestamp in enumerate(timestamps, start=240):
            yield {
                "datetime": datetime.fromisoformat(timestamp),
                "instrument": code,
                **{field: float(local + position) for position, field in enumerate(candidate_fixtures.MINUTE_FIELDS)},
            }


def test_full_baseline_to_mixed_selective_stage_isolated_adopt_and_validate(
    tmp_path,
    dataset_profile,
    monkeypatch,
) -> None:
    (tmp_path / "baseline-fixture").mkdir()
    baseline, factor_receipt, index_receipt, minute_source = _candidate(tmp_path / "baseline-fixture", dataset_profile)
    (baseline / "minute_bin" / "materialization_receipt.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "dataset": "minute_bin",
                "sealed_canonical_rows": minute_source,
            }
        ),
        encoding="utf-8",
    )
    for dataset in ("daily_bin", "minute_bin"):
        (baseline / dataset / "csv_preparation_receipt.json").write_text(
            json.dumps(
                {
                    "schema_version": "fixture_baseline_csv_preparation_v1",
                    "dataset": dataset,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
    baseline_index_receipt = build_stage._existing_index_receipt(baseline)
    baseline_report = CandidateValidator().validate(
        _spec(
            baseline,
            factor_receipt,
            baseline_index_receipt,
            minute_source,
            dataset_profile,
        )
    )
    assert baseline_report.payload["status"] == "PASS"

    control = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(control.root)
    pit = _pit()
    qfq = build_qfq_denominator_authority(
        [{"ts_code": code, "trade_date": day, "adj_factor": 1.0} for code in STOCKS for day in DATES],
        pit_snapshot=pit,
        cutoff=pit.cutoff,
    )
    baseline_root = tmp_path / "candidates"
    baseline_root.mkdir()
    published_baseline = baseline_root / "baseline-release"
    baseline.rename(published_baseline)
    baseline_artifact_root = artifact_tree_digest(published_baseline)
    baseline_manifest_ref = produce_component_artifact_manifest(
        cas,
        candidate_root=published_baseline,
        profile=dataset_profile,
        scope="full",
        cutoff=pit.cutoff,
        candidate_identity="3" * 64,
        artifact_root=baseline_artifact_root,
        producer_fingerprint="4" * 64,
        artifact_fingerprint="5" * 64,
        validation_fingerprint="6" * 64,
        source_content_root="7" * 64,
        artifact_ready_content_root="a" * 64,
        pit_snapshot=pit,
        source_partitions={component: _source_partition(component) for component in Component},
        qfq_authority=qfq,
    )
    baseline_manifest = load_component_artifact_manifest(cas, baseline_manifest_ref)
    actions = []
    action_by_component = {
        Component.DAILY_BIN: ComponentAction.FULL_REBUILD,
        Component.MINUTE_BIN: ComponentAction.REUSE,
        Component.FACTOR_H5_STATIC: ComponentAction.REUSE,
        Component.DOMESTIC_INDEX_CONTEXT: ComponentAction.SELECTIVE_REBUILD,
    }
    for component, action in action_by_component.items():
        actions.append(
            {
                "component": component.value,
                "partition_key": "all",
                "action": action.value,
                "reason": "fixture",
                "changed_fingerprints": [],
                "invalidation_edges": [],
                "estimated_work": {},
                "frozen_reuse": (
                    None
                    if action is ComponentAction.FULL_REBUILD
                    else _frozen(baseline_manifest.component(component), action=action)
                ),
            }
        )
    artifact_ready_ref = cas.put_json({"fixture": "artifact-ready"})
    build_inputs = {
        "profile": dataset_profile.profile,
        "scope": "full",
        "cutoff": pit.cutoff.isoformat(),
        "semantic_profile_digest": dataset_profile.semantic_profile_digest,
        "artifact_ready_content_root": "a" * 64,
        "artifact_ready_contract_ref": artifact_ready_ref.as_dict(),
        "artifact_ready_provenance_root": "b" * 64,
        "require_production_consumer_smoke": False,
        "artifact_ready_effective_partitions": {
            component.value: _source_partition(component) for component in Component
        },
        "source_snapshot": {
            "raw_source_content_root": "7" * 64,
            "pit_snapshot_digest": pit.spans_sha256,
        },
        "baseline": _baseline_build_input(
            baseline_manifest,
            baseline_manifest_ref,
            baseline_artifact_root,
            actions,
        ),
        "fingerprints": {
            "producer_fingerprint": "4" * 64,
            "artifact_fingerprint": "5" * 64,
            "validation_fingerprint": "6" * 64,
        },
    }
    profile = replace(
        dataset_profile,
        candidate_root=PureWindowsPath(str(baseline_root)),
        control_root=PureWindowsPath(str(control.root)),
    )
    plan = {
        "actions": actions,
        "action_plan_digest": digest_named_fields(
            DECISION_SCHEMA_VERSION,
            {
                "actions": sorted(
                    actions,
                    key=lambda value: (
                        value["component"],
                        value["partition_key"],
                    ),
                )
            },
        ),
        "build_inputs": build_inputs,
    }
    source = _BuildSource(qfq)
    monkeypatch.setattr(build_stage, "_build_source", lambda _invocation: (source, pit))
    monkeypatch.setattr(
        build_stage.CanonicalStockTransformer,
        "transform_daily",
        lambda _self, _spec, **_kwargs: _canonical_daily_rows(published_baseline / "factor_bundle"),
    )
    staging = baseline_root / ".staging" / "mixed-release"
    staging.parent.mkdir()

    def invocation(stage: str, prerequisites=None):
        return BuildStageInvocation(
            stage=stage,
            run_id="run-fixture",
            attempt_id="attempt-fixture",
            attempt_fence=1,
            pressure_rung=1,
            stage_timeout_seconds=profile.stage_timeouts_seconds["full_build"],
            release_id="mixed-release",
            release_digest="8" * 64,
            staging_relative_path=".staging/mixed-release",
            project_root=Path(__file__).resolve().parents[3],
            candidate_root=baseline_root,
            staging_root=staging,
            profile=profile,
            cas=cas,
            plan=plan,
            prerequisites=dict(prerequisites or {}),
        )

    baseline_before = {
        path.relative_to(published_baseline).as_posix(): sha256_file(path)
        for path in published_baseline.rglob("*")
        if path.is_file()
    }
    prepare = run_build_stage(invocation("prepare"))
    assert [item["dataset"] for item in prepare["qlib_dump_operations"]] == ["daily_bin"]
    assert prepare["qlib_dump_operations"][0]["mode"] == "batched_full"
    private_qlib = staging / "daily_bin" / ".writer-private" / "daily" / "qlib"
    seeded_calendar = (private_qlib / "calendars" / "day.txt").read_text(encoding="utf-8")
    assert seeded_calendar.splitlines() == [value.isoformat() for value in DATES]
    shutil.rmtree(private_qlib)
    generated = tmp_path / "controlled-full" / "qlib"
    _daily_bin(
        generated,
        staging / "factor_bundle",
        staging / "index_context",
        profile.index_codes,
    )
    shutil.copytree(generated, private_qlib)
    batch_root = private_qlib.parent / "csv"
    manifest = json.loads((batch_root / "batch_manifest.json").read_text(encoding="utf-8"))
    assert all(batch["mode"] == "dump_fix" for phase in manifest["phases"] for batch in phase["batches"])
    assert manifest["max_codes_per_batch"] == profile.pressure_ladder["minute_batch"][1]
    (batch_root / "batched_dump_receipt.json").write_text(
        json.dumps(
            _synthetic_batched_dump_receipt(manifest),
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    prepare_ref = cas.put_json(prepare)
    child_ref = cas.put_json({"returncode": 0, "active_processes": 0, "runtime": "wsl"})
    finalized = run_build_stage(
        invocation(
            "finalize-bins",
            {"prepare": prepare_ref.sha256, "qlib_dump_daily": child_ref.sha256},
        )
    )
    finalize_receipt = cas.get_json(finalized["finalize_receipt_ref"])
    for operation_id, dataset in (
        ("daily", "daily_bin"),
        ("minute", "minute_bin"),
    ):
        materialization_authority = cas.get_json(finalize_receipt[f"{operation_id}_materialization_receipt_file_ref"])
        preparation_authority = cas.get_json(finalize_receipt[f"{operation_id}_preparation_receipt_ref"])
        assert materialization_authority == json.loads(
            (staging / dataset / "materialization_receipt.json").read_text(encoding="utf-8")
        )
        assert preparation_authority == json.loads(
            (staging / dataset / "csv_preparation_receipt.json").read_text(encoding="utf-8")
        )
    finalized_ref = cas.put_json(finalized)
    spec = _spec(
        staging,
        factor_receipt,
        {"rows": 24},
        minute_source,
        profile,
    )
    consumer_ref = cas.put_json(spec.external_consumer_smoke)
    validated = run_build_stage(
        invocation(
            "validate",
            {
                "prepare": prepare_ref.sha256,
                "finalize_bins": finalized_ref.sha256,
                "consumer_smoke": consumer_ref.sha256,
            },
        )
    )

    baseline_after = {
        path.relative_to(published_baseline).as_posix(): sha256_file(path)
        for path in published_baseline.rglob("*")
        if path.is_file()
    }
    assert validated["validation_status"] == "PASS"
    assert validated["required_validation_failures"] == 0
    assert baseline_before == baseline_after
    assert not (staging / ".isolated-patches").exists()
    assert (
        load_component_artifact_manifest(cas, validated["component_artifact_manifest_ref"]).artifact_root
        == validated["artifact_root"]
    )
    prepare_receipt = cas.get_json(prepare["prepare_receipt_ref"])
    adoption = cas.get_json(prepare_receipt["refs"]["reuse_domestic_index_context"])
    assert adoption["action"] == "SELECTIVE_REBUILD"
    assert adoption["writer_target_manifest"]["target_path_count"] > 0
    assert not (baseline_root / "mixed-release").exists()


def test_monthly_incremental_direct_stage_updates_all_data_components_without_publish(
    tmp_path,
    dataset_profile,
    monkeypatch,
) -> None:
    baseline_dates = (date(2026, 7, 30),)
    target_dates = (date(2026, 7, 30), date(2026, 8, 31))
    monkeypatch.setattr(candidate_fixtures, "DATES", baseline_dates)
    (tmp_path / "baseline-fixture").mkdir()
    baseline, _baseline_factor, _baseline_index, baseline_minute = _candidate(
        tmp_path / "baseline-fixture", dataset_profile
    )
    (baseline / "minute_bin" / "materialization_receipt.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "dataset": "minute_bin",
                "sealed_canonical_rows": baseline_minute,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    for dataset in ("daily_bin", "minute_bin"):
        (baseline / dataset / "csv_preparation_receipt.json").write_text(
            json.dumps(
                {
                    "schema_version": "fixture_monthly_csv_preparation_v1",
                    "dataset": dataset,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
    baseline_pit = _pit()
    baseline_qfq = build_qfq_denominator_authority(
        [{"ts_code": code, "trade_date": baseline_dates[0], "adj_factor": 1.0} for code in STOCKS],
        pit_snapshot=baseline_pit,
        cutoff=baseline_pit.cutoff,
    )
    control = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(control.root)
    candidate_root = tmp_path / "candidates"
    candidate_root.mkdir()
    published_baseline = candidate_root / "baseline-release"
    baseline.rename(published_baseline)
    baseline_artifact_root = artifact_tree_digest(published_baseline)
    baseline_manifest_ref = produce_component_artifact_manifest(
        cas,
        candidate_root=published_baseline,
        profile=dataset_profile,
        scope="full",
        cutoff=baseline_pit.cutoff,
        candidate_identity="3" * 64,
        artifact_root=baseline_artifact_root,
        producer_fingerprint="4" * 64,
        artifact_fingerprint="5" * 64,
        validation_fingerprint="6" * 64,
        source_content_root="7" * 64,
        artifact_ready_content_root="a" * 64,
        pit_snapshot=baseline_pit,
        source_partitions={component: _source_partition(component) for component in Component},
        qfq_authority=baseline_qfq,
    )
    baseline_manifest = load_component_artifact_manifest(cas, baseline_manifest_ref)

    monkeypatch.setattr(candidate_fixtures, "DATES", target_dates)
    monkeypatch.setitem(globals(), "DATES", target_dates)
    target_pit = _pit()
    target_qfq = build_qfq_denominator_authority(
        [{"ts_code": code, "trade_date": day, "adj_factor": 1.0} for code in STOCKS for day in target_dates],
        pit_snapshot=target_pit,
        cutoff=target_pit.cutoff,
    )

    def bin_frozen(component: Component, *, include_indices: bool) -> dict:
        evidence = baseline_manifest.component(component)
        requested = (*STOCKS, *dataset_profile.index_codes) if include_indices else STOCKS
        replace_targets, _created = evidence.append_rules[0].targets_for_instruments(requested)
        replace_targets = tuple(path for path in replace_targets if not path.startswith("csv/"))
        key = "202608"
        create_targets = tuple(
            [f"csv_deltas/{key}/{code.casefold()}.csv" for code in requested] + [f"csv_deltas/{key}/manifest.json"]
        )
        baseline_receipt = json.loads(
            (published_baseline / component.value / "materialization_receipt.json").read_text(encoding="utf-8")
        )
        baseline_lineage = baseline_receipt["sealed_canonical_rows"]
        mutation_identity = digest_named_fields(
            "test_monthly_lineage_mutation_v1",
            {"component": component.value, "cutoff": target_pit.cutoff},
        )
        baseline_identity = baseline_lineage.get("lineage_root", evidence.component_manifest_root)
        event_key = lineage_event_key(
            dataset=component.value,
            cutoff=target_pit.cutoff.isoformat(),
            action=ComponentAction.INCREMENTAL.value,
            baseline_identity=baseline_identity,
            mutation_identity=mutation_identity,
        )
        lineage_targets = planned_lineage_paths(
            event_key=event_key,
            instruments=requested,
        )
        anchor_key = None
        if "lineage_root" not in baseline_lineage:
            anchor_identity = {
                "source_release_id": "baseline-release",
                "source_release_digest": "1" * 64,
                "component_file_identity": evidence.file_identity,
                "component_manifest_root": evidence.component_manifest_root,
            }
            anchor_key = lineage_event_key(
                dataset=component.value,
                cutoff=baseline_manifest.cutoff.isoformat(),
                action="LEGACY_ANCHOR",
                baseline_identity=evidence.component_manifest_root,
                mutation_identity=digest_named_fields(
                    "dataset_release_canonical_lineage_anchor_key_v1",
                    anchor_identity,
                ),
            )
            lineage_targets = (
                *lineage_targets,
                *planned_lineage_paths(
                    event_key=anchor_key,
                    instruments=STOCKS,
                    anchor=True,
                ),
            )
        frozen = _bounded_frozen(
            evidence,
            replace_targets=replace_targets,
            create_targets=(*create_targets, *lineage_targets),
            scopes=(
                {
                    "kind": "monthly_tail_extension",
                    "source_partition": "fixture-tail",
                    "new_months": ["2026-08"],
                },
            ),
        )
        frozen["canonical_lineage"] = {
            "capability": CANONICAL_LINEAGE_CAPABILITY,
            "baseline_schema_version": (
                CANONICAL_LINEAGE_SCHEMA if "lineage_root" in baseline_lineage else "legacy_v1_or_composite_v1"
            ),
            "baseline_lineage_root": baseline_lineage.get("lineage_root"),
            "event_key": event_key,
            "mutation_identity": mutation_identity,
            "planned_buckets": sorted({lineage_bucket(code) for code in requested}),
            "anchor_key": anchor_key,
        }
        return frozen

    factor_evidence = baseline_manifest.component(Component.FACTOR_H5_STATIC)
    factor_replace, _unused = factor_evidence.append_rules[0].targets_for_instruments(())
    factor_create = tuple(f"partitions/{dataset}/part-1.parquet" for dataset in (*FACTOR_H5_DATASETS, STATIC_DATASET))
    index_evidence = baseline_manifest.component(Component.DOMESTIC_INDEX_CONTEXT)
    actions = []
    frozen_by_component = {
        Component.DAILY_BIN: bin_frozen(Component.DAILY_BIN, include_indices=True),
        Component.MINUTE_BIN: bin_frozen(Component.MINUTE_BIN, include_indices=False),
        Component.FACTOR_H5_STATIC: _bounded_frozen(
            factor_evidence,
            replace_targets=factor_replace,
            create_targets=factor_create,
            scopes=(
                {
                    "kind": "monthly_tail_extension",
                    "source_partition": "fixture-tail",
                    "new_months": ["part-1"],
                },
            ),
        ),
        Component.DOMESTIC_INDEX_CONTEXT: _bounded_frozen(
            index_evidence,
            replace_targets=tuple(
                path.relative_to(published_baseline / "index_context").as_posix().casefold()
                for path in (published_baseline / "index_context").rglob("*")
                if path.is_file()
            ),
            create_targets=(),
            scopes=(
                {
                    "kind": "monthly_tail_extension",
                    "source_partition": "fixture-index-tail",
                    "new_months": ["2026-08"],
                },
            ),
        ),
    }
    for component in Component:
        actions.append(
            {
                "component": component.value,
                "partition_key": "all",
                "action": ComponentAction.INCREMENTAL.value,
                "reason": "direct monthly fixture",
                "changed_fingerprints": ["source_input_digest"],
                "invalidation_edges": [],
                "estimated_work": {},
                "frozen_reuse": frozen_by_component[component],
            }
        )
    artifact_ready_ref = cas.put_json({"fixture": "artifact-ready"})
    profile = replace(
        dataset_profile,
        candidate_root=PureWindowsPath(str(candidate_root)),
        control_root=PureWindowsPath(str(control.root)),
    )
    plan = {
        "actions": actions,
        "action_plan_digest": digest_named_fields(
            DECISION_SCHEMA_VERSION,
            {
                "actions": sorted(
                    actions,
                    key=lambda value: (
                        value["component"],
                        value["partition_key"],
                    ),
                )
            },
        ),
        "build_inputs": {
            "profile": profile.profile,
            "scope": "full",
            "cutoff": target_pit.cutoff.isoformat(),
            "semantic_profile_digest": profile.semantic_profile_digest,
            "artifact_ready_content_root": "a" * 64,
            "artifact_ready_contract_ref": artifact_ready_ref.as_dict(),
            "artifact_ready_provenance_root": "b" * 64,
            "require_production_consumer_smoke": False,
            "artifact_ready_effective_partitions": {
                component.value: _source_partition(component) for component in Component
            },
            "source_snapshot": {
                "raw_source_content_root": "7" * 64,
                "pit_snapshot_digest": target_pit.spans_sha256,
            },
            "baseline": _baseline_build_input(
                baseline_manifest,
                baseline_manifest_ref,
                baseline_artifact_root,
                actions,
            ),
            "fingerprints": {
                "producer_fingerprint": "4" * 64,
                "artifact_fingerprint": "5" * 64,
                "validation_fingerprint": "6" * 64,
            },
        },
    }
    source = _BuildSource(target_qfq)
    monkeypatch.setattr(build_stage, "_build_source", lambda _invocation: (source, target_pit))
    monkeypatch.setattr(
        build_stage.CanonicalStockTransformer,
        "transform_daily",
        lambda _self, _spec, **_kwargs: _daily_tail_rows(),
    )
    monkeypatch.setattr(
        build_stage.CanonicalStockTransformer,
        "transform_minute",
        lambda _self, _spec, **_kwargs: _minute_tail_rows(),
    )
    staging = candidate_root / ".staging" / "monthly-release"
    staging.parent.mkdir()

    def invocation(stage: str, prerequisites=None):
        return BuildStageInvocation(
            stage=stage,
            run_id="run-monthly",
            attempt_id="attempt-monthly",
            attempt_fence=1,
            pressure_rung=0,
            stage_timeout_seconds=profile.stage_timeouts_seconds["full_build"],
            release_id="monthly-release",
            release_digest="8" * 64,
            staging_relative_path=".staging/monthly-release",
            project_root=Path(__file__).resolve().parents[3],
            candidate_root=candidate_root,
            staging_root=staging,
            profile=profile,
            cas=cas,
            plan=plan,
            prerequisites=dict(prerequisites or {}),
        )

    build_stage._validate_build_transition_preflight(invocation("prepare"))
    bad_candidate = copy.deepcopy(plan)
    bad_candidate["build_inputs"]["baseline"]["candidate_identity"] = "9" * 64
    with pytest.raises(
        build_stage.CandidateBuildStageError,
        match="catalog identity differs",
    ):
        build_stage._validate_build_transition_preflight(replace(invocation("prepare"), plan=bad_candidate))
    bad_reuse = copy.deepcopy(plan)
    bad_reuse["build_inputs"]["baseline"]["reuse_evidence"] = []
    with pytest.raises(
        build_stage.CandidateBuildStageError,
        match="reuse evidence differs",
    ):
        build_stage._validate_build_transition_preflight(replace(invocation("prepare"), plan=bad_reuse))
    bad_release = copy.deepcopy(plan)
    bad_release["actions"][0]["frozen_reuse"]["source_release_id"] = "other-release"
    bad_release["build_inputs"]["baseline"]["reuse_evidence"] = [dict(item) for item in bad_release["actions"]]
    bad_release["action_plan_digest"] = digest_named_fields(
        DECISION_SCHEMA_VERSION,
        {
            "actions": sorted(
                bad_release["actions"],
                key=lambda value: (
                    value["component"],
                    value["partition_key"],
                ),
            )
        },
    )
    with pytest.raises(
        build_stage.CandidateBuildStageError,
        match="release/partition identity differs",
    ):
        build_stage._validate_build_transition_preflight(replace(invocation("prepare"), plan=bad_release))
    bad_fingerprint = copy.deepcopy(plan)
    bad_fingerprint["build_inputs"]["fingerprints"]["producer_fingerprint"] = "9" * 64
    with pytest.raises(
        build_stage.CandidateBuildStageError,
        match="fingerprints differ",
    ):
        build_stage._validate_build_transition_preflight(replace(invocation("prepare"), plan=bad_fingerprint))

    baseline_before = artifact_tree_digest(published_baseline)
    baseline_tree_reads = {component: 0 for component in Component}
    component_roots = {
        (published_baseline / build_stage._COMPONENT_ROOT[component]).resolve(): component for component in Component
    }
    original_tree_merkle = build_stage.tree_merkle

    def counted_tree_merkle(root):
        resolved = Path(root).resolve()
        component = component_roots.get(resolved)
        if component is not None:
            baseline_tree_reads[component] += 1
        return original_tree_merkle(root)

    monkeypatch.setattr(build_stage, "tree_merkle", counted_tree_merkle)
    prepare = run_build_stage(invocation("prepare"))
    # COW/clone performs the one pre-writer content verification internally;
    # build_stage does not immediately hash the same baseline again.  Factor
    # retains one post-writer boundary hash, while index keeps explicit pre/
    # post hashes because its trusted patch is produced before COW setup.
    assert baseline_tree_reads == {
        Component.DAILY_BIN: 0,
        Component.MINUTE_BIN: 0,
        Component.FACTOR_H5_STATIC: 1,
        Component.DOMESTIC_INDEX_CONTEXT: 2,
    }
    assert {item["mode"] for item in prepare["qlib_dump_operations"]} == {"batched_patch"}
    assert all(
        item["batch_manifest_identity"] and item["batch_manifest_sha256"] for item in prepare["qlib_dump_operations"]
    )
    prepare_receipt = cas.get_json(prepare["prepare_receipt_ref"])
    factor_adoption = cas.get_json(prepare_receipt["refs"]["reuse_factor_h5_static"])
    deferred = factor_adoption["adoption"]["deferred_aggregates"]
    assert deferred["baseline_copy_count"] == 0
    assert deferred["final_recopy_count"] == 0
    assert deferred["adopted"]
    for operation_id in ("daily", "minute"):
        bin_preparation = cas.get_json(prepare_receipt["refs"][f"{operation_id}_preparation"])
        phase_metrics = next(iter(bin_preparation["transform_metrics"].values()))
        assert phase_metrics["source_merges"]
        assert all(item["full_frames_materialized"] == 0 for item in phase_metrics["source_merges"].values())
    scratch = tmp_path / "controlled-full"
    daily_scratch = scratch / "daily" / "qlib"
    minute_scratch = scratch / "minute" / "qlib"
    _daily_bin(
        daily_scratch,
        staging / "factor_bundle",
        staging / "index_context",
        profile.index_codes,
    )
    _minute_bin(minute_scratch)
    for operation_id, generated in (
        ("daily", daily_scratch),
        ("minute", minute_scratch),
    ):
        private = staging / f"{operation_id}_bin" / ".writer-private" / operation_id
        qlib = private / "qlib"
        shutil.rmtree(qlib)
        shutil.copytree(generated, qlib)
        manifest = json.loads((private / "csv" / "batch_manifest.json").read_text(encoding="utf-8"))
        (private / "csv" / "batched_dump_receipt.json").write_text(
            json.dumps(
                _synthetic_batched_dump_receipt(manifest),
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
    prepare_ref = cas.put_json(prepare)
    child = cas.put_json({"returncode": 0, "active_processes": 0, "runtime": "wsl"})
    finalized = run_build_stage(
        invocation(
            "finalize-bins",
            {
                "prepare": prepare_ref.sha256,
                "qlib_dump_daily": child.sha256,
                "qlib_dump_minute": child.sha256,
            },
        )
    )
    finalized_ref = cas.put_json(finalized)
    factor_receipt = json.loads((staging / "factor_bundle" / "factor_checkpoint.json").read_text(encoding="utf-8"))
    minute_source = json.loads((staging / "minute_bin" / "materialization_receipt.json").read_text(encoding="utf-8"))[
        "sealed_canonical_rows"
    ]
    consumer = _spec(
        staging,
        factor_receipt,
        {"rows": len(profile.index_codes) * len(target_dates)},
        minute_source,
        profile,
    ).external_consumer_smoke
    consumer_ref = cas.put_json(consumer)
    content_hash_reads = 0
    staging_tree_merkle_reads = 0
    original_hash = component_producer._hash_file_once
    original_tree_merkle = candidate_validator_module.tree_merkle

    def counted_hash(path):
        nonlocal content_hash_reads
        content_hash_reads += 1
        return original_hash(path)

    def counted_tree_merkle(path):
        nonlocal staging_tree_merkle_reads
        resolved = Path(path).resolve(strict=True)
        if resolved == staging or staging in resolved.parents:
            staging_tree_merkle_reads += 1
        return original_tree_merkle(path)

    monkeypatch.setattr(component_producer, "_hash_file_once", counted_hash)
    monkeypatch.setattr(
        candidate_validator_module,
        "tree_merkle",
        counted_tree_merkle,
    )
    validated = run_build_stage(
        invocation(
            "validate",
            {
                "prepare": prepare_ref.sha256,
                "finalize_bins": finalized_ref.sha256,
                "consumer_smoke": consumer_ref.sha256,
            },
        )
    )

    assert validated["validation_status"] == "PASS"
    assert content_hash_reads == validated["artifact_snapshot"]["file_count"]
    assert validated["artifact_snapshot"]["content_read_passes"] == 1
    assert staging_tree_merkle_reads == 0
    assert artifact_tree_digest(published_baseline) == baseline_before
    assert not (candidate_root / "monthly-release").exists()
    for dataset in ("daily_bin", "minute_bin"):
        receipt = json.loads((staging / dataset / "materialization_receipt.json").read_text(encoding="utf-8"))
        assert receipt["adoption"]["baseline_qlib_copy_count"] == 1
        assert receipt["adoption"]["final_qlib_recopy_count"] == 0
        sealed = receipt["sealed_canonical_rows"]
        assert sealed["schema_version"] == CANONICAL_LINEAGE_SCHEMA
        assert "segments" not in sealed
        assert "legacy_anchor" in sealed
        assert {item["end"][:10] for item in receipt["csv"]["instrument_summaries"]} == {target_dates[-1].isoformat()}
        assert (staging / dataset / "csv_lineage").is_dir()
