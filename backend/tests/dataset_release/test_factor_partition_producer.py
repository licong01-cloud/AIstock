from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from backend.data_service.moneyflow_contract import (
    TUSHARE_MONEYFLOW_AMOUNT_COLUMNS,
    TUSHARE_MONEYFLOW_VOLUME_COLUMNS,
)
from backend.services.dataset_release.factor_materializer import (
    FACTOR_SOURCE_SCHEMAS,
    FactorBundleMaterializer,
    FactorMaterializationError,
    FactorMaterializationSpec,
    FactorPartitionProducer,
    FactorPartitionProducerSpec,
    FactorSourcePartition,
    QfqDailyTransformMetrics,
    _build_qfq_daily,
    merge_factor_partition_by_instrument,
    restore_rolling_factor_state_from_produced_partition,
)
from backend.services.dataset_release.canonical_stock_transformer import (
    build_qfq_denominator_authority,
)
from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.services.dataset_release.static_schema import STATIC_ORDERED_COLUMNS
from backend.services.dataset_release.streaming_artifacts import iter_parquet_frames


STOCKS = ("000001.SZ", "600000.SH")
PARTITIONS = (
    FactorSourcePartition("2026-06", date(2026, 6, 1), date(2026, 6, 16)),
    FactorSourcePartition("2026-07", date(2026, 6, 17), date(2026, 7, 2)),
)


def _dates(partition: FactorSourcePartition):
    return pd.bdate_range(partition.start, partition.end).date


class FixtureReader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.frames: dict[tuple[str, str], pd.DataFrame] = {}
        ordinal = 0
        for partition in PARTITIONS:
            daily_rows = []
            adj_rows = []
            aux = {name: [] for name in partition.datasets if name not in {"daily_raw", "adj_factor"}}
            for day in _dates(partition):
                ordinal += 1
                for stock_no, code in enumerate(STOCKS):
                    base = ordinal * 100 + stock_no
                    daily_rows.append(
                        {
                            "trade_date": day,
                            "ts_code": code,
                            "open_li": 10_000 + base,
                            "high_li": 10_100 + base,
                            "low_li": 9_900 + base,
                            "close_li": 10_050 + base,
                            "volume_hand": 100 + base,
                            "amount_li": 1_000_000 + base,
                        }
                    )
                    adj_rows.append(
                        {
                            "trade_date": day,
                            "ts_code": code,
                            "adj_factor": float(ordinal + stock_no + 1),
                        }
                    )
                    daily_basic = {
                        field: float(ordinal + position + 1)
                        for position, field in enumerate(FACTOR_SOURCE_SCHEMAS["daily_basic"])
                    }
                    daily_basic.update(
                        {
                            "trade_date": day,
                            "ts_code": code,
                            "pe_ttm": 10.0 + stock_no,
                            "pb": 2.0,
                            "circ_mv": 1_000_000.0 + base,
                            "turnover_rate": 3.0,
                            "volume_ratio": 1.2,
                            "dv_ratio": 0.5,
                            "dv_ttm": 0.6,
                        }
                    )
                    aux["daily_basic"].append(daily_basic)
                    money = {"trade_date": day, "ts_code": code}
                    money.update(
                        {
                            name: float(ordinal + position + 1)
                            for position, name in enumerate(TUSHARE_MONEYFLOW_VOLUME_COLUMNS)
                        }
                    )
                    money.update(
                        {
                            name: float(ordinal + position + 101)
                            for position, name in enumerate(TUSHARE_MONEYFLOW_AMOUNT_COLUMNS)
                        }
                    )
                    aux["moneyflow"].append(money)
                    for dataset in ("bak_basic", "cyq_perf", "margin_detail"):
                        aux[dataset].append(
                            {
                                "trade_date": day,
                                "ts_code": code,
                                **{
                                    field: float(ordinal + position + 1)
                                    for position, field in enumerate(FACTOR_SOURCE_SCHEMAS[dataset])
                                },
                            }
                        )
                    aux["sector_data"].append(
                        {
                            "trade_date": day,
                            "ts_code": code,
                            **{
                                field: (
                                    (1 if stock_no == 0 else -1)
                                    if field == "l2_code_id"
                                    else float(ordinal + position + 1)
                                )
                                for position, field in enumerate(FACTOR_SOURCE_SCHEMAS["sector_data"])
                            },
                        }
                    )
            self.frames[("daily_raw", partition.partition_key)] = pd.DataFrame(daily_rows)
            self.frames[("adj_factor", partition.partition_key)] = pd.DataFrame(adj_rows)
            for dataset, rows in aux.items():
                self.frames[(dataset, partition.partition_key)] = pd.DataFrame(rows)

    def iter_frames(
        self,
        dataset: str,
        partition_key: str,
        *,
        start: date,
        end: date,
        max_rows: int,
    ):
        self.calls.append((dataset, partition_key))
        frame = self.frames[(dataset, partition_key)].copy()
        observed = pd.to_datetime(frame["trade_date"]).dt.date
        frame = frame.loc[(observed >= start) & (observed <= end)]
        for offset in range(0, len(frame), max_rows):
            yield frame.iloc[offset : offset + max_rows].copy()


def _static_columns() -> tuple[str, ...]:
    return STATIC_ORDERED_COLUMNS


def _pit():
    return freeze_pit_snapshot(
        [
            {
                "ts_code": code,
                "eligible_start": PARTITIONS[0].start,
                "eligible_end": PARTITIONS[-1].end,
                "entry_reason": None,
                "exit_reason": None,
            }
            for code in STOCKS
        ],
        universe_key="shsz_st_pit_active_v1",
        rule_version="st_pub_next_trade_restore_active_l_v1",
        scope_start=PARTITIONS[0].start,
        cutoff=PARTITIONS[-1].end,
        state_identity="fixture",
        source_fingerprint_sha256="a" * 64,
        parameter_hash="b" * 64,
    )


def _spec(root: Path, *, max_rows: int = 250_000) -> FactorPartitionProducerSpec:
    pit = _pit()
    fixture = FixtureReader()
    adj = pd.concat(
        [fixture.frames[("adj_factor", partition.partition_key)] for partition in PARTITIONS],
        ignore_index=True,
    ).sort_values(["ts_code", "trade_date"], kind="mergesort")
    return FactorPartitionProducerSpec(
        output_root=root,
        partitions=PARTITIONS,
        pit_snapshot=pit,
        qfq_denominator_authority=build_qfq_denominator_authority(
            adj.to_dict(orient="records"),
            pit_snapshot=pit,
            cutoff=pit.cutoff,
        ),
        static_ordered_columns=_static_columns(),
        row_group_rows=10,
        max_source_partition_rows=max_rows,
        qfq_source_summary={
            "source_precedence": "db_then_tushare_missing_keys_conflict_fail_v1",
            "overlap_mismatch_cells": 0,
            "provider_fill_rows": 0,
        },
        overlay_summary={
            "source_precedence": "database_then_provider_missing_keys_conflict_fail_v1",
            "overlap_mismatch_cells": 0,
            "provider_override_rows": 0,
            "provider_fill_rows": 0,
        },
    )


def test_factor_partition_producer_is_qfq_global_rolling_and_resumable(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    reader = FixtureReader()
    producer = FactorPartitionProducer()

    first = producer.produce(_spec(staging), reader=reader)
    calls_after_first = len(reader.calls)
    second = producer.produce(_spec(staging), reader=reader)

    assert first.receipt["status"] == "PASS"
    assert first.receipt["memory_contract"]["whole_history_frames_retained"] == 0
    assert len(first.chunks) == len(PARTITIONS) * 8
    assert first.chunks == second.chunks
    # A resume only re-scans sealed adj-factor partitions to re-establish the
    # global QFQ denominator; it does not reload any heavy component partition.
    assert reader.calls[calls_after_first:] == [("adj_factor", partition.partition_key) for partition in PARTITIONS]

    first_daily = pd.read_parquet(first.source_root / PARTITIONS[0].partition_key / "daily_pv.parquet")
    last_daily = pd.read_parquet(first.source_root / PARTITIONS[-1].partition_key / "daily_pv.parquet")
    assert first_daily["factor"].max() < 1.0
    assert last_daily.groupby(level="instrument")["factor"].max().tolist() == pytest.approx([1.0, 1.0])

    last_static = pd.read_parquet(first.source_root / PARTITIONS[-1].partition_key / "static_factors.parquet")
    assert last_static["PriceStrength_10D"].notna().all()
    assert last_static.groupby(level="instrument")["mf_total_net_amt_20d"].tail(1).notna().all()
    assert str(last_static["l2_code_id"].dtype) == "int16"
    assert (first.source_root / PARTITIONS[-1].partition_key / "state" / "price_tail.parquet").is_file()

    bundle = FactorBundleMaterializer().materialize(
        FactorMaterializationSpec(
            source_root=first.source_root,
            staging_root=staging,
            chunks=first.chunks,
            static_ordered_columns=_static_columns(),
            row_group_rows=10,
        )
    )
    assert bundle.receipt["status"] == "PASS"
    assert pd.read_hdf(staging / "factor_bundle" / "daily_pv.h5", key="data").shape[0] == 48


def test_month_end_single_code_revision_carries_state_and_matches_clean_full(
    tmp_path: Path,
) -> None:
    baseline_reader = FixtureReader()
    revised_reader = FixtureReader()
    affected = STOCKS[0]
    last_day = max(_dates(PARTITIONS[0]))
    daily_key = ("daily_raw", PARTITIONS[0].partition_key)
    money_key = ("moneyflow", PARTITIONS[0].partition_key)
    daily_mask = (revised_reader.frames[daily_key]["ts_code"] == affected) & (
        revised_reader.frames[daily_key]["trade_date"] == last_day
    )
    money_mask = (revised_reader.frames[money_key]["ts_code"] == affected) & (
        revised_reader.frames[money_key]["trade_date"] == last_day
    )
    revised_reader.frames[daily_key].loc[daily_mask, "close_li"] += 500
    revised_reader.frames[money_key].loc[money_mask, "buy_sm_vol"] += 100

    baseline_root = tmp_path / "baseline"
    clean_root = tmp_path / "clean"
    baseline_root.mkdir()
    clean_root.mkdir()
    baseline = FactorPartitionProducer().produce(_spec(baseline_root), reader=baseline_reader)
    clean = FactorPartitionProducer().produce(_spec(clean_root), reader=revised_reader)

    class AffectedReader:
        def __init__(self, source: FixtureReader) -> None:
            self.source = source
            self.observed_codes: set[str] = set()

        def iter_frames(
            self,
            dataset: str,
            partition_key: str,
            *,
            start: date,
            end: date,
            max_rows: int,
        ):
            for frame in self.source.iter_frames(
                dataset,
                partition_key,
                start=start,
                end=end,
                max_rows=max_rows,
            ):
                selected = frame.loc[frame["ts_code"] == affected].copy()
                self.observed_codes.update(selected["ts_code"].astype(str))
                if not selected.empty:
                    yield selected

    filtered = AffectedReader(revised_reader)
    base_spec = _spec(tmp_path / "unused")
    rolling = None
    selective_chunks = {}
    for ordinal, partition in enumerate(PARTITIONS):
        run_root = tmp_path / f"selective-{ordinal}"
        run_root.mkdir()
        spec = replace(
            base_spec,
            output_root=run_root,
            partitions=(partition,),
            allow_partial_ranges=True,
            instrument_filter=(affected,),
        )
        produced = FactorPartitionProducer().produce(
            spec,
            reader=filtered,
            initial_state=rolling,
        )
        rolling = restore_rolling_factor_state_from_produced_partition(
            produced.source_root,
            partition_key=partition.partition_key,
            max_rows=spec.row_group_rows,
        )
        for chunk in produced.chunks:
            selective_chunks[(chunk.dataset, chunk.partition_key)] = produced.source_root / chunk.relative_path

    baseline_chunks = {
        (chunk.dataset, chunk.partition_key): baseline.source_root / chunk.relative_path for chunk in baseline.chunks
    }
    clean_chunks = {
        (chunk.dataset, chunk.partition_key): clean.source_root / chunk.relative_path for chunk in clean.chunks
    }
    assert filtered.observed_codes == {affected}
    for key, replacement in sorted(selective_chunks.items()):
        target = tmp_path / "merged" / key[0] / f"{key[1]}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        _chunk, receipt = merge_factor_partition_by_instrument(
            baseline_path=baseline_chunks[key],
            replacement_path=replacement,
            target_path=target,
            dataset=key[0],
            partition_key=key[1],
            affected_instruments=(affected,),
            row_group_rows=10,
            max_rows=1000,
        )
        merged = pd.concat(list(iter_parquet_frames([target], max_rows=10))).sort_index()
        expected = pd.concat(list(iter_parquet_frames([clean_chunks[key]], max_rows=10))).sort_index()
        pd.testing.assert_frame_equal(
            merged,
            expected,
            check_exact=False,
            rtol=2e-6,
            atol=1e-3,
        )
        assert receipt["affected_instruments"] == [affected]
        assert receipt["whole_market_history_frames_retained"] == 0


def test_factor_producer_rejects_unenriched_sector_source_without_stable_l2(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    reader = FixtureReader()
    key = ("sector_data", PARTITIONS[0].partition_key)
    reader.frames[key] = reader.frames[key].drop(columns=["l2_code_id"])

    with pytest.raises(FactorMaterializationError, match="canonical fields missing"):
        FactorPartitionProducer().produce(_spec(staging), reader=reader)


def test_factor_partition_producer_adopts_sealed_crash_window_without_deleting(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    reader = FixtureReader()
    producer = FactorPartitionProducer()
    result = producer.produce(_spec(staging), reader=reader)
    checkpoint = staging / "factor_source_chunks" / "producer_checkpoint.json"
    payload = json.loads(checkpoint.read_text(encoding="utf-8"))
    payload["completed_partitions"] = []
    payload["status"] = "IN_PROGRESS"
    checkpoint.write_text(json.dumps(payload), encoding="utf-8")
    sealed = result.source_root / PARTITIONS[0].partition_key / "daily_pv.parquet"
    before = sealed.read_bytes()

    replay = producer.produce(_spec(staging), reader=reader)

    assert replay.receipt["status"] == "PASS"
    assert sealed.read_bytes() == before


def test_factor_partition_producer_fails_before_unbounded_source_accumulation(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    reader = FixtureReader()
    with pytest.raises(FactorMaterializationError, match="exceeds row bound"):
        FactorPartitionProducer().produce(_spec(staging, max_rows=1), reader=reader)
    assert not any((staging / "factor_source_chunks" / "sealed").iterdir())


def test_factor_producer_rejects_qfq_authority_that_differs_from_sealed_adj_rows(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    reader = FixtureReader()
    pit = _pit()
    adj = pd.concat(
        [reader.frames[("adj_factor", partition.partition_key)] for partition in PARTITIONS],
        ignore_index=True,
    ).sort_values(["ts_code", "trade_date"], kind="mergesort")
    tampered = adj.copy()
    tampered.loc[tampered["ts_code"] == STOCKS[0], "adj_factor"] *= 2.0
    spec = replace(
        _spec(staging),
        qfq_denominator_authority=build_qfq_denominator_authority(
            tampered.to_dict(orient="records"),
            pit_snapshot=pit,
            cutoff=pit.cutoff,
        ),
    )

    with pytest.raises(FactorMaterializationError, match="differs from artifact-ready"):
        FactorPartitionProducer().produce(spec, reader=reader)


def test_qfq_daily_alignment_is_linear_in_rows_and_code_groups() -> None:
    codes = tuple(f"{value:06d}.SZ" for value in range(1, 1001))
    days = (date(2026, 7, 30), date(2026, 7, 31))
    daily = pd.DataFrame.from_records(
        {
            "ts_code": code,
            "trade_date": day,
            "open_li": 10_000,
            "high_li": 10_100,
            "low_li": 9_900,
            "close_li": 10_050,
            "volume_hand": 100,
            "amount_li": 1_000_000,
        }
        for code in codes
        for day in days
    )
    adj = pd.DataFrame.from_records(
        {
            "ts_code": code,
            "trade_date": day,
            "adj_factor": 1.0 if day == days[0] else 1.1,
        }
        for code in codes
        for day in days
    )
    metrics = QfqDailyTransformMetrics()

    output, tail = _build_qfq_daily(
        daily,
        adj,
        base_factors={code: 1.1 for code in codes},
        previous_adj_tail=pd.DataFrame(),
        metrics=metrics,
    )

    assert len(output) == len(codes) * len(days)
    assert len(tail) == len(codes)
    assert metrics.daily_rows == len(output)
    assert metrics.adj_rows == len(adj)
    assert metrics.daily_code_groups == len(codes)
    assert metrics.adj_code_groups == len(codes)
    assert metrics.adj_group_advances == len(codes)


def test_three_month_backing_partition_is_streamed_into_bounded_date_slices(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    base = FixtureReader()
    month_partitions = (
        FactorSourcePartition(
            "2026-05",
            date(2026, 5, 1),
            date(2026, 5, 31),
            source_partition_key="quarter-2026q2q3",
        ),
        FactorSourcePartition(
            "2026-06",
            date(2026, 6, 1),
            date(2026, 6, 30),
            source_partition_key="quarter-2026q2q3",
        ),
        FactorSourcePartition(
            "2026-07",
            date(2026, 7, 1),
            date(2026, 7, 31),
            source_partition_key="quarter-2026q2q3",
        ),
    )

    class SharedBackingReader:
        def __init__(self) -> None:
            self.max_yielded_rows = 0
            self.frames = {}
            for dataset in month_partitions[0].datasets:
                template = base.frames[(dataset, PARTITIONS[0].partition_key)]
                template_dates = sorted(pd.to_datetime(template["trade_date"]).dt.date.unique())
                pieces = []
                for partition in month_partitions:
                    target_dates = list(pd.bdate_range(partition.start, partition.end).date)
                    mapping = dict(zip(template_dates, target_dates[: len(template_dates)]))
                    piece = template.copy()
                    piece["trade_date"] = pd.to_datetime(piece["trade_date"]).dt.date.map(mapping)
                    pieces.append(piece)
                joined = pd.concat(pieces, ignore_index=True).sort_values(["trade_date", "ts_code"], kind="mergesort")
                self.frames[(dataset, "quarter-2026q2q3")] = joined

        def iter_frames(
            self,
            dataset: str,
            partition_key: str,
            *,
            start: date,
            end: date,
            max_rows: int,
        ):
            frame = self.frames[(dataset, partition_key)]
            observed = pd.to_datetime(frame["trade_date"]).dt.date
            selected = frame.loc[(observed >= start) & (observed <= end)]
            for offset in range(0, len(selected), max_rows):
                chunk = selected.iloc[offset : offset + max_rows].copy()
                self.max_yielded_rows = max(self.max_yielded_rows, len(chunk))
                yield chunk

    pit = freeze_pit_snapshot(
        [
            {
                "ts_code": code,
                "eligible_start": month_partitions[0].start,
                "eligible_end": month_partitions[-1].end,
                "entry_reason": None,
                "exit_reason": None,
            }
            for code in STOCKS
        ],
        universe_key="shsz_st_pit_active_v1",
        rule_version="st_pub_next_trade_restore_active_l_v1",
        scope_start=month_partitions[0].start,
        cutoff=month_partitions[-1].end,
        state_identity="fixture-pit-quarter",
        source_fingerprint_sha256="e" * 64,
        parameter_hash="f" * 64,
    )
    reader = SharedBackingReader()
    adj = reader.frames[("adj_factor", "quarter-2026q2q3")].sort_values(["ts_code", "trade_date"], kind="mergesort")
    spec = replace(
        _spec(staging, max_rows=30),
        partitions=month_partitions,
        pit_snapshot=pit,
        qfq_denominator_authority=build_qfq_denominator_authority(
            adj.to_dict(orient="records"),
            pit_snapshot=pit,
            cutoff=pit.cutoff,
        ),
    )
    total_backing_daily_rows = len(reader.frames[("daily_raw", "quarter-2026q2q3")])
    assert total_backing_daily_rows > spec.max_source_partition_rows

    result = FactorPartitionProducer().produce(spec, reader=reader)

    assert result.receipt["status"] == "PASS"
    assert reader.max_yielded_rows <= spec.row_group_rows
    memory = result.receipt["memory_contract"]
    assert memory["whole_source_partition_frames_retained"] == 0
    assert memory["peak_retained_source_rows"] <= memory["hard_retained_source_rows"]
