from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_modeling.dataset_spool import RerankerDatasetSpool
from backend.services.advisory_modeling.feature_builder import (
    FrozenCandidateFeatureInputV1,
    MultiAlphaLegInputV1,
    ShortReboundFeatureBuilderV1,
    StageCandidateInputV1,
    frozen_formula_registry_v1,
)
from backend.services.advisory_modeling.feature_schema import frozen_feature_schema_v1
from backend.services.advisory_modeling.feature_snapshot import (
    FeatureSourceRevisionV1,
    materialize_feature_snapshot,
)
from backend.services.advisory_modeling.feature_sources import frozen_feature_query_registry_v1
from backend.services.advisory_modeling.label_policy import RankingLabelPolicyV1
from backend.services.advisory_modeling.market_regime import MarketRegimePolicyTemplateV1
from backend.services.advisory_modeling.training_export import materialize_training_export
from backend.services.advisory_modeling.training_view import DatasetBuildIntentV1


_COMMIT = "f20cd062285230a1e24829afd7386203891a2897"
_SOURCE_IDENTITY = "7" * 64


def _spool(tmp_path: Path) -> RerankerDatasetSpool:
    artifact = tmp_path / "artifact"
    output = tmp_path / "output"
    artifact.mkdir()
    output.mkdir()
    return RerankerDatasetSpool(
        output_root=output,
        repository_root=Path(__file__).resolve().parents[3],
        artifact_root=artifact,
        operation_id="batch-b-test",
    )


def _append_sources(spool: RerankerDatasetSpool) -> tuple[date, tuple[date, ...]]:
    start = date(2026, 1, 1)
    dates = tuple(start + timedelta(days=index) for index in range(61))
    decision = dates[-1]
    symbols = ("000001.SZ", "000002.SZ")
    spool.append_partition(
        source_kind="FEATURE_SOURCE",
        source_identity=_SOURCE_IDENTITY,
        logical_role="historical_trading_calendar_window",
        partition_key="calendar",
        rows=({"cal_date": day, "is_trading": True} for day in dates),
        identity_fields=("cal_date",),
        trade_date_field="cal_date",
        symbol_field=None,
    )
    spool.append_partition(
        source_kind="FEATURE_SOURCE",
        source_identity=_SOURCE_IDENTITY,
        logical_role="historical_pit_universe_existing_readonly",
        partition_key="universe",
        rows=({"trade_date": decision, "ts_code": symbol} for symbol in symbols),
        identity_fields=("trade_date", "ts_code"),
        trade_date_field="trade_date",
        symbol_field="ts_code",
    )
    market_rows = []
    moneyflow_rows = []
    for day_index, day in enumerate(dates):
        for symbol_index, symbol in enumerate(symbols):
            close = Decimal(10000 + day_index * 10 + symbol_index * 100)
            market_rows.append(
                {
                    "trade_date": day,
                    "ts_code": symbol,
                    "open_li": close - 5,
                    "high_li": close + 20,
                    "low_li": close - 20,
                    "close_li": close,
                    "volume_hand": Decimal(1000 + day_index),
                    "amount_li": Decimal(10_000_000 + day_index * 1000),
                    "adj_factor": Decimal("1.2"),
                }
            )
            moneyflow_rows.append(
                {
                    "trade_date": day,
                    "ts_code": symbol,
                    "turnover_rate": Decimal("2.5") + Decimal(day_index) / Decimal(100),
                    "turnover_rate_f": Decimal("3.0"),
                    "net_mf_amount": Decimal("0.5") + Decimal(symbol_index) / Decimal(10),
                }
            )
    spool.append_partition(
        source_kind="FEATURE_SOURCE",
        source_identity=_SOURCE_IDENTITY,
        logical_role="historical_market_history_window",
        partition_key="market",
        rows=market_rows,
        identity_fields=("trade_date", "ts_code"),
        trade_date_field="trade_date",
        symbol_field="ts_code",
    )
    spool.append_partition(
        source_kind="FEATURE_SOURCE",
        source_identity=_SOURCE_IDENTITY,
        logical_role="historical_fundamental_moneyflow_window",
        partition_key="moneyflow",
        rows=moneyflow_rows,
        identity_fields=("trade_date", "ts_code"),
        trade_date_field="trade_date",
        symbol_field="ts_code",
    )
    spool.append_partition(
        source_kind="FEATURE_SOURCE",
        source_identity=_SOURCE_IDENTITY,
        logical_role="historical_decision_mark_daily_market",
        partition_key="limits",
        rows=(
            {
                "trade_date": decision,
                "ts_code": "000001.SZ",
                "close_li": Decimal("10600"),
                "adj_factor": Decimal("1.2"),
                "pre_close": Decimal("9.64"),
                "up_limit": Decimal("10.60"),
                "down_limit": Decimal("8.68"),
            },
            {
                "trade_date": decision,
                "ts_code": "000002.SZ",
                "close_li": Decimal("10700"),
                "adj_factor": Decimal("1.2"),
                "pre_close": Decimal("10.60"),
                "up_limit": Decimal("11.66"),
                "down_limit": Decimal("9.54"),
            },
        ),
        identity_fields=("trade_date", "ts_code"),
        trade_date_field="trade_date",
        symbol_field="ts_code",
    )
    spool.append_partition(
        source_kind="FEATURE_SOURCE",
        source_identity=_SOURCE_IDENTITY,
        logical_role="historical_industry_membership",
        partition_key="industry",
        rows=(
            {"ts_code": symbol, "l1_code": "10", "l2_code": "1010", "l3_code": "101010", "in_date": date(2020, 1, 1), "out_date": None}
            for symbol in symbols
        ),
        identity_fields=("ts_code", "in_date", "l2_code", "l3_code"),
        trade_date_field=None,
        symbol_field="ts_code",
    )
    return decision, dates


def _candidate(symbol: str, *, rank: int, decision: date) -> FrozenCandidateFeatureInputV1:
    other_score = Decimal("0.5") if rank == 1 else Decimal("1.0")
    score = Decimal("1.0") if rank == 1 else Decimal("0.5")
    stages = tuple(
        StageCandidateInputV1(
            stage=stage,
            rank=rank,
            score=score,
            stage_candidate_count=2,
            previous_rank_score=None if rank == 1 else other_score,
            next_rank_score=other_score if rank == 1 else None,
            stage_evidence_id=f"stage-{stage}-{symbol}",
            candidate_content_hash=canonical_json_sha256({"stage": stage, "symbol": symbol}),
        )
        for stage in ("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective")
    )
    legs = (
        MultiAlphaLegInputV1(
            component_id="leg-a",
            score=Decimal("0.8") if rank == 1 else Decimal("0.3"),
            weight=Decimal("0.6"),
            model_identity_hash="a" * 64,
        ),
        MultiAlphaLegInputV1(
            component_id="leg-b",
            score=Decimal("0.4") if rank == 1 else Decimal("0.2"),
            weight=Decimal("0.4"),
            model_identity_hash="b" * 64,
        ),
    )
    return FrozenCandidateFeatureInputV1(
        base_snapshot_id="snapshot-1",
        canonical_signal_id=f"signal-{rank}",
        stable_signal_semantics_hash="c" * 64,
        canonical_signal_scope_hash=canonical_json_sha256({"symbol": symbol}),
        observation_version_id=f"observation-{rank}",
        observation_content_hash=canonical_json_sha256({"observation": rank}),
        symbol=symbol,
        decision_trade_date=decision,
        decision_cutoff_ts=datetime.combine(decision, datetime.min.time(), tzinfo=UTC).replace(hour=7),
        target_trade_date=decision + timedelta(days=1),
        stage_candidates=stages,
        multi_alpha_legs=legs,
        component_evidence_hash=canonical_json_sha256({"legs": rank}),
        hmm_enabled=True,
        hmm_snapshot_id="hmm-snapshot-1",
        hmm_snapshot_hash="d" * 64,
        hmm_snapshot_status="FROZEN",
        hmm_freshness_trade_days=0,
        hmm_coefficient=Decimal("1.1"),
        risk_enabled=True,
        risk_policy_hash="e" * 64,
        risk_can_buy=True,
        risk_multiplier=Decimal("0.9"),
        risk_delta=Decimal("-0.1"),
        risk_penalty=Decimal("0.1"),
        universe_policy_hash="f" * 64,
    )


def _request(decision: date):
    schema = frozen_feature_schema_v1()
    formulas = frozen_formula_registry_v1()
    queries = frozen_feature_query_registry_v1(repository_commit=_COMMIT)
    regime = MarketRegimePolicyTemplateV1()
    label = RankingLabelPolicyV1()
    intent = DatasetBuildIntentV1(
        style_profile_id="style-v1",
        style_profile_hash="1" * 64,
        package_id="package-v1",
        package_manifest_sha256="2" * 64,
        package_asset_closure_hash="3" * 64,
        selection_runtime_semantics_hash="4" * 64,
        multi_alpha_parent_contract_version="advisory_historical_range_candidate_component_lineage_v1",
        multi_alpha_component_identity_set_hash="5" * 64,
        decision_date_start=decision - timedelta(days=60),
        decision_date_end=decision,
        feature_schema_id=schema.feature_schema_id,
        feature_schema_hash=str(schema.feature_schema_hash),
        feature_formula_registry_hash=str(formulas.registry_hash),
        feature_query_registry_hash=str(queries.registry_hash),
        market_regime_policy_template_id=regime.policy_template_id,
        market_regime_policy_template_hash=str(regime.policy_template_hash),
        label_policy_id=label.label_policy_id,
        label_policy_hash=str(label.label_policy_hash),
        calendar_version="calendar-v1",
        calendar_hash="6" * 64,
        repository_commit=_COMMIT,
        final_fit_as_of=datetime(2026, 8, 1, tzinfo=UTC),
    )
    return intent.finalize(
        source_revision_set_id="sources-v1",
        source_revision_set_hash="8" * 64,
        universe_policy_set_id="universe-v1",
        universe_policy_set_hash="9" * 64,
    )


def _append_labels(spool: RerankerDatasetSpool, *, decision: date) -> None:
    outcomes = []
    selected = []
    source_evidence = []
    values = {"signal-1": ("0.08", "0.12", "-0.03"), "signal-2": ("0.01", "0.04", "-0.06")}
    symbols = {"signal-1": "000001.SZ", "signal-2": "000002.SZ"}
    for signal, projections in values.items():
        for index, (projection, value) in enumerate(
            zip(("RETURN_NET_EXCESS", "EXECUTABLE_MFE", "EXECUTABLE_MAE"), projections, strict=True)
        ):
            version = f"label-{signal}-{index}"
            content_hash = canonical_json_sha256({"version": version, "value": value})
            evidence_hash = canonical_json_sha256({"version": version, "evidence": True})
            outcomes.append(
                {
                    "label_version_id": version,
                    "label_content_hash": content_hash,
                    "owner_type": "CANDIDATE",
                    "canonical_signal_id": signal,
                    "symbol": symbols[signal],
                    "decision_as_of_trade_date": decision,
                    "horizon_trading_days": 5,
                    "projection": projection,
                    "maturity_status": "MATURED",
                    "outcome_event_status": "NONE",
                    "projection_value_decimal": Decimal(value),
                    "calculation_evidence_sha256": evidence_hash,
                    "computed_at": datetime(2026, 7, 15, tzinfo=UTC),
                }
            )
            source_evidence.append(
                {
                    "owner_type": "CANDIDATE",
                    "label_version_id": version,
                    "canonical_signal_id": signal,
                    "symbol": symbols[signal],
                    "horizon_trading_days": 5,
                    "projection": projection,
                    "calculation_evidence_sha256": evidence_hash,
                }
            )
            selected.append(
                {
                    "selected_label_mapping_id": f"mapping-{version}",
                    "terminal_label_version_id": version,
                }
            )
    spool.append_partition(
        source_kind="BASE_SNAPSHOT",
        source_identity="snapshot-1",
        logical_role="outcome_source_evidence",
        partition_key="outcome-source-evidence",
        rows=source_evidence,
        identity_fields=("label_version_id",),
        trade_date_field=None,
        symbol_field="symbol",
    )
    spool.append_partition(
        source_kind="BASE_SNAPSHOT",
        source_identity="snapshot-1",
        logical_role="outcome_labels",
        partition_key="labels",
        rows=outcomes,
        identity_fields=("label_version_id",),
        trade_date_field="decision_as_of_trade_date",
        symbol_field="symbol",
    )
    spool.append_partition(
        source_kind="BASE_SNAPSHOT",
        source_identity="snapshot-1",
        logical_role="selected_labels",
        partition_key="selected-labels",
        rows=selected,
        identity_fields=("selected_label_mapping_id",),
        trade_date_field=None,
        symbol_field=None,
    )


def test_complete_builder_snapshot_and_training_export_are_deterministic(tmp_path: Path) -> None:
    with _spool(tmp_path) as spool:
        decision, dates = _append_sources(spool)
        candidates = (
            _candidate("000001.SZ", rank=1, decision=decision),
            _candidate("000002.SZ", rank=2, decision=decision),
        )
        builder = ShortReboundFeatureBuilderV1(
            source_spool=spool,
            source_identity=_SOURCE_IDENTITY,
        )
        source_revision = FeatureSourceRevisionV1(
            query_template_id="historical_market_history_window",
            query_template_hash="d" * 64,
            bound_parameter_hash="e" * 64,
            partition_key="2026-01-01..2026-03-02",
            partition_hash="f" * 64,
            business_min_date=date(2026, 1, 1),
            business_max_date=decision,
            result_schema_hash="1" * 64,
            cutoff_predicate_hash="2" * 64,
            database_target_hash="3" * 64,
            row_count=122,
        )
        source_set_hash = canonical_json_sha256((str(source_revision.source_revision_hash),))
        rows = builder.build_group(
            candidates=candidates,
            query_registry_hash="b" * 64,
            feature_source_revision_set_hash=source_set_hash,
            builder_code_closure_hash="c" * 64,
        )

        assert len(rows) == 2
        assert set(rows[0].features) == {
            item.name for item in frozen_feature_schema_v1().definitions
        }
        assert rows[0].features["selection_effective_rank_percentile"] == 1.0
        assert rows[1].features["selection_effective_rank_percentile"] == 0.0
        assert rows[0].features["market_member_set_hash"]

        request = _request(decision)
        registry = frozen_feature_query_registry_v1(repository_commit=_COMMIT)
        feature_manifest, feature_payload = materialize_feature_snapshot(
            request=request,
            base_snapshot_id="snapshot-1",
            base_snapshot_content_hash="d" * 64,
            feature_schema=frozen_feature_schema_v1(),
            formula_registry=frozen_formula_registry_v1(),
            query_registry=registry,
            source_revisions=(source_revision,),
            builder_code_closure_hash="c" * 64,
            rows=rows,
        )
        feature_manifest_again, feature_payload_again = materialize_feature_snapshot(
            request=request,
            base_snapshot_id="snapshot-1",
            base_snapshot_content_hash="d" * 64,
            feature_schema=frozen_feature_schema_v1(),
            formula_registry=frozen_formula_registry_v1(),
            query_registry=registry,
            source_revisions=(source_revision,),
            builder_code_closure_hash="c" * 64,
            rows=rows,
        )
        assert feature_manifest_again == feature_manifest
        assert feature_payload_again == feature_payload

        _append_labels(spool, decision=decision)
        training_manifest, training_payload = materialize_training_export(
            request=request,
            base_snapshot_id="snapshot-1",
            base_snapshot_content_hash="d" * 64,
            feature_snapshot=feature_manifest,
            feature_schema=frozen_feature_schema_v1(),
            feature_rows=rows,
            base_spool=spool,
            trading_dates=dates,
            label_policy=RankingLabelPolicyV1(),
        )

        assert training_manifest.row_count == 2
        import pyarrow as pa
        import pyarrow.parquet as pq

        training_rows = pq.read_table(
            pa.BufferReader(training_payload["training_rows.parquet"])
        )
        assert set(training_rows.column("label_outcome_event_status").to_pylist()) == {"NONE"}
        assert training_manifest.split_plan.coverage_status == "INSUFFICIENT_ELIGIBLE_DATES"
        assert tuple(item.window_years for item in training_manifest.views) == (2, 3, 5)
        assert not any(item.trainable for item in training_manifest.views)
        assert set(training_payload) == {
            "training_rows.parquet",
            "split_plan.json",
            "views/2y.json",
            "views/3y.json",
            "views/5y.json",
        }


def test_liquidity_and_moneyflow_windows_do_not_compress_missing_dates() -> None:
    start = date(2026, 1, 1)
    market = [
        {
            "trade_date": start + timedelta(days=index),
            "amount_li": Decimal("1000000") + index,
        }
        for index in range(20)
    ]
    moneyflow = {
        (start + timedelta(days=index)).isoformat(): {
            "turnover_rate": Decimal("2.0"),
            "net_mf_amount": Decimal("1.0"),
        }
        for index in range(20)
        if index != 18
    }

    result = ShortReboundFeatureBuilderV1._liquidity_moneyflow(market, moneyflow)

    assert result["liquidity_missing"] is True
    assert result["moneyflow_missing"] is True
    assert result["moneyflow_ratio_5"] is None
    assert result["moneyflow_ratio_20"] is None
    assert result["moneyflow_sign_consistency_20"] is None
