from datetime import date
from dataclasses import replace
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.monthly_component_preparation import ComponentPreparationError
from backend.services.dataset_release.monthly_preparation_shared import prepare_shared_domain_files
from backend.services.dataset_release.monthly_preparation_shared import MonthlyPrivateSharedPreparationExecutor, recover_prepared_shared_components
from backend.services.dataset_release.monthly_preparation_executor import adopt_pinned_shared_file
from backend.services.dataset_release.source_authority import FrozenSourceAuthoritySnapshot
from backend.services.dataset_release.monthly_component_preparation import component_dependencies
from backend.services.dataset_release.monthly_worker import ProducerContext
from backend.services.dataset_release.shared_sector_context import SECTOR_QUOTE_AVAILABILITY_SCHEMA

from backend.tests.dataset_release.test_monthly_preparation_sector import sector  # noqa: F401


@pytest.fixture
def shared(sector, monkeypatch):  # noqa: F811 - imported pytest fixture
    from backend.services.dataset_release import monthly_preparation_shared as producer

    cas, snapshot, audit, profile, _output, rows = sector
    codes = [f"801{number:03d}.SI" for number in range(131)]
    snapshot.pit_snapshot.spans = tuple(
        SimpleNamespace(ts_code=symbol, eligible_start=date(2026, 9, 29), eligible_end=date(2026, 9, 30))
        for symbol in ("000001.SZ", "000002.SZ")
    )
    profile.universe_key = "test-canonical"
    for row in rows:
        if row["ts_code"] == "000002.SZ":
            row.update(l2_code_id=1, sw2_pct_change=None, sw2_vol=None, sw2_amount=None)
    definitions = {
        "csi300": ("000300.SH", "CSI"),
        "csi500": ("000905.SH", "CSI"),
        "csi1000": ("000852.SH", "CSI"),
        "star50": ("000688.SH", "SSE"),
        "star100": ("000698.SH", "SSE"),
    }
    sources = {
        "trading_calendar": [{"cal_date": day} for day in ("2026-09-29", "2026-09-30")],
        "index_membership_pit": [
            {
                "pool_id": name,
                "index_code": code,
                "ts_code": "000001.SZ",
                "effective_from": "2026-09-29",
                "effective_to_exclusive": None,
                "source_provider": provider,
                "source_reference": "unit-test",
                "updated_at": "2026-10-03T00:00:00+08:00",
            }
            for name, (code, provider) in definitions.items()
        ],
        "suspend_d": [
            {"ts_code": "000001.SZ", "trade_date": "2026-09-29", "suspend_type": "S", "suspend_timing": None}
        ],
        "sw_index_classify": [{"index_code": code, "level": "L2"} for code in codes],
        "sw_index_member": [
            {"ts_code": symbol, "in_date": "2026-09-29", "out_date": None, "l2_code": codes[index]}
            for index, symbol in enumerate(("000001.SZ", "000002.SZ"))
        ],
    }
    monkeypatch.setattr(producer, "_source_rows", lambda _cas, _snapshot, dataset: tuple(sources[dataset]))

    # Synthetic taxonomy is limited to this logic unit test. The actual
    # production publication policy is never weakened or patched on disk.
    def test_quote_policy(*, code_map, required_start, cutoff):
        entries = [
            {
                "canonical_l2_code": code,
                "availability_spans": []
                if code == codes[1]
                else [{"start_date": required_start.isoformat(), "end_date": cutoff.isoformat()}],
            }
            for code in sorted(code_map.code_to_id)
        ]
        authority = dict(code_map.mapping_authority)
        body = {"schema_version": SECTOR_QUOTE_AVAILABILITY_SCHEMA, "mapping_authority": authority, "entries": entries}
        return {
            **body,
            "quote_availability_digest": digest_named_fields(
                SECTOR_QUOTE_AVAILABILITY_SCHEMA, {"mapping_authority": authority, "entries": entries}
            ),
        }

    monkeypatch.setattr(producer, "build_quote_availability_payload", test_quote_policy)
    return cas, snapshot, audit, profile, _output.parent, rows


def run(shared, component):
    cas, snapshot, audit, profile, parent, _rows = shared
    return prepare_shared_domain_files(
        cas=cas,
        snapshot=snapshot,
        audit=audit,
        profile=profile,
        component=component,
        output_root=parent / component,
        sector_membership_start=date(2026, 9, 29),
        max_rows_in_memory=2,
    )


@pytest.mark.parametrize("component", ["stock_pools", "benchmark", "suspend", "sector_context"])
def test_real_private_shared_files_without_full_factor_or_release(shared, component):
    result = run(shared, component)
    root = shared[4] / component
    assert all((root / path).is_file() for path in result["output_paths"])
    assert result["publication_allowed"] is False and result["consistent_input_set_complete"] is False
    assert not list(root.rglob("qe_dataset_manifest.json"))
    assert not list(root.rglob("index_pool_coverage.json"))
    assert not list(root.rglob("static_factors.parquet"))
    if component == "stock_pools":
        assert len(result["output_paths"]) == 6
        assert "000300.SH" not in (root / "stock_universe.txt").read_text()
    elif component == "benchmark":
        assert (root / "benchmark.txt").read_text().startswith("000300.SH")
    elif component == "suspend":
        assert len(pd.read_parquet(root / result["output_paths"][0])) == 1
    else:
        membership = pd.read_parquet(root / "sector_membership_spans.parquet")
        market = pd.read_parquet(root / "market_context.parquet")
        assert len(membership) == 2 and set(membership.instrument) == {"000001.SZ", "000002.SZ"}
        assert market.sw_daily_total_vol.eq(12.0).all()
        assert result["domain_counts"]["catalog_count"] == 131
        assert result["domain_counts"]["membership_gap_count"] == 0
        assert result["domain_counts"]["quote_required_sector_date_count"] == 2


@pytest.mark.parametrize("component", ["stock_pools", "benchmark", "suspend", "sector_context"])
def test_blocked_required_domain_never_writes_shared_output(shared, component):
    gate = next(row for row in shared[2]["gates"] if row["gate_id"] == "calendar_lifecycle")
    gate.update(observed_count=3, unexplained_missing_count=1)
    with pytest.raises(ComponentPreparationError):
        run(shared, component)
    assert not (shared[4] / component).exists()


def test_missing_available_sector_quotes_are_not_filled(shared):
    for row in shared[-1]:
        if row["ts_code"] == "000001.SZ":
            row["sw2_pct_change"] = None
    with pytest.raises(ComponentPreparationError, match="quote-available"):
        run(shared, "sector_context")
    assert not (shared[4] / "sector_context" / "sector_membership_spans.parquet").exists()


@pytest.mark.parametrize("change", ["stopped_quote", "available_nan", "conflicting_quote"])
def test_quote_semantics_fail_closed(shared, change):
    rows = shared[-1]
    if change == "stopped_quote":
        rows[-1]["sw2_vol"] = 12.0
    elif change == "available_nan":
        rows[0]["sw2_amount"] = None
    else:
        rows.append({**rows[0], "ts_code": "000003.SZ", "sw2_pct_change": 13.0})
    with pytest.raises(ComponentPreparationError):
        run(shared, "sector_context")
    assert not (shared[4] / "sector_context" / "sector_membership_spans.parquet").exists()


def test_market_history_is_not_cut_to_membership_policy(shared):
    cas, snapshot, audit, profile, parent, _rows = shared
    result = prepare_shared_domain_files(
        cas=cas,
        snapshot=snapshot,
        audit=audit,
        profile=profile,
        component="sector_context",
        output_root=parent / "narrow-policy",
        sector_membership_start=date(2026, 9, 30),
        max_rows_in_memory=2,
    )
    assert len(pd.read_parquet(parent / "narrow-policy" / "market_context.parquet")) == 2
    assert (
        pd.read_parquet(parent / "narrow-policy" / "sector_membership_spans.parquet")
        .start_date.eq(date(2026, 9, 30))
        .all()
    )
    assert result["domain_counts"]["frozen_stock_trading_day_count"] == 2


def checkpoint_fixture(shared, component):
    cas, original, audit, profile, parent, _rows = shared
    dependencies = component_dependencies()
    required = set().union(*(set(dependencies[name]) for name in ("stock_pools", "benchmark", "suspend", "sector_context"))) - {"stock_universe_pit"}

    class Partition:
        def __init__(self, dataset):
            self.spec = SimpleNamespace(dataset=dataset)
            self.content = "a" * 64

        def as_build_input(self):
            return {
                "dataset": self.spec.dataset,
                "partition_key": "unit-frozen",
                "schema_digest": "b" * 64,
                "content_digest": self.content,
                "merkle_root": "c" * 64,
                "row_count": 4,
                "source_partition_params_digest": "d" * 64,
                "source_code_membership_digest": "e" * 64,
            }

    snapshot = replace(original, partitions=tuple(Partition(name) for name in sorted(required)))
    # Fake source rows are an explicit unit fixture boundary; production
    # source sealing has separate real CAS tests. Files/sealing/recovery below
    # use the real writers, SHA readback and create-exclusive control path.
    catalog = parent / "catalog"
    catalog.mkdir()
    profile.candidate_root = str(catalog)
    profile.semantic_profile_digest = "1" * 64
    profile.qlib_toolchain = SimpleNamespace(digest="2" * 64)
    profile.resource_policy = SimpleNamespace(validation_read_chunk_rows=2)
    context = ProducerContext(
        "SOURCE",
        snapshot.operation_id,
        1,
        {},
        {
            "target_cutoff": snapshot.official_cutoff.isoformat(),
            "predecessor": {"profile_sha256": "3" * 64},
        },
        {},
    )
    executor = MonthlyPrivateSharedPreparationExecutor(profile, cas, date(2026, 9, 29))
    return executor, context, snapshot, audit, catalog


@pytest.mark.parametrize("component", ["stock_pools", "benchmark", "suspend", "sector_context"])
def test_shared_recovery_uses_real_pins_and_does_not_recompute(shared, component, monkeypatch):
    from backend.services.dataset_release import monthly_preparation_shared as module

    executor, context, snapshot, audit, _catalog = checkpoint_fixture(shared, component)
    first = executor.execute(
        context=context, snapshot=snapshot, audit=audit, component=component, source_snapshot_id="postgres:test"
    )

    def forbidden(**_kwargs):
        raise AssertionError("pinned component was recomputed")

    monkeypatch.setattr(module, "prepare_shared_domain_files", forbidden)
    # Completing an unrelated financing domain changes global provenance,
    # but not this component's exact sealed effective inputs.
    snapshot = replace(snapshot, manifest=SimpleNamespace(source_content_root="9" * 64))
    second = executor.execute(
        context=replace(context, attempt=2),
        snapshot=snapshot,
        audit=audit,
        component=component,
        source_snapshot_id="postgres:second",
    )
    assert second == first and second["publication_allowed"] is False


def test_shared_output_tamper_is_not_hidden_by_retry(shared):
    executor, context, snapshot, audit, catalog = checkpoint_fixture(shared, "stock_pools")
    first = executor.execute(
        context=context, snapshot=snapshot, audit=audit, component="stock_pools", source_snapshot_id="postgres:test"
    )
    with (catalog / first["component_root"] / "stock_universe.txt").open("ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(ComponentPreparationError, match="bytes differ"):
        executor.execute(
            context=replace(context, attempt=2),
            snapshot=snapshot,
            audit=audit,
            component="stock_pools",
            source_snapshot_id="postgres:test",
        )


def test_shared_effective_input_drift_creates_a_new_checkpoint(shared):
    executor, context, snapshot, audit, _catalog = checkpoint_fixture(shared, "stock_pools")
    first = executor.execute(
        context=context, snapshot=snapshot, audit=audit, component="stock_pools", source_snapshot_id="postgres:test"
    )
    next(part for part in snapshot.partitions if part.spec.dataset == "stock_basic").content = "f" * 64
    second = executor.execute(
        context=replace(context, attempt=2),
        snapshot=snapshot,
        audit=audit,
        component="stock_pools",
        source_snapshot_id="postgres:test",
    )
    assert first["identity_digest"] != second["identity_digest"]
    assert first["component_root"] != second["component_root"]


def complete_snapshot(cas, private):
    return FrozenSourceAuthoritySnapshot(
        official_cutoff=private.official_cutoff, pit_snapshot=private.pit_snapshot,
        pit_snapshot_ref=private.pit_snapshot_ref, manifest=SimpleNamespace(source_content_root="9" * 64),
        source_manifest_ref=private.source_manifest_ref, source_reuse_manifest_ref=private.source_manifest_ref,
        source_audit_ref=private.source_audit_ref, source_provenance_ref=private.source_audit_ref,
        derived_source_receipt_refs=(), partitions=private.partitions, pit_partitions=private.pit_partitions,
        snapshot_tokens=private.snapshot_tokens, observation_provenance_root="7" * 64, source_cas_usage={},
        artifact_ready_contract_ref=cas.put_json({"unit-full-contract": True}),
    )


@pytest.mark.parametrize("component", ["stock_pools", "benchmark", "suspend", "sector_context"])
def test_full_source_recovery_and_exact_independent_file_adoption(shared, component):
    executor, context, snapshot, audit, catalog = checkpoint_fixture(shared, component)
    record = executor.execute(context=context, snapshot=snapshot, audit=audit, component=component,
                              source_snapshot_id="postgres:test")
    staging = catalog / ".staging" / "full-build"
    staging.mkdir()
    recovered = recover_prepared_shared_components(
        context=replace(context, stage="BUILD", attempt=2), snapshot=complete_snapshot(executor.cas, snapshot),
        profile=executor.profile, sector_membership_start=executor.sector_membership_start, staging_root=staging,
    )
    assert set(recovered) == {component}
    verified = recovered[component]
    for ref in verified.receipt["output_refs"]:
        # Facts belong in the factor builder, not the final sidecar directory.
        if ref["path"].startswith("facts/"):
            continue
        target = staging / component / ref["path"]
        adopted = adopt_pinned_shared_file(root=catalog, verified=verified,
                                           relative_path=ref["path"], destination=target)
        original = catalog / record["component_root"] / ref["path"]
        assert adopted.read_bytes() == original.read_bytes()
        assert adopted.stat().st_ino != original.stat().st_ino
    assert not list(staging.rglob("prepared-component.json"))


def test_private_source_never_adopts_shared_outputs_into_final_build(shared):
    executor, context, snapshot, audit, catalog = checkpoint_fixture(shared, "stock_pools")
    executor.execute(context=context, snapshot=snapshot, audit=audit, component="stock_pools",
                     source_snapshot_id="postgres:test")
    staging = catalog / ".staging" / "full-build"
    staging.mkdir()
    with pytest.raises(ComponentPreparationError, match="complete SOURCE"):
        recover_prepared_shared_components(context=replace(context, stage="BUILD", attempt=2), snapshot=snapshot,
                                           profile=executor.profile, sector_membership_start=executor.sector_membership_start,
                                           staging_root=staging)
    assert list(staging.iterdir()) == []


def test_final_shared_drift_falls_back_to_normal_build(shared):
    executor, context, snapshot, audit, catalog = checkpoint_fixture(shared, "stock_pools")
    executor.execute(context=context, snapshot=snapshot, audit=audit, component="stock_pools",
                     source_snapshot_id="postgres:test")
    next(part for part in snapshot.partitions if part.spec.dataset == "stock_basic").content = "f" * 64
    staging = catalog / ".staging" / "full-build"
    staging.mkdir()
    assert recover_prepared_shared_components(context=replace(context, stage="BUILD", attempt=2),
                                              snapshot=complete_snapshot(executor.cas, snapshot), profile=executor.profile,
                                              sector_membership_start=executor.sector_membership_start, staging_root=staging) == {}
