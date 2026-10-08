"""Original population/PIT contracts, not model-profit assertions or implementation snapshots."""
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_pipeline import file_sha256
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY
from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import (
    FrozenPopulationSourceV1, PopulationMetadataRequestV1,
)
from backend.services.advisory_model_first.generic_population_price_5td_population_v1 import prepare_population_metadata_v1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1


def ref(path):
    return EvidenceReferenceV1(role=path.name, artifact_uri=str(path), sha256=file_sha256(path),
                               size_bytes=path.stat().st_size)


@pytest.fixture
def population(tmp_path):
    calendar = tmp_path / "calendar.json"
    days = [d.date().isoformat() for d in pd.bdate_range("2024-07-04", periods=10)]
    calendar.write_text(json.dumps(days), encoding="utf-8")
    anchor = tmp_path / "anchor.parquet"
    records = [{KEY[0]: pd.Timestamp(days[0]), KEY[1]: pd.Timestamp(days[1]), KEY[2]: f"{600000+i:06}.SH",
        "selection_effective_rank": i+1, "is_candidate_decision": True, "package_id": "anchor",
        "manifest_sha256": "a"*64, "score": "not a financial input"} for i in range(40)]
    # Poison beyond the approved development window must not enter even key validation.
    records.append({**records[0], KEY[0]: pd.Timestamp("2025-10-09"), KEY[1]: pd.Timestamp("1900-01-01")})
    pd.DataFrame(records).to_parquet(anchor, index=False)
    sources = [FrozenPopulationSourceV1(source_id="anchor", package_id="anchor", manifest_sha256="a"*64,
        candidate_format="GP5_LEGACY_TOP20", candidate_ref=ref(anchor), decision_dates=days[:3],
        empty_decision_dates=(days[2],))]
    arm = tmp_path / "arms.parquet"
    pd.DataFrame([{KEY[0]: pd.Timestamp(days[0]), KEY[1]: pd.Timestamp(days[1]), KEY[2]: symbol,
        "selection_effective_rank": rank, "arm_id": name, "score": "unused"}
        for name, symbols in (("train", ["600000.SH", "600999.SH"]), ("held", ["600888.SH"]))
        for rank, symbol in enumerate(symbols, 1)]).to_parquet(arm, index=False)
    lineage = tmp_path / "request.json"
    lineage.write_text(json.dumps({"packages": [{"arm_id": n, "package_id": p, "manifest_sha256": h*64}
        for n, p, h in (("train", "pkg1", "b"), ("held", "pkg2", "c"))]}), encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"files": {p.name: {"sha256": ref(p).sha256, "size_bytes": p.stat().st_size}
        for p in (arm, lineage)}}), encoding="utf-8")
    sources.extend(FrozenPopulationSourceV1(source_id=n, package_id=p, manifest_sha256=h*64,
        candidate_format="FROZEN_ARM_TOP50", candidate_ref=ref(arm), arm_id=n,
        lineage_ref=ref(lineage), bundle_manifest_ref=ref(manifest), decision_dates=days[:3],
        universe_identity={"mode": "single_index" if n == "train" else "index_union", "identity": p})
        for n, p, h in (("held", "pkg2", "c"), ("train", "pkg1", "b")))
    return PopulationMetadataRequestV1(sources=sources, calendar_ref=ref(calendar))


def test_complete_rosters_deduplicate_mass_not_rows_and_keep_held_out(population):
    result = prepare_population_metadata_v1(request=population)
    anchor = result.rosters.loc[result.rosters.source_id.eq("anchor")]
    assert len(anchor) == 20 and anchor.candidate_group_size.eq(20).all()
    assert len(result.rosters) == 23 and len(result.clusters) == 22
    assert result.receipt["potential_anchor_train_clusters"] == 20
    assert result.receipt["potential_transfer_train_clusters"] == 21
    assert result.receipt["new_potential_training_clusters"] == 1
    assert result.receipt["held_source_id"] == "held"
    assert result.rosters.loc[result.rosters.source_id.eq("held"), "universe_identity"].iloc[0] == {
        "mode": "index_union", "identity": "pkg2"}
    assert result.rosters.run_id.isna().all() and not result.receipt["native_receipt_created"]
    assert not result.receipt["labels_ready"] and not result.clusters.supervision_ready.any()
    statuses = set(result.days.roster_status)
    assert statuses == {"PRESENT", "EXPLICIT_EMPTY", "UNKNOWN_ABSENT_FROZEN_DAY"}
    assert result.days.loc[result.days.roster_status.eq("UNKNOWN_ABSENT_FROZEN_DAY"), "candidate_count"].isna().all()


@pytest.mark.parametrize("fault,match", [("hash", "reference differs"), ("target", "D/T"),
    ("duplicate", "unique"), ("manifest", "anchor package"), ("arm", "arm/package")])
def test_frozen_identity_or_original_key_contradiction_fails_closed(population, fault, match):
    sources = list(population.sources)
    if fault == "arm":
        sources[1] = sources[1].model_copy(update={"package_id": "wrong"})
    else:
        path = sources[0].candidate_ref.artifact_uri
        data = pd.read_parquet(path)
        if fault == "hash":
            data.loc[0, "score"] = "changed frozen bytes"
        elif fault == "target":
            data.loc[0, KEY[1]] = pd.Timestamp("2024-07-08")
        elif fault == "duplicate":
            data.loc[1, KEY[2]] = data.loc[0, KEY[2]]
        elif fault == "manifest":
            data.loc[0, "manifest_sha256"] = "f"*64
        data.to_parquet(path, index=False)
        if fault != "hash":
            sources[0] = sources[0].model_copy(update={"candidate_ref": ref(Path(path))})
    with pytest.raises(ValueError, match=match):
        prepare_population_metadata_v1(request=population.model_copy(update={"sources": tuple(sources)}))


def test_projected_reader_never_decodes_score_or_outcome_columns(population, monkeypatch):
    from backend.services.advisory_model_first import generic_population_price_5td_population_v1 as module
    original = module.ds.dataset
    class KeyOnlyDataset:
        def __init__(self, *args, **kwargs):
            self.dataset = original(*args, **kwargs)
            self.schema = self.dataset.schema
        def count_rows(self, **kwargs):
            return self.dataset.count_rows(**kwargs)
        def to_table(self, *, columns, **kwargs):
            assert not set(columns) & {"score", "combined_score", "net_return", "gross_terminal_ratio"}
            return self.dataset.to_table(columns=columns, **kwargs)
    monkeypatch.setattr(module.ds, "dataset", KeyOnlyDataset)
    assert not prepare_population_metadata_v1(request=population).receipt["financial_columns_read"]


def test_five_original_sessions_cutoff_is_not_missing_data_or_ready_supervision(population):
    source = population.sources[0]
    path = Path(source.candidate_ref.artifact_uri)
    frame = pd.read_parquet(path).iloc[:20].copy()
    frame[KEY[0]], frame[KEY[1]] = pd.Timestamp("2025-09-30"), pd.Timestamp("2025-10-09")
    frame.to_parquet(path, index=False)
    calendar = Path(population.calendar_ref.artifact_uri)
    calendar.write_text(json.dumps(["2025-09-30", "2025-10-09", "2025-10-10", "2025-10-13",
                                    "2025-10-14", "2025-10-15"]), encoding="utf-8")
    source = source.model_copy(update={"candidate_ref": ref(path), "decision_dates": (pd.Timestamp("2025-09-30").date(),),
                                       "empty_decision_dates": ()})
    result = prepare_population_metadata_v1(request=population.model_copy(
        update={"sources": (source,), "calendar_ref": ref(calendar)}))
    assert len(result.rosters) == 20 and result.rosters.maturity_at_cutoff.eq("UNSETTLED_CUTOFF").all()
    assert result.rosters.label_information_end.eq(pd.Timestamp("2025-10-15")).all()
    assert not result.clusters.potential_transfer_train.any()
    assert result.receipt["population_contrast_status"] == "NOT_TESTABLE_POPULATION_CONTRAST"
    assert result.receipt["physical_fit_count"] == 0
