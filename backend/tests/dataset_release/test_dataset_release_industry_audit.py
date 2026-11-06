import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest


def _module():
    path = Path(__file__).resolve().parents[3] / "scripts/dataset_release_industry_audit.py"
    spec = importlib.util.spec_from_file_location("industry_history_audit", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _index(rows):
    return pd.MultiIndex.from_tuples([(pd.Timestamp(day), code) for day, code in rows],
                                    names=["datetime", "instrument"])


def test_summary_counts_unknown_and_reports_gaps_without_dropping_rows():
    module = _module()
    index = _index([("2020-01-02", "000001.SZ"), ("2021-08-02", "000001.SZ"),
                    ("2021-08-03", "000002.SZ")])
    result = module.summarize_ids(pd.Series([None, 0, None], index=index))
    assert (result["rows"], result["covered_rows"], result["missing_rows"], result["missing_symbol_count"]) == (3, 1, 2, 2)
    assert result["by_year"]["2020"]["coverage"] == 0
    assert result["by_year"]["2021"]["coverage"] == 0.5


def test_provider_audit_keeps_ninety_percent_threshold_and_no_future_lookup(tmp_path):
    module = _module()
    path = tmp_path / "sector_data.h5"
    pd.DataFrame({"l2_code_id": [7]}, index=_index([("2021-08-02", "000001.SZ")])).to_hdf(path, key="data", format="table")
    target = _index([("2020-01-02", "SZ000001"), ("2021-08-03", "SZ000001")])
    report, ids = module.audit_provider(path, target, "train")
    assert report["threshold"] == 0.9
    assert report["pass"] is False
    assert report["coverage"] == 0.5
    assert pd.isna(ids.iloc[0]) and ids.iloc[1] == 7


def test_invalid_or_duplicate_sample_index_fails_closed():
    module = _module()
    with pytest.raises(ValueError, match="duplicate"):
        module.summarize_ids(pd.Series([1, 1], index=_index([("2021-08-02", "000001.SZ")] * 2)))


def test_report_destination_must_not_be_inside_candidate(tmp_path):
    module = _module()
    root = tmp_path / "candidate"
    root.mkdir()
    with pytest.raises(ValueError, match="candidate"):
        module.validate_output_root(root, root / "reports")


def test_parquet_unknown_sentinel_does_not_get_forward_filled(tmp_path):
    module = _module()
    index = _index([("2021-08-02", "000001.SZ"), ("2021-08-03", "000001.SZ")])
    path = tmp_path / "static_factors.parquet"
    pd.DataFrame({"l2_code_id": [7, -1]}, index=index).to_parquet(path)
    report, ids = module.audit_provider(path, index, "valid")
    assert report["missing_rows"] == 1 and pd.isna(ids.iloc[1])


def test_instrument_partitions_equal_original_provider_and_weighted_coverage(tmp_path):
    module = _module()
    source = _index([("2021-08-02", "000001.SZ"), ("2021-08-04", "000001.SZ"), ("2021-08-02", "000002.SZ")])
    path = tmp_path / "sector_data.h5"
    pd.DataFrame({"l2_code_id": [7, -1, 8]}, index=source).to_hdf(path, key="data", format="table")
    target = _index([("2020-01-02", "SZ000001"), ("2021-08-03", "SZ000001"),
                     ("2021-08-04", "SZ000001"), ("2021-08-03", "SZ000002"), ("2021-08-03", "SZ000003")])
    original, original_ids = module.audit_provider(path, target, "train")
    partitioned, partitioned_ids = module.audit_provider_batched(path, target, "train", batch_size=1)
    pd.testing.assert_series_equal(original_ids, partitioned_ids, check_names=False)
    for field in ("rows", "covered_rows", "missing_rows", "coverage", "pass", "by_year"):
        assert partitioned[field] == original[field]
    assert partitioned["coverage"] == 0.4


def test_static_factor_fallback_requires_manifest_pinned_inventory(tmp_path):
    module = _module()
    inventory = tmp_path / "inventory.json"
    row = {"path": "components/new_component_identity/static_factors.parquet", "sha256": "a" * 64, "size": 1}
    inventory.write_text(json.dumps({"files": [row]}), encoding="utf-8")
    manifest = {"components": {"sector_data": {"path": "sector_data.h5"},
                                "factor_content_manifest": {"path": "inventory.json", "sha256": module.sha256(inventory)}}}
    assert module.industry_file_pins(tmp_path, manifest)["static_factors"] == row
    manifest["components"]["factor_content_manifest"]["sha256"] = "b" * 64
    with pytest.raises(ValueError, match="inventory differs"):
        module.industry_file_pins(tmp_path, manifest)


@pytest.mark.parametrize("relative", ["../other-sector.h5", "C:/outside.h5", "C:outside.h5", "\\\\server\\share\\x.h5"])
def test_manifest_component_path_cannot_escape_release(tmp_path, relative):
    with pytest.raises(ValueError, match="release-relative"):
        _module().pinned_path(tmp_path, {"path": relative})


def test_sample_windows_come_from_current_saved_population_not_old_dates():
    module = _module()
    target = pd.DataFrame({"segment": ["train", "valid", "test"]},
                          index=_index([("2027-01-04", "SZ000001"), ("2027-06-01", "SZ000001"), ("2027-10-01", "SZ000001")]))
    indices = module.consumer_indices(target, cutoff="2027-10-29")
    assert indices["test"][0][0] == pd.Timestamp("2027-10-01")
    with pytest.raises(ValueError, match="cutoff"):
        module.consumer_indices(target, cutoff="2027-09-30")


def test_normalized_alias_duplicates_are_not_dropped():
    target = pd.DataFrame({"segment": ["train", "train", "valid", "test"]}, index=_index([
        ("2027-01-04", "SZ000001"), ("2027-01-04", "000001.SZ"),
        ("2027-06-01", "000001.SZ"), ("2027-10-01", "000001.SZ")]))
    with pytest.raises(ValueError, match="duplicate normalized"):
        _module().consumer_indices(target)


@pytest.mark.parametrize("fmt", ["table", "fixed"])
def test_hdf_storage_format_does_not_change_consumer_semantics(tmp_path, fmt):
    path = tmp_path / "sector_data.h5"
    index = _index([("2027-01-04", "000001.SZ")])
    pd.DataFrame({"l2_code_id": [131]}, index=index).to_hdf(path, key="data", format=fmt)
    report, ids = _module().audit_provider(path, index, "train")
    assert report["pass"] and ids.iloc[0] == 131


def _candidate(tmp_path, conflict=False):
    module = _module()
    root = tmp_path / "candidate"
    root.mkdir()
    index = _index([("2027-01-04", "000001.SZ"), ("2027-06-01", "000001.SZ"), ("2027-10-01", "000001.SZ")])
    pd.DataFrame({"l2_code_id": [1, 2, 3]}, index=index).to_hdf(root / "sector_data.h5", key="data", format="table")
    pd.DataFrame({"l2_code_id": [1, 2, 4 if conflict else 3]}, index=index).to_parquet(root / "static_factors.parquet")
    components = {key: {"path": filename, "sha256": module.sha256(root / filename)} for key, filename in
                  [("sector_data", "sector_data.h5"), ("static_factors", "static_factors.parquet")]}
    (root / "qe_dataset_manifest.json").write_text(json.dumps({"components": components,
        "dataset_manifest_sha256": "d" * 64, "cutoff_trade_date": "2027-10-29"}), encoding="utf-8")
    target = tmp_path / "saved-samples.parquet"
    pd.DataFrame({"segment": ["train", "valid", "test"]}, index=index).to_parquet(target)
    return root, target


@pytest.mark.parametrize("conflict", [False, True])
def test_cli_is_readonly_and_id_only_result_cannot_claim_feature_completeness(tmp_path, conflict):
    module = _module()
    root, target = _candidate(tmp_path, conflict)
    before = {p.name: module.sha256(p) for p in root.iterdir()}
    output = tmp_path / "diagnostic"
    assert module.main(["--candidate-root", str(root), "--target-index", str(target), "--output-root", str(output)]) == 0
    result = json.loads((output / "industry_history_audit.json").read_text(encoding="utf-8"))
    assert result["industry_id_coverage_ready"] is not conflict
    assert result["h5_parquet_comparison"]["test"]["conflicting_ids"] == int(conflict)
    assert result["feature_completeness_evaluated"] is False
    assert "graph_input_ready" not in result
    assert result["database_read"] is result["database_write"] is result["runtime_action"] is False
    assert before == {p.name: module.sha256(p) for p in root.iterdir()}
    with pytest.raises(FileExistsError):
        module.validate_output_root(root, output)


def test_missing_source_pin_is_reported_not_repaired(tmp_path):
    module = _module()
    root, target = _candidate(tmp_path)
    (root / "static_factors.parquet").unlink()
    with pytest.raises(ValueError, match="regular file"):
        module.build_audit(root, target)


def test_concurrently_changed_source_is_not_reported_with_old_identity(tmp_path, monkeypatch):
    module = _module()
    root, target = _candidate(tmp_path)
    original = module.audit_provider_batched

    def mutate(path, *args, **kwargs):
        result = original(path, *args, **kwargs)
        path.touch()
        return result

    monkeypatch.setattr(module, "audit_provider_batched", mutate)
    with pytest.raises(ValueError, match="source changed"):
        module.build_audit(root, target)


def test_ambiguous_static_inventory_cannot_pick_an_arbitrary_file(tmp_path):
    module = _module()
    inventory = tmp_path / "inventory.json"
    inventory.write_text(json.dumps({"files": [{"path": "old/static_factors.parquet"},
                                               {"path": "new/static_factors.parquet"}]}), encoding="utf-8")
    manifest = {"components": {"sector_data": {"path": "sector_data.h5"},
        "factor_content_manifest": {"path": "inventory.json", "sha256": module.sha256(inventory)}}}
    with pytest.raises(ValueError, match="one exact"):
        module.industry_file_pins(tmp_path, manifest)
