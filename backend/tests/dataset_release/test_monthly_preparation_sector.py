from dataclasses import dataclass
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.factor_materializer import FACTOR_H5_SCHEMAS
from backend.services.dataset_release.monthly_component_preparation import ComponentPreparationError
from backend.services.dataset_release.monthly_preparation_sector import materialize_preparation_sector_facts
from backend.services.dataset_release.monthly_preparation_source import PreparationSourceSnapshot
from backend.services.dataset_release.monthly_unified import SOURCE_GATES


@pytest.fixture
def sector(tmp_path, monkeypatch):
    from backend.services.dataset_release import monthly_preparation_sector as producer

    root = tmp_path / "control"
    ControlStore.initialize(root)
    cas = CASStore(root)
    cutoff = date(2026, 9, 30)
    schema = FACTOR_H5_SCHEMAS["sector_data"]
    rows = []
    # Deliberately source-order stocks before dates: H5 must still end up
    # globally ordered, including interleaved batch and partition boundaries.
    for symbol in ("000001.SZ", "000002.SZ"):
        for day in ("2026-09-29", "2026-09-30"):
            rows.append({"ts_code": symbol, "trade_date": day, **dict.fromkeys(schema, 12.0), "l2_code_id": 0})

    @dataclass
    class Partition:
        spec: object

    snapshot = PreparationSourceSnapshot(
        "dmr_" + "1" * 32,
        cutoff,
        SimpleNamespace(cutoff=cutoff, spans_sha256="a" * 64),
        cas.put_json({"pit": "test"}),
        cas.put_json({"private": "test"}),
        cas.put_json({"audit": "test"}),
        (Partition(SimpleNamespace(dataset="sector_data")),),
        (),
        ("postgres:test",),
        ("margin_detail",),
        "b" * 64,
        SimpleNamespace(source_content_root="c" * 64),
    )
    audit = {
        "schema_version": "aistock_monthly_preparation_source_audit_v1",
        "operation_id": snapshot.operation_id,
        "cutoff": cutoff.isoformat(),
        "audit_start": "2018-08-01",
        "source_manifest_ref": snapshot.source_manifest_ref.as_dict(),
        "gates": [
            {
                "gate_id": gate,
                "expected_count": 4,
                "observed_count": 4,
                "explained_missing_count": 0,
                "unexplained_missing_count": 0,
                "duplicate_count": 0,
                "invalid_value_count": 0,
                "snapshot_group_id": "postgres:test",
            }
            for gate in SOURCE_GATES
        ],
    }
    financing = next(g for g in audit["gates"] if g["gate_id"] == "financial_moneyflow")
    financing.update(observed_count=3, unexplained_missing_count=1)

    class View:
        def __init__(self, *_args):
            pass

        def descriptors(self, dataset):
            assert dataset == "sector_data"  # Never opens margin or factor aggregate.
            return ({"dataset": dataset},)

        def iter_partition_rows(self, _descriptor):
            yield from rows

    monkeypatch.setattr(producer, "_SealedSnapshotView", View)
    profile = SimpleNamespace(start_date=date(2018, 8, 1))
    return cas, snapshot, audit, profile, tmp_path / "private-sector", rows


def run(sector, **kwargs):
    cas, snapshot, audit, profile, output, _rows = sector
    return materialize_preparation_sector_facts(
        cas=cas,
        snapshot=snapshot,
        audit=audit,
        profile=profile,
        output_root=output,
        max_rows_in_memory=2,
        **kwargs,
    )


def test_sector_facts_are_bounded_canonical_and_independent_of_margin(sector):
    result = run(sector)
    frame = pd.read_hdf(sector[4] / "sector_data.h5", "data")
    assert frame.index.is_monotonic_increasing and not frame.index.has_duplicates
    assert tuple(frame.columns) == FACTOR_H5_SCHEMAS["sector_data"]
    assert str(frame.l2_code_id.dtype) == "int16"
    assert len(frame) == 4
    assert result["row_count"] == 4 and result["max_batch_rows"] <= 2
    assert result["whole_history_materialized"] is False
    assert result["publication_allowed"] is False
    assert result["consistent_input_set_complete"] is False
    assert not (sector[4] / "static_factors.parquet").exists()
    assert not (sector[4] / "qe_dataset_manifest.json").exists()
    assert list(sector[4].iterdir()) == [sector[4] / "sector_data.h5"]


def test_stopped_quote_nan_does_not_clear_real_moneyflow(sector):
    for row in sector[-1]:
        row.update(
            sw2_pct_change=None,
            sw2_vol=None,
            sw2_amount=None,
            sw2_mf_net_amt=43.0,
            sw2_mf_buy_elg_amt=51.0,
            sw2_mf_sell_elg_amt=8.0,
        )
    run(sector)
    frame = pd.read_hdf(sector[4] / "sector_data.h5", "data")
    assert frame[["sw2_pct_change", "sw2_vol", "sw2_amount"]].isna().all().all()
    assert frame.sw2_mf_net_amt.eq(43.0).all()


@pytest.mark.parametrize(
    "failure", ["operation", "source", "sector_gate", "duplicate", "id_overflow", "cutoff", "value_overflow"]
)
def test_invalid_sector_preparation_fails_closed(sector, failure):
    _, _, audit, _, output, rows = sector
    if failure == "operation":
        audit["operation_id"] = "dmr_" + "2" * 32
    elif failure == "source":
        audit["source_manifest_ref"] = {"sha256": "e" * 64}
    elif failure == "sector_gate":
        gate = next(g for g in audit["gates"] if g["gate_id"] == "sector_authority")
        gate.update(observed_count=3, unexplained_missing_count=1)
    elif failure == "duplicate":
        rows.append(dict(rows[0]))
    elif failure == "id_overflow":
        rows[0]["l2_code_id"] = 40000
    elif failure == "value_overflow":
        rows[0]["sw2_vol"] = 1e40
    else:
        rows[0]["trade_date"] = "2026-10-01"
    with pytest.raises(ComponentPreparationError):
        run(sector)
    assert not (output / "sector_data.h5").exists()


def test_preparation_is_exclusive_and_does_not_overwrite_completed_facts(sector):
    run(sector)
    original = (sector[4] / "sector_data.h5").read_bytes()
    with pytest.raises((FileExistsError, ComponentPreparationError)):
        run(sector)
    assert (sector[4] / "sector_data.h5").read_bytes() == original
