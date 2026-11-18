from decimal import Decimal

import pytest

from scripts import verify_localsim_corrected_ledger as ledger


@pytest.mark.parametrize(
    ("actual", "frozen", "matches"),
    [
        ("2026-11-18T01:31:00Z", "2026-11-18 09:31:00+08:00", True),
        ("2026-11-18T01:31:01Z", "2026-11-18 09:31:00+08:00", False),
        (Decimal("32311.76"), "32311.760000", True),
        (Decimal("32311.76"), "32311.75", False),
        (Decimal("14.123456789987654321"), 14.123456789987654, True),
        (True, 1, False),
    ],
)
def test_compare_frozen_source_projection(actual, frozen, matches):
    assert ledger.same_projection(actual, frozen) is matches


def test_rebinding_preserves_economic_values_and_foreign_keys():
    frames = {
        "run": [{"run_id": "r", "portfolio_id": "source"}],
        "orders": [{"run_id": "r", "order_id": "o", "portfolio_id": "source", "package_id": "pkg"}],
        "fills": [{"run_id": "r", "order_id": "o", "fill_id": "f", "price": Decimal("10.000001")}],
        "cash_ledger": [{"run_id": "r", "fill_id": "f", "portfolio_id": "source"}],
        "order_events": [{"run_id": "r", "order_id": "o", "event_id": "e",
                          "fill_json": {"run_id": "r", "order_id": "o", "fill_id": "f", "portfolio_id": "source"}}],
    }
    scoped = ledger.rebind(frames, "candidate")
    run, order, fill, cash, event = (scoped[t][0] for t in frames)
    assert all(row["run_id"] == run["run_id"] for row in (order, fill, cash, event))
    assert all(row["portfolio_id"] == "candidate" for row in (run, order, cash))
    assert order["package_id"] == "pkg"
    assert fill["order_id"] == event["order_id"] == order["order_id"]
    assert fill["fill_id"] == cash["fill_id"]
    assert fill["price"] == frames["fills"][0]["price"]
    assert event["fill_json"] == {
        "run_id": run["run_id"], "order_id": order["order_id"], "fill_id": fill["fill_id"], "portfolio_id": "candidate",
    }
    assert scoped == ledger.rebind(frames, "candidate")
    assert scoped["fills"][0]["fill_id"] != ledger.rebind(frames, "another")["fills"][0]["fill_id"]
    assert frames["orders"][0]["portfolio_id"] == "source"


def test_duplicate_import_key_fails():
    with pytest.raises(ValueError, match="IMPORT_DUPLICATE_OR_MISSING_KEY_fills"):
        ledger.rebind({"fills": [{"fill_id": "f"}, {"fill_id": "f"}]}, "candidate")


def test_correction_preserves_the_lot_trade_date():
    symbol = "000401.SZ"
    day = {
        "trade_date": "2026-11-18", "snapshot_time": "2026-11-18T15:00:00+08:00",
        "account_version_id": "correction", "nav": "120", "cash": "20", "marks": {symbol: "10"},
        "execution_states": [], "orders": [], "fills": [], "cash_entries": [],
        "positions": {symbol: {"portfolio_id": "correction", "symbol": symbol, "quantity": 10,
                              "available_quantity": 10, "avg_cost": "9", "trade_date": "2026-11-13"}},
    }
    frames = ledger.corrected_frames(day, {"package_id": "pkg"}, {"sha256": "example"})
    position = ledger.rebind(frames, "candidate")["positions"][0]
    assert position["trade_date"] == "2026-11-18"
    assert position["metadata"]["position_trade_date"] == "2026-11-13"


def test_changed_sealed_source_fails(tmp_path):
    path = tmp_path / "facts.json"
    path.write_text('{"nav":"4564160.09"}', encoding="utf-8")
    source = ledger.file_ref(path)
    path.write_text('{"nav":"4564160.08"}', encoding="utf-8")
    with pytest.raises(ValueError, match="SOURCE_CHANGED"):
        ledger.read(source)
