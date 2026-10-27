"""Explicit synthetic fixtures and native-Qlib oracle; no pytest dependency."""
# ruff: noqa: E402 -- run directly in the existing node numeric environment.

import hashlib
import json
from pathlib import Path
import sys
import types
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import qe_l2_continuous_admission as admission
from qe_l2_continuous_consumer import VERSION, PRODUCER, VINTAGES, TARGET
from qe_l2_risk_admission import canonical_sha


def _seal(value):
    return {**value, "receipt_sha256": canonical_sha(value)}


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(value, bytes):
        path.write_bytes(value)
    else:
        path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def make_data(tmp_path, monkeypatch):
    days = pd.bdate_range("2026-04-01", "2026-09-30")
    # Explicit synthetic calendar for bounded IO tests, never a research release.
    days = days.delete([3, 21, 22, 23, 57, 100])
    assert len(days) == 125
    calendar = pd.DatetimeIndex([pd.Timestamp("2026-03-31"), *days])
    catalog = [f"L{i:03d}" for i in range(131)]
    assets = {}
    dayroot = tmp_path / "day"
    minroot = tmp_path / "minute"
    assets["calendar"] = _write(
        dayroot / "calendars/day.txt", ("\n".join(str(x.date()) for x in calendar) + "\n").encode()
    )
    minute = _write(minroot / "calendars/1min.txt", b"2026-09-30 15:00:00\n")
    daymeta = _write(dayroot / "meta_export.json", {})
    minmeta = _write(minroot / "meta_export.json", {})
    assets["pit"] = _write(dayroot / "instruments/stock_universe.txt", b"000001.SZ\t2026-03-31\t2026-09-30\n")
    dataset = _write(tmp_path / "qe_dataset_manifest.json", {"TEST_FIXTURE": True})
    release = {
        "release_id": "fixture",
        "cutoff": "2026-09-30",
        "universe_key": "fixture",
        "rule_version": "fixture",
        "dataset_manifest_sha256": "a" * 64,
        "metadata_sha256": {
            "dataset_manifest": dataset["sha256"],
            "calendar": assets["calendar"]["sha256"],
            "selection_universe": assets["pit"]["sha256"],
        },
    }
    binding = {
        "candidate_root": str(tmp_path),
        "release_id": "fixture",
        "cutoff": "2026-09-30",
        "provider_uri_day": str(dayroot),
        "provider_uri_1min": str(minroot),
        "day_pins": {
            "universe_key": "fixture",
            "rule_version": "fixture",
            "calendar_sha256": assets["calendar"]["sha256"],
            "meta_export_sha256": daymeta["sha256"],
        },
        "minute_pins": {
            "universe_key": "fixture",
            "rule_version": "fixture",
            "calendar_sha256": minute["sha256"],
            "meta_export_sha256": minmeta["sha256"],
        },
        "selection_pins": {"instrument_name": "stock_universe", "instruments_sha256": assets["pit"]["sha256"]},
    }
    authority = {
        "identity": {"bundle_hash": "bundle"},
        "research_basis": {
            "active_mode": "historical_replay",
            "historical_classification_basis": "stable_taxonomy_backcast",
            "canonical_hash": "research",
        },
        "l2_projection": {"rows": [{"canonical_l2_code": x} for x in catalog], "canonical_hash": "projection"},
    }
    assets["authority"] = _write(tmp_path / "authority.json", authority)
    models = {"M0": "f" * 64}
    bundles = []
    contract = {
        "version": PRODUCER,
        "target": TARGET,
        "warning_threshold": 0.20,
        "feature_names": [f"f{i}" for i in range(20)],
    }
    for vintage, span in VINTAGES.items():
        parameters = {"fixture_only": vintage}
        models[vintage] = canonical_sha(parameters)
        bundles.append(
            _seal(
                {
                    "schema_version": PRODUCER + "_model",
                    "contract": contract,
                    "target": TARGET,
                    "catalog": catalog,
                    "parameters": parameters,
                    "model_sha256": models[vintage],
                    "bootstrap_model_sha256": models["M0"],
                    "vintage": dict(
                        zip(["id", "train_start", "train_end", "effective_start", "effective_end"], [vintage, *span])
                    ),
                }
            )
        )
    producer_release = {"dataset_manifest_sha256": "a" * 64, "fixture_only": True}
    risk_objects = {}
    for arm in ("M0", "M1"):
        rows = []
        for i, day in enumerate(days):
            for sector in catalog:
                vintage = "M0" if arm == "M0" else "2026Q2" if day.month <= 6 else "2026Q3"
                rows.append(
                    {
                        "trade_date": str(day.date()),
                        "as_of_date": str(calendar[i].date()),
                        "sector_code": sector,
                        "vintage_id": vintage,
                        "model_sha256": models[vintage],
                        "probability": 0.25,
                        "availability": "available",
                        "warning": True,
                        "reason_code": None,
                    }
                )
        obj = _seal(
            {
                "schema_version": PRODUCER + "_predictions",
                "arm": arm,
                "historical_as_known": False,
                "untouched_holdout": False,
                "research_only": True,
                "contract": contract,
                "input_identity": {"release_identity": producer_release},
                "release_binding": {"manifest_sha256": "a" * 64},
                "model_bundles": bundles if arm == "M1" else [],
                "predictions": rows,
            }
        )
        risk_objects[arm] = obj
        assets[arm] = _write(tmp_path / f"{arm}.json", obj)
    frame = pd.DataFrame(
        [
            {
                "as_of_date": str(day.date()),
                "trade_date": str(calendar[i + 1].date()) if i < 125 else None,
                "instrument": "000001.SZ",
                "mapping_status": "resolved",
                "mapping_reason": None,
                "l2_code": "L000",
                "non_as_known_taxonomy": True,
                "classification_receipt_hash": "class",
                "used_in_125_decision_days": i < 125,
            }
            for i, day in enumerate(calendar)
        ]
    )
    path = tmp_path / "mapping.parquet"
    frame.to_parquet(path)
    assets["mapping"] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    receipt = {
        "mapping_sha256": assets["mapping"]["sha256"],
        "authority_spec_sha256": assets["authority"]["sha256"],
        "membership_file_sha256": assets["pit"]["sha256"],
        "mapping_manifest": {
            "non_as_known_taxonomy": True,
            "active_classification_basis": "stable_taxonomy_backcast",
            "candidate_bundle_hash": "bundle",
            "research_basis_contract_sha256": "research",
            "l2_code_projection_sha256": "projection",
            "classification_authority_receipt_hash": "class",
        },
    }
    assets["mapping_receipt"] = _write(tmp_path / "mapping_receipt.json", receipt)
    pred = pd.DataFrame(
        {"score": np.arange(len(calendar), dtype=float)},
        index=pd.MultiIndex.from_arrays([calendar, ["000001.SZ"] * len(calendar)], names=["datetime", "instrument"]),
    )
    path = tmp_path / "prediction.pkl"
    pred.to_pickle(path)
    assets["prediction"] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    manifest = {
        "schema_version": VERSION,
        "contract": admission.CONTRACT,
        "asset_admission_version": "TEST_ONLY",
        "arm": "M0",
        "seed": 42,
        "start": "2026-04-01",
        "end": "2026-09-30",
        "decision_days": 125,
        "assets": assets,
        "models": models,
        "base_contract_sha256": "b" * 64,
        "dataset_binding": binding,
        "release_identity": release,
        "producer_release_identity": producer_release,
        "authority_canonical_sha256": canonical_sha(authority),
    }
    record = {
        **admission.identity(manifest),
        "AF_predictions_by_seed": {str(s): assets["prediction"]["sha256"] for s in [42, 2024, 2026]},
    }
    monkeypatch.setattr(admission, "ADMISSIONS", {"TEST_ONLY": record})
    import scripts.qe_l2_continuous_admission as package_admission

    monkeypatch.setattr(package_admission, "ADMISSIONS", {"TEST_ONLY": record})
    _write(tmp_path / "manifest.json", manifest)
    return types.SimpleNamespace(
        root=tmp_path, manifest=manifest, days=days, calendar=calendar, risks=risk_objects, frame=frame, record=record
    )


def validate_native(data, monkeypatch):
    """Real Qlib parent/hooks/Order/Position/lot/fee code; synthetic quoted test prices."""
    __import__("qlib")
    import logging
    from collections import defaultdict
    from qlib.backtest.exchange import Exchange
    from qlib.backtest.position import Position
    from qlib.backtest.utils import CommonInfrastructure, LevelInfrastructure
    import qe_l2_continuous_consumer as consumer

    class Calendar:
        step = 0

        def get_trade_step(self):
            return self.step

        def get_step_time(self, step=None, shift=0):
            step = self.step if step is None else step
            day = data.calendar[step + 1 - shift]
            return day, day

        def get_freq(self):
            return "day"

    exchange = object.__new__(Exchange)
    exchange.trade_w_adj_price = False
    exchange.trade_unit = 100.0
    exchange.open_cost = 0.000095
    exchange.close_cost = 0.000595
    exchange.min_cost = 5.0
    exchange.impact_cost = 0.0
    exchange.buy_vol_limit = None
    exchange.sell_vol_limit = None
    exchange.logger = logging.getLogger("real-qlib-test")
    exchange.get_deal_price = lambda *args, **kwargs: 10.0
    exchange.get_close = lambda *args, **kwargs: 10.0
    exchange.get_factor = lambda *args, **kwargs: 2.0
    exchange.get_volume = lambda *args, **kwargs: 1_000_000.0
    exchange.is_stock_tradable = lambda *args, **kwargs: True
    position = Position(cash=10000.0, position_dict={"000001.SZ": {"amount": 1000.0, "price": 10.0}})
    infra = CommonInfrastructure(
        trade_account=types.SimpleNamespace(current_position=position), trade_exchange=exchange
    )
    calendar = Calendar()
    level = LevelInfrastructure(trade_calendar=calendar)
    signal = pd.DataFrame(
        {"score": [1.0, 1.0]},
        index=pd.MultiIndex.from_arrays([data.calendar[:2], ["000001.SZ"] * 2], names=["datetime", "instrument"]),
    )
    klass = consumer.L2ContinuousScoreWeightedTopkStrategyV2
    monkeypatch.chdir(data.root)
    obj = klass(
        signal=signal,
        topk=1,
        n_drop=0,
        max_n_drop=1,
        min_n_drop=0,
        hold_thresh=0,
        max_weight=0.5,
        min_weight=0.0,
        max_position_ratio=0.5,
        enable_dynamic_ndrop=False,
        qe_l2_continuous_manifest_file=str(data.root / "manifest.json"),
        qe_l2_continuous_action_log="native_actions.jsonl",
        common_infra=infra,
        level_infra=level,
    )
    decision = obj.generate_trade_decision()
    orders = list(decision.get_decision())
    assert len(orders) == 1 and orders[0].amount == 250.0 and orders[0].direction == 0
    price, value, cost = exchange._calc_trade_info_by_order(orders[0], position, defaultdict(float))
    position.update_order(orders[0], value, cost, price)
    assert position.get_stock_amount("000001.SZ") == 750.0
    assert cost == 5.0 and position.get_cash() == 12495.0
    assert position.calculate_value() == 19995.0
    calendar.step = 1
    assert list(obj.generate_trade_decision().get_decision()) == []
    actions = [json.loads(x) for x in Path("native_actions.jsonl").read_text().splitlines()]
    assert len(actions) == 2 and actions[0]["target_exposure"] == 0.375
    assert actions[0]["planned_orders"][0]["actual_fill_claimed"] is False
    # Exercise all five runtime classes with the same real AF/Order machinery.
    from qe_l2_continuous_plan import exposure_reference

    for arm in ["B", "M1", "X0", "X1"]:
        manifest = json.loads((data.root / "manifest.json").read_text())
        manifest["arm"] = arm
        if arm in ["X0", "X1"]:
            source_arm = "M0" if arm == "X0" else "M1"
            source_manifest = {**manifest, "arm": source_arm}
            target_actions = [
                {
                    "trade_date": str(day.date()),
                    "as_of_date": str(data.calendar[i].date()),
                    "seed": 42,
                    "arm": source_arm,
                    "target_exposure": 0.375,
                    "target_weights": {"000001.SZ": 0.375},
                    "base_contract_sha256": manifest["base_contract_sha256"],
                }
                for i, day in enumerate(data.days)
            ]
            reference = exposure_reference(target_actions, source_manifest)
            manifest["assets"]["reference"] = _write(data.root / f"{arm}_exposure.json", reference)
        _write(data.root / f"{arm}_manifest.json", manifest)
        position = Position(cash=10000.0, position_dict={"000001.SZ": {"amount": 1000.0, "price": 10.0}})
        infra = CommonInfrastructure(
            trade_account=types.SimpleNamespace(current_position=position), trade_exchange=exchange
        )
        calendar = Calendar()
        level = LevelInfrastructure(trade_calendar=calendar)
        variant = klass(
            signal=signal,
            topk=1,
            n_drop=0,
            max_n_drop=1,
            min_n_drop=0,
            hold_thresh=0,
            max_weight=0.5,
            min_weight=0.0,
            max_position_ratio=0.5,
            enable_dynamic_ndrop=False,
            qe_l2_continuous_manifest_file=str(data.root / f"{arm}_manifest.json"),
            qe_l2_continuous_action_log=f"native_{arm}.jsonl",
            common_infra=infra,
            level_infra=level,
        )
        orders = list(variant.generate_trade_decision().get_decision())
        if arm == "B":
            assert orders == []
        else:
            assert len(orders) == 1 and orders[0].direction == 0 and orders[0].amount == 250.0


if __name__ == "__main__":
    import tempfile
    import os

    class Patch:
        def setattr(self, obj, name, value):
            setattr(obj, name, value)

        def chdir(self, path):
            os.chdir(path)

    original = Path.cwd()
    with tempfile.TemporaryDirectory(prefix="qe-l2-native-") as temporary:
        fixture = make_data(Path(temporary), Patch())
        try:
            validate_native(fixture, Patch())
            print(
                json.dumps(
                    {
                        "status": "PASS",
                        "oracle": "real Qlib AF parent, Order, Position, adjusted lot, exchange fee and cash closure",
                        "test_data": "explicit synthetic prices and probability fixtures; not research predictions",
                        "new_fits": 0,
                        "new_QE_experiments": 0,
                        "expected_sell_amount": 250,
                        "remaining_amount": 750,
                        "cash": 12495,
                        "account_value": 19995,
                        "fee": 5,
                        "second_day_no_compounding": True,
                        "all_five_runtime_arms": True,
                    }
                )
            )
        finally:
            os.chdir(original)
