"""Independent continuous probability consumer. No fitting, DB or network IO."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import pandas as pd

from qe_l2_continuous_admission import CONTRACT, validate_admission, validate_reference
from qe_l2_risk_consumer import checked_asset, require, symbol_key
from qe_l2_risk_admission import canonical_sha

VERSION = "qe_l2_continuous_v1"
PRODUCER = "hmm_risk_l2_quarterly_rolling_v1"
ARMS = ("B", "M0", "M1", "X0", "X1")
VINTAGES = {
    "2026Q2": ("2023-04-01", "2026-03-31", "2026-04-01", "2026-06-30"),
    "2026Q3": ("2023-07-01", "2026-06-30", "2026-07-01", "2026-09-30"),
}
TARGET = {"anchor": "decision_plus_one_open_session", "horizon": 10, "drawdown_boundary": -0.08}


def _receipt(value):
    require(
        value.get("receipt_sha256") == canonical_sha({k: v for k, v in value.items() if k != "receipt_sha256"}),
        "QE_L2_CONTINUOUS_PRODUCER_RECEIPT_INVALID",
    )


def finite(value, label, *, maximum=None):
    require(
        type(value) in (int, float) and math.isfinite(value) and value >= 0, "QE_L2_CONTINUOUS_NUMBER_INVALID", label
    )
    require(maximum is None or value <= maximum, "QE_L2_CONTINUOUS_NUMBER_INVALID", label)
    return float(value)


def adjust_targets(base, probabilities, *, reference_exposure=None):
    """Absolute AF targets, never yesterday's reduced positions or renormalization."""
    weights = {str(k): finite(v, str(k), maximum=1) for k, v in base.items()}
    total = sum(weights.values())
    require(total <= 1 + 1e-10, "QE_L2_CONTINUOUS_BASE_BUDGET_INVALID")
    if reference_exposure is not None:
        target = finite(reference_exposure, "reference_exposure", maximum=1)
        require(target <= total + 1e-10, "QE_L2_CONTINUOUS_REFERENCE_INFEASIBLE")
        scale = target / total if total > 0 else 0.0
        return {k: v * scale for k, v in weights.items()}
    require(set(weights) == set(probabilities), "QE_L2_CONTINUOUS_TARGET_POPULATION_MISMATCH")
    return {
        k: 0.0 if probabilities[k] is None else v * (1 - finite(probabilities[k], k, maximum=1))
        for k, v in weights.items()
    }


class ContinuousInputs:
    def __init__(self, manifest_file):
        self.manifest = m = json.loads(Path(manifest_file).read_text(encoding="utf-8"))
        validate_admission(m)
        require(m.get("schema_version") == VERSION and m.get("arm") in ARMS, "QE_L2_CONTINUOUS_MANIFEST_INVALID")
        require(
            (m.get("start"), m.get("end"), m.get("decision_days")) == ("2026-04-01", "2026-09-30", 125),
            "QE_L2_CONTINUOUS_WINDOW_INVALID",
        )
        paths = {k: checked_asset(v) for k, v in m["assets"].items()}
        required = {"M0", "M1", "prediction", "mapping", "mapping_receipt", "authority", "calendar", "pit"}
        require(required.issubset(paths), "QE_L2_CONTINUOUS_ASSETS_MISSING")
        self._validate_dataset(m)
        calendar = pd.DatetimeIndex(pd.to_datetime(paths["calendar"].read_text().splitlines()))
        require(calendar.is_unique and calendar.is_monotonic_increasing, "QE_L2_CONTINUOUS_CALENDAR_INVALID")
        selected = calendar[(calendar >= m["start"]) & (calendar <= m["end"])]
        require(
            len(selected) == 125
            and calendar.get_loc(selected[0]) > 0
            and str(selected[0].date()) == m["start"]
            and str(selected[-1].date()) == m["end"],
            "QE_L2_CONTINUOUS_WINDOW_INVALID",
        )
        self.as_of = {str(d.date()): str(calendar[calendar.get_loc(d) - 1].date()) for d in selected}
        authority = json.loads(paths["authority"].read_text(encoding="utf-8"))
        catalog = {r["canonical_l2_code"] for r in authority["l2_projection"]["rows"]}
        require(
            len(catalog) == 131
            and authority["research_basis"]["active_mode"] == "historical_replay"
            and authority["research_basis"]["historical_classification_basis"] == "stable_taxonomy_backcast",
            "QE_L2_CONTINUOUS_AUTHORITY_INVALID",
        )
        require(canonical_sha(authority) == m["authority_canonical_sha256"], "QE_L2_CONTINUOUS_AUTHORITY_IDENTITY")
        self.risk = {}
        for arm in ("M0", "M1"):
            risk = json.loads(paths[arm].read_text(encoding="utf-8"))
            self.risk[arm] = self._validate_risk(risk, arm, catalog, m)
        self.mapping = self._mapping(paths, authority, catalog, m)
        self.references = {}
        if m["arm"] in ("X0", "X1"):
            require("reference" in paths, "QE_L2_CONTINUOUS_REFERENCE_MISSING")
            ref = json.loads(paths["reference"].read_text(encoding="utf-8"))
            self.references = validate_reference(ref, m, self.as_of)
        self.receipt = {
            "contract": CONTRACT,
            "arm": m["arm"],
            "days": 125,
            "catalog_count": 131,
            "risk_rows_per_model": 16375,
            "fits": 0,
            "assets": m["assets"],
            "historical_as_known": False,
            "untouched_holdout": False,
        }

    @staticmethod
    def _validate_dataset(m):
        b, r = m["dataset_binding"], m["release_identity"]
        require(
            b["release_id"] == r["release_id"]
            and b["cutoff"] == r["cutoff"]
            and b["day_pins"]["universe_key"] == r["universe_key"]
            and b["day_pins"]["rule_version"] == r["rule_version"],
            "QE_L2_CONTINUOUS_BINDING_INVALID",
        )
        require(
            m["assets"]["calendar"]["sha256"] == b["day_pins"]["calendar_sha256"] == r["metadata_sha256"]["calendar"]
            and m["assets"]["pit"]["sha256"]
            == b["selection_pins"]["instruments_sha256"]
            == r["metadata_sha256"]["selection_universe"],
            "QE_L2_CONTINUOUS_PIT_INVALID",
        )
        checked_asset(
            {
                "path": str(Path(b["candidate_root"]) / "qe_dataset_manifest.json"),
                "sha256": r["metadata_sha256"]["dataset_manifest"],
            }
        )
        for pins, provider, freq in [
            ("day_pins", "provider_uri_day", "day"),
            ("minute_pins", "provider_uri_1min", "1min"),
        ]:
            require(
                b[pins]["universe_key"] == r["universe_key"] and b[pins]["rule_version"] == r["rule_version"],
                "QE_L2_CONTINUOUS_BINDING_INVALID",
            )
            for name, sha in [
                (f"calendars/{freq}.txt", b[pins]["calendar_sha256"]),
                ("meta_export.json", b[pins]["meta_export_sha256"]),
            ]:
                checked_asset({"path": str(Path(b[provider]) / name), "sha256": sha})
        checked_asset(
            {
                "path": str(Path(b["provider_uri_day"]) / "instruments/stock_universe.txt"),
                "sha256": m["assets"]["pit"]["sha256"],
            }
        )

    def _validate_risk(self, risk, arm, catalog, m):
        _receipt(risk)
        require(
            risk["schema_version"] == PRODUCER + "_predictions"
            and risk["arm"] == arm
            and risk["historical_as_known"] is False
            and risk["untouched_holdout"] is False
            and risk["research_only"] is True,
            "QE_L2_CONTINUOUS_PRODUCER_INVALID",
        )
        contract = risk["contract"]
        require(
            contract["version"] == PRODUCER
            and contract["target"] == TARGET
            and contract["warning_threshold"] == 0.20
            and len(contract["feature_names"]) == len(set(contract["feature_names"])) == 20,
            "QE_L2_CONTINUOUS_MODEL_CONTRACT_INVALID",
        )
        require(
            risk["input_identity"]["release_identity"] == m["producer_release_identity"]
            and risk["input_identity"]["calendar_sha256"] == m["assets"]["calendar"]["sha256"]
            and risk["input_identity"]["industry_authority_sha256"] == m["authority_canonical_sha256"]
            and risk["release_binding"]["manifest_sha256"] == m["release_identity"]["dataset_manifest_sha256"],
            "QE_L2_CONTINUOUS_PRODUCER_RELEASE_MISMATCH",
        )
        bundles = risk["model_bundles"]
        require(len(bundles) == (2 if arm == "M1" else 0), "QE_L2_CONTINUOUS_VINTAGE_INVALID")
        for bundle in bundles:
            _receipt(bundle)
            vintage = bundle["vintage"]
            key = vintage["id"]
            require(
                key in VINTAGES
                and tuple(vintage[k] for k in ["train_start", "train_end", "effective_start", "effective_end"])
                == VINTAGES[key],
                "QE_L2_CONTINUOUS_VINTAGE_INVALID",
            )
            require(
                bundle["schema_version"] == PRODUCER + "_model"
                and bundle["contract"] == contract
                and bundle["target"] == TARGET
                and bundle["catalog"] == sorted(catalog)
                and bundle["model_sha256"] == canonical_sha(bundle["parameters"]) == m["models"][key]
                and bundle["bootstrap_model_sha256"] == m["models"]["M0"],
                "QE_L2_CONTINUOUS_MODEL_IDENTITY_MISMATCH",
            )
            require(
                bundle["training_input_identity"] == risk["input_identity"]
                and bundle["training_release_binding"] == risk["release_binding"],
                "QE_L2_CONTINUOUS_TRAINING_INPUT_IDENTITY",
            )
        require(
            {b["vintage"]["id"] for b in bundles} == (set(VINTAGES) if arm == "M1" else set()),
            "QE_L2_CONTINUOUS_VINTAGE_INVALID",
        )
        rows = {}
        for row in risk["predictions"]:
            day, sector = row["trade_date"], row["sector_code"]
            key = day, sector
            require(
                day in self.as_of and row["as_of_date"] == self.as_of[day] and sector in catalog and key not in rows,
                "QE_L2_CONTINUOUS_RISK_KEY_INVALID",
            )
            vintage = "M0" if arm == "M0" else "2026Q2" if day <= "2026-06-30" else "2026Q3"
            require(
                row["vintage_id"] == vintage and row["model_sha256"] == m["models"][vintage],
                "QE_L2_CONTINUOUS_ROW_MODEL_INVALID",
            )
            if row["availability"] == "available":
                probability = finite(row["probability"], "probability", maximum=1)
                require(
                    row["warning"] is (probability >= 0.20) and row["reason_code"] is None,
                    "QE_L2_CONTINUOUS_WARNING_INVALID",
                )
            else:
                require(
                    row["availability"] == "unavailable"
                    and row["probability"] is None
                    and row["warning"] is None
                    and isinstance(row["reason_code"], str)
                    and bool(row["reason_code"].strip()),
                    "QE_L2_CONTINUOUS_NA_INVALID",
                )
            rows[key] = row
        require(set(rows) == {(d, s) for d in self.as_of for s in catalog}, "QE_L2_CONTINUOUS_RISK_GRID_INVALID")
        return rows

    def _mapping(self, paths, authority, catalog, m):
        receipt = json.loads(paths["mapping_receipt"].read_text(encoding="utf-8"))
        mm = receipt["mapping_manifest"]
        require(
            receipt["mapping_sha256"] == m["assets"]["mapping"]["sha256"]
            and receipt["authority_spec_sha256"] == m["assets"]["authority"]["sha256"]
            and receipt["membership_file_sha256"] == m["assets"]["pit"]["sha256"],
            "QE_L2_CONTINUOUS_MAPPING_IDENTITY",
        )
        require(
            mm["non_as_known_taxonomy"] is True
            and mm["active_classification_basis"] == "stable_taxonomy_backcast"
            and mm["candidate_bundle_hash"] == authority["identity"]["bundle_hash"]
            and mm["research_basis_contract_sha256"] == authority["research_basis"]["canonical_hash"]
            and mm["l2_code_projection_sha256"] == authority["l2_projection"]["canonical_hash"],
            "QE_L2_CONTINUOUS_MAPPING_AUTHORITY",
        )
        frame = pd.read_parquet(paths["mapping"])
        for col in ["as_of_date", "trade_date"]:
            frame[col] = pd.to_datetime(frame[col]).dt.strftime("%Y-%m-%d")
        require(not frame.duplicated(["as_of_date", "instrument"]).any(), "QE_L2_CONTINUOUS_MAPPING_DUPLICATE")
        resolved, unavailable = frame.mapping_status.eq("resolved"), frame.mapping_status.eq("unavailable")
        require(
            (resolved | unavailable).all() and frame.loc[resolved, "l2_code"].isin(catalog).all(),
            "QE_L2_CONTINUOUS_MAPPING_CODE",
        )
        require(
            frame.loc[unavailable, "l2_code"].isna().all()
            and frame.loc[unavailable, "mapping_reason"]
            .eq("classification:classification_authority_unavailable")
            .all(),
            "QE_L2_CONTINUOUS_MAPPING_NA",
        )
        require(
            frame.non_as_known_taxonomy.eq(True).all()
            and frame.classification_receipt_hash.eq(mm["classification_authority_receipt_hash"]).all(),
            "QE_L2_CONTINUOUS_MAPPING_LINEAGE",
        )
        used = frame.used_in_125_decision_days.eq(True)
        require(
            frame.loc[used, "trade_date"].isin(self.as_of).all()
            and frame.loc[used, "trade_date"].map(self.as_of).eq(frame.loc[used, "as_of_date"]).all()
            and frame.loc[~used, "as_of_date"].eq(m["end"]).all()
            and frame.loc[~used, "trade_date"].isna().all(),
            "QE_L2_CONTINUOUS_MAPPING_CLOCK",
        )
        pred = pd.read_pickle(paths["prediction"])
        require(
            isinstance(pred, pd.DataFrame)
            and list(pred.columns) == ["score"]
            and pred.index.names == ["datetime", "instrument"]
            and pred.index.is_unique
            and pred.score.map(lambda x: math.isfinite(float(x))).all(),
            "QE_L2_CONTINUOUS_PREDICTION_INVALID",
        )
        require(
            set(pred.index.get_level_values("datetime").strftime("%Y-%m-%d")) == set(self.as_of.values()) | {m["end"]},
            "QE_L2_CONTINUOUS_PREDICTION_DATE_GRID",
        )
        keys = pd.MultiIndex.from_arrays(
            [pred.index.get_level_values("datetime").strftime("%Y-%m-%d"), pred.index.get_level_values("instrument")]
        )
        mapped = pd.MultiIndex.from_frame(frame[["as_of_date", "instrument"]])
        require(len(keys) == len(mapped) and keys.isin(mapped).all(), "QE_L2_CONTINUOUS_MAPPING_POPULATION")
        return frame.set_index(["as_of_date", "instrument"])

    def probability(self, instrument, day):
        require(day in self.as_of, "QE_L2_CONTINUOUS_DECISION_INVALID")
        key = self.as_of[day], symbol_key(instrument)
        require(key in self.mapping.index, "QE_L2_CONTINUOUS_MAPPING_MISSING", str(key))
        mapping = self.mapping.loc[key]
        if mapping.mapping_status == "unavailable":
            return None, {"availability": "unavailable", "reason_code": mapping.mapping_reason, "sector_code": None}
        arm = "M0" if self.manifest["arm"] in ("M0", "X0") else "M1"
        row = self.risk[arm][day, mapping.l2_code]
        return row["probability"], row


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest")
    parser.add_argument("--config", required=True)
    parser.add_argument("--prediction", required=True)
    args = parser.parse_args()
    inputs = ContinuousInputs(args.manifest)
    from qe_l2_risk_consumer import validate_execution_inputs

    validate_execution_inputs(inputs.manifest, args.config, args.prediction)
    print(json.dumps(inputs.receipt, ensure_ascii=False))


class ContinuousMixin:
    """AF target hook plus auditable held-position adjustments; opt-in only."""

    def __init__(
        self,
        *args,
        qe_l2_continuous_manifest_file=None,
        qe_l2_continuous_action_log="qe_l2_continuous_actions.jsonl",
        **kwargs,
    ):
        require(qe_l2_continuous_manifest_file is not None, "QE_L2_CONTINUOUS_MANIFEST_REQUIRED")
        self._continuous_inputs = ContinuousInputs(qe_l2_continuous_manifest_file)
        self._continuous_log = Path(qe_l2_continuous_action_log)
        require(
            not self._continuous_log.is_absolute() and ".." not in self._continuous_log.parts,
            "QE_L2_CONTINUOUS_LOG_PATH_INVALID",
        )
        self._continuous_seen = set()
        self._continuous_reduced = set()
        self._continuous_day = None
        super().__init__(*args, **kwargs)

    def _adjust_target_weight_map(self, weight_map, trade_start_time):
        base = super()._adjust_target_weight_map(weight_map, trade_start_time)
        inputs = self._continuous_inputs
        day = str(pd.Timestamp(trade_start_time).date())
        arm = inputs.manifest["arm"]
        require(day in inputs.as_of, "QE_L2_CONTINUOUS_DECISION_INVALID")
        probabilities = {}
        details = {}
        if arm in ("M0", "M1"):
            for stock in base:
                p, row = inputs.probability(stock, day)
                details[stock] = row
                # Typed NA means zero allocation, not an imputed probability.
                probabilities[stock] = p
            target = adjust_targets(base, probabilities)
        elif arm in ("X0", "X1"):
            target = adjust_targets(base, {}, reference_exposure=inputs.references[day])
        else:
            target = adjust_targets(base, {stock: 0.0 for stock in base})
        if arm in ("M0", "M1"):
            multipliers = {s: 0.0 if probabilities[s] is None else 1.0 - probabilities[s] for s in base}
        elif arm in ("X0", "X1"):
            scale = inputs.references[day] / sum(base.values()) if sum(base.values()) > 0 else 0.0
            multipliers = {s: scale for s in base}
        else:
            multipliers = {s: 1.0 for s in base}
        self._continuous_reduced.update(s for s, multiplier in multipliers.items() if multiplier < 1.0)
        self._continuous_day = {
            "trade_date": day,
            "as_of_date": inputs.as_of[day],
            "arm": arm,
            "seed": inputs.manifest["seed"],
            "base_contract_sha256": inputs.manifest["base_contract_sha256"],
            "base_targets": dict(base),
            "target_weights": target,
            "target_exposure": sum(target.values()),
            "target_cash": 1 - sum(target.values()),
            "risk_inputs": details,
            "multipliers": multipliers,
            "rebalance": [],
        }
        return target

    def _build_additional_rebalance_orders(
        self,
        *,
        weight_map,
        current_holdings,
        existing_sell_ids,
        planned_buy_orders,
        total_account_value,
        trade_step,
        trade_start_time,
        trade_end_time,
    ):
        from qlib.backtest.decision import Order, OrderDir

        require(self._continuous_day is not None, "QE_L2_CONTINUOUS_TARGET_STATE_MISSING")
        arm = self._continuous_inputs.manifest["arm"]
        if arm == "B":
            return []
        finite(total_account_value, "account_value")
        planned = {str(o.stock_id) for o in planned_buy_orders} | set(existing_sell_ids)
        orders = []
        reserved_buy_cash = 0.0
        open_cost = finite(float(self.trade_exchange.open_cost), "open_cost")
        min_cost = finite(float(self.trade_exchange.min_cost), "min_cost")
        for planned_order in planned_buy_orders:
            price = self._get_current_price(planned_order.stock_id, trade_step, OrderDir.BUY)
            require(
                price is not None and math.isfinite(float(price)) and float(price) > 0,
                "QE_L2_CONTINUOUS_PLANNED_PRICE_INVALID",
            )
            value = finite(float(planned_order.amount), "planned_amount") * float(price)
            reserved_buy_cash += value + max(value * open_cost, min_cost) if value > 0 else 0.0
        for stock in sorted(current_holdings):
            if stock in planned or stock not in weight_map:
                continue
            multiplier = self._continuous_day["multipliers"][stock]
            if multiplier == 1.0 and stock not in self._continuous_reduced:
                continue
            target = finite(weight_map[stock], stock, maximum=1) * total_account_value
            price = self._get_current_price(stock, trade_step, OrderDir.SELL)
            amount = self.trade_position.get_stock_amount(stock)
            event = {
                "instrument": stock,
                "target_value": target,
                "target_multiplier": multiplier,
                "order_generated": False,
                "actual_fill_claimed": False,
            }
            self._continuous_day["rebalance"].append(event)
            if price is None:
                event["reason"] = "QUOTE_UNAVAILABLE"
                continue
            require(
                math.isfinite(float(price)) and float(price) > 0 and amount is not None,
                "QE_L2_CONTINUOUS_POSITION_INVALID",
            )
            amount = finite(float(amount), "held_amount")
            current = amount * float(price)
            delta = target - current
            if abs(delta) < 1e-8:
                event["reason"] = "ALREADY_AT_TARGET"
                continue
            direction = OrderDir.BUY if delta > 0 else OrderDir.SELL
            if direction == OrderDir.SELL and multiplier == 1.0:
                event["reason"] = "NEUTRAL_NO_EXTRA_SELL"
                self._continuous_reduced.discard(stock)
                continue
            if direction == OrderDir.SELL and not self._can_sell_under_hold_thresh(stock, trade_start_time):
                event["reason"] = "HOLD_THRESH"
                continue
            if not self._is_orderable_without_warning(stock, trade_start_time, trade_end_time, direction):
                event["reason"] = "NOT_TRADABLE"
                continue
            price = self._get_current_price(stock, trade_step, direction)
            require(
                price is not None and math.isfinite(float(price)) and float(price) > 0, "QE_L2_CONTINUOUS_PRICE_INVALID"
            )
            factor = self._get_current_factor(stock, trade_step)
            require(
                factor is not None and math.isfinite(float(factor)) and float(factor) > 0,
                "QE_L2_CONTINUOUS_FACTOR_INVALID",
            )
            value = abs(delta)
            if direction == OrderDir.BUY:
                if not self.min_trade_price <= price <= self.max_trade_price:
                    event["reason"] = "PRICE_BOUND"
                    continue
                available = finite(float(self.trade_position.get_cash()), "cash") - reserved_buy_cash
                value = min(
                    value,
                    float(self.max_single_order_value),
                    max(0.0, available - min_cost),
                    max(0.0, available / (1 + open_cost)),
                )
            # Position amounts and exchange prices already form Qlib value units.
            # The exchange returns the adjusted lot; do not divide by factor twice.
            quantity = value / float(price)
            unit = self.trade_exchange.get_amount_of_trade_unit(
                factor=factor, stock_id=stock, start_time=trade_start_time, end_time=trade_end_time
            )
            require(
                unit is not None and math.isfinite(float(unit)) and float(unit) > 0,
                "QE_L2_CONTINUOUS_TRADE_UNIT_INVALID",
            )
            quantity = float(math.floor(quantity / float(unit)) * float(unit))
            if direction == OrderDir.SELL:
                quantity = min(quantity, amount)
            if quantity <= 0:
                event["reason"] = "BELOW_TRADE_UNIT_OR_CASH"
                continue
            orders.append(Order(stock, quantity, direction, trade_start_time, trade_end_time))
            if direction == OrderDir.BUY:
                value = quantity * float(price)
                reserved_buy_cash += value + max(value * open_cost, min_cost)
            else:
                self._continuous_reduced.add(stock)
            event.update(order_generated=True, direction=int(direction), amount=quantity, reason="TARGET_DIFFERENCE")
        return orders

    def generate_trade_decision(self, execute_result=None):
        inputs = self._continuous_inputs
        step = self.trade_calendar.get_trade_step()
        day = str(pd.Timestamp(self.trade_calendar.get_step_time(step)[0]).date())
        require(day in inputs.as_of and day not in self._continuous_seen, "QE_L2_CONTINUOUS_DUPLICATE_OR_UNKNOWN_DAY")
        self._continuous_day = None
        decision = super().generate_trade_decision(execute_result)
        if self._continuous_day is None:
            require(not self.trade_position.get_stock_list(), "QE_L2_CONTINUOUS_BASE_TARGET_UNAVAILABLE_WITH_HOLDINGS")
            require(
                inputs.manifest["arm"] not in ("X0", "X1") or inputs.references[day] == 0,
                "QE_L2_CONTINUOUS_REFERENCE_INFEASIBLE",
            )
            self._continuous_day = {
                "trade_date": day,
                "as_of_date": inputs.as_of[day],
                "arm": inputs.manifest["arm"],
                "seed": inputs.manifest["seed"],
                "base_contract_sha256": inputs.manifest["base_contract_sha256"],
                "base_targets": {},
                "target_weights": {},
                "target_exposure": 0.0,
                "target_cash": 1.0,
                "risk_inputs": {},
                "multipliers": {},
                "rebalance": [],
            }
        self._continuous_day["planned_orders"] = [
            {
                "instrument": str(o.stock_id),
                "amount": float(o.amount),
                "direction": int(o.direction),
                "actual_fill_claimed": False,
            }
            for o in decision.get_decision()
        ]
        with self._continuous_log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(self._continuous_day, ensure_ascii=False, allow_nan=False) + "\n")
        self._continuous_seen.add(day)
        return decision


def __getattr__(name):
    if name == "L2ContinuousScoreWeightedTopkStrategyV2":
        from qe_suspend_filter_score_weighted_strategy import SuspendFilterScoreWeightedTopkStrategyV2

        class L2ContinuousScoreWeightedTopkStrategyV2(ContinuousMixin, SuspendFilterScoreWeightedTopkStrategyV2):
            pass

        return L2ContinuousScoreWeightedTopkStrategyV2
    raise AttributeError(name)


if __name__ == "__main__":
    main()
