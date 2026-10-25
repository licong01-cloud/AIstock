"""Opt-in QE L2 entry gate. Frozen research assets; no fitting or data access API."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import pandas as pd

CONTRACT = "QE-L2-RISK-ENTRY-GATE-D1..D7"
POLICY = "qe_l2_risk_entry_gate_v1"
THRESHOLD = 0.20
APPROVED_RISK_SHA256 = "5e6a2784a304b2a34cb8cbf6859aab0a9c154a29ba5fac246ff35eebcd9ca0e5"


class L2ContractError(ValueError):
    def __init__(self, code, detail=""):
        self.reason_code = code
        super().__init__(f"{code}: {detail}")


def require(condition, code, detail=""):
    if not condition:
        raise L2ContractError(code, detail)


def checked_asset(ref):
    path = Path(ref["path"])
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    digest = hasher.hexdigest()
    require(digest == ref["sha256"], "QE_L2_ASSET_HASH_MISMATCH", str(path))
    return path


def canonical_sha(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def symbol_key(value):
    value = str(value).upper()
    if len(value) == 8 and value[:2] in {"SH", "SZ"}:
        value = f"{value[2:]}.{value[:2]}"
    return value


def validate_frozen_dataset(manifest):
    """Verify physical release metadata, including the actual minute calendar."""
    b, release, assets = manifest["dataset_binding"], manifest["release_identity"], manifest["assets"]
    require(assets["risk"]["sha256"] == APPROVED_RISK_SHA256, "QE_L2_UNAPPROVED_RISK_ASSET")
    require(b["release_id"] == release["release_id"] and b["cutoff"] == release["cutoff"]
            and b["day_pins"]["universe_key"] == release["universe_key"]
            and b["day_pins"]["rule_version"] == release["rule_version"]
            and assets["calendar"]["sha256"] == b["day_pins"]["calendar_sha256"]
            == release["metadata_sha256"]["calendar"]
            and assets["pit"]["sha256"] == b["selection_pins"]["instruments_sha256"]
            == release["metadata_sha256"]["selection_universe"], "QE_L2_DATASET_BINDING_MISMATCH")
    checked_asset({"path": str(Path(b["candidate_root"]) / "qe_dataset_manifest.json"),
                   "sha256": release["metadata_sha256"]["dataset_manifest"]})
    for kind, provider, freq in (("day_pins", "provider_uri_day", "day"),
                                 ("minute_pins", "provider_uri_1min", "1min")):
        root = Path(b[provider])
        require(b[kind]["universe_key"] == release["universe_key"]
                and b[kind]["rule_version"] == release["rule_version"], "QE_L2_DATASET_BINDING_MISMATCH")
        checked_asset({"path": str(root / "calendars" / f"{freq}.txt"), "sha256": b[kind]["calendar_sha256"]})
        checked_asset({"path": str(root / "meta_export.json"), "sha256": b[kind]["meta_export_sha256"]})
    checked_asset({"path": str(Path(b["provider_uri_day"]) / "instruments" / "stock_universe.txt"),
                   "sha256": assets["pit"]["sha256"]})


def validate_execution_inputs(manifest, config_file, prediction_file):
    """Fail before Qlib starts if the actual replay differs from the manifest."""
    import yaml

    conf = yaml.safe_load(Path(config_file).read_text(encoding="utf-8"))
    b = manifest["dataset_binding"]
    require(conf["qlib_init"]["provider_uri"] == {
        "day": b["provider_uri_day"], "1min": b["provider_uri_1min"]
    } and conf.get("market") == b["selection_pins"]["instrument_name"], "QE_L2_EXECUTION_PROVIDER_MISMATCH")
    port = conf["port_analysis_config"]
    require(str(port["backtest"]["start_time"]) == manifest["start"]
            and str(port["backtest"]["end_time"]) == manifest["end"], "QE_L2_EXECUTION_WINDOW_MISMATCH")
    checked_asset({"path": str(prediction_file), "sha256": manifest["assets"]["prediction"]["sha256"]})


class L2RiskInputs:
    """Read and verify the complete population before any order can be gated."""

    def __init__(self, manifest_file):
        self.manifest = json.loads(Path(manifest_file).read_text(encoding="utf-8"))
        m = self.manifest
        require(m.get("schema_version") == "qe_l2_risk_consumer_v1"
                and m.get("contract") == CONTRACT and m.get("policy") == POLICY,
                "QE_L2_CONTRACT_INVALID")
        require(m.get("start") == "2026-04-01" and m.get("end") == "2026-09-30"
                and m.get("decision_days") == 125, "QE_L2_WINDOW_INVALID")
        require(m.get("threshold") == THRESHOLD, "QE_L2_THRESHOLD_INVALID")
        assets = {name: checked_asset(ref) for name, ref in m["assets"].items()}
        required = {"risk", "mapping", "mapping_receipt", "authority", "prediction", "calendar", "pit"}
        require(required.issubset(assets), "QE_L2_ASSETS_MISSING")
        validate_frozen_dataset(m)
        calendar = pd.DatetimeIndex(pd.to_datetime(assets["calendar"].read_text().splitlines()))
        require(calendar.is_unique and calendar.is_monotonic_increasing, "QE_L2_CALENDAR_INVALID")
        selected = calendar[(calendar >= m["start"]) & (calendar <= m["end"])]
        require(len(selected) == 125 and str(selected[-1].date()) == m["end"], "QE_L2_WINDOW_INVALID")
        self.as_of = {}
        for day in selected:
            pos = calendar.get_loc(day)
            require(pos > 0, "QE_L2_PREVIOUS_DATE_MISSING")
            self.as_of[str(day.date())] = str(calendar[pos - 1].date())
        self.authority = json.loads(assets["authority"].read_text(encoding="utf-8"))
        require(canonical_sha(self.authority) == m["authority_canonical_sha256"],
                "QE_L2_AUTHORITY_IDENTITY_MISMATCH")
        require(self.authority["research_basis"]["active_mode"] == "historical_replay"
                and self.authority["research_basis"]["historical_classification_basis"] == "stable_taxonomy_backcast",
                "QE_L2_RESEARCH_BASIS_INVALID")
        catalog = {r["canonical_l2_code"] for r in self.authority["l2_projection"]["rows"]}
        require(len(catalog) == 131, "QE_L2_CATALOG_INVALID")
        risk = json.loads(assets["risk"].read_text(encoding="utf-8"))
        require(risk.get("schema_version") == "hmm_risk_l2_date_inference_v1_prediction"
                and risk.get("warning_threshold") == THRESHOLD
                and risk.get("policy_applied") is False, "QE_L2_RISK_SCHEMA_INVALID")
        require(risk["inference_identity"]["calendar_sha256"] == m["assets"]["calendar"]["sha256"]
                and risk["inference_identity"]["industry_authority_sha256"] == m["authority_canonical_sha256"],
                "QE_L2_RISK_IDENTITY_MISMATCH")
        require(risk["inference_identity"]["release_identity"] == m["release_identity"],
                "QE_L2_RELEASE_IDENTITY_MISMATCH")
        self.risk = {}
        for row in risk["predictions"]:
            day, sector = row["trade_date"], row["sector_code"]
            key = (day, sector)
            require(day in self.as_of and row["as_of_date"] == self.as_of[day], "QE_L2_RISK_TIME_INVALID")
            require(sector in catalog and key not in self.risk, "QE_L2_RISK_KEY_INVALID", str(key))
            p = row["probability"]
            if row["availability"] == "available":
                require(type(p) in (float, int) and math.isfinite(p) and 0 <= p <= 1,
                        "QE_L2_PROBABILITY_INVALID")
                require(row["warning"] is (p >= THRESHOLD) and row["reason_code"] is None,
                        "QE_L2_WARNING_INVALID")
            elif row["availability"] == "unavailable":
                require(p is None and row["warning"] is None
                        and isinstance(row["reason_code"], str) and bool(row["reason_code"].strip()),
                        "QE_L2_UNAVAILABLE_INVALID")
            else:
                raise L2ContractError("QE_L2_AVAILABILITY_INVALID")
            self.risk[key] = row
        require(set(self.risk) == {(d, s) for d in self.as_of for s in catalog}, "QE_L2_RISK_ROWS_MISSING")
        dates = risk["dates"]
        require(len(dates) == 125 and {d["trade_date"] for d in dates} == set(self.as_of),
                "QE_L2_DATE_SUMMARY_INVALID")
        for row in dates:
            day = row["trade_date"]
            available = sum(self.risk[day, s]["availability"] == "available" for s in catalog)
            require(row["as_of_date"] == self.as_of[day] and row["catalog_count"] == 131
                    and row["available_count"] == available and row["unavailable_count"] == 131 - available,
                    "QE_L2_DATE_SUMMARY_INVALID")
        receipt = json.loads(assets["mapping_receipt"].read_text(encoding="utf-8"))
        require(receipt["mapping_sha256"] == m["assets"]["mapping"]["sha256"]
                and receipt["authority_spec_sha256"] == m["assets"]["authority"]["sha256"]
                and receipt["membership_file_sha256"] == m["assets"]["pit"]["sha256"],
                "QE_L2_MAPPING_IDENTITY_MISMATCH")
        mm = receipt["mapping_manifest"]
        require(mm["non_as_known_taxonomy"] is True
                and mm["active_classification_basis"] == "stable_taxonomy_backcast"
                and mm["candidate_bundle_hash"] == self.authority["identity"]["bundle_hash"]
                and mm["research_basis_contract_sha256"] == self.authority["research_basis"]["canonical_hash"]
                and mm["l2_code_projection_sha256"] == self.authority["l2_projection"]["canonical_hash"],
                "QE_L2_MAPPING_BASIS_MISMATCH")
        frame = pd.read_parquet(assets["mapping"])
        frame["as_of_date"] = pd.to_datetime(frame["as_of_date"]).dt.strftime("%Y-%m-%d")
        frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y-%m-%d")
        require(not frame.duplicated(["as_of_date", "instrument"]).any(), "QE_L2_MAPPING_DUPLICATE")
        resolved = frame["mapping_status"].eq("resolved")
        unavailable = frame["mapping_status"].eq("unavailable")
        require((resolved | unavailable).all(), "QE_L2_MAPPING_STATUS_INVALID")
        require(frame.loc[resolved, "l2_code"].isin(catalog).all(), "QE_L2_MAPPING_CODE_INVALID")
        require(frame.loc[unavailable, "l2_code"].isna().all()
                and frame.loc[unavailable, "mapping_reason"].eq("classification:classification_authority_unavailable").all(),
                "QE_L2_MAPPING_UNAVAILABLE_INVALID")
        require(frame["non_as_known_taxonomy"].eq(True).all()
                and frame["classification_receipt_hash"].eq(mm["classification_authority_receipt_hash"]).all(),
                "QE_L2_MAPPING_LINEAGE_INVALID")
        used = frame["used_in_125_decision_days"].eq(True)
        require(frame.loc[used, "trade_date"].isin(self.as_of).all(), "QE_L2_MAPPING_TIME_INVALID")
        require(frame.loc[used, "trade_date"].map(self.as_of).eq(frame.loc[used, "as_of_date"]).all(),
                "QE_L2_MAPPING_TIME_INVALID")
        require(frame.loc[~used, "as_of_date"].eq(m["end"]).all()
                and frame.loc[~used, "trade_date"].isna().all(), "QE_L2_UNUSED_SIGNAL_INVALID")
        # Pickles are read only after matching the approved content hash.
        pred = pd.read_pickle(assets["prediction"])
        require(isinstance(pred, pd.DataFrame) and list(pred.columns) == ["score"]
                and pred.index.names == ["datetime", "instrument"] and pred.index.is_unique,
                "QE_L2_BASE_PREDICTION_INVALID")
        pred_keys = pd.MultiIndex.from_arrays([
            pred.index.get_level_values("datetime").strftime("%Y-%m-%d"),
            pred.index.get_level_values("instrument")], names=["as_of_date", "instrument"])
        mapping_keys = pd.MultiIndex.from_frame(frame[["as_of_date", "instrument"]])
        require(len(pred_keys) == len(mapping_keys) and pred_keys.isin(mapping_keys).all(),
                "QE_L2_MAPPING_POPULATION_MISMATCH")
        self.mapping = frame.set_index(["as_of_date", "instrument"])
        self.receipt = {"contract": CONTRACT, "policy": POLICY, "risk_rows": len(self.risk),
                        "mapping_rows": len(frame), "base_prediction_rows": len(pred),
                        "decision_days": len(self.as_of), "full_catalog_count": len(catalog),
                        "assets": m["assets"], "non_as_known_taxonomy": True,
                        "fits": 0, "display_topk_used": False}

    def evaluate_buy(self, instrument, trade_date, as_of_date):
        day = str(pd.Timestamp(trade_date).date())
        as_of = str(pd.Timestamp(as_of_date).date())
        require(self.as_of.get(day) == as_of, "QE_L2_DECISION_TIME_INVALID")
        key = (as_of, symbol_key(instrument))
        require(key in self.mapping.index, "QE_L2_ORDER_MAPPING_MISSING", str(key))
        row = self.mapping.loc[key]
        context = {"trade_date": day, "as_of_date": as_of, "instrument": str(instrument),
                   "mapping_status": row["mapping_status"], "mapping_reason": row["mapping_reason"],
                   "sector_code": None, "availability": None, "reason_code": None, "probability": None, "warning": None}
        if row["mapping_status"] == "unavailable":
            return False, "INPUT_UNAVAILABLE_BLOCK", context
        risk = self.risk[day, row["l2_code"]]
        context.update({k: risk[k] for k in ("sector_code", "availability", "reason_code", "probability", "warning")})
        if risk["availability"] == "unavailable":
            return False, "INPUT_UNAVAILABLE_BLOCK", context
        allowed = risk["probability"] < THRESHOLD
        return allowed, "ALLOW" if allowed else "RISK_WARNING_BLOCK", context


class L2EntryGateMixin:
    """Filter only generated BUY orders; never modify scores, amounts or positions."""

    def __init__(self, *args, qe_l2_risk_enabled=False,
                 qe_l2_manifest_file="qe_l2_risk_manifest.json",
                 qe_l2_action_log="qe_l2_risk_actions.jsonl", **kwargs):
        require(type(qe_l2_risk_enabled) is bool, "QE_L2_ENABLED_INVALID")
        self._l2_enabled = qe_l2_risk_enabled
        self._l2_inputs = L2RiskInputs(qe_l2_manifest_file) if self._l2_enabled else None
        self._l2_log = Path(qe_l2_action_log)
        require(not self._l2_log.is_absolute() and ".." not in self._l2_log.parts,
                "QE_L2_LOG_PATH_INVALID")
        self._l2_seen_dates = set()
        super().__init__(*args, **kwargs)
        if self._l2_enabled:
            require(not self._l2_log.exists(), "QE_L2_LEDGER_ALREADY_EXISTS")
            Path("qe_l2_consumer_read_receipt.json").write_text(
                json.dumps(self._l2_inputs.receipt, ensure_ascii=False, indent=2), encoding="utf-8")

    def generate_trade_decision(self, execute_result=None):
        decision = super().generate_trade_decision(execute_result)
        if not self._l2_enabled:
            return decision
        from qlib.backtest.decision import OrderDir
        step = self.trade_calendar.get_trade_step()
        start, _ = self.trade_calendar.get_step_time(step)
        as_of, _ = self.trade_calendar.get_step_time(step, shift=1)
        day = str(pd.Timestamp(start).date())
        require(self._l2_inputs.as_of.get(day) == str(pd.Timestamp(as_of).date()), "QE_L2_DECISION_TIME_INVALID")
        require(day not in self._l2_seen_dates, "QE_L2_DUPLICATE_DECISION")
        self._l2_seen_dates.add(day)
        original = list(decision.get_decision())
        kept, actions = [], []
        for order in original:
            if order.direction != OrderDir.BUY:
                kept.append(order)
                continue
            allow, reason, context = self._l2_inputs.evaluate_buy(order.stock_id, start, as_of)
            actions.append({**context, "action": reason, "planned_amount": float(order.amount),
                            "retained_amount": float(order.amount) if allow else 0.0,
                            "planned_order_is_fill": False})
            if allow:
                kept.append(order)
        # Preserve the existing decision's time range and metadata; SELL and
        # allowed BUY objects remain exactly the objects created by AF.
        decision.order_list = kept
        with self._l2_log.open("a", encoding="utf-8") as out:
            out.write(json.dumps({"trade_date": day, "as_of_date": str(pd.Timestamp(as_of).date()),
                                  "original_orders": len(original), "retained_orders": len(kept),
                                  "buy_actions": actions}, ensure_ascii=False, allow_nan=False) + "\n")
        return decision


def __getattr__(name):
    # Keep the validated reader importable on the Windows control plane where
    # Qlib is absent. Qlib resolves this class only inside a worker.
    if name == "L2RiskEntryGateScoreWeightedTopkStrategyV2":
        from qe_suspend_filter_score_weighted_strategy import SuspendFilterScoreWeightedTopkStrategyV2

        class L2RiskEntryGateScoreWeightedTopkStrategyV2(L2EntryGateMixin, SuspendFilterScoreWeightedTopkStrategyV2):
            pass

        return L2RiskEntryGateScoreWeightedTopkStrategyV2
    raise AttributeError(name)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("manifest")
    parser.add_argument("--config", required=True)
    parser.add_argument("--prediction", required=True)
    args = parser.parse_args()
    inputs = L2RiskInputs(args.manifest)
    validate_execution_inputs(inputs.manifest, args.config, args.prediction)
    with Path("qe_l2_preflight_receipt.json").open("x", encoding="utf-8") as stream:
        json.dump(inputs.receipt, stream, ensure_ascii=False, indent=2)
    print("QE_L2_PREFLIGHT_PASS: 125 dates, 131 sectors, full prediction mapping")
