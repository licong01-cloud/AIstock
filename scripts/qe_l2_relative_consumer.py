"""Frozen loss reader, bounded target transform and causal B* target schedule."""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
import math
import hashlib
import re
from pathlib import Path

import pandas as pd

from qe_l2_continuous_consumer import ContinuousInputs, VINTAGES, _receipt, finite
from qe_l2_relative_admission import CONTRACT, VERSION, validate_admission
from qe_l2_risk_admission import canonical_sha
from qe_l2_risk_consumer import checked_asset, require, symbol_key, validate_execution_inputs

PRODUCER = "hmm_risk_l2_terminal_loss_ridge_v1"
TARGET = {"anchor": "decision_close", "first_return_offset": 1, "horizon": 10,
          "formula": "max(0,1-product(1+r[t+j,s],j=1..10))"}


def validate_lineage(lineage):
    require(isinstance(lineage, dict) and set(lineage) == {
        "source_task_id", "source_loop_index", "source_actions_sha256", "execution_sha256"},
        "QE_L2_RELATIVE_REFERENCE_LINEAGE")
    require(isinstance(lineage["source_task_id"], str) and lineage["source_task_id"].startswith("qe_")
            and type(lineage["source_loop_index"]) is int and lineage["source_loop_index"] > 0
            and all(isinstance(lineage[k], str) and re.fullmatch("[0-9a-f]{64}", lineage[k])
                    for k in ("source_actions_sha256", "execution_sha256")),
            "QE_L2_RELATIVE_REFERENCE_LINEAGE")


def validate_base(base, ordered):
    require(isinstance(base, dict) and isinstance(ordered, list), "QE_L2_RELATIVE_BASE_TYPE")
    weights = {s: finite(v, s, maximum=0.05) for s, v in base.items()}
    require(all(isinstance(s, str) and v > 0 for s, v in weights.items()), "QE_L2_RELATIVE_BASE_WEIGHT")
    require(len(ordered) == len(set(ordered)) == len(weights) and set(ordered) == set(weights),
            "QE_L2_RELATIVE_BASE_ORDER")
    require(math.fsum(weights.values()) <= 1 + 1e-10, "QE_L2_RELATIVE_BASE_BUDGET")
    return weights


def relative_targets(base, ordered, ranks, *, apply_risk):
    weights = validate_base(base, ordered)
    require(type(apply_risk) is bool and set(ranks) == set(weights), "QE_L2_RELATIVE_RISK_POPULATION")
    ranks = {s: finite(r, s, maximum=1) for s, r in ranks.items()}
    half = (len(ordered) + 1) // 2
    target, groups = {}, []
    for stocks in (ordered[:half], ordered[half:]):
        budget = math.fsum(weights[s] for s in stocks)
        mu = math.fsum(weights[s] * ranks[s] for s in stocks) / budget if stocks else None
        for s in stocks:
            value = weights[s] * (1 + 0.10 * (mu - ranks[s])) if apply_risk else weights[s]
            require(0.9 * weights[s] - 1e-12 <= value <= 1.1 * weights[s] + 1e-12
                    and 0 < value <= 0.05 + 1e-12, "QE_L2_RELATIVE_TARGET_CAP")
            target[s] = value
        require(abs(math.fsum(target[s] for s in stocks) - budget) <= 1e-12,
                "QE_L2_RELATIVE_GROUP_BUDGET")
        groups.append({"stocks": stocks, "base_budget": budget, "risk_mean_rank": mu})
    return target, groups


def target_reference(actions, manifest, *, source_task_id, source_loop_index, source_actions_sha256,
                     execution_sha256):
    require(manifest["arm"] == "BSTAR", "QE_L2_RELATIVE_REFERENCE_SOURCE_ARM")
    lineage = dict(source_task_id=source_task_id, source_loop_index=source_loop_index,
                   source_actions_sha256=source_actions_sha256, execution_sha256=execution_sha256)
    validate_lineage(lineage)
    require(execution_sha256 == manifest["execution_sha256"], "QE_L2_RELATIVE_REFERENCE_EXECUTION_DRIFT")
    rows = []
    for row in actions:
        require(row["arm"] == "BSTAR" and row["seed"] == manifest["seed"]
                and row["contract"] == CONTRACT and row["target_weights"] == row["base_targets"]
                and row["base_contract_sha256"] == manifest["base_contract_sha256"]
                and row["execution_sha256"] == execution_sha256,
                "QE_L2_RELATIVE_REFERENCE_SOURCE_INVALID")
        validate_base(row["base_targets"], row["af_order"])
        rows.append({k: deepcopy(row[k]) for k in ["trade_date", "as_of_date", "base_targets", "af_order"]})
    return {
        "schema_version": VERSION + "_targets", "contract": CONTRACT, "source_arm": "BSTAR",
        "source_task_id": source_task_id, "source_loop_index": source_loop_index,
        "source_actions_sha256": source_actions_sha256, "execution_sha256": execution_sha256,
        "seed": manifest["seed"], "prediction_sha256": manifest["assets"]["prediction"]["sha256"],
        "release_identity_sha256": canonical_sha(manifest["release_identity"]),
        "base_contract_sha256": manifest["base_contract_sha256"], "rows": rows,
    }


def validate_reference(ref, m, as_of):
    require(ref["schema_version"] == VERSION + "_targets" and ref["contract"] == CONTRACT
            and ref["source_arm"] == "BSTAR" and ref["seed"] == m["seed"]
            and ref["prediction_sha256"] == m["assets"]["prediction"]["sha256"]
            and ref["release_identity_sha256"] == canonical_sha(m["release_identity"])
            and ref["base_contract_sha256"] == m["base_contract_sha256"],
            "QE_L2_RELATIVE_REFERENCE_IDENTITY")
    lineage = m.get("target_source")
    validate_lineage(lineage)
    require(lineage == {k: ref[k] for k in
            ["source_task_id", "source_loop_index", "source_actions_sha256", "execution_sha256"]},
            "QE_L2_RELATIVE_REFERENCE_LINEAGE")
    require(m["execution_sha256"] == ref["execution_sha256"], "QE_L2_RELATIVE_REFERENCE_EXECUTION_DRIFT")
    rows = {}
    for row in ref["rows"]:
        require(set(row) == {"trade_date", "as_of_date", "base_targets", "af_order"}
                and row["trade_date"] in as_of and row["trade_date"] not in rows
                and row["as_of_date"] == as_of[row["trade_date"]], "QE_L2_RELATIVE_REFERENCE_CLOCK")
        validate_base(row["base_targets"], row["af_order"])
        rows[row["trade_date"]] = row
    require(set(rows) == set(as_of), "QE_L2_RELATIVE_REFERENCE_GRID")
    return rows


class RelativeInputs:
    def __init__(self, manifest_file):
        self.manifest = m = json.loads(Path(manifest_file).read_text(encoding="utf-8"))
        validate_admission(m)
        if "execution_files_sha256" in m:
            code = m["execution_files_sha256"]
            require(isinstance(code, dict) and code and canonical_sha({
                "files": code, "contract": m["execution_contract"]}) == m["execution_sha256"],
                    "QE_L2_RELATIVE_EXECUTION_CODE_IDENTITY")
            for name, digest in code.items():
                require(not Path(name).is_absolute() and ".." not in Path(name).parts and name.endswith(".py")
                        and hashlib.sha256(Path(name).read_bytes()).hexdigest() == digest,
                        "QE_L2_RELATIVE_EXECUTION_CODE_DRIFT", name)
        require((m["start"], m["end"], m["decision_days"]) == ("2026-04-01", "2026-09-30", 125),
                "QE_L2_RELATIVE_WINDOW")
        fields = {"loss", "prediction", "mapping", "mapping_receipt", "authority", "calendar", "pit"}
        if m["arm"] == "R":
            fields.add("targets")
        require(set(m["assets"]) == fields, "QE_L2_RELATIVE_ASSET_FIELDS")
        paths = {k: checked_asset(v) for k, v in m["assets"].items()}
        ContinuousInputs._validate_dataset(m)
        self._calendar = pd.DatetimeIndex(pd.to_datetime(paths["calendar"].read_text().splitlines()))
        require(self._calendar.is_unique and self._calendar.is_monotonic_increasing,
                "QE_L2_RELATIVE_CALENDAR")
        selected = self._calendar[(self._calendar >= m["start"]) & (self._calendar <= m["end"])]
        require(len(selected) == 125 and self._calendar.get_loc(selected[0]) > 0
                and str(selected[0].date()) == m["start"] and str(selected[-1].date()) == m["end"],
                "QE_L2_RELATIVE_WINDOW")
        self.as_of = {str(d.date()): str(self._calendar[self._calendar.get_loc(d) - 1].date()) for d in selected}
        authority = json.loads(paths["authority"].read_text(encoding="utf-8"))
        catalog = {r["canonical_l2_code"] for r in authority["l2_projection"]["rows"]}
        require(len(catalog) == 131 and canonical_sha(authority) == m["authority_canonical_sha256"]
                and authority["research_basis"]["active_mode"] == "historical_replay"
                and authority["research_basis"]["historical_classification_basis"] == "stable_taxonomy_backcast",
                "QE_L2_RELATIVE_AUTHORITY")
        self.mapping = ContinuousInputs._mapping(self, paths, authority, catalog, m)
        self.risk = self._loss(json.loads(paths["loss"].read_text(encoding="utf-8")), catalog)
        self.references = (validate_reference(json.loads(paths["targets"].read_text(encoding="utf-8")), m, self.as_of)
                           if m["arm"] == "R" else {})
        self.receipt = {"contract": CONTRACT, "arm": m["arm"], "seed": m["seed"], "days": 125,
                        "industries": 131, "rows": len(self.risk), "new_fits": 0, "new_inferences": 0,
                        "historical_as_known": False, "untouched_holdout": False}

    def _loss(self, risk, catalog):
        m = self.manifest
        _receipt(risk)
        require(risk["schema_version"] == PRODUCER + "_predictions" and risk["research_only"] is True
                and risk["historical_as_known"] is False and risk["untouched_holdout"] is False
                and risk["policy_applied"] is False, "QE_L2_RELATIVE_PRODUCER")
        require(risk["contract"]["version"] == PRODUCER and risk["contract"]["target"] == TARGET
                and risk["contract"]["prediction_projection"] == "clip_raw_to_0_1",
                "QE_L2_RELATIVE_LOSS_CONTRACT")
        require(risk["input_identity"]["release_identity"] == m["producer_release_identity"]
                and risk["input_identity"]["calendar_sha256"] == m["assets"]["calendar"]["sha256"]
                and risk["input_identity"]["industry_authority_sha256"] == m["authority_canonical_sha256"]
                and risk["release_binding"]["manifest_sha256"] == m["release_identity"]["dataset_manifest_sha256"],
                "QE_L2_RELATIVE_PRODUCER_IDENTITY")
        bundles = risk["model_bundles"]
        require(len(bundles) == 2 and {b["vintage"]["id"] for b in bundles} == set(VINTAGES),
                "QE_L2_RELATIVE_VINTAGES")
        for bundle in bundles:
            _receipt(bundle)
            v = bundle["vintage"]
            require(tuple(v[k] for k in ["train_start", "train_end", "effective_start", "effective_end"])
                    == VINTAGES[v["id"]] and bundle["schema_version"] == PRODUCER + "_model"
                    and bundle["contract"] == risk["contract"] and bundle["catalog"] == sorted(catalog)
                    and bundle["model_sha256"] == canonical_sha(bundle["parameters"]) == m["models"][v["id"]]
                    and bundle["training_input_identity"] == risk["input_identity"]
                    and bundle["training_release_binding"] == risk["release_binding"], "QE_L2_RELATIVE_MODEL")
        rows = {}
        for original in risk["predictions"]:
            row = deepcopy(original)
            day, sector = row["trade_date"], row["sector_code"]
            require(day in self.as_of and sector in catalog and (day, sector) not in rows
                    and row["as_of_date"] == self.as_of[day], "QE_L2_RELATIVE_RISK_KEY")
            vintage = "2026Q2" if day <= "2026-06-30" else "2026Q3"
            require(row["vintage_id"] == vintage and row["model_sha256"] == m["models"][vintage],
                    "QE_L2_RELATIVE_ROW_MODEL")
            require(row["availability"] == "available" and row["reason_code"] is None,
                    "QE_L2_RELATIVE_RISK_UNAVAILABLE")
            loss = finite(row["expected_loss"], "expected_loss", maximum=1)
            raw = row["raw_expected_loss"]
            require(type(raw) in (float, int) and math.isfinite(raw) and loss == min(1, max(0, raw)),
                    "QE_L2_RELATIVE_RAW_LOSS")
            rows[day, sector] = row
        require(set(rows) == {(d, s) for d in self.as_of for s in catalog}, "QE_L2_RELATIVE_FULL_POPULATION")
        for day in self.as_of:
            values = pd.Series({s: rows[day, s]["expected_loss"] for s in sorted(catalog)})
            ranks = (values.rank(method="average") - 1) / 130
            for sector in catalog:
                rows[day, sector]["relative_rank"] = float(ranks[sector])
        return rows

    def targets(self, day, base, ordered):
        require(day in self.as_of, "QE_L2_RELATIVE_DECISION_DAY")
        validate_base(base, ordered)
        details = {}
        for stock in ordered:
            key = self.as_of[day], symbol_key(stock)
            require(key in self.mapping.index, "QE_L2_RELATIVE_MAPPING_MISSING", str(key))
            mapping = self.mapping.loc[key]
            require(mapping.mapping_status == "resolved", "QE_L2_RELATIVE_MAPPING_UNAVAILABLE", str(key))
            details[stock] = deepcopy(self.risk[day, mapping.l2_code])
        target, groups = relative_targets(base, ordered, {s: r["relative_rank"] for s, r in details.items()},
                                          apply_risk=self.manifest["arm"] == "R")
        return target, groups, details


def main():
    p = argparse.ArgumentParser()
    p.add_argument("manifest")
    p.add_argument("--config", required=True)
    p.add_argument("--prediction", required=True)
    a = p.parse_args()
    inputs = RelativeInputs(a.manifest)
    require(bool(inputs.manifest.get("execution_files_sha256")), "QE_L2_RELATIVE_EXECUTION_CODE_REQUIRED")
    validate_execution_inputs(inputs.manifest, a.config, a.prediction)
    import yaml
    conf = yaml.safe_load(Path(a.config).read_text(encoding="utf-8"))
    actual = json.loads(json.dumps({k: conf[k] for k in ("port_analysis_config", "qlib_init", "market")},
                                  default=lambda x: x.isoformat()))
    require(actual == inputs.manifest["execution_contract"], "QE_L2_RELATIVE_EXECUTION_CONTRACT_DRIFT")
    print(json.dumps(inputs.receipt, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
