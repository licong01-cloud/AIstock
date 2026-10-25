"""One immutable six-fit inner study; no final, outer, sealed, SQL or activation."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

import pandas as pd
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first import report_arrival_top10_5td_v1 as model
from backend.services.advisory_model_first import report_arrival_top10_5td_inputs_v1 as inputs_api
from backend.services.advisory_model_first import peer_residual_top10_5td_pipeline_v1 as shared
from backend.services.advisory_model_first import peer_lead_lag_top10_5td_pipeline_v1 as lineage
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.prequential_top10_5td_study_v1 import projected_finance
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    _exclusive_file_lock,
    evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import build_trial_record

previous = shared.previous


def implementation_sha256():
    names = (
        "report_arrival_top10_5td_inputs_v1",
        "report_arrival_top10_5td_v1",
        "report_arrival_top10_5td_pipeline_v1",
    )
    return model.sha(
        dict(
            shared=lineage.implementation_sha256(),
            files={
                name: hashlib.sha256(
                    Path(__file__).with_name(name + ".py").read_bytes().replace(b"\r\n", b"\n")
                ).hexdigest()
                for name in names
            },
        )
    )


class Study:
    def __init__(self, root, plan=None):
        self.root = previous.node_path(root)
        if not (self.root.as_posix().lower().startswith("/mnt/x/") or self.root.drive.upper() == "X:"):
            raise ValueError("output must be on X")
        self.plan = plan or previous.load(self.root / "preregistered/plan.json")
        identity = (
            "advreportarrivaltop10_" + model.sha({k: v for k, v in self.plan.items() if k != "experiment_id"})[:24]
        )
        if (
            self.plan["experiment_id"] != identity
            or self.plan["contract"] != json.loads(_json_bytes(model.CONTRACT))
            or self.plan["implementation_sha256"] != implementation_sha256()
            or not re.fullmatch("[0-9a-f]{40}", self.plan["source_commit"])
            or self.plan["versions"] != previous.versions()
        ):
            raise ValueError("plan, implementation or environment identity differs")
        self.source = previous.Study(self.plan["source_root"])
        self.verify_source()
        inputs_api.sources(self.source.plan, self.plan["input_source_stamps"])
        self.digest, self.started = model.sha(self.plan), time.monotonic()

    def verify_source(self):
        # Hash verification is not financial value decoding.
        if (
            self.source.read("evaluated") != self.plan["source_stage_sha256"]
            or self.source.plan["binding"]["binding_hash"] != self.plan["binding_hash"]
        ):
            raise ValueError("frozen source identity drift")

    def budget(self):
        if (
            previous.peak_memory_bytes() > model.CONTRACT["max_rss_bytes"]
            or time.monotonic() - self.started > model.CONTRACT["max_seconds"]
        ):
            raise ValueError("2GiB/30-minute resource budget exceeded")

    def node(self):
        if os.name == "nt" or Path(sys.executable).resolve() != Path(self.plan["node_python_uri"]).resolve():
            raise ValueError("use the frozen existing WSL environment")

    def read(self, relative, parent):
        path = self.root / relative
        return read_stage(path, stage=path.name, plan_sha256=self.digest, parent_sha256=parent)["stage_sha256"]

    def publish(self, relative, parent, files):
        self.budget()
        if (
            sum(map(len, files.values())) + sum(p.stat().st_size for p in self.root.rglob("*") if p.is_file())
            > model.CONTRACT["max_output_bytes"]
        ):
            raise ValueError("200MiB output budget exceeded")
        path = self.root / relative
        publish_stage(
            study_root=path.parent, stage=path.name, plan_sha256=self.digest, parent_sha256=parent, artifacts=files
        )
        return self.read(relative, parent)

    def finance(self, first, last, *, cutoff, outer=False):
        if (
            outer
            or pd.Timestamp(first) > pd.Timestamp(last)
            or pd.Timestamp(cutoff) < pd.Timestamp(last)
            or pd.Timestamp(first) < pd.Timestamp(model.CONTRACT["start"])
            or pd.Timestamp(last) > pd.Timestamp(model.CONTRACT["end"])
            or pd.Timestamp(cutoff) > pd.Timestamp(model.CONTRACT["end"])
        ):
            raise ValueError("financial window outside development contract")
        path = self.source.root / "prepared/train_finance.parquet"
        filters = [(model.base.KEY[0], ">=", pd.Timestamp(first)), (model.base.KEY[0], "<=", pd.Timestamp(last))]
        meta = pd.read_parquet(path, columns=[*model.KEY, "label_information_end"], filters=filters)
        observed = pd.read_parquet(path, filters=filters + [("label_information_end", "<=", pd.Timestamp(cutoff))])
        return projected_finance(meta, observed, cutoff)

    def claim(self, key, stage_hash=None):
        allowed = {f"{prefix}_{arm}" for prefix in ("fold1", "fold2", "fold3") for arm in ("matched", "candidate")}
        if key not in allowed:
            raise ValueError("unregistered fit")
        self.budget()
        folder = self.root / "fit_claims"
        folder.mkdir(exist_ok=True)
        start = folder / (key + "_started.json")
        if stage_hash is None:
            if len(list(folder.glob("*_started.json"))) >= model.CONTRACT["max_fits"]:
                raise ValueError("fit budget exhausted")
            path = start
        else:
            expected = previous.load(start)
            if expected["key"] != key or expected["plan_sha256"] != self.digest or expected["stage_sha256"] is not None:
                raise ValueError("fit claim identity drift")
            path = folder / (key + "_finished.json")
            if path.exists():
                done = previous.load(path)
                if done["key"] != key or done["plan_sha256"] != self.digest or done["stage_sha256"] != stage_hash:
                    raise ValueError("fit completion identity drift")
                return
        payload = dict(
            key=key, plan_sha256=self.digest, stage_sha256=stage_hash, at=datetime.now(timezone.utc).isoformat()
        )
        with path.open("xb") as stream:
            stream.write(_json_bytes(payload))
            stream.flush()
            os.fsync(stream.fileno())

    def register(self, relative, *, evaluated=False):
        record = build_trial_record(
            experiment_id=self.plan["experiment_id"],
            attempt_id="exact_v1",
            research_stage=relative.replace("/", "_").upper(),
            study_type="EXPLORATORY_SCREEN",
            objective_contract="ALPHA_RANKING",
            decision_use="NAVIGATION_ONLY",
            hypothesis_family_id=model.CONTRACT["hypothesis"],
            unique_variable="LITERAL_ORG_DAY_30_90_ARRIVAL_ACCELERATION_PROXY",
            parent_lineage=(self.source.plan["experiment_id"],),
            dataset_identity=self.plan["binding_hash"],
            schema_identity=model.sha(model.CONTRACT),
            policy_identity=model.base.VALUATION_POLICY_SHA256,
            planned_trial_count=1,
            generated_trial_count=int(evaluated),
            evaluated_trial_count=int(evaluated),
            selected_trial_count=0,
            result_class="EXPLORATORY",
            consumed_windows=(
                dict(
                    window_id="PREVIOUSLY_CONSUMED_DEVELOPMENT",
                    dataset_identity=self.plan["binding_hash"],
                    start_date=model.CONTRACT["start"],
                    end_date=model.CONTRACT["end"],
                ),
            ),
            evidence_refs=(
                evidence_reference_for_file(
                    self.root / relative / "manifest.json", role="report_arrival_" + relative.replace("/", "_")
                ),
            ),
        )
        AdvisoryResearchTrialRegistryV1(self.root.parent / "trial_registry.jsonl").append_batch((record,))


def preregister(output, source_root, commit):
    source = previous.Study(source_root)
    plan = dict(
        contract=model.CONTRACT,
        source_root=str(source.root),
        source_stage_sha256=source.read("evaluated"),
        binding_hash=source.plan["binding"]["binding_hash"],
        source_commit=commit,
        input_source_stamps=inputs_api.sources(source.plan)[2],
        implementation_sha256=implementation_sha256(),
        versions=previous.versions(),
        node_python_uri=source.plan["node_python_uri"],
    )
    plan = json.loads(_json_bytes(plan))
    plan["experiment_id"] = "advreportarrivaltop10_" + model.sha(plan)[:24]
    study = Study(previous.node_path(output) / plan["experiment_id"], plan)
    study.publish("preregistered", None, {"plan.json": _json_bytes(plan)})
    study.register("preregistered")
    return study.root


def prepare(study, parent, *, progress=lambda event: None):
    relative, path = "inner/prepared", study.root / "inner/prepared"
    if path.exists():
        digest = study.read(relative, parent)
        receipt = previous.load(path / "receipt.json")
        frame = pd.read_parquet(path / "inputs.parquet")
        inputs_api.verify_consumed(study.source.plan, receipt)
        inputs_api.verify_screen_alignment(frame)
        return frame, digest
    columns = [*model.IDENTITY, "source_id", "parent_score", *model.INPUT_FEATURES]
    inputs = pd.read_parquet(
        study.source.root / "prepared/inputs.parquet",
        columns=columns,
        filters=[
            (model.base.KEY[0], ">=", pd.Timestamp(model.CONTRACT["start"])),
            (model.base.KEY[0], "<=", pd.Timestamp(model.CONTRACT["end"])),
        ],
    )
    model.validate_population(inputs)
    if len(inputs) != model.CONTRACT["max_rows"] or inputs[model.base.KEY[0]].nunique() != 305:
        raise ValueError("original inner population count differs")
    if set(inputs.package_id) != {p["package_id"] for p in study.source.plan["sources"]}:
        raise ValueError("original package source differs")
    frame, receipt = inputs_api.read_panel(study.source.plan, inputs, study.plan["input_source_stamps"], study.budget)
    screen_alignment = inputs_api.verify_screen_alignment(frame)
    receipt["screen_alignment"] = screen_alignment
    supported = model.support(model.numeric(frame))
    if not supported.any():
        raise ValueError("no common candidate support; no fit")
    receipt.update(
        original_rows=len(frame),
        original_days=int(frame[model.base.KEY[0]].nunique()),
        common_supported_rows=int(supported.sum()),
        parameter_fits=0,
        deterministic_feature_windows=len(frame.drop_duplicates([model.base.KEY[0], "instrument"])),
        reasons={str(k): int(v) for k, v in frame.input_status.value_counts().items()},
        by_group=[
            dict(
                day=str(d.date()),
                package_id=p,
                rows=len(g),
                available=int(g.input_status.eq("AVAILABLE_NON_VINTAGE_ARRIVAL_PROXY").sum()),
                reasons={str(k): int(v) for k, v in g.input_status.value_counts().items()},
            )
            for (d, p), g in frame.groupby([model.base.KEY[0], "package_id"])
        ],
        elapsed_seconds=time.monotonic() - study.started,
        peak_rss_bytes=previous.peak_memory_bytes(),
    )
    digest = study.publish(
        relative, parent, {"inputs.parquet": parquet_bytes(frame), "receipt.json": _json_bytes(receipt)}
    )
    progress(
        dict(phase=relative, rows=len(frame), supported_rows=int(supported.sum()), seconds=receipt["elapsed_seconds"])
    )
    return frame, digest


def fit_pair(study, parent, inputs, targets, *, prefix, cutoff, first, last, progress, evaluate=True):
    allowed = {f"fold{i}": tuple(values) for i, values in enumerate(model.base.FOLDS, 1)}
    if prefix not in allowed or (cutoff, first, last) != allowed[prefix]:
        raise ValueError("unregistered fold or window")
    if targets is not None and not targets[model.base.KEY[0]].between(pd.Timestamp(first), pd.Timestamp(last)).all():
        raise ValueError("prediction target outside registered fold")
    predictions, models, train = {}, {}, None
    for arm in ("matched", "candidate"):
        key = prefix + "_" + arm
        relative = key + "/trained"
        path = study.root / relative
        if path.exists():
            parent = study.read(relative, parent)
            study.claim(key, parent)
        else:
            if (study.root / "fit_claims" / (key + "_started.json")).exists():
                raise ValueError("interrupted unpublished fit: no automatic refit")
            if train is None:
                finance = study.finance(model.CONTRACT["start"], cutoff, cutoff=cutoff)
                selected = inputs.loc[inputs[model.base.KEY[0]].le(pd.Timestamp(cutoff))]
                # <=30,500 rows; exact package/D/T/instrument keys, one_to_one forbids row explosion.
                train = selected.merge(finance, on=model.KEY, how="left", validate="one_to_one", indicator=True)
                if not train["_merge"].eq("both").all():
                    raise ValueError("training finance population differs")
                train = train.drop(columns="_merge")
            packet = model.fit(train, cutoff, arm, before_fit=lambda: study.claim(key))
            prediction = model.predict(targets, packet)
            parent = study.publish(
                relative, parent, {"model.json": _json_bytes(packet), "predictions.parquet": parquet_bytes(prediction)}
            )
            study.claim(key, parent)
            progress(
                dict(phase="FIT_COMPLETED", key=key, train_rows=packet["train_rows"], iterations=packet["iterations"])
            )
        models[arm] = previous.load(path / "model.json")
        predictions[arm] = pd.read_parquet(path / "predictions.parquet")
        model.evaluation.same_population(targets, predictions[arm])
        if models[arm]["cutoff"] != cutoff or models[arm]["arm"] != arm:
            raise ValueError("persisted fit clock/arm differs")
    if models["matched"]["training_keys_sha256"] != models["candidate"]["training_keys_sha256"]:
        raise ValueError("arms used different supervised support")
    for name in models["matched"]["fields"]:
        left, right = models["matched"]["fields"].index(name), models["candidate"]["fields"].index(name)
        if any(models["matched"][key][left] != models["candidate"][key][right] for key in ("median", "scale")):
            raise ValueError("arms used different common feature encoding")
    if not predictions["matched"].no_rank_advice.equals(predictions["candidate"].no_rank_advice):
        raise ValueError("arms used different prediction support")
    if not evaluate:
        return None, parent
    # No validation financial decode until both predictions are immutable.
    finance = study.finance(first, last, cutoff=last, outer=False)
    return model.panel(predictions["candidate"], predictions["matched"], finance), parent


def evaluate_inner(study, parent, inputs, progress):
    panels = []
    path = study.root / "inner/evaluated"
    for fold, (cutoff, first, last) in enumerate(model.base.FOLDS, 1):
        targets = inputs.loc[inputs[model.base.KEY[0]].between(pd.Timestamp(first), pd.Timestamp(last))].reset_index(
            drop=True
        )
        cohort, parent = fit_pair(
            study,
            parent,
            inputs,
            targets,
            prefix="fold" + str(fold),
            cutoff=cutoff,
            first=first,
            last=last,
            progress=progress,
            evaluate=not path.exists(),
        )
        if cohort is not None:
            cohort["fold"] = fold
            panels.append(cohort)
    if path.exists():
        parent = study.read("inner/evaluated", parent)
        return previous.load(path / "report.json"), parent
    cohorts = pd.concat(panels, ignore_index=True)
    report, daily = model.summarize(cohorts)
    coefficients = {}
    for fold, _ in enumerate(model.base.FOLDS, 1):
        packet = previous.load(study.root / f"fold{fold}_candidate/trained/model.json")
        coefficients[str(fold)] = {name: packet["coef"][packet["fields"].index(name)] for name in model.NEW}
    # Attribution only: neither sign nor matched delta can override the native endpoint.
    report.update(arrival_coefficients=coefficients)
    parent = study.publish(
        "inner/evaluated",
        parent,
        {
            "report.json": _json_bytes(report),
            "panel.parquet": parquet_bytes(cohorts),
            "daily.parquet": parquet_bytes(daily),
        },
    )
    return report, parent


def run(root, progress=lambda event: None):
    study = Study(root)
    study.node()
    with _exclusive_file_lock(study.root / "study.lock"), threadpool_limits(limits=2):
        parent = study.read("preregistered", None)
        inputs, parent = prepare(study, parent, progress=progress)
        inner, parent = evaluate_inner(study, parent, inputs, progress)
        study.register("inner/evaluated", evaluated=True)
        expected = {f"{prefix}_{arm}" for prefix in ("fold1", "fold2", "fold3") for arm in ("matched", "candidate")}
        for suffix in ("_started.json", "_finished.json"):
            if {p.name.removesuffix(suffix) for p in (study.root / "fit_claims").glob("*" + suffix)} != expected:
                raise ValueError("actual fit identities differ from six-fit terminal state")
        study.verify_source()
        inputs_api.verify_consumed(study.source.plan, previous.load(study.root / "inner/prepared/receipt.json"))
        if (study.root / "evaluated").exists():
            study.read("evaluated", parent)
            return {**previous.load(study.root / "evaluated/result.json"), "exact_retry": True}
        receipt = previous.load(study.root / "inner/prepared/receipt.json")
        result = dict(
            experiment_id=study.plan["experiment_id"],
            classification=inner["classification"],
            inner=inner,
            outer=None,
            physical_fit_count=6,
            prior_physical_fits=model.CONTRACT["prior_fits"],
            cumulative_physical_fits=model.CONTRACT["prior_fits"] + 6,
            historical_index_count=1,
            new_information_candidates=1,
            representation_candidates=0,
            outer_finance_decoded=False,
            sealed_finance_decoded=False,
            input_parameter_fits=0,
            deterministic_feature_windows=receipt["deterministic_feature_windows"],
            sql_count=0,
            actual_fill_proven=False,
            activation_evidence=False,
            independent_oos_evidence=False,
            elapsed_seconds=time.monotonic() - study.started,
            peak_rss_bytes=previous.peak_memory_bytes(),
            previous_output_bytes=sum(p.stat().st_size for p in study.root.rglob("*") if p.is_file()),
            consumed_range_end=model.CONTRACT["end"],
            code_commit=study.plan["source_commit"],
        )
        study.publish("evaluated", parent, {"result.json": _json_bytes(result)})
        study.register("evaluated", evaluated=True)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    register = commands.add_parser("preregister")
    for name in ("output", "source-root", "source-commit"):
        register.add_argument("--" + name, required=True)
    execute = commands.add_parser("run")
    execute.add_argument("--study-root", required=True)
    args = parser.parse_args()
    if args.command == "preregister":
        print(
            json.dumps(dict(study_root=str(preregister(args.output, args.source_root, args.source_commit)))), flush=True
        )
    else:
        result = run(args.study_root, lambda event: print(json.dumps(event), flush=True))
        print(
            json.dumps(
                {
                    k: result[k]
                    for k in (
                        "experiment_id",
                        "classification",
                        "physical_fit_count",
                        "outer_finance_decoded",
                        "elapsed_seconds",
                        "peak_rss_bytes",
                    )
                }
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
