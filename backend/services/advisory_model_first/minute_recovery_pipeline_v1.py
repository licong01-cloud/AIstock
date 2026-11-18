"""Immutable eighteen-fit execution; all forecasts precede aggregate evaluation."""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess

import pandas as pd
import sklearn
from threadpoolctl import threadpool_limits

from backend.data_service.frozen_dataset import FrozenDataset
from backend.services.advisory_model_first import minute_recovery_experiment_v1 as model
from backend.services.advisory_model_first import minute_recovery_inputs_v1 as inputs
from backend.services.advisory_model_first.economic_entry_pipeline import (
    _json_bytes, file_sha256, publish_stage, read_stage,
)
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY, finance_inputs
from backend.services.advisory_model_first.prequential_top10_5td_study_v1 import projected_finance
from backend.services.advisory_model_first.research_control import (
    _exclusive_file_lock, AdvisoryResearchTrialRegistryV1, evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import build_trial_record

FEATURE_STAGE = "89a1754f5fd09c4161316f5e8120c892a8ce1bc60271eecce0fcc7dba17111b9"
DAY = inputs.DAY


def source_metadata(feature_root, source_root):
    feature_root, source_root = Path(feature_root), Path(source_root)
    for p in (feature_root, source_root):
        if not p.is_absolute() or p.resolve() != p:
            raise ValueError("explicit resolved source root required")
    reg = json.loads((feature_root / "preregistered/manifest.json").read_bytes())
    read_stage(feature_root / "preregistered", stage="preregistered",
               plan_sha256=reg["plan_sha256"], parent_sha256=None)
    stage = read_stage(feature_root / "prepared", stage="prepared", plan_sha256=reg["plan_sha256"],
                       parent_sha256=reg["stage_sha256"])
    if stage["stage_sha256"] != FEATURE_STAGE:
        raise ValueError("P4 feature stage identity differs")
    # P4 historical implementation hashes are provenance, not current code requirements.
    source_plan = json.loads((source_root / "preregistered/plan.json").read_bytes())
    if inputs.sha(source_plan) != inputs.SOURCE_PLAN:
        raise ValueError("original source plan identity differs")
    finance_files, stage_hashes = {}, {}
    for stage_name, name in (("prepared", "train_finance.parquet"), ("evaluated", "finance.parquet")):
        manifest_path = source_root / stage_name / "manifest.json"
        m = json.loads(manifest_path.read_bytes())
        if (inputs.sha({k:v for k,v in m.items() if k != "stage_sha256"}) != m["stage_sha256"]
                or m["stage"] != stage_name or m["plan_sha256"] != inputs.SOURCE_PLAN):
            raise ValueError("original finance manifest identity differs")
        path = source_root / stage_name / name
        if path.resolve() != path or (path.stat().st_size, file_sha256(path)) != (
                m["files"][name]["size_bytes"], m["files"][name]["sha256"]):
            raise ValueError("original finance file identity differs")
        finance_files[str(path)] = dict(m["files"][name])
        stage_hashes[stage_name] = m["stage_sha256"]
    if stage_hashes["prepared"] != inputs.SOURCE_PREPARED:
        raise ValueError("original prepared stage differs")
    return source_plan, dict(feature_root=str(feature_root), source_root=str(source_root),
                             feature_stage_sha256=FEATURE_STAGE, finance_files=finance_files,
                             original_stage_hashes=stage_hashes)


class FinanceReader:
    """Project D/H before numeric decode; reuse old labels and repair stale IMMATURE only."""
    def __init__(self, dataset, sources, budget):
        self.dataset, self.sources, self.budget = dataset, sources, budget
        self.calendar = pd.DatetimeIndex(dataset.calendar)

    def read(self, rows, cutoff):
        cutoff = pd.Timestamp(cutoff)
        if cutoff > pd.Timestamp(inputs.END) or rows[DAY].gt(cutoff).any():
            raise ValueError("financial read exceeds declared D/cutoff")
        first, last = rows[DAY].min(), rows[DAY].max()
        meta, observed = [], []
        for path in self.sources["finance_files"]:
            filters = [("package_id", "=", inputs.PACKAGE), (DAY, ">=", first), (DAY, "<=", last)]
            meta.append(pd.read_parquet(path, columns=[*inputs.KEY, "label_information_end"], filters=filters))
            observed.append(pd.read_parquet(path, filters=filters+[("label_information_end", "<=", cutoff)]))
        metadata = pd.concat(meta, ignore_index=True)
        values = pd.concat(observed, ignore_index=True)
        expected = rows[inputs.KEY].merge(metadata, on=inputs.KEY, how="left", validate="one_to_one", indicator=True)
        if len(metadata) != len(rows) or not expected._merge.eq("both").all():
            raise ValueError("original finance metadata population differs")
        positions = self.calendar.get_indexer(expected[DAY])
        h = pd.Series([self.calendar[i+5] if i >= 0 and i+5 < len(self.calendar) else pd.NaT
                       for i in positions], index=expected.index)
        if not expected.label_information_end.reset_index(drop=True).equals(h):
            raise ValueError("original finance H differs from fixed fifth session")
        stale = values.loc[values.valuation_status.eq("IMMATURE"), inputs.KEY]
        repair_receipt = None
        if len(stale):
            repair_rows = rows.merge(stale, on=inputs.KEY, validate="one_to_one")
            corrected, repair_receipt = finance_inputs(self.dataset, repair_rows, cutoff, self.budget.check)
            if corrected.valuation_status.eq("IMMATURE").any():
                raise ValueError("mature financial repair still immature")
            values = pd.concat([values.loc[~values.valuation_status.eq("IMMATURE")], corrected], ignore_index=True)
        result = projected_finance(metadata, values, cutoff)
        # Preserve original row order for deterministic joins and training key seals.
        result = rows[inputs.KEY].merge(result, on=inputs.KEY, validate="one_to_one")
        model._validate_finance(result)
        self.budget.check()
        return result, dict(cutoff=str(cutoff.date()), original_rows=len(rows),
                            decoded_mature_rows=len(values), stale_immature_repaired=len(stale),
                            statuses={str(k):int(v) for k,v in result.valuation_status.value_counts().items()},
                            repair_source=repair_receipt, sql_count=0)


class Study:
    def __init__(self, root, plan, budget):
        self.root, self.plan, self.budget = Path(root), plan, budget
        self.digest = inputs.sha(plan)

    def stage(self, relative, parent, artifacts=None):
        self.budget.check()
        path = self.root / relative
        if artifacts is not None:
            total = sum(p.stat().st_size for p in self.root.rglob("*") if p.is_file())
            if total + sum(map(len, artifacts.values())) > inputs.MAX_BYTES:
                raise ValueError("experiment output budget exceeded")
            publish_stage(study_root=path.parent, stage=path.name, plan_sha256=self.digest,
                          parent_sha256=parent, artifacts=artifacts)
        return read_stage(path, stage=path.name, plan_sha256=self.digest, parent_sha256=parent)["stage_sha256"]

    def claim(self, key, stage_hash=None):
        allowed = {f"fold{i}_{arm}" for i in range(1, 10) for arm in ("matched", "candidate")}
        if key not in allowed:
            raise ValueError("unregistered fit key")
        folder = self.root / "fit_claims"
        folder.mkdir(exist_ok=True)
        started, finished = (folder / (key+suffix+".json") for suffix in ("_started", "_finished"))
        if stage_hash is None:
            if started.exists() or len(list(folder.glob("*_started.json"))) >= 18:
                raise ValueError("fit already claimed or budget exhausted; no automatic refit")
            path = started
        else:
            old = json.loads(started.read_bytes())
            if old["key"] != key or old["plan_sha256"] != self.digest or old["stage_sha256"] is not None:
                raise ValueError("fit start claim identity differs")
            if finished.exists():
                old = json.loads(finished.read_bytes())
                if old["key"] != key or old["plan_sha256"] != self.digest or old["stage_sha256"] != stage_hash:
                    raise ValueError("fit finish claim identity differs")
                return
            path = finished
        with path.open("xb") as stream:
            stream.write(_json_bytes(dict(key=key, plan_sha256=self.digest, stage_sha256=stage_hash,
                                           at=datetime.now(timezone.utc).isoformat())))
            stream.flush()
            os.fsync(stream.fileno())


def verify_common_packets(a, b):
    for name in ("training_keys_sha256", "training_values_sha256"):
        if a[name] != b[name]:
            raise ValueError("paired training support/target/weights differs")
    # Every base field must have identical train-only encoding in the two arms.
    for name in model.features.MATCHED:
        if (name in a["fields"]) != (name in b["fields"]):
            raise ValueError("paired base active fields differ")
        if name in a["fields"]:
            i, j = a["fields"].index(name), b["fields"].index(name)
            if a["median"][i] != b["median"][j] or a["scale"][i] != b["scale"][j]:
                raise ValueError("paired base encoding differs")


def forecasts(study, inputs_frame, reader, parent, *, progress):
    models, predictions = {}, {arm:[] for arm in ("matched", "candidate")}
    for number, (cutoff, first, last) in enumerate(model.FOLDS, 1):
        calendar = reader.calendar
        effective_cutoff = calendar[calendar <= cutoff][-1]
        targets = inputs_frame.loc[inputs_frame[DAY].between(first, last)].reset_index(drop=True)
        if targets.empty or calendar[calendar.get_loc(targets[DAY].min())-1] != effective_cutoff:
            raise ValueError("fold cutoff does not immediately precede target D")
        relative = f"fold{number}/prepared"
        path = study.root / relative
        if not path.exists():
            rows = inputs_frame.loc[inputs_frame[DAY].le(effective_cutoff)].reset_index(drop=True)
            finance, receipt = reader.read(rows, effective_cutoff)
            parent = study.stage(relative, parent, {"finance.parquet": inputs.parquet_bytes(finance),
                                                   "receipt.json":_json_bytes(receipt)})
        else:
            parent = study.stage(relative, parent)
        train_rows = None
        for arm in ("matched", "candidate"):
            key = f"fold{number}_{arm}"
            relative = key+"/trained"
            path = study.root / relative
            if path.exists():
                parent = study.stage(relative, parent)
                study.claim(key, parent)
            else:
                if (study.root / "fit_claims" / (key+"_started.json")).exists():
                    raise ValueError("interrupted unpublished fit; no automatic refit")
                if train_rows is None:
                    finance = pd.read_parquet(study.root / f"fold{number}/prepared/finance.parquet")
                    train_rows = inputs_frame.loc[inputs_frame[DAY].le(effective_cutoff)].merge(
                        finance, on=inputs.KEY, validate="one_to_one")
                    if len(train_rows) != len(finance):
                        raise ValueError("training population join differs")
                packet = model.train(train_rows, effective_cutoff, arm, before_fit=lambda k=key:study.claim(k))
                prediction = model.predict(targets, packet)
                parent = study.stage(relative, parent, {"model.json":_json_bytes(packet),
                                                       "predictions.parquet":inputs.parquet_bytes(prediction)})
                study.claim(key, parent)
                progress(dict(phase="FIT_FINISHED", key=key, train_rows=packet["train_rows"],
                              train_days=packet["train_days"], iterations=packet["iterations"], stage_sha256=parent))
            packet = json.loads((path / "model.json").read_bytes())
            prediction = pd.read_parquet(path / "predictions.parquet")
            if (packet["arm"] != arm or packet["cutoff"] != str(effective_cutoff.date())
                    or not prediction[list(IDENTITY)].equals(targets[list(IDENTITY)])):
                raise ValueError("persisted fold identity differs")
            models[key] = packet
            prediction["fold"] = number
            predictions[arm].append(prediction)
        verify_common_packets(models[f"fold{number}_matched"], models[f"fold{number}_candidate"])
    forecasts_by_arm = {arm:pd.concat(p, ignore_index=True) for arm,p in predictions.items()}
    parent = study.stage("forecasts/trained", parent, {arm+"_predictions.parquet":inputs.parquet_bytes(p)
                         for arm,p in forecasts_by_arm.items()})
    return forecasts_by_arm, parent


def run(*, feature_root, source_root, output_root, source_commit, progress=lambda e:None):
    budget = inputs.Budget()
    target = Path(output_root)
    if (not target.is_absolute() or target.resolve() != target or target.drive.upper() != "X:"
            or not re.fullmatch(r"[0-9a-f]{40}", source_commit)):
        raise ValueError("experiment requires explicit X output and full source commit")
    for root in (Path(feature_root), Path(source_root)):
        if target.is_relative_to(root) or root.is_relative_to(target):
            raise ValueError("experiment cannot overlap source roots")
    if target.exists() and not (target / "preregistered/manifest.json").exists():
        raise ValueError("refuse existing unregistered output")
    if not target.exists():
        repository = Path(__file__).resolve().parents[3]
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
        dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=repository, text=True).strip()
        if actual != source_commit or dirty:
            raise ValueError("new experiment needs a clean checkout at the declared source commit")
    source_plan, sources = source_metadata(feature_root, source_root)
    implementation = inputs.implementation_identity(entries=["scripts/advisory_minute_recovery_experiment.py"])
    plan = dict(contract=model.CONTRACT, sources=sources, source_commit=source_commit,
                implementation=implementation, sklearn=sklearn.__version__, authorization_fit_budget=18)
    study = Study(target, json.loads(_json_bytes(plan)), budget)
    parent = study.stage("preregistered", None, {"plan.json":_json_bytes(plan)})
    with _exclusive_file_lock(target / "study.lock"), threadpool_limits(limits=2), closing(
            FrozenDataset(source_plan["binding"], consumer_scope="ADVISORY", run_id="MINUTE_RECOVERY_P5")) as dataset:
        frame = pd.read_parquet(Path(feature_root) / "prepared/inputs.parquet")
        inputs.validate_population(frame[list(inputs.ORIGINAL)], dataset.calendar)
        model.support(frame)
        reader = FinanceReader(dataset, sources, budget)
        predictions, parent = forecasts(study, frame, reader, parent, progress=progress)
        # Evaluation outcome decoding starts only after all 18 forecasts are frozen.
        relative = "evaluation_finance/prepared"
        if not (target / relative).exists():
            eval_rows = frame.loc[frame[DAY].ge(model.CONTRACT["evaluation_start"])].reset_index(drop=True)
            finance, receipt = reader.read(eval_rows, inputs.END)
            parent = study.stage(relative, parent, {"finance.parquet":inputs.parquet_bytes(finance),
                                                   "receipt.json":_json_bytes(receipt)})
        else:
            parent = study.stage(relative, parent)
        if not (target / "evaluated").exists():
            finance = pd.read_parquet(target / relative / "finance.parquet")
            panel = model.comparison(predictions["candidate"], predictions["matched"], finance)
            panel["fold"] = predictions["candidate"].groupby(DAY, sort=True).fold.first().to_numpy()
            report = model.summarize(panel)
            report.update(physical_fit_count=18, prior_financial_fit_count=269,
                          cumulative_financial_fit_count=287, old_index_count=1,
                          source_commit=source_commit, **budget.receipt())
            parent = study.stage("evaluated", parent, {"panel.parquet":inputs.parquet_bytes(panel),
                                                       "report.json":_json_bytes(report)})
        else:
            parent = study.stage("evaluated", parent)
            report = json.loads((target / "evaluated/report.json").read_bytes())
        # Verify mutable external inputs/code again, independent of completed-stage reuse.
        if source_metadata(feature_root, source_root)[1] != sources:
            raise ValueError("source identity changed during experiment")
        if inputs.implementation_identity(entries=["scripts/advisory_minute_recovery_experiment.py"]) != implementation:
            raise ValueError("implementation changed during experiment")
        dataset.assert_unchanged()
        record = build_trial_record(experiment_id="minute_recovery_"+study.digest[:24], attempt_id="exact_v1",
            research_stage="EXPANDED_EVALUATED", study_type="EXPLORATORY_SCREEN", objective_contract="ALPHA_RANKING",
            decision_use="NAVIGATION_ONLY", hypothesis_family_id=model.CONTRACT["schema"],
            unique_variable="D_CLOSE_DRAWDOWN_RECOVERY_BLOCK", parent_lineage=(source_plan["experiment_id"],),
            dataset_identity=inputs.BINDING, schema_identity=inputs.sha(model.CONTRACT),
            policy_identity=model.VALUATION_POLICY_SHA256, planned_trial_count=1, generated_trial_count=1,
            evaluated_trial_count=1, selected_trial_count=0, result_class="EXPLORATORY",
            consumed_windows=(dict(window_id="PREVIOUSLY_CONSUMED_EXPANDED_HISTORY", dataset_identity=inputs.BINDING,
                                   start_date=inputs.START, end_date=inputs.END),),
            evidence_refs=(evidence_reference_for_file(target / "evaluated/manifest.json", role="recovery_evaluated"),))
        AdvisoryResearchTrialRegistryV1(target / "trial_registry.jsonl").append_batch((record,))
        return dict(status=report["classification"], stage_sha256=parent, physical_fit_count=18,
                    output_root=str(target), **budget.receipt())
