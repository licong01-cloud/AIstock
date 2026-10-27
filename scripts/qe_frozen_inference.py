"""Worker-only frozen fitted-pipeline consumption. Never constructs or fits a handler.

Only generated QE manifests and pinned original artifacts are accepted. The worker
retains Qlib's native DatasetH/TSDatasetH and sampler semantics.
"""
from __future__ import annotations

import base64
from contextlib import ExitStack, contextmanager
import hashlib
import importlib
import json
from pathlib import Path
import pickle
from unittest.mock import patch

import numpy as np
import pandas as pd


def canonical_sha(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def read_pinned(data, digest, label):
    require(hashlib.sha256(data).hexdigest() == digest, "QE_FROZEN_SHA_MISMATCH: " + label)
    return data


def processor_fingerprint(processors):
    state = [(type(p).__module__, type(p).__name__, {k: v for k, v in vars(p).items() if k != "fit"})
             for p in processors]
    return hashlib.sha256(pickle.dumps(state, protocol=4)).hexdigest()


def weight_fingerprint(model):
    if type(model).__name__ == "LGBModel":
        return hashlib.sha256(model.model.model_to_string().encode()).hexdigest()
    require(type(model).__name__ == "GeneralPTNN", "QE_FROZEN_MODEL_CLASS_UNSUPPORTED")
    h = hashlib.sha256()
    for name, tensor in sorted(model.dnn_model.state_dict().items()):
        array = tensor.detach().cpu().numpy()
        h.update(name.encode())
        h.update(str(array.dtype).encode())
        h.update(str(array.shape).encode())
        h.update(array.tobytes())
    return h.hexdigest()


def _fail_fit(*args, **kwargs):
    raise RuntimeError("QE_FROZEN_FIT_FORBIDDEN")


def _score(frame):
    if isinstance(frame, pd.Series):
        frame = frame.to_frame("score")
    require(isinstance(frame, pd.DataFrame) and list(frame.columns) == ["score"], "QE_FROZEN_SCORE_SCHEMA_INVALID")
    require(isinstance(frame.index, pd.MultiIndex) and set(frame.index.names) == {"datetime", "instrument"},
            "QE_FROZEN_SCORE_INDEX_INVALID")
    require(frame.index.is_unique and frame["score"].notna().all(), "QE_FROZEN_SCORE_INDEX_OR_NA")
    require(np.isfinite(frame["score"].to_numpy()).all(), "QE_FROZEN_SCORE_NOT_FINITE")
    return frame.reorder_levels(["datetime", "instrument"]).sort_index()


class FrozenPipeline:
    def __init__(self, config, source_dir, cwd=None):
        self.cwd = Path(cwd or Path.cwd())
        self.config = config
        self.spec = config["qe_frozen_inference"]
        self.manifest = json.loads((self.cwd / "qe_frozen_inference_manifest.json").read_text(encoding="utf-8"))
        require(self.manifest.get("schema_version") == "qe_frozen_inference_manifest_v1"
                and self.manifest.get("spec") == self.spec, "QE_FROZEN_MANIFEST_MISMATCH")
        binding = json.loads((self.cwd / "qe_direct_v2_dataset_binding.json").read_text(encoding="utf-8"))
        require(canonical_sha(binding) == self.manifest.get("input_binding_sha256"), "QE_FROZEN_BINDING_MISMATCH")
        self.source_files = {}
        for name, digest in self.spec["code_sha256"].items():
            path = self.cwd / name
            require(path.resolve().is_relative_to(self.cwd.resolve()), "QE_FROZEN_CODE_PATH_INVALID")
            read_pinned(path.read_bytes(), digest, name)
            self.source_files[path] = digest
        for module, digest in self.spec["model_module_sha256"].items():
            loaded = importlib.import_module(module)
            path = Path(loaded.__file__)
            read_pinned(path.read_bytes(), digest, module)
            self.source_files[path] = digest
        assets_path = self.cwd / "qe_frozen_source_assets.json"
        assets = json.loads(assets_path.read_text(encoding="utf-8"))
        require(assets.get("schema_version") == "qe_frozen_source_assets_v1", "QE_FROZEN_ASSET_SCHEMA_INVALID")
        require(canonical_sha(assets["source_model_config"]) == self.spec["source_model_config_sha256"],
                "QE_FROZEN_SOURCE_MODEL_CONFIG_MISMATCH")
        require(canonical_sha(config["task"]["model"]) == self.spec["source_model_config_sha256"],
                "QE_FROZEN_TARGET_MODEL_CONFIG_MISMATCH")
        self.source_test = tuple(assets["source_test_segment"])
        dataset_bytes = read_pinned(base64.b64decode(assets["dataset_b64"], validate=True),
                                    self.spec["dataset_sha256"], "dataset")
        prediction_bytes = read_pinned(base64.b64decode(assets["reference_prediction_b64"], validate=True),
                                       self.spec["reference_prediction_sha256"], "reference_prediction")
        params = list(Path(source_dir).rglob("params.pkl"))
        require(len(params) == 1 and not params[0].is_symlink(), "QE_FROZEN_MODEL_CARDINALITY")
        model_bytes = read_pinned(params[0].read_bytes(), self.spec["model_sha256"], "params.pkl")
        # All artifact and executable code checks precede deserialization.
        self.model = pickle.loads(model_bytes)
        self.dataset = pickle.loads(dataset_bytes)
        self.reference = _score(pickle.loads(prediction_bytes))
        self.source_files[params[0]] = self.spec["model_sha256"]
        self.source_files[assets_path] = hashlib.sha256(assets_path.read_bytes()).hexdigest()
        self.handler = self.dataset.handler
        require(type(self.dataset).__name__ == self.spec["dataset_class"], "QE_FROZEN_DATASET_CLASS_MISMATCH")
        require(type(self.handler).__name__ == "DataHandlerLP", "QE_FROZEN_HANDLER_CLASS_UNSUPPORTED")
        require(type(self.handler.data_loader).__name__ == "DynamicFactorsOnlyLoader"
                and type(self.handler.data_loader).__module__ == "qe_custom_loaders", "QE_FROZEN_LOADER_UNSUPPORTED")
        self.infer = list(self.handler.shared_processors) + list(self.handler.infer_processors)
        self.all_processors = self.infer + list(self.handler.learn_processors)
        normalizers = []
        for proc in self.infer:
            require(type(proc).__module__ == "qlib.data.dataset.processor"
                    and type(proc).__name__ in {"RobustZScoreNorm", "Fillna"}, "QE_FROZEN_PROCESSOR_UNSUPPORTED")
            require(proc.fields_group == "feature" and proc.is_for_infer(), "QE_FROZEN_PROCESSOR_SCOPE_INVALID")
            if type(proc).__name__ == "RobustZScoreNorm":
                normalizers.append(proc)
                require(list(proc.cols) == [("feature", f) for f in self.spec["feature_names"]], "QE_FROZEN_FITTED_COLUMN_ORDER_MISMATCH")
                require(hasattr(proc, "mean_train") and hasattr(proc, "std_train"), "QE_FROZEN_NORMALIZER_NOT_FITTED")
                require(pd.Timestamp(proc.fit_start_time) == pd.Timestamp(self.spec["normalizer_fit_start"])
                        and pd.Timestamp(proc.fit_end_time) == pd.Timestamp(self.spec["normalizer_fit_end"]),
                        "QE_FROZEN_NORMALIZER_FIT_WINDOW_MISMATCH")
                require(len(proc.mean_train) == len(self.spec["feature_names"])
                        and len(proc.std_train) == len(self.spec["feature_names"]), "QE_FROZEN_NORMALIZER_WIDTH_MISMATCH")
        require(len(normalizers) == 1, "QE_FROZEN_NORMALIZER_CARDINALITY")
        if self.spec["dataset_class"] == "TSDatasetH":
            require(self.dataset.step_len == self.spec["step_len"] == 20, "QE_FROZEN_TEMPORAL_CONTRACT_INVALID")
        else:
            require(self.spec["step_len"] == 1, "QE_FROZEN_TEMPORAL_CONTRACT_INVALID")
        if type(self.model).__name__ == "GeneralPTNN":
            require(self.model.fitted is True, "QE_FROZEN_MODEL_NOT_FITTED")
        self.processor_before = processor_fingerprint(self.all_processors)
        self.weights_before = weight_fingerprint(self.model)
        self.prediction_checked = False
        self.result = None

    def prepare(self):
        segment = self.config["task"]["dataset"]["kwargs"]["segments"]["test"]
        self.test = tuple(pd.Timestamp(t) for t in segment)
        require(self.test[0] <= pd.Timestamp(self.source_test[0])
                and self.test[1] >= pd.Timestamp(self.source_test[1]), "QE_FROZEN_REFERENCE_WINDOW_NOT_COVERED")
        self.handler.config(start_time=self.handler.start_time, end_time=self.test[1])
        raw = self.handler.data_loader.load(self.handler.instruments, self.handler.start_time, self.test[1])
        require(isinstance(raw, pd.DataFrame) and raw.columns.is_unique and raw.index.is_unique,
                "QE_FROZEN_RAW_SCHEMA_INVALID")
        require(isinstance(raw.columns, pd.MultiIndex) and "feature" in raw.columns.get_level_values(0),
                "QE_FROZEN_RAW_FEATURES_MISSING")
        observed = list(raw["feature"].columns)
        expected = self.spec["feature_names"]
        excluded = self.spec["excluded_reference_columns"]
        require(set(observed) == set(expected + excluded), "QE_FROZEN_RAW_COLUMN_SET_MISMATCH")
        require([name for name in observed if name not in excluded] == expected, "QE_FROZEN_RAW_COLUMN_ORDER_MISMATCH")
        raw = raw.loc[:, [(kind, name) for kind, name in raw.columns if kind != "feature" or name not in excluded]].copy()
        require(set(raw.index.names) == {"datetime", "instrument"}, "QE_FROZEN_RAW_INDEX_INVALID")
        transformed = raw.copy()
        for processor in self.infer:
            transformed = processor(transformed)
        require(transformed.index.equals(raw.index) and transformed.columns.equals(raw.columns),
                "QE_FROZEN_PROCESSOR_SAMPLE_OR_SCHEMA_CHANGED")
        self.handler._data = raw
        self.handler._infer = transformed
        # Learning processors remain original evidence but are never invoked.
        self.handler.__dict__.pop("_learn", None)
        self.dataset.segments = {"test": segment}
        if self.spec["dataset_class"] == "TSDatasetH":
            self.dataset.cal = pd.Index(raw.index.get_level_values("datetime").unique()).sort_values().to_numpy()
            require(len(self.dataset.cal[self.dataset.cal < self.test[0]]) >= 19, "QE_FROZEN_TEMPORAL_CONTEXT_MISSING")
        self.dataset.config(dump_all=False, recursive=True)
        self.raw_rows = len(raw)
        self.raw_start = str(raw.index.get_level_values("datetime").min())

    def check_prediction(self, prediction):
        require(not self.prediction_checked, "QE_FROZEN_PREDICTION_ALREADY_CHECKED")
        score = _score(prediction)
        dates = score.index.get_level_values("datetime")
        require(dates.min() == self.test[0] and dates.max() == self.test[1], "QE_FROZEN_OUTPUT_WINDOW_MISMATCH")
        left, right = map(pd.Timestamp, self.source_test)
        overlap = score.loc[(dates >= left) & (dates <= right)]
        ref_dates = self.reference.index.get_level_values("datetime")
        reference = self.reference.loc[(ref_dates >= left) & (ref_dates <= right)]
        require(len(reference) > 0 and overlap.index.equals(reference.index), "QE_FROZEN_REFERENCE_PANEL_MISMATCH")
        is_float32 = reference["score"].dtype == np.dtype("float32")
        atol, rtol = (1e-6, 1e-5) if is_float32 else (1e-10, 1e-8)
        require(np.allclose(overlap["score"], reference["score"], atol=atol, rtol=rtol),
                "QE_FROZEN_REFERENCE_PREDICTION_MISMATCH")
        # Tolerance diagnoses numerical compatibility, but cannot prove unchanged
        # threshold crossings, ranks, lot rounding, and target weights. Any
        # residual difference needs an explicit review before an account runs.
        require(np.array_equal(overlap["score"].to_numpy(), reference["score"].to_numpy()),
                "QE_FROZEN_REFERENCE_NUMERIC_DRIFT_REQUIRES_REVIEW")
        self.prediction_checked = True
        metadata = json.dumps({"index_names": [str(v) for v in score.index.names],
                               "columns": [str(v) for v in score.columns],
                               "dtypes": [str(v) for v in score.dtypes], "rows": len(score)},
                              sort_keys=True, separators=(",", ":")).encode("utf-8")
        values = pd.util.hash_pandas_object(score, index=True, categorize=True).values.tobytes()
        self.result = {"executable_prediction_panel_sha256": hashlib.sha256(metadata + b"\n" + values).hexdigest(),
                       "mode": "frozen_inference", "schema_version": "qe_frozen_inference_result_v1",
                       "fit_calls": 0, "source_task_id": self.manifest["source_task_id"],
                       "source_loop_index": self.manifest["source_loop_index"], "spec": self.spec,
                       "raw_rows": self.raw_rows, "raw_start": self.raw_start,
                       "feature_names": self.spec["feature_names"], "step_len": self.spec["step_len"],
                       "output_rows": len(score), "output_start": str(dates.min()), "output_end": str(dates.max()),
                       "reference_rows": len(reference), "reference_equivalent": True,
                       "reference_atol": atol, "reference_rtol": rtol,
                       "reference_max_abs_error": float(np.max(np.abs(overlap["score"].to_numpy() - reference["score"].to_numpy())))}

    def finish(self):
        require(self.prediction_checked, "QE_FROZEN_REFERENCE_CHECK_REQUIRED_BEFORE_BACKTEST")
        after_processor = processor_fingerprint(self.all_processors)
        after_weight = weight_fingerprint(self.model)
        require(after_processor == self.processor_before, "QE_FROZEN_PROCESSOR_STATE_CHANGED")
        require(after_weight == self.weights_before, "QE_FROZEN_MODEL_WEIGHTS_CHANGED")
        for path, digest in self.source_files.items():
            read_pinned(path.read_bytes(), digest, str(path))
        self.result.update(processor_state_before=self.processor_before, processor_state_after=after_processor,
                           model_weights_before=self.weights_before, model_weights_after=after_weight,
                           source_bytes_unchanged=True, status="verified")
        path = self.cwd / "qe_frozen_inference_result.json"
        with path.open("x", encoding="utf-8") as stream:
            json.dump(self.result, stream, ensure_ascii=False, indent=2, allow_nan=False)
        return self.result


@contextmanager
def consume(config, source_dir, cwd=None):
    pipeline = FrozenPipeline(config, source_dir, cwd)
    with ExitStack() as stack:
        for target in [pipeline.model, *pipeline.all_processors]:
            stack.enter_context(patch.object(target, "fit", _fail_fit))
        # Any accidental default Qlib setup path must fail, including fit_process_data.
        stack.enter_context(patch.object(pipeline.handler, "setup_data", _fail_fit))
        pipeline.prepare()
        yield pipeline
    pipeline.finish()


def validate_mode(config, *, backtest_only):
    present = config.get("qe_frozen_inference") is not None
    require(present == Path("qe_frozen_inference_manifest.json").exists(), "QE_FROZEN_MANIFEST_OPT_IN_MISMATCH")
    if present:
        require(backtest_only, "QE_FROZEN_REQUIRES_BACKTEST_ONLY")
