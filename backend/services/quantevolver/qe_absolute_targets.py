"""Hash-pinned absolute stock targets; no prediction, label or model dependency."""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date
from pathlib import Path

SCHEMA = "qe_absolute_target_replay_v1"
PARAM = "absolute_target_replay"
MANIFEST_FILE = "qe_absolute_target_manifest.json"
RESULT_FILE = "qe_absolute_target_result.json"
TARGET_FILE = "absolute_targets.parquet"
SHA = re.compile(r"^[0-9a-f]{64}$")


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def normalize_contract(value):
    require(isinstance(value, dict), "QE_ABSOLUTE_TARGET_CONTRACT_REQUIRED")
    value = json.loads(json.dumps(value, allow_nan=False))
    require(set(value) == {"schema_version", "arm", "targets", "dataset", "window", "initial_cash",
                           "execution_reference", "sector_ids", "known_at"}, "QE_ABSOLUTE_TARGET_FIELDS")
    require(value["schema_version"] == SCHEMA, "QE_ABSOLUTE_TARGET_SCHEMA")
    require(value["arm"] in {"T", "B"}, "QE_ABSOLUTE_TARGET_ARM")
    for field in ("targets", "execution_reference"):
        ref = value[field]
        require(isinstance(ref, dict) and set(ref) == {"path", "sha256"}, "QE_ABSOLUTE_TARGET_ASSET_REF")
        require(isinstance(ref["path"], str) and ref["path"].strip(), "QE_ABSOLUTE_TARGET_PATH")
        require(bool(SHA.fullmatch(str(ref["sha256"]))), "QE_ABSOLUTE_TARGET_SHA256")
    binding = value["dataset"]
    require(isinstance(binding, dict) and set(binding) == {"release_id", "manifest_sha256", "profile_sha256"},
            "QE_ABSOLUTE_TARGET_DATASET_FIELDS")
    require(isinstance(binding["release_id"], str) and bool(binding["release_id"]), "QE_ABSOLUTE_TARGET_RELEASE")
    for key in ("manifest_sha256", "profile_sha256"):
        require(bool(SHA.fullmatch(str(binding[key]))), "QE_ABSOLUTE_TARGET_DATASET_SHA256")
    window = value["window"]
    require(isinstance(window, dict) and set(window) == {"signal_start", "signal_end", "account_start",
                                                       "account_end", "account_days"}, "QE_ABSOLUTE_TARGET_WINDOW")
    for field in ("signal_start", "signal_end", "account_start", "account_end"):
        date.fromisoformat(window[field])
    require(window["signal_start"] < window["account_start"] <= window["account_end"]
            and window["signal_start"] <= window["signal_end"] < window["account_end"],
            "QE_ABSOLUTE_TARGET_WINDOW_ORDER")
    require(type(window["account_days"]) is int and window["account_days"] > 0, "QE_ABSOLUTE_TARGET_DAYS")
    require(type(value["initial_cash"]) in (int, float) and math.isfinite(value["initial_cash"])
            and value["initial_cash"] > 0, "QE_ABSOLUTE_TARGET_INITIAL_CASH")
    ids = value["sector_ids"]
    require(isinstance(ids, list) and bool(ids) and all(type(i) is int for i in ids)
            and len(set(ids)) == len(ids), "QE_ABSOLUTE_TARGET_SECTORS")
    require(value["known_at"] == "UNVERIFIED", "QE_ABSOLUTE_TARGET_RETROSPECTIVE_DISCLOSURE")
    return value


def validate_mode(loop):
    value = loop.get(PARAM)
    if value is None:
        return None
    contract = normalize_contract(value)
    for key in ("backtest_only", "prediction_replay", "frozen_inference", "prediction_recovery",
                "enable_sector_hmm", "hmm", "multi_alpha_config", "ensemble", "feature_processing"):
        require(not loop.get(key), "QE_ABSOLUTE_TARGET_MODE_CONFLICT: " + key)
    for key in ("factor_keys", "factor_names", "factor_list", "model_id", "strategy_id", "model_params",
                "model_params_base", "model_source_task_id", "prediction_source_task_id", "label_type", "label_horizon"):
        require(not loop.get(key), "QE_ABSOLUTE_TARGET_MODEL_FREE: " + key)
    for group in ("runtime_flags", "strategy_params", "extra_params"):
        flags = loop.get(group) or {}
        require(not any(flags.get(k) is not None for k in ("random_seed", "seed", "random_state", "ensemble")),
                "QE_ABSOLUTE_TARGET_SEED_NOT_APPLICABLE")
    require(loop.get("alpha_mode", "single") == "single", "QE_ABSOLUTE_TARGET_ALPHA_MODE")
    return contract


class AbsoluteTargets:
    """Validate sequentially, then load at most one target day at a time.

    The original artifact is never rewritten. Row groups may contain several
    days; index construction stores only group numbers, never an N-by-N array.
    """

    def __init__(self, contract, *, path=None, calendar):
        import pyarrow.parquet as pq

        self.contract = normalize_contract(contract)
        self.path = Path(path or self.contract["targets"]["path"])
        require(file_sha256(self.path) == self.contract["targets"]["sha256"], "QE_ABSOLUTE_TARGET_HASH_MISMATCH")
        self.parquet = pq.ParquetFile(self.path)
        columns = ["signal_date", "target_date", "instrument", "sector", "state", "member_count", "weight_T", "weight_B"]
        require(self.parquet.schema_arrow.names == columns, "QE_ABSOLUTE_TARGET_COLUMNS")
        self.calendar = [str(x)[:10] for x in calendar]
        require(self.calendar == sorted(set(self.calendar)), "QE_ABSOLUTE_TARGET_CALENDAR_ORDER")
        self.next_date = dict(zip(self.calendar[:-1], self.calendar[1:]))
        w = self.contract["window"]
        self.days = [d for d in self.calendar if w["account_start"] <= d <= w["account_end"]]
        require(len(self.days) == w["account_days"] and self.days[0] == w["account_start"]
                and self.days[-1] == w["account_end"], "QE_ABSOLUTE_TARGET_FULL_CALENDAR")
        self.groups = {}
        for index in range(self.parquet.num_row_groups):
            table = self.parquet.read_row_group(index, columns=["target_date"])
            for day in table.column(0).unique().to_pylist():
                self.groups.setdefault(str(day)[:10], []).append(index)
        require(set(self.days) <= set(self.groups), "QE_ABSOLUTE_TARGET_MISSING_DAY")
        self.coverage = []
        for day in self.days:
            frame, summary = self._day(day)
            self.coverage.append(summary)
            del frame
        require(self.coverage[0]["signal_date"] == w["signal_start"]
                and self.coverage[-1]["signal_date"] == w["signal_end"], "QE_ABSOLUTE_TARGET_SIGNAL_BOUNDARY")

    def _day(self, day):
        import numpy as np
        import pandas as pd

        require(day in self.days, "QE_ABSOLUTE_TARGET_UNKNOWN_DAY")
        frame = self.parquet.read_row_groups(self.groups[day]).to_pandas()
        frame = frame[frame.target_date.eq(pd.Timestamp(day))].copy()
        require(not frame.empty and not frame.isna().any().any(), "QE_ABSOLUTE_TARGET_NULL_OR_EMPTY")
        signals = frame.signal_date.unique()
        require(len(signals) == 1, "QE_ABSOLUTE_TARGET_MULTIPLE_SIGNALS")
        signal = str(pd.Timestamp(signals[0]).date())
        require(self.next_date.get(signal) == day, "QE_ABSOLUTE_TARGET_CLOCK_NOT_NEXT_SESSION")
        require(frame.instrument.is_unique and frame.instrument.map(
            lambda s: bool(re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", str(s)))).all(), "QE_ABSOLUTE_TARGET_INSTRUMENT_KEYS")
        require(set(frame.sector) <= set(self.contract["sector_ids"]), "QE_ABSOLUTE_TARGET_UNKNOWN_SECTOR")
        require(frame.state.isin([-1, 0, 1]).all(), "QE_ABSOLUTE_TARGET_STATE")
        require(frame.state.ge(0).any(), "QE_ABSOLUTE_TARGET_NO_DETERMINABLE_INDUSTRY")
        require((frame.member_count > 0).all(), "QE_ABSOLUTE_TARGET_MEMBER_COUNT")
        for column in ("weight_T", "weight_B"):
            weights = frame[column].to_numpy(dtype=float)
            require(np.isfinite(weights).all() and (weights >= 0).all(), "QE_ABSOLUTE_TARGET_INVALID_WEIGHT")
            require(math.fsum(weights) <= 1 + 1e-10, "QE_ABSOLUTE_TARGET_LEVERAGE")
            require((frame.loc[frame.state.eq(-1), column] == 0).all(), "QE_ABSOLUTE_TARGET_UNKNOWN_BUDGET")
        exposure_t = math.fsum(frame.weight_T)
        exposure_b = math.fsum(frame.weight_B)
        require(abs(exposure_t - exposure_b) <= 1e-10, "QE_ABSOLUTE_TARGET_PAIR_EXPOSURE")
        sectors = []
        grouped = {int(key): group for key, group in frame.groupby("sector", sort=True)}
        for sector in self.contract["sector_ids"]:
            group = grouped.get(sector)
            if group is None:
                sectors.append({"sector": sector, "state": -1, "availability": False,
                                "reason": "NO_ELIGIBLE_MEMBERS", "target_weight": 0.0})
                continue
            require(group.state.nunique() == 1 and group.member_count.nunique() == 1,
                    "QE_ABSOLUTE_TARGET_INCONSISTENT_SECTOR")
            state = int(group.state.iloc[0])
            sectors.append({"sector": sector, "state": state, "availability": state != -1,
                            "reason": "SOURCE_UNKNOWN" if state == -1 else "SOURCE_KNOWN",
                            "target_weight": math.fsum(group["weight_" + self.contract["arm"]])})
        return frame, {"trade_date": day, "signal_date": signal, "rows": len(frame),
                       "target_exposure": exposure_t, "sectors": sectors}

    def day(self, day):
        frame, summary = self._day(day)
        weights = frame["weight_" + self.contract["arm"]]
        # This release's instruments and feature directories use the same native
        # six-digit exchange-suffix keys as the frozen target asset.
        target = {str(s): float(w) for s, w in zip(frame.instrument, weights) if w > 0}
        return target, summary
