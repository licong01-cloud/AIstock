"""Frozen, bounded inputs for the QE finite natural-entry trend contract."""
from __future__ import annotations

from copy import deepcopy
import base64
import hashlib
import json
from pathlib import Path
import re
import zipfile

import numpy as np
import pandas as pd

CONTRACT = "QE-L7-TREND-NATURAL-ENTRY-V2"
SCHEMA = "qe_trend_natural_entry_v1"
BASE_SOURCE_SHA = "1a3d00c2c6ed160ac6a1114f740259505b5cf140281a59c40679f3257d78c1bb"
MANIFEST_FILE = "qe_trend_entry_manifest.json"
PANEL_FILE = "qe_trend_entry_panel.npz"
ACTION_FILE = "qe_trend_entry_actions.jsonl"
MAX_ASSET_BYTES = 128 * 1024 * 1024
REASONS = {0: "AVAILABLE", 1: "BOUND_PHYSICAL_NA_WITH_DECLARED_SUSPENSION"}


class TrendEntryContractError(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise TrendEntryContractError("QE_TREND_ENTRY_" + code)


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def seal(manifest):
    m = deepcopy(manifest)
    m.pop("manifest_sha256", None)
    return {**m, "manifest_sha256": canonical_sha(m)}


def physical_binding(binding):
    b = deepcopy(binding)
    b.get("selection_pins", {}).pop("coverage_receipt_sha256", None)
    return b


def symbol_key(value):
    value = str(value).upper()
    if re.fullmatch(r"(?:SH|SZ)\d{6}", value):
        value = value[2:] + "." + value[:2]
    require(bool(re.fullmatch(r"\d{6}\.(?:SH|SZ)", value)), "SYMBOL_INVALID")
    return value


def date_key(value):
    stamp = pd.Timestamp(value)
    require(not pd.isna(stamp) and stamp.tz is None, "DATE_INVALID")
    return stamp.strftime("%Y-%m-%d")


def validate_manifest(manifest):
    m = manifest
    require(isinstance(m, dict) and m.get("schema_version") == SCHEMA
            and m.get("contract") == CONTRACT, "CONTRACT_INVALID")
    require(m.get("manifest_sha256") == seal(m)["manifest_sha256"], "MANIFEST_HASH")
    require(m.get("arm") in ("S10", "L7") and m.get("seed") in (42, 2024, 2026), "ARM_SEED")
    require(m.get("policy") == "post_decision_natural_buy_finite_r20_r60_positive"
            and m.get("source_sha256") == BASE_SOURCE_SHA, "POLICY_OR_SOURCE")
    b = m.get("dataset_binding", {})
    require(b.get("schema_version") == "qe_direct_v2_dataset_binding_v3", "DATASET_BINDING")
    for key in ("candidate_root", "provider_uri_day", "provider_uri_1min", "release_id"):
        require(isinstance(b.get(key), str) and bool(b[key]), "DATASET_BINDING")
    for key in ("day_pins", "minute_pins", "selection_pins"):
        require(isinstance(b.get(key), dict) and bool(b[key]), "PIT_BINDING")
    for value in (m.get("dataset_manifest_identity"), m.get("dataset_manifest_file_sha256"),
                  m.get("original_port_sha256"), m.get("panel", {}).get("sha256"),
                  m.get("panel", {}).get("index_sha256")):
        require(isinstance(value, str) and bool(re.fullmatch("[0-9a-f]{64}", value)), "HASH_INVALID")
    p, a = m["panel"], m.get("account", {})
    require(p.get("path") == PANEL_FILE and type(p.get("rows")) is int
            and 0 < p["rows"] <= 5_000_000, "PANEL_METADATA")
    require((p.get("start"), p.get("end"), p.get("days")) ==
            ("2024-07-01", "2026-09-30", 549), "SIGNAL_WINDOW")
    require(a in ({"role": "producer", "start": "2024-07-01", "end": "2026-09-29", "days": 548},
                  {"role": "final", "start": "2024-07-01", "end": "2026-09-30", "days": 549}),
            "ACCOUNT_WINDOW")
    require(isinstance(m.get("source_receipts"), dict) and bool(m["source_receipts"]), "PROVENANCE")
    return m


def checked_calendar(m):
    b = m["dataset_binding"]
    checks = [(Path(b["candidate_root"]) / "qe_dataset_manifest.json", m["dataset_manifest_file_sha256"])]
    for provider, pins, freq in (("provider_uri_day", "day_pins", "day"),
                                  ("provider_uri_1min", "minute_pins", "1min")):
        root = Path(b[provider])
        checks += [(root / "calendars" / (freq + ".txt"), b[pins]["calendar_sha256"]),
                   (root / "meta_export.json", b[pins]["meta_export_sha256"])]
    checks.append((Path(b["provider_uri_day"]) / "instruments" / b["selection_pins"]["instruments_file"],
                   b["selection_pins"]["instruments_sha256"]))
    for path, expected in checks:
        require(sha256(path) == expected, "PHYSICAL_DATASET_HASH:" + path.name)
    days = pd.DatetimeIndex(pd.to_datetime((Path(b["provider_uri_day"]) / "calendars/day.txt").read_text().splitlines()))
    require(days.is_unique and days.is_monotonic_increasing and days.equals(days.normalize()), "CALENDAR")
    return days


def panel_index_sha(dates, codes, offsets, stock_indices):
    """Canonical keys, streamed by day; never construct a stock-day outer product."""
    h = hashlib.sha256()
    for i, day in enumerate(dates):
        names = codes[stock_indices[offsets[i]:offsets[i + 1]]]
        h.update((str(day) + "\n" + "\n".join(names.tolist()) + "\n").encode())
    return h.hexdigest()


class TrendEntryInputs:
    def __init__(self, manifest_file):
        path = Path(manifest_file)
        self.manifest = validate_manifest(json.loads(path.read_text(encoding="utf-8")))
        m = self.manifest
        self.calendar = checked_calendar(m)
        asset = path.parent / PANEL_FILE
        if not asset.exists():
            encoded = asset.with_name(asset.name + ".b64")
            require(encoded.stat().st_size <= MAX_ASSET_BYTES * 4 // 3 + 8, "ASSET_TOO_LARGE")
            raw = base64.b64decode(encoded.read_bytes(), validate=True)
            require(hashlib.sha256(raw).hexdigest() == m["panel"]["sha256"], "TRANSPORT_HASH")
            with asset.open("xb") as stream:
                stream.write(raw)
        require(asset.stat().st_size <= MAX_ASSET_BYTES and sha256(asset) == m["panel"]["sha256"], "ASSET_HASH")
        with zipfile.ZipFile(asset) as archive:
            require(sum(x.file_size for x in archive.infolist()) <= MAX_ASSET_BYTES, "EXPANDED_ASSET_TOO_LARGE")
        with np.load(asset, allow_pickle=False) as z:
            self.arrays = {name: z[name] for name in z.files}
        self._validate_panel()
        self._cache_day, self._cache = None, {}
        # A composed run seals both old execution dependencies and new source.
        require(isinstance(m.get("execution_files_sha256"), dict) and bool(m["execution_files_sha256"]),
                "EXECUTION_SEAL_MISSING")
        for name, digest in m["execution_files_sha256"].items():
            require(Path(name).name == name, "EXECUTION_PATH")
            # Composer transfers text using universal newlines, as other QE modules do.
            observed = hashlib.sha256((path.parent / name).read_text(encoding="utf-8").encode()).hexdigest()
            require(observed == digest, "EXECUTION_SOURCE_HASH:" + name)
        import yaml
        config = yaml.safe_load((path.parent / "conf.yaml").read_text(encoding="utf-8"))
        execution = json.loads(json.dumps({k: config[k] for k in
                                          ("port_analysis_config", "qlib_init", "market")}, default=str))
        require(canonical_sha(execution) == m.get("execution_contract_sha256"), "EXECUTION_CONFIG")

    def _validate_panel(self):
        a, m = self.arrays, self.manifest
        require(set(a) == {"dates", "execution_dates", "codes", "offsets", "stock_indices", "return20", "return60", "reason"},
                "PANEL_FIELDS")
        require(all(x.ndim == 1 for x in a.values()), "ARRAY_SHAPE")
        d, c, o, ids = a["dates"], a["codes"], a["offsets"], a["stock_indices"]
        require(d.dtype.kind == c.dtype.kind == a["execution_dates"].dtype.kind == "U"
                and o.dtype == np.dtype("int64") and ids.dtype == np.dtype("int32")
                and a["reason"].dtype == np.dtype("uint8")
                and a["return20"].dtype == a["return60"].dtype == np.dtype("float32"), "ARRAY_DTYPE")
        require(len(d) == m["panel"]["days"] and len(c) <= 10000 and len(c) > 0
                and len(o) == len(d) + 1 and o[0] == 0 and np.all(np.diff(o) > 0)
                and o[-1] == len(ids) == m["panel"]["rows"], "ARRAY_LENGTH")
        require(all(len(a[k]) == len(ids) for k in ("return20", "return60", "reason")), "VALUES_LENGTH")
        require(c.tolist() == sorted(set(c.tolist())) and all(symbol_key(s) == s for s in c), "CODE_DICTIONARY")
        require(np.all((ids >= 0) & (ids < len(c))), "STOCK_INDEX_RANGE")
        expected = self.calendar[(self.calendar >= m["panel"]["start"]) & (self.calendar <= m["panel"]["end"])]
        require(d.tolist() == expected.strftime("%Y-%m-%d").tolist(), "PANEL_DATES")
        positions = self.calendar.get_indexer(expected)
        require(np.all(positions + 1 < len(self.calendar)) and a["execution_dates"].tolist() ==
                self.calendar[positions + 1].strftime("%Y-%m-%d").tolist(), "SIGNAL_EXECUTION_LAG")
        for i in range(len(d)):
            require(np.all(np.diff(ids[o[i]:o[i + 1]]) > 0), "DUPLICATE_OR_UNSORTED_KEYS")
        r20, r60, reason = a["return20"], a["return60"], a["reason"]
        require(not np.isinf(r20).any() and not np.isinf(r60).any(), "INF_INPUT")
        finite = np.isfinite(r20) & np.isfinite(r60)
        require(np.all(reason[finite] == 0) and np.all(reason[~finite] == 1), "UNEXPLAINED_NA_OR_REASON")
        require(panel_index_sha(d, c, o, ids) == m["panel"]["index_sha256"], "PANEL_KEY_HASH")
        self._dates = {str(day): i for i, day in enumerate(d)}
        account = m["account"]
        self.decision_dates = self.calendar[(self.calendar >= account["start"]) & (self.calendar <= account["end"])]
        require(len(self.decision_dates) == account["days"], "ACCOUNT_CALENDAR")

    def validate_time(self, execution, signal):
        day, prior = date_key(execution), date_key(signal)
        index = self.calendar.get_indexer([pd.Timestamp(day)])[0]
        require(index > 0 and pd.Timestamp(day) in self.decision_dates
                and date_key(self.calendar[index - 1]) == prior, "SIGNAL_TIME")
        return day, prior

    def _day(self, signal):
        require(signal in self._dates, "MISSING_SIGNAL_DAY:" + signal)
        if signal != self._cache_day:
            a, i = self.arrays, self._dates[signal]
            lo, hi = a["offsets"][i:i + 2]
            self._cache = {str(a["codes"][a["stock_indices"][j]]): int(j) for j in range(lo, hi)}
            self._cache_day = signal
        return self._cache

    def check_prediction(self, predictions, signal, execution):
        empty = predictions is None or predictions.empty
        if signal not in self._dates:
            require(execution == self.manifest["account"]["start"] and empty, "UNDECLARED_EMPTY_SIGNAL")
            return
        require(not empty, "MISSING_PREDICTION_DAY")
        index = predictions.index
        if isinstance(index, pd.MultiIndex):
            require(set(index.names) == {"datetime", "instrument"}, "PREDICTION_INDEX")
            require(all(date_key(d) == signal for d in index.get_level_values("datetime").unique()), "PREDICTION_TIME")
            values = index.get_level_values("instrument")
        else:
            values = index
        names = [symbol_key(s) for s in values]
        require(len(names) == len(set(names)) and set(names) == set(self._day(signal)), "COMMON_PREDICTION_KEYS")

    def evaluate(self, instrument, signal):
        sid = symbol_key(instrument)
        lookup = self._day(signal)
        require(sid in lookup, "MISSING_KEY:" + signal + ":" + sid)
        j, a = lookup[sid], self.arrays
        v20, v60 = float(a["return20"][j]), float(a["return60"][j])
        available = int(a["reason"][j]) == 0
        return {"instrument": sid, "return20": v20 if np.isfinite(v20) else None,
                "return60": v60 if np.isfinite(v60) else None,
                "availability": "available" if available else "unavailable",
                "reason": REASONS[int(a["reason"][j])],
                "allowed": bool(available and v20 > 0 and v60 > 0)}
