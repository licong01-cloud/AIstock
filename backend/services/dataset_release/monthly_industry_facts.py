"""Frozen official quotes and PIT-cohort flows for an explicit history repair.

SOURCE freezes independent published quotes. BUILD aggregates already sealed
stock moneyflow; it never reuses a previously aggregated industry's cohort,
opens a database, changes membership, or synthesizes a missing observation.
"""
from __future__ import annotations

from datetime import date
from functools import lru_cache
import hashlib
from typing import Mapping

import numpy as np
import pandas as pd

from .canonical import canonical_json_bytes
from .errors import SourceManifestError

CONTRACT = "official_quote_plus_pit_stock_flow_v1"
SCHEMA = "aistock_monthly_industry_quote_source_v1"
QUOTE_FIELDS = ("open", "high", "low", "close", "pct_change", "vol", "amount", "pe", "pb", "total_mv")
FLOW_FIELDS = {f"sw2_mf_{side}_{size}_amt": f"mf_{size}_{side}_amt"
               for side in ("buy", "sell") for size in ("sm", "md", "lg", "elg")}
FLOW_FIELDS.update(sw2_mf_net_amt="mf_net_amt", sw2_mf_buy_elg_vol="mf_elg_buy_vol",
                   sw2_mf_sell_elg_vol="mf_elg_sell_vol", sw2_mf_net_vol="mf_net_vol")


def capture_industry_quote_source(connection, *, cas, projection):
    """Registered read-only SOURCE query; small monthly CAS partitions only."""
    partitions = []
    codes = sorted({entry["canonical_l2_code"] for entry in projection.payload["entries"] if entry["taxonomy_version"] == "SW2021"})
    for period in pd.period_range(projection.payload["start_date"], projection.payload["end_date"], freq="M"):
        start = max(date.fromisoformat(projection.payload["start_date"]), period.start_time.date())
        end = min(date.fromisoformat(projection.payload["end_date"]), period.end_time.date())
        rows = []
        if end >= date(2021, 7, 30):
            with connection.cursor() as cursor:
                cursor.execute("SELECT ts_code,trade_date," + ",".join(QUOTE_FIELDS) +
                    " FROM market.sw_daily WHERE trade_date BETWEEN %s AND %s AND ts_code=ANY(%s)"
                    " ORDER BY trade_date,ts_code LIMIT 10001", (max(start, date(2021, 7, 30)), end, codes))
                rows = cursor.fetchall()
        if len(rows) > 10000:
            raise SourceManifestError("official industry quote partition exceeds its row bound")
        facts = [{"index_code": row[0], "trade_date": row[1].isoformat(),
                  **{key: None if value is None else str(value) for key, value in zip(QUOTE_FIELDS, row[2:], strict=True)}}
                 for row in rows]
        reference = cas.put_json({"schema_version": SCHEMA, "partition_key": str(period), "rows": facts})
        partitions.append({"partition_key": str(period), "row_count": len(facts), "ref": reference.as_dict()})
    return {"schema_version": SCHEMA, "projection_sha256": projection.sha256,
            "source_dataset": "market.sw_daily", "start_date": projection.payload["start_date"],
            "end_date": projection.payload["end_date"], "quote_units": "sw_daily_native_v1",
            "quote_taxonomy": "SW2021", "quote_start_date": "2021-07-30",
            "partitions": partitions, "database_write": False}


def load_frozen_industry_quote_source(cas, *, context_plan, build_inputs, projection):
    binding = context_plan.get("monthly_industry_quote_source")
    if binding is not None:
        reference = context_plan.get("monthly_industry_quote_source_ref") or {}
        if hashlib.sha256(canonical_json_bytes(binding) + b"\n").hexdigest() != reference.get("sha256"):
            raise SourceManifestError("industry quote supplement bytes differ")
        expected = {"operation_id": context_plan["operation_id"], "target_cutoff": context_plan["target_cutoff"],
                    "predecessor_manifest_sha256": context_plan["predecessor"]["dataset_manifest_sha256"],
                    "source_bundle_sha256": build_inputs["source_bundle_sha256"], "projection_sha256": projection.sha256}
        if (binding.get("schema_version") != "aistock_monthly_industry_quote_binding_v1" or
                binding.get("database_write") is not False or binding.get("activation_authorized") is not False or
                not binding.get("bound_by") or any(binding.get(key) != value for key, value in expected.items())):
            raise SourceManifestError("industry quote supplement is not bound to sealed SOURCE/history")
        return binding["quote_source"]
    manifest = build_inputs.get("source_manifest_ref")
    if manifest is None:
        return None
    source = cas.get_json_bounded(manifest, max_bytes=16 * 1024 * 1024)
    return (source.get("monthly_sector_mapping") or {}).get("historical_quote_source")


def predecessor_context_matches(prefix, projection):
    """Only small pinned authority metadata; ordinary months never audit history."""
    import json
    from .monthly_legacy_prefix import _plain_chain, _relative, _signature

    pins = [pin for pin in prefix.manifest["components"].values() if pin.get("path", "").endswith("/industry_l2_history_authority.json")]
    if len(pins) > 1:
        raise SourceManifestError("predecessor industry history authority is ambiguous")
    if not pins:
        return False
    pin = pins[0]
    path = prefix.root / _relative(pin["path"])
    _plain_chain(path)
    before = _signature(path)
    raw = path.read_bytes()
    if len(raw) != pin["size"] or hashlib.sha256(raw).hexdigest() != pin["sha256"] or before != _signature(path):
        raise SourceManifestError("predecessor industry history pin changed")
    payload = json.loads(raw)
    data = {key: payload[key] for key in ("authority_sha256", "entries", "effective_intervals")}
    return payload.get("industry_context_contract") == CONTRACT and hashlib.sha256(canonical_json_bytes(data)).hexdigest() == projection.data_sha256


def rebuild_industry_context(frame, *, projection, quotes, moneyflow, pit):
    """Separate published quotes from sums of the corrected strategy PIT cohort.

    Stock moneyflow is CNY/shares; sector storage remains native 10k CNY/hands.
    Missing facts stay NaN. SQL-compatible sums ignore individual NULLs, but an
    all-NULL group is not zero. No alias inference or current-membership join.
    """
    from .pit import filter_frame_to_pit_spans

    actual = projection.project(frame)
    days = frame.index.get_level_values("datetime")
    selected = ((days >= pd.Timestamp(projection.payload["start_date"])) &
                (days <= pd.Timestamp(projection.payload["end_date"])))
    flow_columns = [name for name in frame if name in FLOW_FIELDS]
    quote_columns = [name for name in frame if name.startswith("sw2_") and name not in FLOW_FIELDS]
    if quotes.index.has_duplicates or moneyflow.index.has_duplicates:
        raise SourceManifestError("frozen industry quote/moneyflow keys are duplicated")
    required = [FLOW_FIELDS[name] for name in flow_columns]
    if set(required).difference(moneyflow):
        raise SourceManifestError("frozen stock moneyflow fields are incomplete")
    cohort, _ = filter_frame_to_pit_spans(moneyflow, pit)
    if required and not cohort.empty:
        if np.isinf(cohort[required].to_numpy(dtype=float)).any():
            raise SourceManifestError("frozen stock moneyflow is infinite")
        cohort = projection.project(cohort.assign(l2_code_id=np.int16(-1)))
        cohort = cohort.loc[cohort.l2_code_id >= 0]
        cohort = cohort.astype({name: "float64" for name in required})
        aggregates = cohort.reset_index().groupby(["datetime", "l2_code_id"])[required].sum(min_count=1)
    else:
        aggregates = pd.DataFrame(columns=required, index=pd.MultiIndex.from_arrays([[], []], names=["datetime", "l2_code_id"]))
    keys = pd.MultiIndex.from_arrays([days[selected], actual.loc[selected, "l2_code_id"]], names=["datetime", "l2_code_id"])
    for column in quote_columns:
        if column not in quotes:
            raise SourceManifestError(f"official industry quote field is not frozen: {column}")
        values = quotes[column].reindex(keys).to_numpy(dtype=float)
        if np.isinf(values).any():
            raise SourceManifestError("official industry quote is infinite")
        converted = values.astype(frame[column].dtype)
        if np.isinf(converted).any():
            raise SourceManifestError("official industry quote exceeds storage precision")
        actual.loc[selected, column] = converted
    for column in flow_columns:
        values = aggregates[FLOW_FIELDS[column]].reindex(keys).to_numpy(dtype=float)
        values /= 100.0 if column.endswith("_vol") else 10000.0
        converted = values.astype(frame[column].dtype)
        if np.isinf(converted).any():
            raise SourceManifestError("industry moneyflow sum exceeds storage precision")
        actual.loc[selected, column] = converted
    return actual


class FrozenIndustryContext:
    """Bounded month cache shared by sector and static writers/readbacks."""

    def __init__(self, *, cas, source: Mapping, projection, moneyflow_path, pit, identity):
        if (source.get("schema_version") != SCHEMA or source.get("projection_sha256") != projection.sha256
                or source.get("source_dataset") != "market.sw_daily" or source.get("quote_units") != "sw_daily_native_v1"
                or source.get("database_write") is not False or source.get("quote_taxonomy") != "SW2021"
                or source.get("quote_start_date") != "2021-07-30"):
            raise SourceManifestError("independent industry quote source identity differs")
        self.cas, self.source, self.projection = cas, source, projection
        self.moneyflow_path, self.pit, self.identity = moneyflow_path, pit, identity
        self.parts = {part["partition_key"]: part for part in source["partitions"]}
        expected = [str(period) for period in pd.period_range(projection.payload["start_date"], projection.payload["end_date"], freq="M")]
        if list(self.parts) != expected or len(self.parts) != len(source["partitions"]):
            raise SourceManifestError("independent industry quote monthly scope differs")

    @lru_cache(maxsize=2)
    def month(self, key):
        from backend.data_service.security_source_identity import MONEYFLOW_DATASET

        part = self.parts[key]
        payload = self.cas.get_json_bounded(part["ref"], max_bytes=16 * 1024 * 1024)
        rows = payload.get("rows")
        if (payload.get("schema_version") != SCHEMA or payload.get("partition_key") != key or
                not isinstance(rows, list) or len(rows) > 10000 or len(rows) != part["row_count"]):
            raise SourceManifestError("independent quote partition differs")
        codes = {(entry["taxonomy_version"], entry["canonical_l2_code"]): entry["l2_code_id"]
                 for entry in self.projection.payload["entries"]}
        records = []
        period = pd.Period(key, freq="M")
        for row in rows:
            if set(row) != {"index_code", "trade_date", *QUOTE_FIELDS} or not period.start_time.date() <= date.fromisoformat(row["trade_date"]) <= period.end_time.date():
                raise SourceManifestError("official quote row fields/date differ")
            if row["trade_date"] < "2021-07-30" or ("SW2021", row["index_code"]) not in codes:
                raise SourceManifestError("official quote crosses its taxonomy/code boundary")
            id_ = codes[("SW2021", row["index_code"])]
            records.append({"datetime": pd.Timestamp(row["trade_date"]), "l2_code_id": id_,
                            **{"sw2_" + name: np.nan if row[name] is None else float(row[name]) for name in QUOTE_FIELDS}})
        quotes = pd.DataFrame(records, columns=["datetime", "l2_code_id", *["sw2_" + name for name in QUOTE_FIELDS]]).set_index(["datetime", "l2_code_id"])
        where = [f"datetime >= {str(period.start_time.date())!r}", f"datetime <= {str(period.end_time.date())!r}"]
        with pd.HDFStore(self.moneyflow_path, "r") as store:
            moneyflow = store.select("data", where=where)
        raw = moneyflow.reset_index().rename(columns={"datetime": "trade_date", "instrument": "ts_code"})
        canonical = self.identity.remap_source_rows(raw, canonical_codes=set(self.pit.to_frame().ts_code), source_dataset=MONEYFLOW_DATASET)
        canonical = canonical.rename(columns={"trade_date": "datetime", "ts_code": "instrument"}).set_index(["datetime", "instrument"])
        return quotes, canonical

    def remap(self, frame):
        actual = frame.copy()
        days = frame.index.get_level_values("datetime")
        selected = (days >= pd.Timestamp(self.projection.payload["start_date"])) & (days <= pd.Timestamp(self.projection.payload["end_date"]))
        for period in days[selected].to_period("M").unique():
            quotes, moneyflow = self.month(str(period))
            mask = selected & (days.to_period("M") == period)
            actual.loc[mask] = rebuild_industry_context(frame.loc[mask], projection=self.projection,
                                                       quotes=quotes, moneyflow=moneyflow, pit=self.pit)
        return actual
