"""Immutable report_rc raw and standardized file delivery. Never evaluates a signal."""

from __future__ import annotations

from datetime import date, timedelta
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from backend.services.report_rc_data import (
    CONTRACT,
    FIELDS,
    NORMALIZATION_VERSION,
    create_json,
    digest,
    encoded,
    file_identity,
    normalize_rows,
    raw_records,
    utcnow,
)


def add_prior_context(panel):
    """Report-date sequence only, NOT a historical as-of connection or revision signal."""
    group = ["ts_code", "org_key", "quarter"]
    valid = panel.loc[panel["forecast_period_quality"].eq("STANDARD_QUARTER_SYNTAX")].dropna(
        subset=group + ["report_date"]
    )
    day = (
        valid.groupby(group + ["report_date"], dropna=False, sort=True)
        .agg(
            finite_eps_count=("eps", lambda s: s.dropna().nunique()),
            eps_unique=("eps", lambda s: s.dropna().iloc[0] if s.dropna().nunique() == 1 else np.nan),
            report_count=("report_lineage_key", "nunique"),
        )
        .reset_index()
        .sort_values(group + ["report_date"])
    )
    for source, target in [
        ("report_date", "prior_report_date"),
        ("eps_unique", "prior_eps"),
        ("finite_eps_count", "prior_eps_value_count"),
    ]:
        day[target] = day.groupby(group, dropna=False)[source].shift(1)
    result = panel.merge(day, on=group + ["report_date"], how="left", validate="many_to_one")
    conditions = [
        result["forecast_period_quality"].ne("STANDARD_QUARTER_SYNTAX"),
        result["same_report_quarter_conflict"] | result["finite_eps_count"].gt(1),
        result["prior_report_date"].isna(),
        result["prior_eps_value_count"].gt(1),
        result["eps"].isna() | result["prior_eps"].isna(),
        result["eps"].eq(result["prior_eps"]),
    ]
    result["within_snapshot_pair_status"] = np.select(
        conditions,
        [
            "FORECAST_PERIOD_UNKNOWN",
            "CURRENT_DATE_CONFLICT",
            "NO_PRIOR_IN_CAPTURE_WINDOW",
            "PRIOR_DATE_CONFLICT",
            "EPS_VALUE_UNKNOWN",
            "UNCHANGED_VALUE_BASIS_UNKNOWN",
        ],
        default="CHANGED_VALUE_BASIS_UNKNOWN",
    )
    result["pair_time_semantics"] = "REPORT_DATE_SEQUENCE_NOT_ASOF_VALIDATED"
    result["report_identity_quality"] = "INTERNAL_TITLE_DATE_LINEAGE_NOT_PROVIDER_ID"
    result["first_coverage_quality"] = np.where(
        result["prior_report_date"].isna(),
        "FIRST_OBSERVED_IN_CAPTURE_NOT_PROOF_OF_FIRST_EVER_COVERAGE",
        "PREVIOUS_REPORT_OBSERVED_IN_CAPTURE",
    )
    first = valid.groupby(group, dropna=False)["report_date"].min().reset_index(name="first_quarter_date")
    first_any = (
        valid.groupby(["ts_code", "org_key"], dropna=False)["report_date"]
        .min()
        .reset_index(name="first_any_quarter_date")
    )
    first = first.merge(first_any, on=["ts_code", "org_key"], validate="many_to_one")
    first["later_forecast_period_observed"] = first["first_quarter_date"].gt(first["first_any_quarter_date"])
    return result.merge(first[group + ["later_forecast_period_observed"]], on=group, how="left", validate="many_to_one")


def population_coverage(panel, identity_file):
    # Column projection is deliberate: never load parent scores or return labels.
    columns = [
        "package_id",
        "manifest_sha256",
        "run_id",
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
    ]
    identities = pd.read_parquet(identity_file, columns=columns)
    if identities.duplicated(["package_id", "decision_as_of_trade_date", "instrument"]).any():
        raise ValueError("original frozen population identity duplicates")
    days = pd.to_datetime(identities["decision_as_of_trade_date"])
    identities["inside_capture_scope"] = days.between(CONTRACT["start_date"], CONTRACT["end_date"])
    stocks = set(panel["ts_code"])
    identities["instrument_source_symbol"] = identities["instrument"].astype(str)

    # Qlib CN instruments are SH600000/SZ000001, not current-stock or PIT remapping.
    def symbol(value):
        text = str(value).upper()
        return text[2:] + "." + text[:2] if len(text) == 8 and text[:2] in ("SH", "SZ", "BJ") else text

    identities["report_rc_symbol"] = identities["instrument"].map(symbol)
    identities["has_any_report_in_capture"] = identities["report_rc_symbol"].isin(stocks)
    first_report = panel.groupby("ts_code")["report_date"].min()
    pair_rows = panel.loc[
        panel["within_snapshot_pair_status"].isin(["CHANGED_VALUE_BASIS_UNKNOWN", "UNCHANGED_VALUE_BASIS_UNKNOWN"])
    ]
    first_pair = pair_rows.groupby("ts_code")["report_date"].min()
    identities["has_report_dated_on_or_before_D"] = identities["report_rc_symbol"].map(first_report).le(days)
    identities["has_same_quarter_pair_dated_on_or_before_D"] = identities["report_rc_symbol"].map(first_pair).le(days)
    identities["input_support_status"] = np.select(
        [
            ~identities["inside_capture_scope"],
            ~identities["has_report_dated_on_or_before_D"],
            ~identities["has_same_quarter_pair_dated_on_or_before_D"],
        ],
        [
            "OUTSIDE_CAPTURE_SCOPE_UNKNOWN",
            "NO_REPORT_IN_CURRENT_SNAPSHOT_UP_TO_D",
            "NO_FINITE_PRIOR_PAIR_IN_CAPTURE_UP_TO_D",
        ],
        default="PAIR_PRESENT_CLOCK_AND_BASIS_UNVERIFIED",
    )
    identities["coverage_semantics"] = "INPUT_EXISTENCE_ONLY_NOT_HISTORICAL_AVAILABILITY"
    summary = {
        "status": "EXISTING_ORIGINAL_IDENTITY_COLUMNS_ONLY",
        "source_path": str(identity_file),
        "identity_columns_read": columns,
        "original_rows": len(identities),
        "identity_canonical_sha256": digest(identities[columns].astype(str).to_dict("records")),
        "rows_inside_capture_scope": int(identities["inside_capture_scope"].sum()),
        "rows_outside_capture_scope": int((~identities["inside_capture_scope"]).sum()),
        "original_population_unchanged": True,
        "outcome_columns_read": False,
        "not_a_signal_asof_coverage_claim": True,
    }
    summary["by_package"] = {
        str(k): {
            "identity_rows": len(g),
            "symbols": g["instrument"].nunique(),
            "rows_inside_capture_scope": int(g["inside_capture_scope"].sum()),
            "any_report_stock_rows": int(g["has_any_report_in_capture"].sum()),
            "input_support_counts": {str(k): int(v) for k, v in g["input_support_status"].value_counts().items()},
        }
        for k, g in identities.groupby("package_id")
    }
    return identities, summary


def export_delivery(capture_root, destination, *, population_identity=None):
    capture_root, destination = Path(capture_root), Path(destination)
    if (capture_root / "collector.lock").exists():
        raise ValueError("collector is still active; do not freeze an evolving capture")
    start, end = map(date.fromisoformat, (CONTRACT["start_date"], CONTRACT["end_date"]))
    expected = {(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)}
    receipts = {p.stem: json.loads(p.read_bytes()) for p in (capture_root / "days").glob("*.json")}
    missing = sorted(expected - set(receipts))
    if missing or set(receipts) - expected:
        raise ValueError(f"capture dates incomplete: missing={missing[:5]} count={len(missing)}")
    destination.mkdir(parents=True, exist_ok=False)
    records = list(raw_records(capture_root))
    if not records:
        raise ValueError("entire report_rc range is empty; source support unproven")
    total = sum(r["rows"] for r in receipts.values())
    if len(records) != total:
        raise ValueError("raw rows differ from completed-date receipts")
    raw = []
    for record in records:
        source = record["row"]
        item = {k: v for k, v in record.items() if k != "row"}
        item.update({field: None if source.get(field) is None else str(source[field]) for field in FIELDS})
        item["source_row_sha256"] = digest(source)
        item["raw_payload_json"] = encoded(source).decode().strip()
        item["vintage_status"] = "NON_VINTAGE"
        raw.append(item)
    raw_panel = pd.DataFrame(raw)
    panel = add_prior_context(normalize_rows(records))
    panel["provider_report_id"] = panel["provider_report_id"].astype("string")
    panel["publication_available_at"] = pd.to_datetime(panel["publication_available_at"], utc=True)
    for column in ("op_rt", "op_pr", "tp", "np", "pe", "rd", "roe", "ev_ebitda", "max_price", "min_price"):
        panel[column] = pd.to_numeric(panel[column], errors="coerce").replace([np.inf, -np.inf], np.nan)
    files = []

    def parquet(name, frame):
        path = destination / name
        frame.to_parquet(path, index=False)
        info = file_identity(path)
        info.update(relative_path=name, rows=len(frame), schema=str(pq.read_schema(path)))
        files.append(info)

    parquet("raw.parquet", raw_panel)
    parquet("panel.parquet", panel)
    date_frame = pd.DataFrame(
        [
            {
                "report_date": d,
                "status": r["status"],
                "row_count": r["rows"],
                "completed_at": r["completed_at"],
                "pages": len(r["pages"]),
            }
            for d, r in sorted(receipts.items())
        ]
    )
    parquet("date_coverage.parquet", date_frame)
    population = {"status": "NOT_PROVIDED_ADVISORY_MUST_CHECK_ORIGINAL_TOP50", "population_rebuilt": False}
    if population_identity is not None:
        identities, population = population_coverage(panel, population_identity)
        parquet("original_population_input_coverage.parquet", identities)
    quality = {
        "schema_version": "aistock_report_rc_quality_v1",
        "rows": len(panel),
        "raw_rows": len(raw_panel),
        "symbols": panel["ts_code"].nunique(),
        "org_literal_keys": panel["org_key"].nunique(),
        "quarters": sorted(panel["quarter"].dropna().unique().tolist()),
        "expected_calendar_dates": len(expected),
        "completed_dates": len(receipts),
        "missing_dates": missing,
        "failed_dates": [],
        "recovered_failed_request_attempts": len(list((capture_root / "attempts").glob("*.json"))),
        "provider_historical_total_count_known": False,
        "pagination_closed_for_each_requested_day": True,
        "successful_empty_dates": sorted(d for d, r in receipts.items() if r["rows"] == 0),
        "empty_does_not_prove_historical_no_report": True,
        "duplicate_occurrence_rows": int(panel["duplicate_source_row"].sum()),
        "conflicting_occurrence_rows": int(panel["same_report_quarter_conflict"].sum()),
        "late_provider_update_rows": int(panel["create_time_after_report_day"].sum()),
        "invalid_or_missing_eps_rows": int(panel["eps"].isna().sum()),
        "forecast_period_quality_counts": {
            str(k): int(v) for k, v in panel["forecast_period_quality"].value_counts().items()
        },
        "pair_status_counts": {str(k): int(v) for k, v in panel["within_snapshot_pair_status"].value_counts().items()},
        "missingness_contract": {
            "no_report": "no record in successful current-provider snapshot up to D; historical absence unproven",
            "first_coverage": "first observed in capture; true first-ever coverage UNKNOWN",
            "no_prior": "no preceding same-stock/org/quarter report inside capture range",
            "no_change": "unchanged source EPS; basis and historical availability UNKNOWN",
            "quarter_roll": "different quarter is never joined as an EPS revision",
            "conflict": "retain all occurrences; no arbitrary latest/min/max winner",
        },
        "later_forecast_period_rows": int(panel["later_forecast_period_observed"].eq(True).sum()),
        "population_input_coverage": population,
        "vintage_status": "NON_VINTAGE",
        "historical_point_in_time_established": False,
        "production_research_confirmation_ready": False,
        "field_definitions": {
            "eps": "supplier documented yuan/share; currency field absent and share basis UNKNOWN",
            "create_time": "provider update timestamp, timezone UNKNOWN",
            "captured_at": "actual UTC response capture",
            "quarter": "raw forecast-period string",
            "provider_report_id": "null if supplier did not provide ID",
            "prior_eps": "unique previous report-date value in current snapshot, NOT causal signal",
        },
        "raw_field_units": {
            "op_rt/op_pr/tp/np": "supplier documented 10,000 yuan; currency field not provided",
            "eps": "supplier documented yuan/share; capital basis UNKNOWN",
            "pe": "supplier forecast PE, unchanged raw value",
            "rd/roe/ev_ebitda/max_price/min_price": "raw provider numeric; undocumented unit details UNKNOWN, no conversion",
        },
        "limitations": [
            "provider reconstructs historical records; removed or older revision vintages cannot be recovered",
            "offset pages are not an atomic provider snapshot; exact duplicates retained, changing-page consistency unverified",
            "institution aliases and share basis unresolved; no economic EPS revision signal is asserted",
        ],
        "database_production_written": False,
        "dataset_profile_changed": False,
        "runtime_action": False,
        "outcomes_read": False,
        "training": False,
    }
    create_json(destination / "quality.json", quality)
    files.append({**file_identity(destination / "quality.json"), "relative_path": "quality.json"})
    samples = {}
    wanted = [
        "ts_code",
        "org_name",
        "report_date",
        "quarter",
        "eps",
        "prior_report_date",
        "prior_eps",
        "create_time",
        "captured_at",
        "capture_id",
        "row_ordinal",
        "source_row_sha256",
        "within_snapshot_pair_status",
    ]
    selections = {
        "same_quarter_pairs": panel["prior_report_date"].notna(),
        "duplicates": panel["duplicate_source_row"],
        "conflicts": panel["same_report_quarter_conflict"],
        "late_updates": panel["create_time_after_report_day"],
        "forecast_period_roll": panel["later_forecast_period_observed"].eq(True),
        "unknown_forecast_period": panel["forecast_period_quality"].ne("STANDARD_QUARTER_SYNTAX"),
    }
    for name, mask in selections.items():
        selected = panel.loc[mask, wanted].head(5).astype(object).where(pd.notna(panel.loc[mask, wanted].head(5)), None)
        samples[name] = selected.to_dict("records")
    create_json(destination / "examples.json", samples)
    files.append({**file_identity(destination / "examples.json"), "relative_path": "examples.json"})
    schemas = {
        "schema_version": "aistock_report_rc_file_schema_v1",
        "tables": {f["relative_path"]: f.get("schema") for f in files if "rows" in f},
        "normalization_version": NORMALIZATION_VERSION,
        "raw_rows_never_deduplicated": True,
    }
    create_json(destination / "schema.json", schemas)
    files.append({**file_identity(destination / "schema.json"), "relative_path": "schema.json"})
    capture_inventory = []
    for d, r in sorted(receipts.items()):
        capture_inventory.append(
            {"report_date": d, "receipt": file_identity(capture_root / "days" / f"{d}.json"), "pages": r["pages"]}
        )
    create_json(destination / "capture_inventory.json", capture_inventory)
    files.append({**file_identity(destination / "capture_inventory.json"), "relative_path": "capture_inventory.json"})
    readme = """# report_rc 历史原始面板

范围：2023-07-01..2025-09-30（自然日）。2023-07-01..2024-07-03仅用于寻找前次预测，
不是已确定的信号回看期。原始层 raw.parquet 保留全部重复、冲突及每个 quarter；
raw_payload_json 保留供应商字段原值，行身份是 capture_id + row_ordinal，不是供应商报告ID。

panel.parquet 是同一原始层的版本化标准化，不选择冲突赢家，不跨预测期连接。
缺失或非标准 quarter 保留原文并标记 FORECAST_PERIOD_UNKNOWN，不进入普通季度配对。
prior_report_date/prior_eps 仅为当前快照内同股票、同机构字面键、同 quarter 的前一报告日上下文，
不是历史 as-of 结果或已确认同股本口径的 EPS 修正信号。

## 时钟和质量

统一 NON_VINTAGE：report_date 不代表收盘前可得，create_time 是供应商更新时间，
时区及历史首次公开时间未知。captured_at 为实际 UTC 抓取时间，不能倒填。
EPS 文档单位为元/股；币种字段未提供，股本口径 UNKNOWN。机构文字别名不做推断合并。
source documentation: https://tushare.pro/document/2?doc_id=292 。
本轮官网计划为当日21:15和次日09:15更新，但不是逐条历史首次公开证明。

成功空日只说明本次供应商快照返回空；不证明历史从未有报告。请求失败与空响应分开，
日期完成及页面哈希见 date_coverage.parquet/capture_inventory.json，有限失败尝试留在 capture_root。
所有日期请求分页闭环，不代表已证明供应商历史所有曾有记录和原始修订vintage完整。

## 最小读取

```python
from pathlib import Path
import json, hashlib
import pandas as pd
root = Path(__file__).parent  # 或替换为该版本交付目录
manifest = json.loads((root / "manifest.json").read_bytes())
for item in manifest["files"]:
    assert hashlib.sha256((root / item["relative_path"]).read_bytes()).hexdigest() == item["sha256"]
panel = pd.read_parquet(root / "panel.parquet", columns=[
    "ts_code", "org_name", "report_date", "report_title", "quarter", "eps",
    "create_time", "captured_at", "capture_id", "row_ordinal", "within_snapshot_pair_status"])
print(panel.head())
```

Advisory负责可得时钟、as-of连接、法人别名/股本口径、信号定义及有界研究。
原Top50覆盖文件只读取原包身份/日期，不重建人口、不读取收益；报告日支持不等于D时可得。
本交付无训练、收益筛选、生产荐股或数据集profile切换。
"""
    with (destination / "README.md").open("x", encoding="utf-8") as handle:
        handle.write(readme)
    files.append({**file_identity(destination / "README.md"), "relative_path": "README.md"})
    producer_files = [file_identity(Path(__file__)), file_identity(Path(__file__).with_name("report_rc_data.py"))]
    manifest = {
        "schema_version": "aistock_report_rc_history_delivery_v1",
        "data_version": destination.name,
        "created_at": utcnow(),
        "source_api": "report_rc",
        "contract": CONTRACT,
        "contract_sha256": digest(CONTRACT),
        "normalization_version": NORMALIZATION_VERSION,
        "raw_capture_contract_unchanged": True,
        "producer_files_at_export": producer_files,
        "capture_root": str(capture_root),
        "files": files,
        "data_range": [CONTRACT["start_date"], CONTRACT["end_date"]],
        "provider_report_date_range": [
            panel["report_date"].min().date().isoformat(),
            panel["report_date"].max().date().isoformat(),
        ],
        "actual_capture_range": [panel["captured_at"].min().isoformat(), panel["captured_at"].max().isoformat()],
        "calendar_capture_complete": True,
        "raw_panel_delivered": True,
        "historical_pit_established": False,
        "research_usability": "NON_VINTAGE_EXPLORATORY_INPUT_ONLY_ASOF_AND_BASIS_REVIEW_REQUIRED",
        "database_persistence_status": "DEV_VALIDATED_PRODUCTION_PENDING_AUTHORIZATION",
        "outcomes_read": False,
        "active_profile_write": False,
        "runtime_action": False,
    }
    create_json(destination / "manifest.json", manifest)
    return {"manifest": file_identity(destination / "manifest.json"), "quality": quality}


def verify_delivery(destination):
    destination = Path(destination)
    manifest = json.loads((destination / "manifest.json").read_bytes())
    for entry in manifest["files"]:
        actual = file_identity(destination / entry["relative_path"])
        if actual["sha256"] != entry["sha256"] or actual["bytes"] != entry["bytes"]:
            raise ValueError("delivery file identity differs")
    raw = pd.read_parquet(destination / "raw.parquet")
    panel = pd.read_parquet(destination / "panel.parquet")
    if (
        len(raw) != len(panel)
        or not panel["vintage_status"].eq("NON_VINTAGE").all()
        or panel["publication_available_at"].notna().any()
    ):
        raise ValueError("delivery row/clock contract differs")
    if raw.duplicated(["capture_id", "row_ordinal"]).any() or panel.duplicated(["capture_id", "row_ordinal"]).any():
        raise ValueError("capture occurrence identities are not unique")
    keys = ["capture_id", "row_ordinal"]
    left = raw.set_index(keys)
    right = panel.set_index(keys).reindex(left.index)
    for column in ("ts_code", "org_name", "quarter", "source_row_sha256"):
        if not left[column].fillna("").astype(str).eq(right[column].fillna("").astype(str)).all():
            raise ValueError("raw/normalized identity or quarter differs")
    source_eps = pd.to_numeric(left["eps"], errors="coerce").replace([np.inf, -np.inf], np.nan)
    if not (source_eps.eq(right["eps"]) | (source_eps.isna() & right["eps"].isna())).all():
        raise ValueError("raw/normalized EPS differs")
    return {
        "verified": True,
        "raw_rows": len(raw),
        "panel_rows": len(panel),
        "files": len(manifest["files"]),
        "vintage_status": "NON_VINTAGE",
        "historical_pit_established": False,
        "outcomes_read": False,
        "database_access": False,
        "runtime_action": False,
        "manifest": file_identity(destination / "manifest.json"),
    }
