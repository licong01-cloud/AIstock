"""Read and validate database facts for causal HMM daily inference.

Completion does not establish model input validity.  The caller must still
validate the full, causal input prefix before enabling inference.
"""

from __future__ import annotations

from contextlib import nullcontext
from datetime import date
from datetime import timedelta
import json
import math
import re
from typing import Any

from backend.services.data_refresh_audit import DataRefreshAuditRepository


REQUIRED_DAILY_DATASETS = ("sector_data", "sw_daily", "index_daily")
QUOTE_COLUMNS = ("sw2_pct_change", "sw2_vol", "sw2_amount")
MONEYFLOW_COLUMNS = ("sw2_mf_net_amt", "sw2_mf_buy_elg_amt", "sw2_mf_sell_elg_amt")


class DailyInputError(RuntimeError):
    def __init__(self, reason: str, message: str):
        self.reason_code = reason
        super().__init__(f"{reason}: {message}")


def _fail(message: str, reason: str = "hmm_database_daily_input_unavailable") -> None:
    raise DailyInputError(reason, message)


def snapshot_inference_origin(metrics: Any, *, as_of: date) -> dict[str, str]:
    """Recover the original precompute origin from immutable snapshot metrics.

    The legacy worker uses validation_start - (3*365+30) calendar days. Anchor
    that existing rule to the snapshot, NOT to each requested output window.
    Mutable config dates and unrelated historical coefficient assets are not
    a source of model/semantic calibration identity.
    """
    if isinstance(metrics, str):
        metrics = json.loads(metrics)
    if not isinstance(metrics, dict):
        _fail("snapshot has no frozen training/calibration boundaries", "hmm_daily_model_boundary_unavailable")
    periods = {}
    for field in ("train_period", "val_period"):
        match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})\s*~\s*(\d{4}-\d{2}-\d{2})", str(metrics.get(field, "")))
        if match is None:
            _fail(f"missing frozen {field}", "hmm_daily_model_boundary_unavailable")
        start, end = (date.fromisoformat(value) for value in match.groups())
        if start > end:
            _fail(f"invalid frozen {field}", "hmm_daily_model_boundary_unavailable")
        periods[field] = (start, end)
    ts, te = periods["train_period"]
    vs, ve = periods["val_period"]
    if not ts <= te < vs <= ve < as_of:
        _fail("as_of must follow frozen train/semantic calibration", "hmm_daily_model_boundary_unavailable")
    return {
        "history_start": (vs - timedelta(days=int(3.0 * 365 + 30))).isoformat(),
        "train_start": ts.isoformat(),
        "train_end": te.isoformat(),
        "validation_start": vs.isoformat(),
        "validation_end": ve.isoformat(),
        "origin_authority": "snapshot_metrics_original_precompute_origin_v1",
    }


def _number(value: Any, *, key: str, optional: bool = False) -> float | None:
    if optional and value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise DailyInputError("hmm_database_daily_input_unavailable", f"missing numeric input {key}") from exc
    if optional and math.isnan(result):
        return None
    if not math.isfinite(result):
        _fail(f"non-finite numeric input {key}")
    return result


def validate_database_models(models: Any, *, catalog: list[str], preset: dict[str, float]) -> None:
    """Verify the supported frozen schema without fitting or substituting features."""
    import numpy as np
    from scripts.precompute_hmm_coefficients import restore_hmm

    if not isinstance(models, dict) or set(models) != set(catalog):
        _fail("model/official L2 catalog mismatch", "hmm_daily_model_identity_changed")
    schemas = set()
    for code, info in models.items():
        try:
            means = np.asarray(info["means"], dtype=float)
            states = int(info["n_states"])
            if means.ndim != 2 or means.shape[0] != states or states < 1:
                _fail(f"invalid model dimensions {code}", "hmm_daily_model_identity_changed")
            horizon = bool(info.get("feature_names") and info.get("preprocess"))
            dimensions = means.shape[1]
            if (dimensions not in {4, 7} and not horizon) or (horizon and dimensions != 10):
                _fail(
                    f"unsupported feature schema for {code}; no feature substitution",
                    "hmm_daily_model_schema_unsupported",
                )
            schemas.add(
                (dimensions, horizon, bool(info.get("state_validation_stats") and not info.get("state_labels")))
            )
            if horizon:
                from scripts.hmm_horizon_v2_train import FEATURE_NAMES

                if info["feature_names"] != list(FEATURE_NAMES) or not isinstance(info["preprocess"], dict):
                    _fail(f"frozen feature order mismatch {code}", "hmm_daily_model_schema_unsupported")
                preprocessing = {
                    k: np.asarray(info["preprocess"][k], dtype=float)
                    for k in ("winsor_lower", "winsor_upper", "zscore_mean", "zscore_std")
                }
                if (
                    any(v.shape != (dimensions,) or not np.isfinite(v).all() for v in preprocessing.values())
                    or (preprocessing["zscore_std"] <= 0).any()
                    or (preprocessing["winsor_lower"] > preprocessing["winsor_upper"]).any()
                ):
                    _fail(f"invalid frozen preprocessing {code}", "hmm_daily_model_identity_changed")
            for key in ("means", "covars", "transmat"):
                if not np.isfinite(np.asarray(info[key], dtype=float)).all():
                    _fail(f"non-finite model {code}/{key}", "hmm_daily_model_identity_changed")
            covars = np.asarray(info["covars"], dtype=float)
            if info.get("covariance_type", "diag") == "diag":
                if covars.ndim == 3:
                    if covars.shape != (states, dimensions, dimensions):
                        _fail(f"invalid covariance shape {code}", "hmm_daily_model_identity_changed")
                    covars = np.diagonal(covars, axis1=1, axis2=2)
                if covars.shape != (states, dimensions) or (covars <= 0).any():
                    _fail(f"invalid covariance {code}", "hmm_daily_model_identity_changed")
            elif info.get("covariance_type") == "full":
                if covars.shape != (states, dimensions, dimensions):
                    _fail(f"invalid covariance shape {code}", "hmm_daily_model_identity_changed")
                if not np.allclose(covars, covars.transpose(0, 2, 1)) or (np.linalg.eigvalsh(covars) <= 0).any():
                    _fail(f"invalid covariance {code}", "hmm_daily_model_identity_changed")
            else:
                _fail(f"unsupported covariance schema {code}", "hmm_daily_model_schema_unsupported")
            if ("zscore_mean" in info) != ("zscore_std" in info):
                _fail(f"incomplete frozen preprocessing {code}", "hmm_daily_model_identity_changed")
            if "zscore_mean" in info:
                mean, std = np.asarray(info["zscore_mean"]), np.asarray(info["zscore_std"])
                if (
                    mean.shape != (dimensions,)
                    or std.shape != mean.shape
                    or not np.isfinite(mean).all()
                    or not np.isfinite(std).all()
                    or (std <= 0).any()
                ):
                    _fail(f"invalid frozen preprocessing {code}", "hmm_daily_model_identity_changed")
            labels = info.get("state_labels")
            if labels is not None:
                if set(labels) != {str(i) for i in range(int(info["n_states"]))} or not set(labels.values()) <= set(
                    preset
                ):
                    _fail(f"incomplete semantic/preset binding {code}", "hmm_daily_model_identity_changed")
            elif not isinstance(info.get("state_validation_stats"), dict):
                _fail(f"missing frozen semantics {code}", "hmm_daily_model_identity_changed")
            restored = restore_hmm(info)
            restored._check()
        except DailyInputError:
            raise
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise DailyInputError(
                "hmm_daily_model_identity_changed", f"invalid frozen model/preprocessing {code}"
            ) from exc
    if len(schemas) != 1:
        _fail("mixed model schemas", "hmm_daily_model_schema_unsupported")


def prepare_database_daily_inputs(
    *,
    completion: dict[str, Any],
    origin: dict[str, str],
    as_of: date,
    effective: date,
    calendar: list[date],
    shared_code_to_id: dict[str, int],
    membership_rows: list[dict[str, Any]],
    sector_rows: list[dict[str, Any]],
    index_rows: list[tuple[date, Any]],
    volume_rows: list[tuple[date, Any]],
) -> dict[str, Any]:
    """Validate one current-forward DB snapshot; no IO, fill, or model fitting."""
    from .coefficient_extension import fingerprint
    from backend.services.dataset_release.shared_sector_context import (
        build_release_sw_l2_code_map_payload,
        validate_release_sw_l2_code_map,
    )
    from backend.services.dataset_release.sw_l2_quote_policy import (
        build_quote_availability_payload,
        quote_publication_policy_identity,
    )
    from scripts.precompute_hmm_coefficients import (
        _quote_is_available,
        build_stock_sector_maps_by_date,
        build_stock_sector_membership_spans,
    )

    history_start = date.fromisoformat(origin["history_start"])
    if not calendar or calendar != sorted(set(calendar)) or calendar[-1] != as_of:
        _fail("incomplete or duplicate prefix calendar")
    if calendar[0] < history_start or effective <= as_of:
        _fail("invalid input/output date boundary")
    # Current membership observations can support current/future decisions,
    # but cannot be rebranded as historically available-at facts.
    from datetime import datetime, timezone

    read_at = datetime.fromisoformat(completion["read_cutoff_at"])
    if read_at.tzinfo is None or as_of > read_at.astimezone(timezone(timedelta(hours=8))).date():
        _fail("invalid read cutoff or future numeric observations")
    if effective < read_at.astimezone(timezone(timedelta(hours=8))).date():
        _fail("current membership is not historical as-known evidence", "hmm_daily_historical_as_known_unavailable")
    normalized_members = []
    intervals: dict[str, list[tuple[date, date | None, str]]] = {}
    for raw in membership_rows:
        symbol, code = str(raw["ts_code"]).strip().upper(), str(raw["l2_code"]).strip().upper()
        start, end = raw["in_date"], raw["out_date"]
        if not symbol or code not in shared_code_to_id or not isinstance(start, date) or (end and end < start):
            _fail("invalid PIT membership identity or interval")
        normalized_members.append({"ts_code": symbol, "l2_code": code, "in_date": start, "out_date": end})
        intervals.setdefault(symbol, []).append((start, end, code))
    for symbol, spans in intervals.items():
        ordered = sorted(spans, key=lambda value: value[0])
        for previous, current in zip(ordered, ordered[1:]):
            if previous[1] is None or previous[1] >= current[0]:
                _fail(f"overlapping/duplicate PIT intervals: {symbol}")
    normalized_members.sort(key=lambda r: (r["ts_code"], r["in_date"], r["l2_code"]))
    maps = build_stock_sector_maps_by_date(normalized_members, sorted({as_of, effective}))
    catalog = sorted({row["l2_code"] for row in normalized_members})
    mapping_facts = [
        {**r, "in_date": r["in_date"].isoformat(), "out_date": r["out_date"].isoformat() if r["out_date"] else None}
        for r in normalized_members
    ]
    authority_sha = fingerprint({"code_to_id": shared_code_to_id, "membership": mapping_facts})
    payload = build_release_sw_l2_code_map_payload(
        code_to_id={code: shared_code_to_id[code] for code in catalog},
        member_backed_codes=catalog,
        authority_id="database_current_forward_read_snapshot_v1",
        authority_sha256=authority_sha,
    )
    code_map = validate_release_sw_l2_code_map(payload)
    quote_payload = build_quote_availability_payload(code_map=code_map, required_start=history_start, cutoff=effective)
    availability = {
        row["canonical_l2_code"]: tuple(
            (date.fromisoformat(s["start_date"]), date.fromisoformat(s["end_date"])) for s in row["availability_spans"]
        )
        for row in quote_payload["entries"]
    }
    output_codes = set(maps[effective.isoformat()].values())
    quoted = sorted(code for code in output_codes if _quote_is_available(availability, code, effective))
    unavailable = sorted(output_codes - set(quoted))
    if not quoted:
        _fail("no quote-available output industries")
    sectors: dict[str, dict[date, dict[str, Any]]] = {}
    canonical_sectors = []
    for raw in sector_rows:
        code, day = raw["sector_code"], raw["trade_date"]
        if code not in catalog or day not in calendar or day > as_of:
            _fail("sector input outside the canonical prefix/taxonomy")
        if day in sectors.setdefault(code, {}):
            _fail(f"duplicate sector/date input {code}/{day}")
        available = _quote_is_available(availability, code, day)
        record: dict[str, Any] = {"trade_date": day}
        for field in (*QUOTE_COLUMNS, *MONEYFLOW_COLUMNS):
            value = _number(raw[field], key=f"{code}/{day}/{field}", optional=not available)
            if not available and field in QUOTE_COLUMNS and value is not None:
                _fail(f"quote value outside publication authority {code}/{day}/{field}")
            if field in {"sw2_vol", "sw2_amount"} and value is not None and value < 0:
                _fail(f"negative quote quantity {code}/{day}/{field}")
            record[field] = value
        sectors[code][day] = record
        canonical_sectors.append({"sector_code": code, **record, "trade_date": day.isoformat()})
    for code in quoted:
        if set(sectors.get(code, {})) != set(calendar):
            missing = sorted(set(calendar) - set(sectors.get(code, {})))
            _fail(f"continuous inference prefix missing {code}: {missing[:3]}")
    closes = {}
    for day, value in index_rows:
        if day in closes or day not in calendar:
            _fail("duplicate/out-of-prefix CSI300 date")
        close = _number(value, key=f"CSI300/{day}")
        if close <= 0:
            _fail("non-positive CSI300 close")
        closes[day] = close
    volumes = {}
    for day, value in volume_rows:
        if day in volumes or day not in calendar:
            _fail("duplicate/out-of-prefix market volume date")
        volume = _number(value, key=f"market_volume/{day}")
        if volume <= 0:
            _fail("non-positive market total volume")
        volumes[day] = volume
    if set(closes) != set(calendar) or set(volumes) != set(calendar):
        _fail("CSI300/market-volume prefix calendar incomplete")
    csi = {day: (closes[day] / closes[calendar[i - 1]] - 1.0) * 100.0 for i, day in enumerate(calendar) if i}
    if len(csi) < 20:
        _fail("insufficient continuous observations")
    facts_sha = fingerprint(
        {
            "origin": origin,
            "calendar": [d.isoformat() for d in calendar],
            "sector_data": sorted(canonical_sectors, key=lambda r: (r["sector_code"], r["trade_date"])),
            "index_close": [[d.isoformat(), closes[d]] for d in calendar],
            "market_volume": [[d.isoformat(), volumes[d]] for d in calendar],
            "mapping_sha256": authority_sha,
            "quote_availability": quote_payload,
        }
    )
    identity = {
        "schema_version": "hmm_database_daily_input_v1",
        "source_mode": "database_daily",
        "as_of_trade_date": as_of.isoformat(),
        "effective_trade_date": effective.isoformat(),
        "read_cutoff_at": completion["read_cutoff_at"],
        "input_sha256": facts_sha,
        "inference_origin": origin,
        "completion_authority": completion["completion_authority"],
        "completion_statuses": completion["completion_statuses"],
        "mapping_authority": dict(code_map.mapping_authority),
        "quote_policy_sha256": quote_publication_policy_identity(),
        "knowledge_mode": "forward_current_read_snapshot",
        "historical_as_known_verified": False,
        "market_volume_definition": "sum_market_sw_daily_vol_all_rows_v1",
    }
    return {
        "sector_data": sectors,
        "sector_rows": {
            c: [
                {
                    "trade_date": d,
                    "l2_name": c,
                    **{field.removeprefix("sw2_"): r[d][field] for field in (*QUOTE_COLUMNS, *MONEYFLOW_COLUMNS)},
                }
                for d in sorted(r)
            ]
            for c, r in sectors.items()
        },
        "sector_dates": calendar,
        "csi300": csi,
        "index_dates": sorted(csi),
        "market_vol": volumes,
        "market_volume_dates": calendar,
        "stock_sector_maps_by_date": maps,
        "stock_sector_membership_spans": build_stock_sector_membership_spans(maps),
        "ordered_sector_codes": catalog,
        "active_sector_codes": sorted(output_codes),
        "coefficient_sector_codes": quoted,
        "quote_available_sector_codes_by_date": {as_of.isoformat(): quoted},
        "quote_unavailable_sector_codes_by_date": {as_of.isoformat(): unavailable},
        "quote_availability_authority": dict(code_map.mapping_authority),
        "quote_availability_digest": quote_payload["quote_availability_digest"],
        "sector_code_map_authority": dict(code_map.mapping_authority),
        "sector_code_map_digest": code_map.code_map_digest,
        "input_identity": identity,
    }


def read_database_daily_inputs(
    connection: Any,
    *,
    as_of: date,
    effective: date,
    origin: dict[str, str],
    cutoff: date,
    known_read_cutoff: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read only the exact feature prefix, using a single repeatable-read snapshot."""
    from backend.services.industry_code_map import load_sw_l2_code_map

    day, completion = read_database_daily_completion(connection, requested_as_of=as_of, cutoff=cutoff)
    if day != as_of:
        _fail("completion changed")
    start = date.fromisoformat(origin["history_start"])
    with connection.cursor() as cur:
        cur.execute(
            "SELECT cal_date FROM market.trading_calendar WHERE cal_date BETWEEN %s AND %s AND is_trading=TRUE ORDER BY cal_date",
            (start, as_of),
        )
        calendar = [row[0] for row in cur.fetchall()]
        cur.execute(
            "SELECT ts_code,l2_code,in_date,out_date FROM market.sw_index_member WHERE in_date<=%s AND (out_date IS NULL OR out_date>=%s) ORDER BY ts_code,in_date,l2_code",
            (effective, start),
        )
        members = [dict(zip(("ts_code", "l2_code", "in_date", "out_date"), row)) for row in cur.fetchall()]
        cur.execute(
            "SELECT trade_date,close FROM market.index_daily WHERE ts_code='000300.SH' AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
            (start, as_of),
        )
        index_rows = cur.fetchall()
        cur.execute(
            "SELECT trade_date,SUM(vol) FROM market.sw_daily WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date ORDER BY trade_date",
            (start, as_of),
        )
        volume_rows = cur.fetchall()
        columns = (*QUOTE_COLUMNS, *MONEYFLOW_COLUMNS)
        projections = ",".join(f"MIN(sd.{c}),MAX(sd.{c}),COUNT(sd.{c})" for c in columns)
        cur.execute(
            "SELECT m.l2_code,sd.trade_date,COUNT(*),COUNT(DISTINCT sd.ts_code),"
            + projections
            + " FROM market.sector_data sd JOIN market.sw_index_member m ON m.ts_code=sd.ts_code"
            " AND m.in_date<=sd.trade_date AND (m.out_date IS NULL OR m.out_date>=sd.trade_date)"
            " WHERE sd.trade_date BETWEEN %s AND %s GROUP BY m.l2_code,sd.trade_date ORDER BY m.l2_code,sd.trade_date",
            (start, as_of),
        )
        rows = []
        for code, trade_date, count, distinct_count, *values in cur.fetchall():
            if count != distinct_count:
                _fail(f"ambiguous stock/sector join {code}/{trade_date}")
            record = {"sector_code": code, "trade_date": trade_date}
            for i, field in enumerate(columns):
                low, high, finite_count = values[3 * i : 3 * i + 3]
                # All repeated sector facts must agree. All-null quotes for an
                # unpublished industry remain legal; missing values within an
                # otherwise finite repeated fact are a source integrity error.
                both_nan = low is not None and high is not None and math.isnan(float(low)) and math.isnan(float(high))
                if not both_nan and (low != high or finite_count not in {0, count}):
                    _fail(f"conflicting repeated sector fact {code}/{trade_date}/{field}")
                record[field] = low
            rows.append(record)
    bound_completion = dict(completion)
    if known_read_cutoff is not None:
        from datetime import datetime

        original = datetime.fromisoformat(known_read_cutoff)
        actual = datetime.fromisoformat(completion["read_cutoff_at"])
        if (
            original.tzinfo is None
            or original > actual
            or any(
                datetime.fromisoformat(status["refreshed_at"]) > original
                for status in completion["completion_statuses"].values()
            )
        ):
            _fail("completion is newer than the frozen durable plan", "hmm_daily_input_identity_changed")
        bound_completion["read_cutoff_at"] = known_read_cutoff
    loaded = prepare_database_daily_inputs(
        completion=bound_completion,
        origin=origin,
        as_of=as_of,
        effective=effective,
        calendar=calendar,
        shared_code_to_id=load_sw_l2_code_map(connection),
        membership_rows=members,
        sector_rows=rows,
        index_rows=index_rows,
        volume_rows=volume_rows,
    )
    return loaded, completion


def read_database_daily_completion(
    connection: Any,
    *,
    requested_as_of: date | None,
    cutoff: date,
) -> tuple[date, dict[str, Any]]:
    """Use the existing refresh authority, never MAX(market.trade_date).

    The caller supplies one read-only, repeatable-read transaction.  Re-reading
    the most recent attempt through the public repository also rejects a newer
    failure which would otherwise be hidden by a previous success row.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT current_setting('transaction_read_only'), transaction_timestamp(), current_setting('transaction_isolation')"
        )
        read_only, read_cutoff_at, isolation = cursor.fetchone()
        if read_only != "on" or isolation != "repeatable read":
            raise RuntimeError("hmm_database_daily_input_unavailable: read-only repeatable-read transaction required")
        if requested_as_of is not None and requested_as_of > cutoff:
            raise RuntimeError("hmm_database_daily_input_unavailable: future as_of date")
        cursor.execute(
            "SELECT dataset, MAX(trade_date) FROM market.dataset_date_refresh_audit "
            "WHERE dataset=ANY(%s) AND status='success' AND row_count>0 "
            "AND trade_date<=%s AND refreshed_at<=%s GROUP BY dataset",
            (list(REQUIRED_DAILY_DATASETS), cutoff, read_cutoff_at),
        )
        completed = dict(cursor.fetchall())
        if set(completed) != set(REQUIRED_DAILY_DATASETS):
            raise RuntimeError("hmm_database_daily_input_unavailable: required completion audit missing")
        candidate = requested_as_of or min(completed.values())
        cursor.execute(
            "SELECT is_trading FROM market.trading_calendar WHERE cal_date=%s",
            (candidate,),
        )
        rows = cursor.fetchall()
        if len(rows) != 1 or rows[0][0] is not True:
            raise RuntimeError("hmm_database_daily_input_unavailable: canonical trading date missing")

    repository = DataRefreshAuditRepository(conn_factory=lambda: nullcontext(connection))
    statuses: dict[str, Any] = {}
    for dataset in REQUIRED_DAILY_DATASETS:
        try:
            status = repository.require_success(dataset=dataset, trade_date=candidate)
        except RuntimeError as exc:
            raise DailyInputError(
                "hmm_database_daily_input_unavailable", f"required completion not valid: {dataset}; {exc}"
            ) from exc
        if status.row_count <= 0 or status.refreshed_at > read_cutoff_at:
            raise RuntimeError("hmm_database_daily_input_unavailable: invalid completion audit")
        statuses[dataset] = {
            "dataset": status.dataset,
            "trade_date": status.trade_date.isoformat(),
            "data_source": status.data_source,
            "status": status.status,
            "quality_status": status.quality_status,
            "row_count": status.row_count,
            "refreshed_at": status.refreshed_at.isoformat(),
            "job_id": status.job_id,
        }
    diagnostic_queries = {
        "sector_data": "SELECT MAX(trade_date) FROM market.sector_data WHERE trade_date<=%s",
        "sw_daily": "SELECT MAX(trade_date) FROM market.sw_daily WHERE trade_date<=%s",
        "index_daily_000300": (
            "SELECT MAX(trade_date) FROM market.index_daily WHERE ts_code='000300.SH' AND trade_date<=%s"
        ),
    }
    max_dates: dict[str, str | None] = {}
    with connection.cursor() as cursor:
        for key, query in diagnostic_queries.items():
            cursor.execute(query, (cutoff,))
            maximum = cursor.fetchone()[0]
            max_dates[key] = maximum.isoformat() if maximum else None
    return candidate, {
        "read_cutoff_at": read_cutoff_at.isoformat(),
        "completion_authority": "market.dataset_date_refresh_audit",
        "completion_statuses": statuses,
        "max_dates": max_dates,
        "max_dates_diagnostic_only": True,
    }
