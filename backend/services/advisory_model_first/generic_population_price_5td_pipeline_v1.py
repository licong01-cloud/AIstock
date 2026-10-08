"""Advisory-only readonly source and input components; not a model study or deployment."""
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import time

import pandas as pd
import pyarrow.dataset as ds

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, file_sha256, publish_stage, read_stage
from backend.services.advisory_model_first.economic_entry_sources import _month_batches, _suspension_states
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import DAILY_FIELDS, INDEX_FIELDS, PopulationInputPlanV1
from backend.services.advisory_model_first.generic_population_price_5td_population_v1 import (
    _verify, build_population_inputs_v1, prepare_population_metadata_v1,
)
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

SOURCE = "CURRENT_DATABASE_NON_VINTAGE"


def preparation_implementation_sha256():
    directory = Path(__file__).parent
    names = [f"generic_population_price_5td_{n}_v1.py" for n in ("contracts", "population", "pipeline", "cli")]
    names += ["generic_daily_price_input_v1.py", "generic_price_5td_labels_v1.py", "generic_price_5td_models_v1.py", "economic_entry_sources.py"]
    return sha({n: file_sha256(directory/n) for n in names})


def _root(value):
    path = Path(value)
    if not path.is_absolute() or path.resolve() != path or path.drive.upper() == "C:":
        raise ValueError("population components require an explicit non-C real output root")
    return path


def _parquet(frame):
    stream = io.BytesIO()
    frame.to_parquet(stream, index=False)
    return stream.getvalue()


def _read_database(*, symbols, calendar, cutoff, connection_context_factory=None, progress=None):
    from backend.db.pg_pool import get_conn
    factory = connection_context_factory or (lambda: get_conn(autocommit=False, manage_transaction=False))
    days = [d for d in calendar if d <= cutoff]
    maximum = len(symbols)*len(days)
    queries, parts = [], []
    started = datetime.now(timezone.utc)
    with factory() as connection:
        cursor = connection.cursor()
        def read(name, sql, params, columns, budget):
            began = time.monotonic()
            cursor.execute(sql, params)
            values = cursor.fetchall()
            if len(values) > budget:
                raise ValueError(f"population readonly source exceeds exact row budget: {name}")
            queries.append(dict(phase=name, rows=len(values), seconds=round(time.monotonic()-began, 3)))
            if progress is not None:
                progress(queries[-1])
            return pd.DataFrame(values, columns=columns)
        try:
            connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            cursor.execute("SET LOCAL statement_timeout = %s", (30000,))
            original = read("calendar", "SELECT cal_date FROM market.trading_calendar WHERE is_trading=TRUE AND cal_date BETWEEN %s AND %s ORDER BY cal_date",
                (days[0], days[-1]), ("trade_date",), len(days))
            if original.trade_date.map(_day).tolist() != days:
                raise ValueError("population DB calendar contradicts original frozen calendar")
            for start, end in _month_batches(days[0], days[-1]):
                bound = len(symbols)*sum(start <= d <= end for d in days)
                part = read("daily:"+start.isoformat(), """SELECT p.trade_date,p.ts_code,p.open_li,p.high_li,p.low_li,p.close_li,
                    a.adj_factor,p.volume_hand,l.up_limit,l.down_limit
                    FROM market.kline_daily_raw p
                    LEFT JOIN market.adj_factor a ON a.trade_date=p.trade_date AND a.ts_code=p.ts_code AND a.trade_date BETWEEN %s AND %s
                    LEFT JOIN market.stk_limit l ON l.trade_date=p.trade_date AND l.ts_code=p.ts_code AND l.trade_date BETWEEN %s AND %s
                    WHERE p.ts_code=ANY(%s::text[]) AND p.trade_date BETWEEN %s AND %s
                    ORDER BY p.trade_date,p.ts_code LIMIT %s""",
                    (start, end, start, end, symbols, start, end, bound+1),
                    (*DAILY_FIELDS[:2], "open_li", "high_li", "low_li", "close_li", "adj_factor", "volume_hand", "up_limit", "down_limit"), bound)
                parts.append(part)
            suspension = read("suspend", """SELECT trade_date,ts_code,suspend_type,suspend_timing FROM market.suspend_d
                WHERE ts_code=ANY(%s::text[]) AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date,ts_code,suspend_type,suspend_timing NULLS FIRST LIMIT %s""",
                (symbols, days[0], days[-1], 2*maximum+1), ("trade_date", "instrument", "suspend_type", "suspend_timing"), 2*maximum)
            indices = read("index", "SELECT trade_date,ts_code,close FROM market.index_daily WHERE ts_code='000300.SH' AND trade_date BETWEEN %s AND %s ORDER BY trade_date LIMIT %s",
                (days[0], days[-1], len(days)+1), INDEX_FIELDS, len(days))
        finally:
            connection.rollback()
            cursor.close()
    daily = pd.concat(parts, ignore_index=True)
    for frame in (daily, suspension, indices):
        frame["trade_date"] = frame.trade_date.map(_day).map(pd.Timestamp)
        if not frame.trade_date.isin(pd.to_datetime(days)).all():
            raise ValueError("population readonly source returned foreign dates")
    if (daily.duplicated(["trade_date", "instrument"]).any() or indices.duplicated(["trade_date", "instrument"]).any()
            or not set(daily.instrument).issubset(symbols) or not set(suspension.instrument).issubset(symbols)
            or not indices.instrument.eq("000300.SH").all() or not suspension.suspend_type.isin(["S", "R"]).all()):
        raise ValueError("population readonly source contains conflicting keys/states")
    daily["price_placeholder_fields"] = ""
    for name in ("open", "high", "low", "close"):
        original = daily[name+"_li"].map(lambda v: _number(v, nonnegative=True)).astype(float)
        daily.loc[original.eq(0), "price_placeholder_fields"] += name+";"
        daily["raw_"+name+"_cny"] = (original/1000.).mask(original.eq(0))
    states = _suspension_states(suspension)
    daily = daily.merge(states, on=["trade_date", "instrument"], how="outer", validate="one_to_one")
    for name in ("suspended", "tradability_unknown"):
        daily[name] = daily[name].astype("boolean").fillna(False).astype(bool)
    daily["price_placeholder_fields"] = daily.price_placeholder_fields.fillna("NO_RAW_BAR_SUSPENSION_RECORD")
    daily = daily.loc[:, DAILY_FIELDS]
    receipt = dict(source_evidence=SOURCE, readonly=True, isolation="REPEATABLE READ", statement_timeout_ms=30000,
        read_started_at=started.isoformat(), read_completed_at=datetime.now(timezone.utc).isoformat(), queries=queries,
        price_unit_divisor=1000, volume_unit_multiplier=100, zero_placeholder_rows=int(daily.price_placeholder_fields.ne("").sum()),
        market_up_ratio="UNKNOWN_DEFINITION_NOT_REQUESTED", native_capture=False, database_written=False,
        physical_fit_count=0, research_run_created=False, source_financial_cutoff=cutoff.isoformat())
    return daily, indices, receipt


def freeze_population_source_v1(*, metadata_request, output_root, connection_context_factory=None, progress=None):
    identity = preparation_implementation_sha256()
    recipe = dict(metadata_request=metadata_request.model_dump(mode="json"), implementation_sha256=identity,
                  component="POPULATION_SOURCE_NOT_MODEL_RUN", source_evidence=SOURCE)
    key = sha(recipe)
    root = _root(output_root)/("advgp5popsource_"+key[:24])
    initial = publish_stage(study_root=root, stage="preregistered", plan_sha256=key, parent_sha256=None,
        artifacts={"recipe.json": _json_bytes(recipe)})
    parent = read_stage(initial, stage="preregistered", plan_sha256=key, parent_sha256=None)
    if (root/"prepared").exists():
        read_stage(root/"prepared", stage="prepared", plan_sha256=key, parent_sha256=parent["stage_sha256"])
        return root
    metadata = prepare_population_metadata_v1(request=metadata_request)
    calendar = [_day(d) for d in json.loads(_verify(metadata_request.calendar_ref).read_text(encoding="utf-8"))]
    daily, indices, receipt = _read_database(symbols=sorted(set(metadata.rosters.instrument)), calendar=calendar,
        cutoff=metadata_request.evaluation_end, connection_context_factory=connection_context_factory, progress=progress)
    if identity != preparation_implementation_sha256():
        raise ValueError("population source code changed during the readonly snapshot")
    for source in metadata_request.sources:
        for ref in (source.candidate_ref, source.lineage_ref, source.bundle_manifest_ref):
            if ref is not None:
                _verify(ref)
    _verify(metadata_request.calendar_ref)
    receipt.update(metadata_request_sha256=sha(metadata_request.model_dump(mode="json")), source_recipe_sha256=key)
    publish_stage(study_root=root, stage="prepared", plan_sha256=key, parent_sha256=parent["stage_sha256"],
        artifacts={"daily.parquet": _parquet(daily), "index_daily.parquet": _parquet(indices), "receipt.json": _json_bytes(receipt)})
    return root


def prepare_population_inputs_v1(*, metadata_request, source_root, output_root, progress=None):
    source_root = _root(source_root)
    recipe = json.loads((source_root/"preregistered"/"recipe.json").read_text(encoding="utf-8"))
    key = sha(recipe)
    if recipe["metadata_request"] != metadata_request.model_dump(mode="json") or recipe["source_evidence"] != SOURCE:
        raise ValueError("population source recipe contradicts requested original rosters")
    initial = read_stage(source_root/"preregistered", stage="preregistered", plan_sha256=key, parent_sha256=None)
    stage = read_stage(source_root/"prepared", stage="prepared", plan_sha256=key, parent_sha256=initial["stage_sha256"])
    def ref(name):
        descriptor = stage["files"][name]
        return EvidenceReferenceV1(role=name, artifact_uri=str(source_root/"prepared"/name), **descriptor)
    plan = PopulationInputPlanV1(metadata_request=metadata_request, daily_ref=ref("daily.parquet"),
        index_ref=ref("index_daily.parquet"), source_receipt_ref=ref("receipt.json"), preparation_implementation_sha256=preparation_implementation_sha256())
    source_receipt = json.loads(_verify(plan.source_receipt_ref).read_text(encoding="utf-8"))
    if (source_receipt.get("source_recipe_sha256") != key
            or source_receipt.get("metadata_request_sha256") != sha(metadata_request.model_dump(mode="json"))
            or source_receipt.get("source_financial_cutoff") != metadata_request.evaluation_end.isoformat()
            or source_receipt.get("source_evidence") != SOURCE or source_receipt.get("readonly") is not True
            or source_receipt.get("native_capture") is not False or source_receipt.get("database_written") is not False):
        raise ValueError("population source receipt contradicts readonly development scope")
    root = _root(output_root)/plan.component_id
    parent_path = publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None,
        artifacts={"plan.json": _json_bytes(plan.model_dump(mode="json"))})
    parent = read_stage(parent_path, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    if (root/"prepared").exists():
        read_stage(root/"prepared", stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=parent["stage_sha256"])
        return root
    metadata = prepare_population_metadata_v1(request=metadata_request)
    calendar = [_day(d) for d in json.loads(_verify(metadata_request.calendar_ref).read_text(encoding="utf-8"))]
    cutoff = metadata_request.evaluation_end
    lower = calendar[0]
    def financial(ref, columns):
        path = _verify(ref)
        dataset = ds.dataset(path, format="parquet")
        predicate = (ds.field("trade_date") >= pd.Timestamp(lower)) & (ds.field("trade_date") <= pd.Timestamp(cutoff))
        if dataset.count_rows(filter=predicate) > max(1, len(set(metadata.rosters.instrument)))*len(calendar):
            raise ValueError("population source exceeds bounded projected finance")
        return dataset.to_table(columns=list(columns), filter=predicate).to_pandas()
    daily, indices = financial(plan.daily_ref, DAILY_FIELDS), financial(plan.index_ref, INDEX_FIELDS)
    rows, clusters, encoding = build_population_inputs_v1(metadata=metadata, request=metadata_request, calendar=calendar,
        daily=daily, index_daily=indices, source_context=dict(calendar_sha256=metadata_request.calendar_ref.sha256,
            prices_sha256=plan.daily_ref.sha256, references_sha256=plan.daily_ref.sha256,
            label_price_basis="D_REFERENCE_POLICY_RATIO", source_evidence=SOURCE), progress=progress)
    receipt = dict(metadata=metadata.receipt, encoding=encoding, source_evidence=SOURCE,
        original_rows=len(rows), unique_clusters=len(clusters), physical_fit_count=0, research_run_created=False,
        source_root=str(source_root), plan_sha256=plan.plan_sha256, implementation_sha256=plan.preparation_implementation_sha256,
        label_counts={str(k): int(v) for k, v in rows.label_status.value_counts().items()},
        maturity_counts={str(k): int(v) for k, v in rows.maturity_at_cutoff.value_counts().items()},
        feature_known_counts={name: int(rows[name].notna().sum()) for name in encoding["matrix_order"][:9]}, deployable=False)
    for evidence in (plan.daily_ref, plan.index_ref, plan.source_receipt_ref, metadata_request.calendar_ref):
        _verify(evidence)
    for source in metadata_request.sources:
        for evidence in (source.candidate_ref, source.lineage_ref, source.bundle_manifest_ref):
            if evidence is not None:
                _verify(evidence)
    if plan.preparation_implementation_sha256 != preparation_implementation_sha256():
        raise ValueError("population preparation code changed during calculation")
    publish_stage(study_root=root, stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=parent["stage_sha256"],
        artifacts={"rows.parquet": _parquet(rows), "clusters.parquet": _parquet(clusters), "days.parquet": _parquet(metadata.days),
                   "encoding.json": _json_bytes(encoding), "receipt.json": _json_bytes(receipt)})
    return root
