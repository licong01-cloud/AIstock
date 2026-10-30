"""Readonly native score and raw-market adapters for the exact LSTM observation."""
from contextlib import contextmanager
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd

from .entry_price_daily_service import BoundedEntryReadSession, EntryWorkBudget
from .generic_daily_db_input_v1 import GenericDailyReadonlyDBInputV1
from .generic_daily_price_input_v1 import ROSTER, _records
from .lstm_spo_shadow_v1 import BINDING, PROGRAM, RELEASE, TARGET, TZ, clock, frame
from .multiperiod_top10_5td_inputs_v1 import IDENTITY, INPUT_FEATURES, STOCK_FEATURES, make_finance
from .spo_top10_5td_v1 import sha
from backend.services.data_integrity.inputs import (
    InputReadinessError, check_binding, partition_pins, verify_capture, verify_current, verify_partition_refs,
)


def top50(run, artifact, publication):
    """V1 has no direct asset id: require unique config match and exact consumed score parity."""
    from backend.services.strategy_package.selection_artifact import _canonical_json_sha256
    d, t = publication["decision_date"], publication["target_date"]
    source = (run.runtime_config.get("package_runtime_configs", {}).get(TARGET["package_id"], {})
              .get("qe_backtest_runtime_contract", {}).get("source", {}))
    metadata, scores = artifact.metadata, artifact.scores_json
    if (run.package_ids != [TARGET["package_id"]] or run.manifest_sha256_by_package != {TARGET["package_id"]: TARGET["manifest_sha256"]}
            or run.status != "SUCCEEDED" or run.trade_date.isoformat() != t
            or source.get("run_id") != TARGET["run_id"] or source.get("source_id") != TARGET["source_id"]
            or artifact.package_id != TARGET["package_id"] or artifact.manifest_sha256 != TARGET["manifest_sha256"]
            or artifact.trade_date.isoformat() != t or artifact.data_source != run.data_source
            or artifact.status != "SUCCEEDED" or metadata.get("score_trade_date") != d
            or metadata.get("cutoff_date") != d or metadata.get("source_id") != TARGET["source_id"]
            or _canonical_json_sha256(scores) != artifact.artifact_sha256 or artifact.score_count != len(scores)
            or not 50 <= len(scores) <= 10000):
        raise ValueError("native run, score artifact, package or D/T identity differs")
    table = pd.DataFrame(scores)
    if (table['rank'].tolist() != list(range(1, len(table)+1)) or table.symbol.duplicated().any()
            or not table.symbol.str.fullmatch(r"\d{6}\.(SH|SZ|BJ)").all()
            or not np.isfinite(table.score.to_numpy(float)).all()
            or not table.score.is_monotonic_decreasing):
        raise ValueError("original score order/keys are invalid; no re-selection")
    by_symbol = table.set_index("symbol")
    originals = run.package_results.get(TARGET["package_id"], [])
    if not originals:
        raise ValueError("native run lacks consumed candidate scores")
    for row in originals:
        if (row.symbol not in by_symbol.index or row.score != by_symbol.loc[row.symbol, "score"]
                or row.component_scores.get("raw_rank") != by_symbol.loc[row.symbol, "rank"]):
            raise ValueError("native consumed scores disagree with uniquely bound artifact")
    first = table.iloc[:50]
    roster = pd.DataFrame({**{k: [v]*50 for k, v in TARGET.items()},
        ROSTER[0]: pd.Timestamp(d), ROSTER[1]: pd.Timestamp(t), "instrument": first.symbol.to_list(),
        "selection_effective_rank": first['rank'].to_list(), "candidate_group_size": 50,
        "parent_score": first.score.to_list()})
    return roster


def attach_features(roster, features):
    from .generic_daily_price_input_v1 import KEY
    if len(features) != 50 or not features.loc[:, ROSTER].equals(roster.loc[:, ROSTER]):
        raise ValueError("daily features changed original Top50 keys/order")
    output = roster.merge(features.loc[:, [*KEY, *STOCK_FEATURES]], on=list(KEY), validate="one_to_one", sort=False)
    values = output.parent_score
    spread = values.std(ddof=0)
    output["parent_score_z"] = (values-values.mean()) / spread if spread > 0 else np.nan
    output["parent_rank_fraction"] = output.selection_effective_rank / 50.
    return output.loc[:, [*IDENTITY, "source_id", "parent_score", *INPUT_FEATURES]]


class LstmSpoShadowSource:
    def __init__(self, *, session_factory=None, now=None):
        self.session_factory = session_factory or (lambda: BoundedEntryReadSession(EntryWorkBudget()))
        self.now = now or (lambda: datetime.now(timezone.utc))

    @contextmanager
    def connection(self):
        session = self.session_factory()
        try:
            with session.connection() as connection:
                yield connection
        finally:
            session.close()

    def publication(self):
        from backend.services.advisory_program import AdvisoryProgramPGRepository
        with self.connection() as connection:
            @contextmanager
            def pinned():
                yield connection
            repo = AdvisoryProgramPGRepository(conn_factory=pinned)
            program = repo.get_program(PROGRAM)
            if program.status != "ENABLED":
                return None
            binding = repo.get_active_binding_version(PROGRAM)
            if (program.package_ids != [TARGET["package_id"]] or binding is None
                    or binding.binding_version_id != BINDING):
                raise ValueError("target program/package binding changed")
            version = repo.latest_list_version(PROGRAM, status="PUBLISHED")
            if version is None:
                return None
            context = (version.summary_json or {}).get("advisory_date_context") or {}
            if version.binding_version_id != BINDING or not context.get("decision_as_of_trade_date"):
                raise ValueError("original publication lost binding or explicit D")
            return dict(program_id=PROGRAM, binding_version_id=BINDING, list_version_id=version.list_version_id,
                        selection_run_id=version.summary_json["selection_run_id"],
                        decision_date=context["decision_as_of_trade_date"], target_date=context["target_trade_date"],
                        created_at=version.created_at.isoformat())

    def publications(self):
        # A restart must account for all missed published days, never just the latest date.
        latest = self.publication()
        if latest is None:
            return []
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""SELECT list_version_id,binding_version_id,summary_json,created_at
                    FROM app.advisory_recommendation_list_version
                    WHERE program_id=%s AND version_status='PUBLISHED' AND trade_date > %s
                    ORDER BY trade_date,created_at LIMIT 1001""", (PROGRAM, RELEASE["first_decision_date"]))
                rows = cursor.fetchall()
        if len(rows) > 1000:
            raise ValueError("shadow publication catalog exceeds bounded read")
        result = []
        for identifier, binding, summary, created in rows:
            context = summary.get("advisory_date_context") or {}
            if binding != BINDING or not context.get("decision_as_of_trade_date"):
                raise ValueError("trial publication binding or clock differs")
            result.append(dict(program_id=PROGRAM, binding_version_id=binding, list_version_id=identifier,
                selection_run_id=summary["selection_run_id"], decision_date=context["decision_as_of_trade_date"],
                target_date=context["target_trade_date"], created_at=created.isoformat()))
        return result

    def inputs(self, publication):
        from backend.services.advisory_program import AdvisoryProgramPGRepository
        from backend.services.selection_center.repository import SelectionCenterRepository
        from backend.services.strategy_package.selection_artifact import (
            StrategyPackageSelectionArtifactRepository, selection_artifact_runtime_hash_v2,
        )
        started = self.now()
        published = datetime.fromisoformat(publication["created_at"])
        with self.connection() as connection:
            @contextmanager
            def pinned():
                yield connection
            repo = AdvisoryProgramPGRepository(conn_factory=pinned)
            version = repo.get_list_version(publication["list_version_id"])
            context = version.summary_json.get("advisory_date_context") or {}
            if (publication["program_id"] != PROGRAM or publication["binding_version_id"] != BINDING
                    or version.program_id != PROGRAM or version.binding_version_id != BINDING
                    or version.version_status != "PUBLISHED"
                    or version.summary_json.get("selection_run_id") != publication["selection_run_id"]
                    or context.get("decision_as_of_trade_date") != publication["decision_date"]
                    or context.get("target_trade_date") != publication["target_date"]
                    or published.utcoffset() is None or version.created_at.utcoffset() is None
                    or version.created_at != published
                    or version.created_at > started):
                raise ValueError("original publication reference differs")
            run = SelectionCenterRepository(conn_factory=pinned).get_run(publication["selection_run_id"])
            config = run.runtime_config.get("package_runtime_configs", {}).get(TARGET["package_id"])
            if not isinstance(config, dict):
                raise ValueError("original per-package artifact configuration missing")
            digest = selection_artifact_runtime_hash_v2(config)
            # No latest/MAX score choice: the persisted original config resolves one asset.
            artifact = StrategyPackageSelectionArtifactRepository(conn_factory=pinned).get(
                package_id=TARGET["package_id"], manifest_sha256=TARGET["manifest_sha256"],
                trade_date=run.trade_date, data_source=run.data_source, runtime_config_hash=digest)
            if artifact is None or artifact.created_at > run.completed_at:
                raise ValueError("original configuration's score asset is absent or newer than the native run")
            bindings = (config.get("selection_artifact_config") or {}).get("input_readiness_by_package") or {}
            baseline_binding = bindings.get(TARGET["package_id"])
            check_binding(baseline_binding)
            published_binding = (version.summary_json.get("input_readiness") or {}).get(TARGET["package_id"], {})
            if published_binding.get("binding_sha256") != baseline_binding["binding_sha256"]:
                raise InputReadinessError("原始推荐名单缺少同版本输入绑定")
            captured = (artifact.metadata.get("artifact_input_context") or {}).get("input_capture")
            verify_capture(baseline_binding, captured)
            if captured.get("readiness_binding_sha256") != baseline_binding["binding_sha256"]:
                raise InputReadinessError("原始分数资产输入绑定不符")
            verify_current(connection, bindings)
            roster = top50(run, artifact, publication)
        def check_extra(connection, dates):
            verify_current(connection, bindings)
            return extra_input_capture(connection, dates)
        features, price_receipt = GenericDailyReadonlyDBInputV1(session_factory=self.session_factory, now=self.now,
            integrity_check=check_extra).load_day(
            decision_date=date.fromisoformat(publication["decision_date"]), target_date=date.fromisoformat(publication["target_date"]),
            candidates=roster.loc[:, ROSTER], metadata=dict(package_id=TARGET["package_id"], run_id=run.run_id,
                list_version_id=version.list_version_id, universe_identity=run.runtime_config.get("universe_selection")))
        inputs = attach_features(roster, features)
        receipt = dict(source="CURRENT_DATABASE_NON_VINTAGE", started_at=started.isoformat(), completed_at=self.now().isoformat(),
            score_artifact_id=artifact.artifact_id, score_artifact_sha256=artifact.artifact_sha256,
            score_artifact_payload_sha256=artifact.artifact_payload_sha256, artifact_runtime_config_sha256=digest,
            original_score_count=artifact.score_count, native_consumed_count=len(run.package_results[TARGET["package_id"]]),
            score_binding="ORIGINAL_CONFIG_UNIQUE_ASSET_PLUS_CONSUMED_SCORE_PARITY", price=price_receipt,
            missing_by_field={name: int(inputs[name].isna().sum()) for name in INPUT_FEATURES},
            original_top50_sha256=sha(_records(roster)), native_list_changed=False, outcomes_read=False,
            historical_vintage_proven=False, eod_ingestion_completeness_attested=True,
            baseline_input_bindings=bindings)
        return inputs, receipt

    def verify_capture(self, receipt):
        with self.connection() as connection:
            verify_current(connection, receipt["baseline_input_bindings"])
            extra = receipt["price"]["input_readiness"]
            verify_partition_refs(extra["partition_refs"], partition_pins(connection,
                date.fromisoformat(extra["start"]), date.fromisoformat(extra["end"]), EXTRA_DATASETS))

    def finance(self, prediction):
        """Calendar-only maturity check precedes all D..H price/limit/suspension reads."""
        roster = frame(prediction["inputs"])
        d, t = prediction["publication"]["decision_date"], prediction["publication"]["target_date"]
        symbols = roster.instrument.to_list()
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""SELECT cal_date FROM market.trading_calendar
                    WHERE is_trading=TRUE AND cal_date >= %s ORDER BY cal_date LIMIT 6""", (d,))
                calendar = [row[0] for row in cursor.fetchall()]
                if len(calendar) < 6:
                    return None
                if str(calendar[0]) != d or str(calendar[1]) != t:
                    raise ValueError("frozen D/T no longer matches authoritative calendar")
                h = calendar[-1]
                if self.now().astimezone(TZ) < clock(h, 15):
                    return None
                cursor.execute("""SELECT p.trade_date,p.ts_code,p.open_li,p.high_li,p.low_li,p.close_li,
                    p.volume_hand,a.adj_factor FROM market.kline_daily_raw p LEFT JOIN market.adj_factor a
                    ON a.ts_code=p.ts_code AND a.trade_date=p.trade_date
                    WHERE p.ts_code=ANY(%s) AND p.trade_date=ANY(%s) ORDER BY p.trade_date,p.ts_code""", (symbols, calendar))
                daily = pd.DataFrame(cursor.fetchall(), columns=["trade_date", "instrument", "open_li", "high_li", "low_li",
                                                               "close_li", "volume_hand", "adj_factor"])
                cursor.execute("""SELECT trade_date,ts_code,up_limit,down_limit FROM market.stk_limit
                    WHERE ts_code=ANY(%s) AND trade_date=ANY(%s) ORDER BY trade_date,ts_code""", (symbols, calendar[1:]))
                limits = pd.DataFrame(cursor.fetchall(), columns=["trade_date", "instrument", "up_limit", "down_limit"])
                cursor.execute("""SELECT ts_code,trade_date,suspend_type,suspend_timing FROM market.suspend_d
                    WHERE ts_code=ANY(%s) AND trade_date=ANY(%s) ORDER BY trade_date,ts_code""", (symbols, calendar))
                suspensions = cursor.fetchall()
        for data in (daily, limits):
            data["trade_date"] = pd.to_datetime(data.trade_date)
            if len(data) > 300 or data.duplicated(["trade_date", "instrument"]).any():
                raise ValueError("mature source rows duplicate or exceed fifty by six sessions")
            for column in data.columns.difference(["trade_date", "instrument"]):
                data[column] = pd.to_numeric(data[column], errors="raise").astype(float)
                values = data[column].dropna()
                if not np.isfinite(values).all() or (values < 0 if column == "volume_hand" else values <= 0).any():
                    raise ValueError("mature source numeric units invalid")
        if (daily.high_li.lt(daily.low_li).any() or daily.open_li.lt(daily.low_li).any()
                or daily.open_li.gt(daily.high_li).any() or daily.close_li.lt(daily.low_li).any()
                or daily.close_li.gt(daily.high_li).any() or limits.up_limit.lt(limits.down_limit).any()):
            raise ValueError("mature source OHLC/limit range contradicts units")
        for field in ("open", "high", "low", "close"):
            daily["raw_"+field+"_cny"] = daily.pop(field+"_li") / 1000.
        finance = make_finance(roster, daily, limits, suspensions, calendar, h)
        receipt = dict(source="CURRENT_DATABASE_NON_VINTAGE", horizon_end=h.isoformat(),
            calendar=[str(v) for v in calendar], daily_sha256=sha(_records(daily)), limits_sha256=sha(_records(limits)),
            suspension_sha256=sha([[s, str(day), kind, timing] for s, day, kind, timing in suspensions]),
            read_after_maturity=True, actual_fill_proven=False, database_write=False)
        return finance, receipt


EXTRA_DATASETS = ["kline_daily_raw", "adj_factor", "index_daily"]


def extra_input_capture(connection, dates):
    """The unchanged nine features need twenty sessions, including the benchmark."""
    from backend.services.data_integrity.catalog import SCOPE_VERSION, RULE_VERSION
    dates = [str(d) for d in dates]
    if len(dates) != 20 or dates != sorted(set(dates)):
        raise InputReadinessError("重排序输入需精确二十个交易日")
    rows = partition_pins(connection, date.fromisoformat(dates[0]), date.fromisoformat(dates[-1]), EXTRA_DATASETS)
    by_key = {(r["dataset"], r["trade_date"]): r for r in (rows or [])}
    refs = [{"dataset": dataset, "fact_date": day,
        "data_revision": by_key.get((dataset, day), {}).get("data_revision"),
        "validation_id": by_key.get((dataset, day), {}).get("validation_id"),
        "scope_version": SCOPE_VERSION, "rule_version": RULE_VERSION}
        for day in dates for dataset in EXTRA_DATASETS]
    verify_partition_refs(refs, rows)
    return dict(start=dates[0], end=dates[-1], partition_refs=refs,
        snapshot_kind="REPEATABLE_READ_READ_ONLY", historical_first_availability_claimed=False)
