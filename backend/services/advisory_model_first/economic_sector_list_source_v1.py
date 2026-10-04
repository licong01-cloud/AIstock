"""Read the original published Top20, never reselect or qualify a QE package."""

from contextlib import contextmanager
from datetime import date, datetime, timezone
import re

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import _records
from backend.services.advisory_model_first.economic_entry_daily_source import (
    project_economic_frozen_candidate_roster_v1,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.model_inference import _candidate_rows_for_recommendation_list
from backend.services.advisory_model_first.realtime_feature_source import PostgresRealtimeFeatureSource
from backend.services.advisory_program import AdvisoryProgramPGRepository, list_item_to_dict, list_version_to_dict
from backend.services.advisory_universe import normalize_advisory_universe_selection
from backend.services.selection_center.repository import SelectionCenterRepository
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def _invalid(message, reason="ADVISORY_SECTOR_FROZEN_LIST_INVALID"):
    raise AdvisoryModelFirstError(message, reason_code=reason)


def _day(value):
    if isinstance(value, datetime):
        _invalid("daily list clock must be a date, not an intraday timestamp")
    if type(value) is date:
        return value
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        _invalid("daily list date is malformed")
    try:
        return date.fromisoformat(value)
    except ValueError:
        _invalid("daily list date is invalid")


def _decision_date(version, selection, review_config):
    declarations = [version.get("selection_as_of_trade_date")]
    for config in (selection.runtime_config, review_config):
        context = (config or {}).get("advisory_date_context") or {}
        declarations.extend(context.get(key) for key in ("decision_as_of_trade_date", "selection_as_of_trade_date"))
        point = (config or {}).get("point_in_time_context") or {}
        declarations.append(point.get("as_of_trade_date"))
    days = {_day(value) for value in declarations if value is not None}
    if len(days) != 1:
        _invalid("original list/run do not declare one decision cutoff")
    decision = next(iter(days))
    # An older quote for a suspended stock is NOT a second decision cutoff.
    for row in selection.aggregate_results:
        if row.selection_entry_price_time:
            try:
                observed = date.fromisoformat(str(row.selection_entry_price_time)[:10])
            except ValueError:
                _invalid("original candidate quote clock is malformed")
            if observed > decision:
                _invalid("original candidate quote is later than the declared D")
    return decision


def _published_top20(version, items, selection):
    for item in items:
        if any(item.get(key) != version[key] for key in ("list_version_id", "program_id", "binding_version_id")):
            _invalid("list item is foreign to its original published list")
        if item.get("action") not in {"ENTER", "HOLD", "WAITING", "WATCH", "EXIT"}:
            _invalid("published list action is invalid")
        rank = item.get("rank")
        if rank is not None and (type(rank) is not int or rank < 1):
            _invalid("published list rank is malformed")
    selected = [
        item for item in items if item["action"] != "EXIT" and item.get("rank") is not None and item["rank"] <= 20
    ]
    selected = sorted(selected, key=lambda item: item["rank"])
    symbols = [item["symbol"] for item in selected]
    expected = min(
        20,
        max((item["rank"] for item in items if item["action"] != "EXIT" and item.get("rank") is not None), default=0),
    )
    admission = (version.get("summary_json") or {}).get("advisory_universe_receipt") or {}
    if "output_candidate_count" in admission:
        count = admission["output_candidate_count"]
        if type(count) is not int or not 0 <= count <= len(selection.aggregate_results):
            _invalid("original candidate-count declaration is invalid")
        expected = min(20, count)
    if (
        len(selected) > 20
        or len(set(symbols)) != len(selected)
        or [item["rank"] for item in selected] != list(range(1, expected + 1))
    ):
        _invalid("published Top20 has duplicate or incomplete original ranks")
    raw = selection.aggregate_results
    if len(raw) > 1000 or len({row.symbol for row in raw}) != len(raw):
        _invalid("persisted Selection roster is duplicate or exceeds its bounded read domain")
    by_symbol = {row.symbol: row for row in raw}
    for item in selected:
        row = by_symbol.get(item["symbol"])
        if row is None or item.get("score") != row.score:
            _invalid("published candidate score/legs differ from the original Selection run")
        # Compare consumed score legs, not optional display/PriceGuard enrichment.
        scores = item.get("component_scores_json")
        if not isinstance(scores, dict) or any(
            scores.get(key) != value
            for key, value in row.component_scores.items()
            if isinstance(value, dict) and "normalized_score" in value
        ):
            _invalid("published candidate legs differ from the original Selection run")
        source_run = (item.get("evidence_json") or {}).get("source_run_id")
        if source_run is not None and source_run != selection.run_id:
            _invalid("published candidate references another Selection run")
    # A published index admission may legitimately accept zero of the raw run.
    # Only its original explicit declaration can explain that empty list.
    if raw and not selected and admission.get("output_candidate_count") != 0:
        _invalid("nonempty original Selection cannot be presented as an empty model roster")
    unmodeled = [
        dict(
            instrument=item["symbol"],
            rank=item.get("rank"),
            action=item["action"],
            reason_code="NOT_ENTRY_CANDIDATE" if item["action"] == "EXIT" else "OUTSIDE_MODEL_TOP20_SCOPE",
        )
        for item in items
        if item not in selected
    ]
    return _candidate_rows_for_recommendation_list(raw, selected), selected, unmodeled


def _review_policy(version, items, config):
    declarations = [
        (version.get("summary_json") or {}).get("review_policy_sha256"),
        (config or {}).get("review_policy_sha256"),
    ]
    declarations.extend((item.get("evidence_json") or {}).get("review_policy_sha256") for item in items)
    values = [value for value in declarations if value is not None]
    if any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value) for value in values):
        _invalid("original review policy declarations conflict or are malformed")
    hashes = set(values)
    if len(hashes) > 1:
        _invalid("original review policy declarations conflict or are malformed")
    return next(iter(hashes)) if hashes else None


class EconomicSectorPublishedListSourceV1:
    """Advisory-owned adapter; one read snapshot, no native/archive/current-pool gate."""

    def __init__(self, *, read_session, price_context_mode="LIVE_DB", pit_universe_key=None):
        if (
            price_context_mode not in {"LIVE_DB", "CANONICAL_HISTORICAL"}
            or price_context_mode == "LIVE_DB"
            and pit_universe_key is not None
            or price_context_mode == "CANONICAL_HISTORICAL"
            and not pit_universe_key
        ):
            _invalid("price reader needs its explicit live or historical source mode")
        if price_context_mode == "CANONICAL_HISTORICAL":
            from backend.services.canonical_equity_pit import require_canonical_rolling_universe_key

            require_canonical_rolling_universe_key(pit_universe_key)
        self._session, self._mode, self._key = read_session, price_context_mode, pit_universe_key

    def load_day(
        self, *, program_id, component_roles, terminal_weights, model_scope, target_date=None, list_version_id=None
    ):
        if not isinstance(program_id, str) or not program_id.strip():
            _invalid("original daily program id is missing")
        target = _day(target_date) if target_date is not None else None
        if program_id != model_scope["program_id"]:
            _invalid("M1 model inputs belong to another program", "ADVISORY_SECTOR_MODEL_INPUT_INCOMPATIBLE")
        started = datetime.now(timezone.utc)
        with self._session.connection() as connection:

            @contextmanager
            def pinned():
                yield connection

            repo = AdvisoryProgramPGRepository(conn_factory=pinned)
            original = (
                repo.get_list_version(list_version_id)
                if list_version_id
                else (
                    repo.list_version_for_date(program_id, target, status="PUBLISHED")
                    if target
                    else repo.latest_list_version(program_id, status="PUBLISHED")
                )
            )
            if original is None:
                _invalid("original published daily list is not available", "ADVISORY_SECTOR_FROZEN_LIST_NOT_READY")
            version = list_version_to_dict(original)
            actual_target = _day(version["target_trade_date"])
            if (
                version["program_id"] != program_id
                or version["version_status"] != "PUBLISHED"
                or target is not None
                and target != actual_target
            ):
                _invalid("original list program/target/publication differs")
            items = [list_item_to_dict(item) for item in repo.list_version_items(version["list_version_id"])]
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT review_run_id, program_id, binding_version_id, trade_date,
                    selection_run_id, selection_run_ids, runtime_config_json
                    FROM app.advisory_review_run WHERE review_run_id = %s""",
                    (version["review_run_id"],),
                )
                review = cursor.fetchone()
                cursor.execute(
                    """SELECT program_id, binding_version_id, package_ids, runtime_config_json
                    FROM app.advisory_strategy_binding_version WHERE binding_version_id = %s""",
                    (version["binding_version_id"],),
                )
                binding = cursor.fetchone()
            if (
                not review
                or tuple(review[:4])
                != (version["review_run_id"], program_id, version["binding_version_id"], actual_target)
                or not review[4]
                or tuple(review[5] or ()) != (review[4],)
                or not binding
                or tuple(binding[:2]) != (program_id, version["binding_version_id"])
            ):
                _invalid("original list/review/binding identities differ")
            selection = SelectionCenterRepository(conn_factory=pinned).get_run(review[4])
            package = model_scope["package_id"]
            if (
                selection.run_id != review[4]
                or selection.trade_date != actual_target
                or selection.package_ids != [package]
                or list(binding[2]) != [package]
                or selection.manifest_sha256_by_package != {package: model_scope["manifest_sha256"]}
            ):
                _invalid("original package/manifest inputs differ", "ADVISORY_SECTOR_MODEL_INPUT_INCOMPATIBLE")
            if (
                selection.status not in {"SUCCEEDED", "VALID_NO_CANDIDATE"}
                or selection.status == "VALID_NO_CANDIDATE"
                and (
                    not selection.valid_no_candidate or selection.aggregate_results or not selection.no_candidate_reason
                )
                or selection.status == "SUCCEEDED"
                and selection.valid_no_candidate
            ):
                _invalid("original Selection is unfinished or has a contradictory empty-roster state")
            universe = normalize_advisory_universe_selection((binding[3] or {}).get("universe_selection"))
            if universe != normalize_advisory_universe_selection(model_scope["universe_selection"]):
                _invalid("M1 pool inputs differ", "ADVISORY_SECTOR_MODEL_INPUT_INCOMPATIBLE")
            declared = [
                selection.runtime_config.get("universe_selection"),
                (version.get("summary_json", {}).get("advisory_universe_receipt") or {}).get("universe_selection"),
            ]
            if any(
                value is not None and normalize_advisory_universe_selection(value) != universe for value in declared
            ):
                _invalid("original run/list/binding pool declarations disagree")
            decision = _decision_date(version, selection, review[6])
            rows, chosen, unmodeled = _published_top20(version, items, selection)
            policy = _review_policy(version, chosen, review[6])
            candidates = project_economic_frozen_candidate_roster_v1(
                rows=rows,
                decision_date=decision,
                target_date=actual_target,
                component_roles=component_roles,
                terminal_weights=terminal_weights,
                candidate_group_size=len(rows),
            )
            with connection.cursor() as cursor:
                history = PostgresRealtimeFeatureSource._recent_trading_dates(cursor, end_date=decision, limit=21)
                next_days = PostgresRealtimeFeatureSource._trading_calendar(
                    cursor, start_date=decision, end_date=actual_target
                )
                if (
                    len(history) != 21
                    or history[-1].date() != decision
                    or tuple(next_days.date) != (decision, actual_target)
                ):
                    _invalid("M1 needs the authoritative 21D and immediate next T")
                try:
                    if candidates.empty:
                        contexts, unavailable = {}, ()
                    else:
                        contexts, unavailable = PostgresRealtimeFeatureSource._price_range_contexts(
                            cursor,
                            symbols=candidates.instrument.tolist(),
                            decision_as_of_trade_date=decision,
                            target_trade_date=actual_target,
                            pit_universe_key=self._key,
                        )
                except AdvisoryModelFirstError as exc:
                    if exc.reason_code != "ADVISORY_PRICE_RANGE_PIT_ATTRIBUTE_UNAVAILABLE":
                        raise
                    contexts = {}
                    unavailable = tuple(
                        dict(symbol=symbol, reason_code=exc.reason_code) for symbol in candidates.instrument
                    )
            missing = [item["symbol"] for item in unavailable]
            if (
                len(set(missing)) != len(missing)
                or set(contexts) & set(missing)
                or set(contexts) | set(missing) != set(candidates.instrument)
            ):
                _invalid("price source changed the exact original candidate roster")
        receipt = dict(
            list_version_id=version["list_version_id"],
            review_run_id=review[0],
            selection_run_id=selection.run_id,
            program_id=program_id,
            binding_version_id=version["binding_version_id"],
            decision_date=decision.isoformat(),
            target_date=actual_target.isoformat(),
            package_id=package,
            manifest_sha256=model_scope["manifest_sha256"],
            universe_selection=universe,
            source_review_policy_sha256=policy,
            source_policy_state="DECLARED_ORIGINAL" if policy else "UNKNOWN_POLICY",
            candidate_scope="ORIGINAL_PUBLISHED_TOP20",
            original_list_item_count=len(items),
            candidate_count=len(candidates),
            candidate_roster_sha256=sha(_records(candidates)),
            unmodeled_items=unmodeled,
            price_context_mode=self._mode,
            pit_universe_key=self._key,
            price_context_unavailable=list(unavailable),
            source_evidence="CURRENT_DB_ORIGINAL_PUBLISHED_LIST_NOT_ORIGINAL_DATA_CAPTURE",
            native_receipt_created=False,
            package_qualification_rechecked=False,
            new_selection_runs=0,
            database_written=False,
            outcomes_read=False,
        )
        receipt["input_identity_sha256"] = sha(receipt)
        receipt.update(read_started_at=started.isoformat(), read_completed_at=datetime.now(timezone.utc).isoformat())
        return dict(
            candidates=candidates,
            calendar=(*history.date, actual_target),
            price_contexts=contexts,
            scope={**model_scope, "universe_selection": universe},
            candidate_receipt=receipt,
        )
