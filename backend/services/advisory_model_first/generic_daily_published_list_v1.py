"""Read original published entry population without M1 legs or package admission."""
from contextlib import contextmanager
import re

import pandas as pd

from backend.services.advisory_model_first.economic_sector_list_source_v1 import _decision_date, _review_policy
from backend.services.advisory_model_first.generic_daily_price_input_v1 import KEY, ROSTER, _day, _records
from backend.services.advisory_model_first.generic_price_set_consumer_v1 import _fail
from backend.services.advisory_program import AdvisoryProgramPGRepository, list_item_to_dict, list_version_to_dict
from backend.services.advisory_universe import normalize_advisory_universe_selection
from backend.services.selection_center.repository import SelectionCenterRepository
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def _population(version, items, run):
    if len(items) > 1000 or len(run.aggregate_results) > 1000:
        _fail("generic original list exceeds its bounded source domain")
    by_symbol = {row.symbol: row for row in run.aggregate_results}
    if len(by_symbol) != len(run.aggregate_results):
        _fail("generic original Selection contains duplicate symbols")
    chosen, other, symbols = [], [], set()
    for item in items:
        symbol, rank, action = item.get("symbol"), item.get("rank"), item.get("action")
        if (any(item.get(key) != version[key] for key in ("program_id", "binding_version_id", "list_version_id"))
                or not isinstance(symbol, str) or not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", symbol) or symbol in symbols
                or action not in {"ENTER", "HOLD", "WAITING", "WATCH", "EXIT"}
                or rank is not None and (type(rank) is not int or rank < 1)):
            _fail("generic original list has foreign items, duplicate symbols or invalid actions/ranks")
        symbols.add(symbol)
        if rank is None and action == "ENTER":
            _fail("generic published entry item lost its original rank")
        declared_run = (item.get("evidence_json") or {}).get("source_run_id")
        if declared_run is not None and declared_run != run.run_id:
            _fail("generic original item refers to a different source run")
        if action == "EXIT" or rank is None:
            other.append({**item, "unmodeled_reason": "ORIGINAL_EXIT" if action == "EXIT" else "NO_PUBLISHED_ENTRY_RANK"})
            continue
        original = by_symbol.get(symbol)
        if original is None or item.get("score") != original.score:
            _fail("generic published stock/score differs from its original source run")
        chosen.append(dict(item))
    chosen.sort(key=lambda item: item["rank"])
    if len(chosen) > 50 or [item["rank"] for item in chosen] != list(range(1, len(chosen) + 1)):
        _fail("generic original entry population is incomplete or exceeds fifty; no truncation allowed")
    admission = (version.get("summary_json") or {}).get("advisory_universe_receipt") or {}
    count = admission.get("output_candidate_count")
    if count is not None and (type(count) is not int or not 0 <= count <= len(by_symbol) or count == 0 and chosen):
        _fail("generic original candidate-count declaration contradicts its published list")
    return chosen, other


class GenericDailyPublishedListSourceV1:
    def __init__(self, *, read_session):
        self._session = read_session

    def load_day(self, *, program_id, target_date=None, list_version_id=None):
        if (not isinstance(program_id, str) or not program_id.strip()
                or list_version_id is not None and (not isinstance(list_version_id, str) or not list_version_id.strip())):
            _fail("generic original program/list identity is malformed")
        target = _day(target_date) if target_date is not None else None
        with self._session.connection() as connection:
            @contextmanager
            def pinned():
                yield connection
            repo = AdvisoryProgramPGRepository(conn_factory=pinned)
            version_object = (repo.get_list_version(list_version_id) if list_version_id else
                repo.list_version_for_date(program_id, target, status="PUBLISHED") if target else
                repo.latest_list_version(program_id, status="PUBLISHED"))
            if version_object is None:
                _fail("generic original published list is unavailable", "ADVISORY_GENERIC_ORIGINAL_LIST_NOT_READY")
            version = list_version_to_dict(version_object)
            actual = _day(version["target_trade_date"])
            if (version["program_id"] != program_id or version["version_status"] != "PUBLISHED"
                    or target is not None and actual != target
                    or list_version_id is not None and version["list_version_id"] != list_version_id):
                _fail("generic original publication/program/target/list differs")
            items = [list_item_to_dict(item) for item in repo.list_version_items(version["list_version_id"])]
            with connection.cursor() as cursor:
                cursor.execute("""SELECT review_run_id,program_id,binding_version_id,trade_date,
                    selection_run_id,selection_run_ids,runtime_config_json
                    FROM app.advisory_review_run WHERE review_run_id=%s""", (version["review_run_id"],))
                review = cursor.fetchone()
                cursor.execute("""SELECT program_id,binding_version_id,package_ids,runtime_config_json
                    FROM app.advisory_strategy_binding_version WHERE binding_version_id=%s""", (version["binding_version_id"],))
                binding = cursor.fetchone()
            if (not review or tuple(review[:4]) != (version["review_run_id"],program_id,version["binding_version_id"],actual)
                    or not review[4] or tuple(review[5] or ()) != (review[4],)
                    or not binding or tuple(binding[:2]) != (program_id, version["binding_version_id"])):
                _fail("generic original review/binding/run references disagree")
            run = SelectionCenterRepository(conn_factory=pinned).get_run(review[4])
            if (run.run_id != review[4] or run.trade_date != actual or len(run.package_ids) != 1
                    or list(binding[2] or ()) != run.package_ids or run.status not in {"SUCCEEDED", "VALID_NO_CANDIDATE"}
                    or run.status == "SUCCEEDED" and run.valid_no_candidate
                    or run.status == "VALID_NO_CANDIDATE" and (not run.valid_no_candidate or run.aggregate_results or not run.no_candidate_reason)):
                _fail("generic original source package/date/state contradicts its published review")
            package = run.package_ids[0]
            manifests = dict(run.manifest_sha256_by_package or {})
            if (not isinstance(package, str) or not package.strip() or set(manifests) - {package}
                    or any(value is not None and (not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value))
                           for value in manifests.values())):
                _fail("generic original package/declared manifest identity is malformed; unknown is not fabricated")
            decision = _decision_date(version, run, review[6])
            if decision >= actual:
                _fail("generic original D must precede its original target T")
            chosen, unmodeled = _population(version, items, run)
            policy = _review_policy(version, chosen, review[6])
            values = [(binding[3] or {}).get("universe_selection"), (run.runtime_config or {}).get("universe_selection"),
                      ((version.get("summary_json") or {}).get("advisory_universe_receipt") or {}).get("universe_selection")]
            declared = [normalize_advisory_universe_selection(value) for value in values if value is not None]
            if declared and any(value != declared[0] for value in declared):
                _fail("generic original pool declarations disagree; current pool cannot repair them")
            universe = declared[0] if declared else None
            count = len(chosen)
            candidates = pd.DataFrame([{KEY[0]: decision.isoformat(), KEY[1]: actual.isoformat(), KEY[2]: item["symbol"],
                "selection_effective_rank": item["rank"], "candidate_group_size": count} for item in chosen], columns=ROSTER)
        metadata = dict(package_id=package, run_id=run.run_id, list_version_id=version["list_version_id"], universe_identity=universe)
        receipt = dict(schema_version="generic_daily_published_list_v1", program_id=program_id,
            binding_version_id=version["binding_version_id"], review_run_id=review[0], **metadata,
            decision_date=decision.isoformat(), target_date=actual.isoformat(), candidate_count=count,
            candidate_scope="ORIGINAL_PUBLISHED_ENTRY_POPULATION_MAX50", original_items=items, unmodeled_items=unmodeled,
            candidate_roster_sha256=sha(_records(candidates)), source_review_policy_sha256=policy,
            declared_manifest_by_package=manifests,
            universe_evidence="DECLARED_ORIGINAL" if declared else "UNKNOWN_ORIGINAL_POOL_METADATA",
            source_evidence="CURRENT_DB_ORIGINAL_PUBLISHED_LIST_NOT_ORIGINAL_DATA_CAPTURE",
            new_selection_runs=0, qualification_rechecked=False, native_receipt_created=False, outcomes_read=False,
            database_write=False, original_list_status="NO_CANDIDATES" if not count else "ORIGINAL_CANDIDATES")
        receipt["input_identity_sha256"] = sha(receipt)
        return dict(packet=dict(decision_date=decision, target_date=actual, candidates=candidates, metadata=metadata),
                    candidate_receipt=receipt)
