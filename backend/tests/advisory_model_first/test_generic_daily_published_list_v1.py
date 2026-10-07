"""Reuse original-list fixture; no parent/model inference or database writes."""
from copy import deepcopy

import pytest

from backend.services.advisory_model_first import generic_daily_published_list_v1 as m
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_sector_list_source_v1 import D, T, m as legacy
from backend.tests.advisory_model_first.test_economic_sector_list_source_v1 import original as original


def load(state, monkeypatch):
    for name in ("AdvisoryProgramPGRepository", "SelectionCenterRepository", "list_version_to_dict", "list_item_to_dict"):
        monkeypatch.setattr(m, name, getattr(legacy, name))
    return m.GenericDailyPublishedListSourceV1(read_session=state.source._session).load_day(program_id="p", target_date=T)


def test_original_thirty_and_legacy_pool_unknown_are_consumable(original, monkeypatch):
    before = deepcopy(original.items)
    output = load(original, monkeypatch)
    frame, receipt = output["packet"]["candidates"], output["candidate_receipt"]
    assert frame.selection_effective_rank.tolist() == list(range(1, 31)) and frame.candidate_group_size.eq(30).all()
    assert frame.decision_as_of_trade_date.eq(D.isoformat()).all() and frame.target_trade_date.eq(T.isoformat()).all()
    assert output["packet"]["metadata"]["universe_identity"] is None and receipt["universe_evidence"] == "UNKNOWN_ORIGINAL_POOL_METADATA"
    assert receipt["original_items"] == before and original.items == before and original.price_calls == 0
    assert not receipt["qualification_rechecked"] and not receipt["native_receipt_created"] and receipt["new_selection_runs"] == 0


def test_original_empty_and_exit_items_remain_explicit(original, monkeypatch):
    for item in original.items:
        item["action"] = "EXIT"
    output = load(original, monkeypatch)
    assert output["packet"]["candidates"].empty and output["candidate_receipt"]["original_list_status"] == "NO_CANDIDATES"
    assert len(output["candidate_receipt"]["unmodeled_items"]) == 30


@pytest.mark.parametrize("change", ["duplicate", "gap", "missing_rank", "foreign_run", "pool_conflict"])
def test_foreign_or_incomplete_original_data_fails(original, monkeypatch, change):
    if change == "duplicate":
        original.items.append(deepcopy(original.items[0]))
    elif change == "gap":
        original.items.pop(0)
    elif change == "missing_rank":
        original.items[0]["rank"] = None
    elif change == "foreign_run":
        original.items[0]["evidence_json"]["source_run_id"] = "other"
    else:
        original.run.runtime_config["universe_selection"] = {"mode": "stock_universe", "pool_ids": []}
        original.binding[3]["universe_selection"] = {"mode": "single_index", "pool_ids": ["csi300"]}
    with pytest.raises(AdvisoryModelFirstError):
        load(original, monkeypatch)
