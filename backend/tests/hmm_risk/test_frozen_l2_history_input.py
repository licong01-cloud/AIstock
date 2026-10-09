"""Pinned file input contracts; no numerical new-window source read."""

import json

import pytest

from backend.services.hmm_risk import frozen_l2_history as h
from backend.services.hmm_risk import frozen_l2_history_input as source
from backend.services.hmm_risk.formal_state_model import receipt


def test_request_hash_drift_stops_before_source_access(tmp_path, monkeypatch):
    p = tmp_path / "request.json"
    p.write_text(
        json.dumps(receipt({"schema_version": h.VERSION + "_request", "contract": h.CONTRACT, "kind": "P1"})),
        encoding="utf8",
    )
    with pytest.raises(Exception, match="identity differs"):
        source._asset(p, "0" * 64)
    monkeypatch.setattr(source, "_rotation_source", lambda _: pytest.fail("must not enter source on wrong contract"))
    with pytest.raises(Exception, match="request contract"):
        source.prepare(receipt({"schema_version": "unknown", "contract": h.CONTRACT, "kind": "P1"}), "a" * 40)


def test_source_mutation_is_not_a_legal_data_na(tmp_path):
    path = tmp_path / "source.json"
    path.write_text("{}", encoding="utf8")
    stamps = {"source": source._stamp(path)}
    path.write_text('{"changed":true}', encoding="utf8")
    with pytest.raises(Exception, match="source changed"):
        source._stable_files({"source": path}, stamps)
