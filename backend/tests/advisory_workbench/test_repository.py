from datetime import date

from backend.services.advisory_workbench.repository import EPISODES, ReadRepository


class CaptureRepository(ReadRepository):
    def __init__(self):
        self.calls = []

    def rows(self, sql, params=()):
        self.calls.append((sql, params))
        return [{"n": 41}] if "count(*) AS n" in sql else []


def test_historical_page_filters_before_limit_and_preserves_episode_identity():
    repo = CaptureRepository()
    _, total = repo.episodes("program", {"q": "' OR TRUE; --", "source": "FORWARD", "binding": "b",
        "start": "2026-10-12", "end": "2026-10-28"}, limit=20, offset=20)
    assert total == 41
    sql, params = repo.calls[-1]
    assert "ORDER BY signal_date DESC,episode_id LIMIT %s OFFSET %s" in sql
    assert "' OR TRUE" not in sql and "%" + "' OR TRUE; --" + "%" in params
    assert "origin_source=%s" in sql and "origin_binding_id=%s" in sql
    assert params[-2:] == [20, 20]
    assert "d.action='ENTER'" in EPISODES and "origin.run_type='REPLAY'" in EPISODES


def test_versions_are_sql_paged_and_large_payloads_are_excluded():
    repo = CaptureRepository()
    repo.versions("p", 50, 100)
    sql, args = repo.calls[-1]
    assert "LIMIT %s OFFSET %s" in sql and args == ("p", 50, 100)
    assert "summary_json->'advisory_date_context'" in sql and "SELECT *" not in sql
    repo.forward("p", date(2026, 10, 28))
    sql, _ = repo.calls[-1]
    assert "publication_payload_json" not in sql and "selection_payload_json" not in sql


def test_empty_episode_identity_filter_does_not_fall_back_to_all_history():
    where, values = ReadRepository.episode_filter({"episode_ids": []})
    assert "episode_id=ANY(%s)" in where and values == [[]]
