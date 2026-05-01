import asyncio

from backend.services.quantevolver import qe_evolution_service as qes


class FakeConnContext:
    def __init__(self, state):
        self.state = state

    def __enter__(self):
        self.state["entered"] += 1
        return FakeConn(self.state)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.state["exited"] += 1
        return False


class FakeConn:
    def __init__(self, state):
        self.state = state

    def cursor(self, *args, **kwargs):
        return FakeCursor(self.state)

    def commit(self):
        self.state["commits"] += 1


class FakeCursor:
    def __init__(self, state):
        self.state = state
        self.sql = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, sql, params=None):
        self.sql = " ".join(str(sql).split())
        self.state["sql"].append(self.sql)

    def fetchone(self):
        if "pg_try_advisory_lock" in self.sql:
            return (True,)
        if "FROM qe_evolution_tasks" in self.sql:
            return {
                "task_id": "task-a",
                "task_type": "custom_evo",
                "node_id": "node-a",
                "strategy_evo_config": {
                    "loops": [
                        {"loop_index": 1, "node_id": "node-a", "label": "base"},
                        {"loop_index": 2, "node_id": "node-b", "label": "old"},
                    ],
                    "node_parallelism": {"node-a": 1, "node-b": 1},
                },
                "strategy_evo_execution_mode": "serial",
            }
        return None

    def fetchall(self):
        if "FROM qe_evolution_loops" in self.sql:
            return []
        return []


def _patch_fake_db(monkeypatch):
    state = {"entered": 0, "exited": 0, "commits": 0, "sql": []}
    monkeypatch.setattr(qes, "get_conn", lambda: FakeConnContext(state))
    return state


def test_rerun_custom_evo_loop_uses_get_conn_context_manager_for_lock(monkeypatch):
    state = _patch_fake_db(monkeypatch)
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)

    async def fake_delete(task_id, loop_index):
        return {"loop_id": f"{task_id}_Loop{loop_index}", "deleted_counts": {}}

    scheduler.delete_custom_evo_loop_result = fake_delete

    result = asyncio.run(
        scheduler.rerun_custom_evo_loop(
            "task-a",
            2,
            {"loop_index": 2, "node_id": "node-b", "label": "replacement"},
            execution_mode="parallel_2",
            node_parallelism={"node-a": 1, "node-b": 1},
        )
    )

    assert result["loop_id"] == "task-a_Loop2"
    assert state["entered"] == state["exited"]
    assert any("pg_advisory_unlock" in sql for sql in state["sql"])


def test_append_custom_evo_loops_uses_get_conn_context_manager_for_lock(monkeypatch):
    state = _patch_fake_db(monkeypatch)
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)

    result = asyncio.run(
        scheduler.append_custom_evo_loops(
            "task-a",
            [{"node_id": "node-b", "label": "new"}],
            execution_mode="parallel_2",
            node_parallelism={"node-a": 1, "node-b": 1},
            ack_failed_loop_warning=True,
        )
    )

    assert result["new_loop_indexes"] == [3]
    assert state["entered"] == state["exited"]
    assert any("pg_advisory_unlock" in sql for sql in state["sql"])
