import asyncio

from backend.services.quantevolver import qe_evolution_service as qes


def test_stop_task_cancels_all_non_terminal_loops_and_kills_each_candidate(monkeypatch):
    task_id = "qe_stop_test"
    db = {
        "task_status": "running",
        "loops": [
            {"loop_id": f"{task_id}_Loop1", "loop_index": 1, "status": "running"},
            {"loop_id": f"{task_id}_Loop2", "loop_index": 2, "status": "processing"},
            {"loop_id": f"{task_id}_Loop3", "loop_index": 3, "status": "pending"},
            {"loop_id": f"{task_id}_Loop4", "loop_index": 4, "status": "failed"},
            {"loop_id": f"{task_id}_Loop5", "loop_index": 5, "status": "completed"},
        ],
    }

    class FakeCursor:
        def __init__(self):
            self.rows = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            normalized = " ".join(sql.lower().split())
            if normalized.startswith("select task_id, node_id from qe_evolution_tasks"):
                self.rows = [{"task_id": params[0], "node_id": "wsl2-5080"}] if params[0] == task_id else []
            elif normalized.startswith("select loop_id, loop_index, status, node_id from qe_evolution_loops"):
                self.rows = [row.copy() for row in db["loops"] if row["status"] != "completed"]
            elif normalized.startswith("update qe_evolution_tasks set status = 'paused'"):
                db["task_status"] = "paused"
                self.rows = []
            elif normalized.startswith("update qe_evolution_loops set status = 'cancelled'"):
                changed = []
                for row in db["loops"]:
                    if row["status"] in {"running", "processing", "pending"}:
                        row["status"] = "cancelled"
                        changed.append({"loop_id": row["loop_id"], "loop_index": row["loop_index"]})
                self.rows = changed
            else:
                raise AssertionError(f"Unexpected SQL: {sql}")

        def fetchone(self):
            return self.rows[0] if self.rows else None

        def fetchall(self):
            return self.rows

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self, *args, **kwargs):
            return FakeCursor()

        def commit(self):
            pass

    class FakeClient:
        def __init__(self):
            self.calls = []

        async def kill_loop(self, _task_id, loop_id):
            self.calls.append(loop_id)
            if loop_id in {"Loop3", "Loop4"}:
                raise RuntimeError("Client error '404 Not Found': No pid.txt found")
            return {"killed": True, "status": "cancelled"}

    fake_client = FakeClient()
    monkeypatch.setattr(qes, "get_conn", lambda: FakeConn())
    scheduler = qes.AutoEvolutionScheduler.__new__(qes.AutoEvolutionScheduler)
    scheduler._get_workspace_client_for_node_id = lambda _node_id: fake_client

    result = asyncio.run(scheduler.stop_task(task_id))

    assert db["task_status"] == "paused"
    assert [row["status"] for row in db["loops"]] == [
        "cancelled",
        "cancelled",
        "cancelled",
        "failed",
        "completed",
    ]
    assert fake_client.calls == ["Loop1", "Loop2", "Loop3", "Loop4"]
    assert len(result["loops_cancelled"]) == 3
    assert len(result["loops_killed"]) == 4
    assert [item["error"] for item in result["loops_killed"]] == [None, None, None, None]
