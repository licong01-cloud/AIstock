"""PostgreSQL task projection plus compact records; no schema bootstrap."""
from __future__ import annotations

from contextlib import contextmanager

from psycopg2 import IntegrityError, sql
from psycopg2.extras import Json, RealDictCursor

from .models import ResearchError, identifier, request, response


class ResearchRepository:
    def __init__(self, connection_factory=None):
        if connection_factory is None:
            from backend.db.pg_pool import get_conn
            connection_factory = get_conn
        self.connection_factory = connection_factory

    @contextmanager
    def cursor(self, *, write=False):
        with self.connection_factory(autocommit=False, manage_transaction=True) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if not write:
                    cur.execute("SET TRANSACTION READ ONLY")
                yield cur

    @staticmethod
    def _result(row, *, replayed):
        return response(task_id=str(row["task_id"]), record_id=str(row["record_id"]),
                        revision=row["task_revision"], applied=not replayed,
                        replayed=replayed, result=dict(row))

    def replay(self, req):
        with self.cursor() as cur:
            cur.execute("SELECT * FROM public.factor_research_records WHERE record_id=%s", (req["record_id"],))
            row = cur.fetchone()
        if row is None:
            return None
        if row["payload_json"]["request"] != req:
            raise ResearchError("request_conflict", "record_id already identifies a different request")
        return self._result(row, replayed=True)

    @staticmethod
    def _insert_record(cur, req, revision, record_type):
        payload = {**req["payload"], "request": req}
        cur.execute("""INSERT INTO public.factor_research_records
            (record_id,task_id,task_revision,record_type,attempt_id,related_record_id,summary,payload_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
                    (req["record_id"], req["task_id"], revision, record_type,
                     req.get("attempt_id"), req.get("related_record_id"), req["summary"], Json(payload)))
        return cur.fetchone()

    def create(self, value):
        req = request(value, create=True)
        previous = self.replay(req)
        if previous:
            return previous
        values = {"status": "active", "phase": "research", "completed_summary": "",
                  "next_action": "", "blocking_reason": "", "factor_names": [], **req["task"]}
        columns = sorted(values)
        try:
            with self.cursor(write=True) as cur:
                cur.execute(sql.SQL("INSERT INTO public.factor_research_tasks (task_id,{}) VALUES (%s,{})").format(
                    sql.SQL(",").join(map(sql.Identifier, columns)),
                    sql.SQL(",").join(sql.Placeholder() for _ in columns)),
                    [req["task_id"]] + [Json(values[k]) if k == "context_json" else values[k] for k in columns])
                row = self._insert_record(cur, req, 1, "created")
            return self._result(row, replayed=False)
        except IntegrityError as exc:
            replay = self.replay(req)
            if replay:
                return replay
            raise ResearchError("request_conflict", "Task or record already exists, or violates the schema") from exc

    def record(self, value):
        req = request(value)
        previous = self.replay(req)
        if previous:
            return previous
        try:
            with self.cursor(write=True) as cur:
                if req.get("related_record_id"):
                    cur.execute("SELECT task_id FROM public.factor_research_records WHERE record_id=%s",
                                (req["related_record_id"],))
                    parent = cur.fetchone()
                    if not parent or str(parent["task_id"]) != req["task_id"]:
                        raise ResearchError("invalid_relation", "related_record_id must belong to this task")
                if req.get("attempt_id") and req["record_type"] != "attempt":
                    cur.execute("""SELECT record_id FROM public.factor_research_records
                        WHERE task_id=%s AND attempt_id=%s AND record_type='attempt'""",
                                (req["task_id"], req["attempt_id"]))
                    if not cur.fetchone():
                        raise ResearchError("attempt_missing", "No start record for this attempt")
                updates = [sql.SQL("revision=revision+1"), sql.SQL("updated_at=clock_timestamp()")]
                params = []
                for key, val in sorted(req["task_update"].items()):
                    updates.append(sql.SQL("{}=%s").format(sql.Identifier(key)))
                    params.append(Json(val) if key == "context_json" else val)
                cur.execute(sql.SQL("UPDATE public.factor_research_tasks SET {} WHERE task_id=%s AND revision=%s RETURNING revision").format(
                    sql.SQL(",").join(updates)), params + [req["task_id"], req["expected_revision"]])
                changed = cur.fetchone()
                if not changed:
                    raise ResearchError("revision_conflict", "Read current progress before merging your update")
                row = self._insert_record(cur, req, changed["revision"], req["record_type"])
            return self._result(row, replayed=False)
        except (IntegrityError, ResearchError) as exc:
            replay = self.replay(req)
            if replay:
                return replay
            if req["record_type"] == "attempt":
                existing = self.attempt(req["task_id"], req["attempt_id"], required=False)
                if existing:
                    raise ResearchError("attempt_exists", "Attempt already registered; do not launch again",
                                        record_id=str(existing["record_id"])) from exc
            if isinstance(exc, ResearchError) and exc.code != "revision_conflict":
                raise
            with self.cursor() as cur:
                cur.execute("SELECT revision,phase,next_action FROM public.factor_research_tasks WHERE task_id=%s", (req["task_id"],))
                current = cur.fetchone()
            if current is None:
                raise ResearchError("task_missing", "Task does not exist") from exc
            if isinstance(exc, IntegrityError):
                raise ResearchError("record_conflict", "Record violates existing research schema") from exc
            raise ResearchError("revision_conflict", "Read and merge current progress", current=dict(current)) from exc

    def attempt(self, task_id, attempt_id, *, required=True):
        with self.cursor() as cur:
            cur.execute("""SELECT * FROM public.factor_research_records
                WHERE task_id=%s AND attempt_id=%s AND record_type='attempt'""",
                        (identifier(task_id, "task_id"), identifier(attempt_id, "attempt_id")))
            row = cur.fetchone()
        if row is None and required:
            raise ResearchError("attempt_missing", "Attempt does not exist")
        return dict(row) if row else None

    def show(self, task_id, *, before_revision=None, limit=20):
        task_id = identifier(task_id, "task_id")
        if limit < 1 or (before_revision is not None and before_revision < 1):
            raise ResearchError("invalid_request", "Page size/revision must be positive")
        with self.cursor() as cur:
            cur.execute("SELECT * FROM public.factor_research_tasks WHERE task_id=%s", (task_id,))
            task = cur.fetchone()
            if task is None:
                raise ResearchError("task_missing", "Task does not exist")
            cur.execute("""SELECT * FROM public.factor_research_records WHERE task_id=%s
                AND (%s IS NULL OR task_revision < %s) ORDER BY task_revision DESC LIMIT %s""",
                        (task_id, before_revision, before_revision, limit + 1))
            rows = cur.fetchall()
        return {"task": dict(task), "records": [dict(r) for r in rows[:limit]],
                "before_revision": rows[limit - 1]["task_revision"] if len(rows) > limit else None}

    def list(self, *, status=None, factor=None, query=None, limit=20, offset=0):
        if limit < 1 or offset < 0:
            raise ResearchError("invalid_request", "Invalid pagination")
        with self.cursor() as cur:
            cur.execute("""SELECT task_id,title,task_type,status,phase,next_action,factor_names,revision,updated_at
                FROM public.factor_research_tasks
                WHERE (%s IS NULL OR status=%s) AND (%s IS NULL OR %s=ANY(factor_names))
                AND (%s IS NULL OR title ILIKE %s OR objective ILIKE %s)
                ORDER BY updated_at DESC,task_id DESC LIMIT %s OFFSET %s""",
                        (status, status, factor, factor, query, f"%{query}%", f"%{query}%", limit + 1, offset))
            rows = cur.fetchall()
        return {"tasks": [dict(r) for r in rows[:limit]], "next_offset": offset + limit if len(rows) > limit else None}
