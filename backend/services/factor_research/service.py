"""Research-only orchestration; formal factor writers stay with their owners."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from .models import ResearchError, read_json
from .runner import execute, validate_spec, write_json


class ResearchService:
    def __init__(self, repository, *, target=None):
        self.repository = repository
        self.target = target

    def run(self, value):
        spec, output = validate_spec(value)
        req = {"task_id": spec["task_id"], "record_id": spec["record_id"],
               "expected_revision": spec["expected_revision"], "record_type": "attempt",
               "attempt_id": spec["attempt_id"], "summary": "开始候选执行与研究评价",
               "payload": {"execution": {"status": "registered", "spec": spec, "output": str(output),
                                           "database_target": self.target}}}
        # Replayed requests return the DB record even when their output now exists.
        from .models import request
        previous = self.repository.replay(request(req))
        if previous:
            return previous
        if output.exists():
            raise ResearchError("output_exists", "Attempt output already exists; inspect or attach, do not overwrite")
        started = self.repository.record(req)
        if not started["applied"]:
            return started
        try:
            result = execute(spec, output)
            result["database_target"] = self.target
            path = output / "result.json"
            write_json(path, result)
        except Exception as exc:
            error = {"type": type(exc).__name__, "code": getattr(exc, "code", "candidate_failed")}
            failed = {"task_id": spec["task_id"], "record_id": str(uuid4()),
                      "expected_revision": started["revision"], "record_type": "result",
                      "attempt_id": spec["attempt_id"], "summary": "候选执行或评价失败",
                      "payload": {"execution": {"status": "failed", "error": error, "output": str(output)}}}
            if output.is_dir():
                write_json(output / "failure.json", failed)
            try:
                self.repository.record(failed)
            except Exception as record_exc:
                raise ResearchError("failure_not_recorded", "Execution failed; recover the retained failure record",
                                    artifact=str(output), execution_error=error,
                                    record_error=type(record_exc).__name__) from record_exc
            raise ResearchError("candidate_failed", "Execution or evaluation failed; inspect the task record",
                                artifact=str(output), execution_error=error) from exc
        attachment = {"task_id": spec["task_id"], "attempt_id": spec["attempt_id"],
                      "record_id": str(uuid4()), "expected_revision": started["revision"],
                      "result_path": str(path)}
        try:
            write_json(output / "attach.json", attachment)
            return self.attach(attachment)
        except Exception as exc:
            raise ResearchError("computed_not_recorded", "Computation retained; attach without recomputing",
                                attach_path=str(output / "attach.json") if (output / "attach.json").is_file() else None,
                                result_path=str(path), attachment=attachment,
                                record_error=type(exc).__name__) from exc

    def attach(self, value):
        allowed = {"task_id", "attempt_id", "record_id", "expected_revision", "result_path"}
        if set(value) != allowed:
            raise ResearchError("invalid_request", "attach requires task/attempt/record IDs, revision and result_path")
        start = self.repository.attempt(value["task_id"], value["attempt_id"])
        execution = start["payload_json"]["execution"]
        expected = Path(execution["output"]) / "result.json"
        supplied = Path(value["result_path"])
        if supplied.is_symlink() or supplied.resolve() != expected.resolve():
            raise ResearchError("result_mismatch", "Result does not belong to the recorded attempt")
        result = read_json(supplied)
        if (result.get("request") != execution["spec"] or result.get("task_id") != value["task_id"]
                or result.get("attempt_id") != value["attempt_id"] or result.get("status") != "computed"):
            raise ResearchError("result_mismatch", "Result identity or state differs from recorded attempt")
        expected_names = [item["factor_name"] for item in execution["spec"]["candidates"]]
        candidates = result.get("candidates")
        if (result.get("scope") != "research_candidate" or not isinstance(candidates, list)
                or [item.get("factor_name") for item in candidates if isinstance(item, dict)] != expected_names
                or len(candidates) != len(expected_names)):
            raise ResearchError("result_mismatch", "Result must contain every candidate in the recorded attempt")
        for item in candidates:
            if not isinstance(item.get("metrics"), dict) or item.get("scope") != "research_candidate":
                raise ResearchError("result_mismatch", "Missing candidate evaluation result")
            folder = expected.parent / item["factor_name"]
            for field, filename in (("values", "values.h5"), ("source_script", "factor.py")):
                path = Path(item.get(field, ""))
                if path.is_symlink() or path.resolve() != (folder / filename).resolve() or not path.is_file():
                    raise ResearchError("result_mismatch", "Candidate artifact missing or belongs to another attempt")
        return self.repository.record({"task_id": value["task_id"], "record_id": value["record_id"],
                                       "expected_revision": value["expected_revision"], "record_type": "result",
                                       "attempt_id": value["attempt_id"], "summary": "候选评价完成（不代表因子有效）",
                                       "payload": {"execution": {"status": "computed"},
                                                   "artifacts": {"result_path": str(expected)},
                                                   "research_result": result}})

    def context(self, names, *, start_date, end_date, limit=100, offset=0):
        """Small existing official summaries; no ensure/compute/cache refresh calls."""
        from datetime import date
        if not names or date.fromisoformat(start_date) > date.fromisoformat(end_date) or limit < 1 or offset < 0:
            raise ResearchError("invalid_request", "Explicit factors, valid dates and pagination are required")
        with self.repository.cursor() as cur:
            cur.execute("""SELECT id,factor_name,source,asset_path,is_available FROM aistock_factor_catalog
                WHERE factor_name=ANY(%s) ORDER BY factor_name,source,id LIMIT %s OFFSET %s""", (names, limit + 1, offset))
            catalog = [dict(r) for r in cur.fetchall()]
            cur.execute("""SELECT id,factor_name,eval_window,calc_engine,calculated_at,ic_mean,icir,
                    rank_ic_mean,h20_ic_mean,h20_rank_ic_mean,coverage,coverage_semantics,
                    data_start,data_end,return_horizon,universe,universe_rule_version,calc_batch_id,
                    snapshot_date,direction,coverage_numerator,coverage_denominator
                FROM aistock_factor_metrics
                WHERE factor_name=ANY(%s) AND calculated_at::date BETWEEN %s AND %s
                ORDER BY calculated_at DESC,id DESC LIMIT %s OFFSET %s""", (names, start_date, end_date, limit + 1, offset))
            metrics = [dict(r) for r in cur.fetchall()]
            cur.execute("""SELECT c.id,a.factor_name AS factor_a,b.factor_name AS factor_b,c.correlation,
                    c.method,c.as_of_date,c.data_window_days,c.universe,c.computed_at
                FROM qe_factor_correlations c
                JOIN aistock_factor_catalog a ON a.id=c.factor_a_id
                JOIN aistock_factor_catalog b ON b.id=c.factor_b_id
                WHERE (a.factor_name=ANY(%s) OR b.factor_name=ANY(%s)) AND c.as_of_date BETWEEN %s AND %s
                ORDER BY c.as_of_date DESC,c.id DESC LIMIT %s OFFSET %s""", (names, names, start_date, end_date, limit + 1, offset))
            correlations = [dict(r) for r in cur.fetchall()]
        return {"database_target": self.target, "catalog": catalog[:limit], "metrics": metrics[:limit], "correlations": correlations[:limit],
                "next_offset": offset + limit if any(len(rows) > limit for rows in (catalog, metrics, correlations)) else None,
                "date_filter": {"metrics": "calculated_at", "correlations": "as_of_date"},
                "availability": {"catalog": bool(catalog), "metrics": bool(metrics), "correlations": bool(correlations)},
                "comparability": "inspect_recorded_windows_and_basis",
                "correlation_sample_evidence": "unavailable_in_this_summary; zero_is_not_proof_of_independence"}
