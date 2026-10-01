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
        comparison = result.get("research_comparison")
        expected_comparison = execution["spec"].get("comparison")
        if expected_comparison is not None:
            result_windows = comparison.get("windows") if isinstance(comparison, dict) else None
            expected_windows = expected_comparison["evaluation_windows"]

            def window_matches(actual, expected):
                fit_index = expected.get("fit_window_index") if isinstance(expected, dict) else None
                return (
                    isinstance(actual, dict)
                    and type(fit_index) is int
                    and 0 <= fit_index < len(expected_comparison["fit_windows"])
                    and actual.get("fit_window_index") == fit_index
                    and actual.get("fit_window") == expected_comparison["fit_windows"][fit_index]
                    and actual.get("evaluation_window") == {key: expected.get(key) for key in ("start", "end")}
                )

            window_identity_matches = (
                isinstance(result_windows, list)
                and len(result_windows) == len(expected_windows)
                and all(window_matches(actual, expected) for actual, expected in zip(result_windows, expected_windows))
            )
            actual_knowledge = comparison.get("knowledge_cutoff", {}) if isinstance(comparison, dict) else {}
            if (not isinstance(comparison, dict)
                    or comparison.get("schema_version") != "factor_research_comparison_v1"
                    or comparison.get("scope") != "research_comparison_not_official_metrics_or_qe_result"
                    or comparison.get("method_version") != execution["spec"]["method_version"]
                    or comparison.get("research_role") != expected_comparison["research_role"]
                    or comparison.get("horizon") != expected_comparison["horizon"]
                    or comparison.get("baseline") != expected_comparison["baseline"]
                    or comparison.get("candidate") != expected_comparison["candidate"]
                    or comparison.get("controls") != expected_comparison["controls"]
                    or comparison.get("fit_windows") != expected_comparison["fit_windows"]
                    or comparison.get("evaluation_windows") != expected_comparison["evaluation_windows"]
                    or comparison.get("direction") != expected_comparison["direction"]
                    or {key: actual_knowledge.get(key) for key in ("date", "phase")}
                    != expected_comparison["knowledge_cutoff"]
                    or not window_identity_matches
                    or not isinstance(comparison.get("cost"), dict)
                    or not isinstance(comparison.get("information_relation"), dict)
                    or not isinstance(comparison.get("use_value"), dict)):
                raise ResearchError("result_mismatch", "Comparison result is missing or has the wrong contract")
        elif comparison is not None:
            raise ResearchError("result_mismatch", "Legacy request cannot attach an undeclared comparison")
        full_evaluation = result.get("full_evaluation")
        expected_full_evaluation = execution["spec"].get("full_evaluation")
        if expected_full_evaluation is not None:
            correlations = (
                full_evaluation.get("correlations")
                if isinstance(full_evaluation, dict)
                else None
            )
            expected_reference_count = len(
                set(expected_full_evaluation["reference_value_artifacts"])
                - set(expected_names)
            )
            expected_reference_names = sorted(
                set(expected_full_evaluation["reference_value_artifacts"])
                - set(expected_names)
            )
            expected_parameters = {
                key: expected_full_evaluation[key]
                for key in (
                    "correlation_batch_size",
                    "correlation_half_life",
                    "correlation_min_stocks",
                    "correlation_min_effective_days",
                )
            }
            windows = (
                correlations.get("windows") if isinstance(correlations, dict) else None
            )
            candidate_windows = (
                correlations.get("candidate_candidate_windows")
                if isinstance(correlations, dict)
                else None
            )

            expected_external_pairs = {
                (candidate, reference)
                for candidate in expected_names
                for reference in expected_reference_names
            }
            expected_internal_pairs = {
                (left, right)
                for position, left in enumerate(sorted(expected_names))
                for right in sorted(expected_names)[position + 1 :]
            }

            def closed(rows, expected_pairs):
                return (
                    isinstance(rows, list)
                    and all(isinstance(row, dict) for row in rows)
                    and all(
                        isinstance(row.get("candidate"), str)
                        and isinstance(row.get("reference"), str)
                        and row.get("status") in {"available", "unavailable"}
                        for row in rows
                    )
                    and len(rows) == len(expected_pairs)
                    and {
                        (row.get("candidate"), row.get("reference")) for row in rows
                    }
                    == expected_pairs
                )

            external_closed = (
                isinstance(windows, list)
                and all(
                    isinstance(window, dict)
                    and window.get("requested_pairs")
                    == len(expected_names) * expected_reference_count
                    and closed(
                        window.get("records"), expected_external_pairs,
                    )
                    and window.get("available_pairs")
                    == sum(
                        row.get("status") == "available"
                        for row in window.get("records", [])
                    )
                    and window.get("unavailable_pairs")
                    == sum(
                        row.get("status") == "unavailable"
                        for row in window.get("records", [])
                    )
                    and window.get("available_pairs", 0)
                    + window.get("unavailable_pairs", 0)
                    == window.get("requested_pairs")
                    for window in windows
                )
            )
            internal_pair_count = len(expected_names) * (len(expected_names) - 1) // 2
            internal_closed = (
                isinstance(candidate_windows, list)
                and all(
                    isinstance(window, dict)
                    and window.get("requested_pairs") == internal_pair_count
                    and closed(window.get("records"), expected_internal_pairs)
                    and window.get("available_pairs")
                    == sum(
                        row.get("status") == "available"
                        for row in window.get("records", [])
                    )
                    and window.get("unavailable_pairs")
                    == sum(
                        row.get("status") == "unavailable"
                        for row in window.get("records", [])
                    )
                    and window.get("available_pairs", 0)
                    + window.get("unavailable_pairs", 0)
                    == internal_pair_count
                    for window in candidate_windows
                )
            )
            if (
                not isinstance(full_evaluation, dict)
                or full_evaluation.get("schema_version") != "factor_research_full_evaluation_v1"
                or full_evaluation.get("scope")
                != "research_only_not_official_metrics_correlations_or_qe_result"
                or full_evaluation.get("candidate_names") != expected_names
                or full_evaluation.get("official_database_writes") != 0
                or not isinstance(full_evaluation.get("windows"), dict)
                or not isinstance(correlations, dict)
                or correlations.get("reference_count") != expected_reference_count
                or correlations.get("reference_names") != expected_reference_names
                or correlations.get("parameters") != expected_parameters
                or correlations.get("reference_reference_pairs_computed") != 0
                or not isinstance(correlations.get("window_names"), list)
                or correlations.get("window_names")
                != [window.get("window") for window in windows]
                or correlations.get("window_names")
                != [window.get("window") for window in candidate_windows]
                or len(set(correlations.get("window_names")))
                != len(correlations.get("window_names"))
                or any(
                    name not in full_evaluation.get("windows", {})
                    or name.startswith("month_")
                    for name in correlations.get("window_names")
                )
                or not external_closed
                or not internal_closed
            ):
                raise ResearchError(
                    "result_mismatch",
                    "Full evaluation result is missing or has the wrong contract",
                )
        elif full_evaluation is not None:
            raise ResearchError(
                "result_mismatch", "Legacy request cannot attach an undeclared full evaluation"
            )
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
            cur.execute("""SELECT id,factor_name,source,asset_path,is_available,expression,description_cn FROM aistock_factor_catalog
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
        from .comparison_context import enrich_catalog_context
        visible_catalog, neighbor_hints = enrich_catalog_context(catalog[:limit])
        return {"database_target": self.target, "catalog": visible_catalog, "metrics": metrics[:limit], "correlations": correlations[:limit],
                "next_offset": offset + limit if any(len(rows) > limit for rows in (catalog, metrics, correlations)) else None,
                "date_filter": {"metrics": "calculated_at", "correlations": "as_of_date"},
                "availability": {"catalog": bool(catalog), "metrics": bool(metrics), "correlations": bool(correlations)},
                "comparison_context": {"neighbor_hints": neighbor_hints,
                                       "scope": "bounded_requested_catalog_rows",
                                       "unknown_dependencies_are_not_inferred": True},
                "comparability": "inspect_recorded_windows_and_basis",
                "correlation_sample_evidence": "unavailable_in_this_summary; zero_is_not_proof_of_independence"}
