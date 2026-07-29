from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from backend.services.qe_archive.long_trend_repository import (
    QELongTrendEvaluationResultRepository,
    QELongTrendResultPersistenceUnavailable,
    QELongTrendResultQueryError,
    QELongTrendResultRepositoryError,
    build_artifact_records,
    build_metric_records,
    _verify_control_receipt_identity,
    _encode_cursor,
)
from backend.services.quantevolver.long_trend_artifact_store import QELongTrendArtifactStore
from backend.services.quantevolver.long_trend_evaluation_control_repository import QELongTrendControlLease
from backend.services.quantevolver.qe_dataset_contract import QE_DATASET_CONTRACT_ID

EVALUATION_ID = "qelt_" + "a" * 64


def _terminal() -> dict[str, object]:
    return {
        "schema_version": "qe_long_trend_worker_terminal_v1",
        "evaluation_id": EVALUATION_ID,
        "status": "partial",
        "family_status": {
            "signal_path": {"status": "NOT_COMPUTABLE", "reason_codes": ["prediction_missing"]},
            "position_episode": {"status": "NOT_VERIFIABLE", "reason_codes": ["position_missing"]},
        },
        "metrics": [
            {
                "metric_scope": "signal_path",
                "metric_key": "rank_ic",
                "slice": "last_252_signal_days",
                "horizon": 60,
                "barrier": None,
                "k": None,
                "value_num": 0.081,
                "value_json": {"date_count": 252, "hac_lag": 59, "raw_p_value": 0.01},
                "quality_flag": "ok",
            },
            {
                "metric_scope": "portfolio_result",
                "metric_key": "authoritative_portfolio_summary",
                "slice": "all_oos",
                "horizon": None,
                "barrier": None,
                "k": None,
                "value_num": 0.72,
                "value_json": {"annualized_return": 0.72, "max_drawdown": -0.12},
                "quality_flag": "computed_with_limitations",
            },
        ],
    }


def _write_json(path: Path, payload: dict[str, object]) -> dict[str, object]:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(encoded)
    return {"sha256": hashlib.sha256(encoded).hexdigest(), "size_bytes": len(encoded)}


def _published_assets(tmp_path: Path) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    terminal = _terminal()
    terminal_path = tmp_path / "worker_terminal_receipt.json"
    compact_path = tmp_path / "worker_compact_receipt.json"
    terminal_meta = _write_json(terminal_path, terminal)
    compact = {
        "schema_version": "qe_long_trend_worker_compact_v1",
        "evaluation_id": EVALUATION_ID,
    }
    compact_meta = _write_json(compact_path, compact)
    store = QELongTrendArtifactStore(
        tmp_path / "long-trend-cas",
        prediction_store_root=tmp_path / "prediction-store",
    )
    manifest = store.publish(
        evaluation_id=EVALUATION_ID,
        worker_terminal=terminal,
        artifact_files={
            "worker_terminal_receipt": terminal_path,
            "worker_compact_receipt": compact_path,
        },
        expected_catalog={
            "worker_terminal_receipt": terminal_meta,
            "worker_compact_receipt": compact_meta,
        },
    )
    published = {
        "schema_version": "qe_long_trend_published_compact_v1",
        "receipt_stage": "cas_published",
        "evaluation_id": EVALUATION_ID,
        "artifact_manifest_uri": manifest["uri"],
        "artifact_manifest_sha256": manifest["artifact_manifest_sha256"],
    }
    published_meta = store.publish_compact_receipt(
        evaluation_id=EVALUATION_ID,
        receipt=published,
    )
    return terminal, manifest, published_meta


def test_metric_records_preserve_primary_scalar_and_bounded_diagnostics() -> None:
    records = build_metric_records(evaluation_id=EVALUATION_ID, worker_terminal=_terminal())

    assert len(records) == 2
    assert records[0].value_num == pytest.approx(0.081)
    assert records[0].value_json == {"date_count": 252, "hac_lag": 59, "raw_p_value": 0.01}
    assert records[0].dimension_json == {
        "schema_version": "qelt_metric_dimension_v2",
        "metric_scope": "signal_path",
        "period_start": None,
        "period_end": None,
        "slice": "last_252_signal_days",
        "horizon": 60,
        "sector_code": None,
        "barrier": None,
        "k": None,
        "entry_execution_status": None,
        "exit_execution_status": None,
    }
    assert len(records[0].dimension_key) == 64
    assert records[0].source_payload_path == "$.metrics[0]"


def test_metric_records_reject_duplicate_canonical_dimension() -> None:
    terminal = _terminal()
    terminal["metrics"] = [terminal["metrics"][0], dict(terminal["metrics"][0])]  # type: ignore[index]

    with pytest.raises(QELongTrendResultRepositoryError, match="duplicate long-trend metric identity"):
        build_metric_records(evaluation_id=EVALUATION_ID, worker_terminal=terminal)


def test_metric_records_reject_inverted_period() -> None:
    terminal = _terminal()
    terminal["metrics"][0]["period_start"] = "2026-06-30"  # type: ignore[index]
    terminal["metrics"][0]["period_end"] = "2026-01-01"  # type: ignore[index]

    with pytest.raises(QELongTrendResultRepositoryError, match="period_start is after period_end"):
        build_metric_records(evaluation_id=EVALUATION_ID, worker_terminal=terminal)


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"metric_key": "unknown"}, "unknown long-trend metric_key"),
        ({"metric_scope": "unknown"}, "invalid long-trend metric_scope"),
        ({"metric_scope": "portfolio_result"}, "requires metric_scope=signal_path"),
        ({"slice": "last_252d"}, "unregistered slice"),
        ({"quality_flag": "unknown"}, "invalid long-trend quality_flag"),
        ({"horizon": 30}, "invalid long-trend metric horizon"),
        ({"horizon": None}, "requires horizon"),
        ({"sector_code": "801010"}, "invalid sector_code dimension"),
        ({"k": 20}, "does not allow k"),
        ({"entry_execution_status": "filled_t1"}, "does not allow execution-status dimensions"),
        ({"value_text": "enum"}, "value_text cannot coexist"),
        ({"value_json": None}, "requires value_json"),
        ({"value_num": None, "value_json": None}, "has no value"),
        ({"barrier": True}, "must be numeric, not boolean"),
        ({"k": 1.5}, "must not lose precision"),
        ({"period_start": "not-a-date"}, "must be an ISO date"),
        ({"value_text": "x" * 8193, "value_num": None, "value_json": None}, "exceeds maximum length"),
        ({"value_num": float("nan")}, "must be finite"),
    ],
)
def test_metric_contract_rejects_unknown_or_lossy_values(updates: dict[str, object], message: str) -> None:
    terminal = _terminal()
    terminal["metrics"][0].update(updates)  # type: ignore[index]

    with pytest.raises(QELongTrendResultRepositoryError, match=message):
        build_metric_records(evaluation_id=EVALUATION_ID, worker_terminal=terminal)


def test_artifact_records_are_manifest_verified_and_path_free(tmp_path: Path) -> None:
    terminal, manifest, published_meta = _published_assets(tmp_path)

    records = build_artifact_records(
        evaluation_id=EVALUATION_ID,
        worker_terminal=terminal,
        manifest=manifest,
        published_meta=published_meta,
    )

    assert {record.artifact_type for record in records} == {
        "worker_terminal_receipt",
        "worker_compact_receipt",
        "artifact_manifest",
        "published_compact_receipt",
    }
    assert all(record.artifact_uri.startswith("aistock-qe-long-trend://evaluations/") for record in records)
    assert all("path" not in record.metadata and "blob_rel_path" not in record.metadata for record in records)


def test_artifact_records_reject_manifest_content_drift(tmp_path: Path) -> None:
    terminal, manifest, published_meta = _published_assets(tmp_path)
    manifest["typed_absence"] = {"signal_observations": {"status": "COMPUTED"}}

    with pytest.raises(QELongTrendResultRepositoryError, match="manifest content hash mismatch"):
        build_artifact_records(
            evaluation_id=EVALUATION_ID,
            worker_terminal=terminal,
            manifest=manifest,
            published_meta=published_meta,
        )


def test_artifact_records_reject_family_matrix_drift_even_with_valid_manifest_hash(tmp_path: Path) -> None:
    terminal, manifest, published_meta = _published_assets(tmp_path)
    manifest["required_artifacts"] = []
    content = {
        key: value
        for key, value in manifest.items()
        if key not in {"artifact_manifest_sha256", "uri", "published_at"}
    }
    manifest["artifact_manifest_sha256"] = hashlib.sha256(
        json.dumps(content, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    with pytest.raises(QELongTrendResultRepositoryError, match="required_artifacts differ"):
        build_artifact_records(
            evaluation_id=EVALUATION_ID,
            worker_terminal=terminal,
            manifest=manifest,
            published_meta=published_meta,
        )


def test_terminal_cas_materialization_requires_control_hash_parity(tmp_path: Path) -> None:
    terminal, manifest, published_meta = _published_assets(tmp_path)
    control = {
        "status": "partial",
        "worker_terminal_sha256": hashlib.sha256(
            json.dumps(terminal, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "artifact_manifest_sha256": manifest["artifact_manifest_sha256"],
        "stats_json": {"published_compact_receipt": published_meta},
    }

    _verify_control_receipt_identity(
        control,
        worker_terminal=terminal,
        manifest=manifest,
        published_meta=published_meta,
        lease=None,
    )

    control["artifact_manifest_sha256"] = "0" * 64
    with pytest.raises(QELongTrendResultRepositoryError, match="artifact manifest hash differs"):
        _verify_control_receipt_identity(
            control,
            worker_terminal=terminal,
            manifest=manifest,
            published_meta=published_meta,
            lease=None,
        )


class _RowsCursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.inserts: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, params=()) -> None:  # type: ignore[no-untyped-def]
        normalized = " ".join(sql.split())
        if normalized.startswith("INSERT INTO"):
            self.inserts.append((normalized, tuple(params)))
        elif not normalized.startswith("SELECT"):
            raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchall(self):
        return self.rows


class _ScriptedCursor:
    def __init__(self, handler):  # type: ignore[no-untyped-def]
        self.handler = handler
        self.current = None
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params=()) -> None:  # type: ignore[no-untyped-def]
        normalized = " ".join(sql.split())
        self.calls.append((normalized, tuple(params)))
        self.current = self.handler(normalized, tuple(params))

    def fetchone(self):
        if isinstance(self.current, list):
            return self.current[0] if self.current else None
        return self.current

    def fetchall(self):
        if self.current is None:
            return []
        return self.current if isinstance(self.current, list) else [self.current]


class _ScriptedConnection:
    def __init__(self, handler):  # type: ignore[no-untyped-def]
        self.cursor_value = _ScriptedCursor(handler)
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return self.cursor_value

    def commit(self) -> None:
        self.commits += 1


def _without_evaluation_id(record) -> dict[str, object]:  # type: ignore[no-untyped-def]
    payload = dict(record.__dict__)
    payload.pop("evaluation_id")
    return payload


def _phase3_schema_columns() -> dict[str, list[tuple[str, str, str]]]:
    return {
        "run_evaluation_metric": [
            ("evaluation_metric_id", "int8", "NO"), ("evaluation_id", "text", "NO"),
            ("metric_key", "text", "NO"), ("metric_scope", "text", "NO"),
            ("period_start", "date", "YES"), ("period_end", "date", "YES"),
            ("horizon", "int4", "YES"), ("sector_code", "text", "YES"),
            ("dimension_key", "text", "NO"), ("dimension_json", "jsonb", "NO"),
            ("value_num", "float8", "YES"), ("value_text", "text", "YES"),
            ("value_json", "jsonb", "YES"), ("unit", "text", "YES"),
            ("direction", "text", "YES"), ("source_payload_path", "text", "NO"),
            ("quality_flag", "text", "NO"), ("created_at", "timestamptz", "NO"),
        ],
        "run_evaluation_artifact": [
            ("evaluation_artifact_id", "int8", "NO"), ("evaluation_id", "text", "NO"),
            ("artifact_type", "text", "NO"), ("artifact_uri", "text", "NO"),
            ("sha256", "text", "NO"), ("schema_sha256", "text", "YES"),
            ("size_bytes", "int8", "YES"), ("row_count", "int8", "YES"),
            ("status", "text", "NO"), ("metadata", "jsonb", "NO"),
            ("created_at", "timestamptz", "NO"),
        ],
    }


def test_metric_writer_is_insert_once_replay_noop_and_conflict_loud() -> None:
    records = build_metric_records(evaluation_id=EVALUATION_ID, worker_terminal=_terminal())
    first = _RowsCursor([])

    assert QELongTrendEvaluationResultRepository._write_or_verify_metrics(
        first,
        evaluation_id=EVALUATION_ID,
        records=records,
    ) is False
    assert len(first.inserts) == 1
    assert first.inserts[0][0].count("(%s") == len(records)

    existing = [_without_evaluation_id(record) for record in records]
    existing.sort(key=lambda row: (row["metric_key"], row["dimension_key"]))
    replay = _RowsCursor(existing)
    assert QELongTrendEvaluationResultRepository._write_or_verify_metrics(
        replay,
        evaluation_id=EVALUATION_ID,
        records=records,
    ) is True
    assert replay.inserts == []

    conflicting = [dict(row) for row in existing]
    conflicting[0]["value_num"] = 999.0
    with pytest.raises(QELongTrendResultRepositoryError, match="metric rows conflict"):
        QELongTrendEvaluationResultRepository._write_or_verify_metrics(
            _RowsCursor(conflicting),
            evaluation_id=EVALUATION_ID,
            records=records,
        )


def test_schema_readiness_checks_both_phase3_tables() -> None:
    required = {
        f"qe_archive.{table}": columns
        for table, columns in _phase3_schema_columns().items()
    }
    selected_table = ""

    def handler(sql, params):  # type: ignore[no-untyped-def]
        nonlocal selected_table
        if "FROM qe_archive.schema_version" in sql:
            return (1,)
        if "SELECT to_regclass" in sql:
            selected_table = params[0]
            return (selected_table,)
        if "information_schema.columns" in sql:
            return required[selected_table]
        raise AssertionError(sql)

    connection = _ScriptedConnection(handler)
    QELongTrendEvaluationResultRepository(connection_provider=lambda: connection).ensure_schema_ready()
    assert len(connection.cursor_value.calls) == 5


def test_schema_readiness_rejects_missing_version_and_incompatible_column_contract() -> None:
    missing_version = _ScriptedConnection(
        lambda sql, _params: None
        if "FROM qe_archive.schema_version" in sql
        else (_ for _ in ()).throw(AssertionError(sql))
    )
    with pytest.raises(QELongTrendResultRepositoryError, match="is not registered"):
        QELongTrendEvaluationResultRepository(
            connection_provider=lambda: missing_version
        ).ensure_schema_ready()

    columns = _phase3_schema_columns()
    columns["run_evaluation_metric"][0] = ("evaluation_metric_id", "int4", "NO")

    def incompatible_handler(sql, params):  # type: ignore[no-untyped-def]
        if "FROM qe_archive.schema_version" in sql:
            return (1,)
        if "SELECT to_regclass" in sql:
            return (params[0],)
        if "information_schema.columns" in sql:
            return columns[params[1]]
        raise AssertionError(sql)

    incompatible = _ScriptedConnection(incompatible_handler)
    with pytest.raises(QELongTrendResultRepositoryError, match="missing or incompatible"):
        QELongTrendEvaluationResultRepository(
            connection_provider=lambda: incompatible
        ).ensure_schema_ready()


def test_schema_inspection_does_not_misclassify_database_outage() -> None:
    class BrokenConnection:
        def __enter__(self):
            raise OSError("database offline")

        def __exit__(self, *_args):
            return False

    repository = QELongTrendEvaluationResultRepository(connection_provider=lambda: BrokenConnection())
    with pytest.raises(QELongTrendResultPersistenceUnavailable) as exc_info:
        repository.ensure_schema_ready()
    assert exc_info.value.reason_code == "QELT_RESULT_PERSISTENCE_UNAVAILABLE"


def test_quality_query_rejects_unknown_dimensions_before_database_access() -> None:
    repository = QELongTrendEvaluationResultRepository(
        connection_provider=lambda: (_ for _ in ()).throw(AssertionError("database must not be accessed"))
    )
    with pytest.raises(QELongTrendResultQueryError):
        repository.query_quality(exit_execution_status="filled")
    with pytest.raises(QELongTrendResultQueryError):
        repository.query_quality(
            cursor=_encode_cursor(
                datetime(2026, 6, 30, tzinfo=timezone.utc),
                EVALUATION_ID,
                "rank_ic",
                "a" * 64,
            )
        )


def test_materializable_candidate_lookup_is_exact_bounded_and_cas_published() -> None:
    schema_columns = _phase3_schema_columns()
    expected = {
        "evaluation_id": EVALUATION_ID,
        "run_id": "run-1",
        "parent_task_id": "task-1",
        "parent_loop_index": 4,
        "profile_id": "qe_long_trend_v1",
        "outcome_dataset_snapshot_id": "outcome-1",
        "status": "partial",
    }

    def handler(sql, params):  # type: ignore[no-untyped-def]
        if "FROM qe_archive.schema_version" in sql:
            return (1,)
        if "SELECT to_regclass" in sql:
            return (params[0],)
        if "information_schema.columns" in sql:
            return schema_columns[params[1]]
        if "platform_delivery_status_json->>'cas' = 'published'" in sql:
            assert params == ("run-1", "task-1", 4, "qe_long_trend_v1", "outcome-1")
            assert "evaluation_type = 'long_trend'" in sql
            assert "worker_terminal_sha256 IS NOT NULL" in sql
            assert "artifact_manifest_sha256 IS NOT NULL" in sql
            assert "DENSE_RANK() OVER" in sql
            assert "WHEN 'succeeded' THEN 0" in sql
            assert "WHEN 'partial' THEN 1" in sql
            assert "WHEN 'failed' THEN 2" in sql
            assert "WHEN 'cancelled' THEN 3" in sql
            assert "WHERE status_priority_rank = 1" in sql
            assert "LIMIT 2" in sql
            return [expected]
        raise AssertionError(sql)

    repository = QELongTrendEvaluationResultRepository(
        connection_provider=lambda: _ScriptedConnection(handler)
    )
    candidates = repository.find_materializable_candidates(
        run_id="run-1",
        task_id="task-1",
        loop_index=4,
        profile_id="qe_long_trend_v1",
        outcome_dataset_snapshot_id="outcome-1",
    )

    assert candidates == (expected,)


def test_repository_get_list_and_quality_queries_are_bounded_and_keyset_stable() -> None:
    now = datetime(2026, 7, 28, tzinfo=timezone.utc)
    metric_rows = [
        {"metric_key": "rank_ic", "dimension_key": "a" * 64},
        {"metric_key": "rank_ic", "dimension_key": "b" * 64},
    ]
    evaluation_rows = [
        {"evaluation_id": EVALUATION_ID, "created_at": now},
        {"evaluation_id": "qelt_" + "b" * 64, "created_at": now},
    ]
    quality_rows = [
        {
            "evaluation_id": EVALUATION_ID,
            "evaluation_asof_sort": date(2026, 6, 30),
            "metric_key": "rank_ic",
            "dimension_key": "a" * 64,
        },
        {
            "evaluation_id": "qelt_" + "b" * 64,
            "evaluation_asof_sort": date(2026, 6, 30),
            "metric_key": "rank_ic",
            "dimension_key": "b" * 64,
        },
    ]
    schema_columns = _phase3_schema_columns()

    def handler(sql, params):  # type: ignore[no-untyped-def]
        if "FROM qe_archive.schema_version" in sql:
            return (1,)
        if "SELECT to_regclass" in sql:
            return (params[0],)
        if "information_schema.columns" in sql:
            return schema_columns[params[1]]
        if "FROM qe_archive.run_evaluation WHERE evaluation_id" in sql:
            return {"evaluation_id": EVALUATION_ID, "status": "partial"}
        if "FROM qe_archive.run_evaluation_metric" in sql and "JOIN" not in sql:
            return metric_rows
        if "FROM qe_archive.run_evaluation_artifact" in sql:
            return [{"artifact_type": "artifact_manifest", "sha256": "c" * 64}]
        if "JOIN qe_archive.run_evaluation_metric" in sql:
            assert "ORDER BY evaluation_asof_sort DESC" in sql
            if "filled_t1" in params:
                assert "entry_status_counts" in sql
                assert "exit_status_counts" in sql
                assert params.count("filled_t1") == 2
                assert params.count("filled_on_exit_signal_day") == 2
            return quality_rows
        if "FROM qe_archive.run_evaluation" in sql and "parent_task_id" in sql:
            return evaluation_rows
        raise AssertionError(sql)

    connection = _ScriptedConnection(handler)
    repository = QELongTrendEvaluationResultRepository(connection_provider=lambda: connection)
    detail = repository.get_evaluation(EVALUATION_ID, metric_limit=1)
    assert detail is not None
    assert len(detail["metrics"]) == 1
    assert detail["metric_next_cursor"] is not None
    assert detail["artifacts"][0]["artifact_type"] == "artifact_manifest"
    control_query = next(
        sql
        for sql, _params in connection.cursor_value.calls
        if "FROM qe_archive.run_evaluation" in sql and "WHERE evaluation_id = %s" in sql
    )
    assert "SELECT *" not in control_query
    assert "request_json" not in control_query
    assert "owner_id" not in control_query
    assert "lease_expires_at" not in control_query
    assert "FOR SHARE" in control_query

    listed = repository.list_evaluations(task_id="task-1", loop_index=3, limit=1)
    assert len(listed["items"]) == 1
    assert listed["next_cursor"] is not None

    quality = repository.query_quality(
        evaluation_id=EVALUATION_ID,
        run_id="run-1",
        task_id="task-1",
        loop_index=3,
        model_type="LGBM",
        label_horizon=60,
        evaluation_asof=date(2026, 6, 30),
        outcome_dataset_snapshot_id="outcome-1",
        horizon=60,
        sector_code="801010",
        family_status="COMPUTED",
        entry_execution_status="filled_t1",
        exit_execution_status="filled_on_exit_signal_day",
        limit=1,
    )
    assert len(quality["items"]) == 1
    assert "evaluation_asof_sort" not in quality["items"][0]
    assert quality["next_cursor"] is not None

    cursor = _encode_cursor(date(2026, 6, 30), EVALUATION_ID, "rank_ic", "a" * 64)
    repository.query_quality(limit=1, cursor=cursor)


def test_persist_published_receipt_is_atomic_and_advances_control_row(tmp_path: Path) -> None:
    terminal, manifest, published_meta = _published_assets(tmp_path)
    lease = QELongTrendControlLease(
        evaluation_id=EVALUATION_ID,
        owner_id="owner",
        fencing_token=7,
        row_version=11,
    )
    control = {
        "evaluation_id": EVALUATION_ID,
        "run_id": "run-1",
        "parent_task_id": "task-1",
        "parent_loop_index": 3,
        "evaluation_type": "long_trend",
        "qe_dataset_contract_id": QE_DATASET_CONTRACT_ID,
        "owner_id": "owner",
        "fencing_token": 7,
        "row_version": 11,
        "status": "collecting",
        "platform_delivery_status_json": {"cas": "published"},
        "worker_terminal_sha256": None,
        "artifact_manifest_sha256": None,
    }
    updated = {**control, "row_version": 12, "platform_delivery_status_json": {}}
    required_columns = _phase3_schema_columns()

    def schema_handler(sql, params):  # type: ignore[no-untyped-def]
        if "FROM qe_archive.schema_version" in sql:
            return (1,)
        if "SELECT to_regclass" in sql:
            return (params[0],)
        if "information_schema.columns" in sql:
            return required_columns[params[1]]
        raise AssertionError(sql)

    def transaction_handler(sql, params):  # type: ignore[no-untyped-def]
        if "SELECT * FROM qe_archive.run_evaluation" in sql:
            return control
        if "FROM qe_archive.run" in sql and "FOR SHARE" in sql:
            return {
                "source_system": "qe_evolution",
                "run_type": "evolution_loop",
                "task_id": "task-1",
                "loop_index": 3,
            }
        if sql.startswith("SELECT metric_key") or sql.startswith("SELECT artifact_type"):
            return []
        if sql.startswith("INSERT INTO"):
            return None
        if sql.startswith("UPDATE qe_archive.run_evaluation"):
            updated["platform_delivery_status_json"] = params[0].adapted
            return updated
        raise AssertionError(sql)

    schema_connection = _ScriptedConnection(schema_handler)
    transaction_connection = _ScriptedConnection(transaction_handler)
    connections = iter([schema_connection, transaction_connection])
    repository = QELongTrendEvaluationResultRepository(connection_provider=lambda: next(connections))

    receipt = repository.persist_published_receipt(
        evaluation_id=EVALUATION_ID,
        worker_terminal=terminal,
        manifest=manifest,
        published_meta=published_meta,
        lease=lease,
    )

    assert receipt.metric_count == 2
    assert receipt.artifact_count == 4
    assert receipt.replayed is False
    assert receipt.control_row["row_version"] == 12
    assert receipt.control_row["platform_delivery_status_json"]["db"] == "published"
    assert transaction_connection.commits == 1
