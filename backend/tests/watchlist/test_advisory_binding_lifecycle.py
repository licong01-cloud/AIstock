from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from backend.services.advisory_program import (
    PACKAGE_MODE_SINGLE,
    REVIEW_RUN_STATUS_FAILED,
    REVIEW_RUN_TYPE_RUN,
    AdvisoryProgramPGRepository,
    AdvisoryProgramService,
    AdvisoryReviewRun,
    InMemoryAdvisoryProgramRepository,
    REASON_BINDING_EFFECTIVE_DATE_IN_PAST,
    REASON_BINDING_EXPECTED_VERSION_CONFLICT,
    REASON_BINDING_INTERVAL_OVERLAP,
    REASON_LEGACY_NULL_BINDING_RESEARCH_ONLY,
    binding_to_dict,
)
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError


class _Calendar:
    def __init__(self, trading_days: list[date]) -> None:
        self._trading_days = trading_days

    def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
        return [item for item in self._trading_days if start_date <= item <= end_date]

    def next_trading_day(self, anchor_date: date, *, inclusive: bool = False) -> date:
        floor = anchor_date if inclusive else anchor_date + timedelta(days=1)
        return next(item for item in self._trading_days if item >= floor)


def _service() -> tuple[AdvisoryProgramService, InMemoryAdvisoryProgramRepository]:
    repository = InMemoryAdvisoryProgramRepository()
    service = AdvisoryProgramService(
        repository=repository,
        selection_service=None,
        calendar_provider=_Calendar([date(2026, 6, day) for day in range(1, 10)]),
        now_provider=lambda: datetime(2026, 5, 29, 20, 0, tzinfo=UTC),
    )
    return service, repository


def _program(service: AdvisoryProgramService):
    return service.create_program(
        program_name="Binding lifecycle",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
        target_count=5,
    )


class _RecordingPgCursor:
    def __init__(self, active_binding, *, fail_binding_insert: bool = False, latest_acquired_date: date | None = None) -> None:
        self._active_binding = active_binding
        self._fail_binding_insert = fail_binding_insert
        self._latest_acquired_date = latest_acquired_date
        self._one = None
        self._many = []
        self.rowcount = 1
        self.statements: list[str] = []
        self.retired_binding_payload = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False

    def execute(self, statement: str, params=None) -> None:
        normalized = " ".join(statement.split())
        self.statements.append(normalized)
        if normalized.startswith("SELECT version FROM app.advisory_program"):
            self._one = {"version": self._active_binding.program_version}
        elif normalized.startswith("SELECT program_id FROM app.advisory_program"):
            self._one = {"program_id": self._active_binding.program_id}
        elif normalized.startswith("SELECT * FROM app.advisory_strategy_binding_version"):
            self._many = [binding_to_dict(self._active_binding)]
        elif normalized.startswith("SELECT MAX(trade_date) AS latest_trade_date"):
            self._one = {"latest_trade_date": self._latest_acquired_date}
        elif normalized.startswith("UPDATE app.advisory_strategy_binding_version"):
            self.retired_binding_payload = params[2].adapted
        elif self._fail_binding_insert and normalized.startswith("INSERT INTO app.advisory_strategy_binding_version"):
            raise RuntimeError("simulated binding insert failure")

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class _RecordingPgConnection:
    def __init__(
        self,
        active_binding,
        *,
        fail_binding_insert: bool = False,
        latest_acquired_date: date | None = None,
    ) -> None:
        self.cursor_instance = _RecordingPgCursor(
            active_binding,
            fail_binding_insert=fail_binding_insert,
            latest_acquired_date=latest_acquired_date,
        )
        self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        self.rolled_back = exc_type is not None
        return False

    def cursor(self, *args, **kwargs):
        return self.cursor_instance


def test_create_and_apply_binding_use_dated_left_closed_right_open_intervals() -> None:
    service, repository = _service()
    program = _program(service)
    initial = repository.get_active_binding_version(program.program_id)
    assert initial is not None
    assert initial.effective_from_trade_date == date(2026, 6, 1)

    repository.create_review_run(
        AdvisoryReviewRun(
            review_run_id="run_failed_but_acquired",
            program_id=program.program_id,
            binding_version_id=initial.binding_version_id,
            trade_date=date(2026, 6, 1),
            run_type=REVIEW_RUN_TYPE_RUN,
            status=REVIEW_RUN_STATUS_FAILED,
            data_source="DB_HISTORICAL",
        )
    )
    defaults = service.binding_defaults(program.program_id)
    assert defaults["effective_from_trade_date"] == "2026-06-02"
    assert defaults["binding_interval_semantics"] == "LEFT_CLOSED_RIGHT_OPEN"

    result = service.apply_binding(
        program.program_id,
        binding={"package_mode": PACKAGE_MODE_SINGLE, "package_ids": ["pkg_b"], "target_count": 5},
        activation_reason="replace package identity prospectively",
        expected_program_version=defaults["expected_program_version"],
        expected_binding_version_id=defaults["expected_binding_version_id"],
    )

    successor = result["binding"]
    assert successor["effective_from_trade_date"] == "2026-06-02"
    assert successor["binding_interval_semantics"] == "LEFT_CLOSED_RIGHT_OPEN"
    retired = next(row for row in repository.list_binding_versions(program.program_id) if row.binding_version_id == initial.binding_version_id)
    assert retired.effective_to_trade_date == date(2026, 6, 2)


def test_apply_binding_rejects_retroactive_date_and_stale_versions() -> None:
    service, repository = _service()
    program = _program(service)
    initial = repository.get_active_binding_version(program.program_id)
    assert initial is not None

    with pytest.raises(RuntimeConfigInvalidError) as date_error:
        service.apply_binding(
            program.program_id,
            binding={"package_mode": PACKAGE_MODE_SINGLE, "package_ids": ["pkg_b"], "target_count": 5},
            activation_reason="must not backdate",
            effective_from_trade_date=date(2026, 6, 1),
            expected_program_version=program.version,
            expected_binding_version_id=initial.binding_version_id,
        )
    assert date_error.value.context["reason_code"] == REASON_BINDING_EFFECTIVE_DATE_IN_PAST

    with pytest.raises(RuntimeConfigInvalidError) as version_error:
        service.apply_binding(
            program.program_id,
            binding={"package_mode": PACKAGE_MODE_SINGLE, "package_ids": ["pkg_b"], "target_count": 5},
            activation_reason="must detect concurrent replacement",
            expected_program_version=program.version + 1,
            expected_binding_version_id=initial.binding_version_id,
        )
    assert version_error.value.context["reason_code"] == REASON_BINDING_EXPECTED_VERSION_CONFLICT


def test_nonsemantic_update_keeps_binding_but_package_update_creates_successor() -> None:
    service, repository = _service()
    program = _program(service)
    initial = repository.get_active_binding_version(program.program_id)
    assert initial is not None

    renamed = service.update_program(program.program_id, {"program_name": "Renamed only"})
    assert len(repository.list_binding_versions(program.program_id)) == 1
    preview = service.run_review(program.program_id, trade_date=date(2026, 6, 1), candidates=[], preview=True)
    assert preview.program.version == renamed.version

    updated = service.update_program(
        program.program_id,
        {
            "package_ids": ["pkg_b"],
            "expected_program_version": renamed.version,
            "expected_binding_version_id": initial.binding_version_id,
        },
    )
    bindings = repository.list_binding_versions(program.program_id)
    assert updated.version == renamed.version + 1
    assert len(bindings) == 2
    assert next(item for item in bindings if item.activation_status == "ACTIVE").package_ids == ["pkg_b"]


def test_legacy_nonsemantic_update_fails_before_program_write() -> None:
    service, repository = _service()
    program = _program(service)
    active = repository.get_active_binding_version(program.program_id)
    assert active is not None
    repository.binding_versions = [replace(active, effective_from_trade_date=None)]

    with pytest.raises(DataUnavailableError) as excinfo:
        service.update_program(program.program_id, {"program_name": "must not be persisted"})

    assert excinfo.value.context["reason_code"] == REASON_LEGACY_NULL_BINDING_RESEARCH_ONLY
    assert repository.get_program(program.program_id).program_name == program.program_name


def test_apply_clone_and_legacy_repair_preserve_runtime_when_omitted() -> None:
    service, repository = _service()
    program = _program(service)
    active = repository.get_active_binding_version(program.program_id)
    assert active is not None
    runtime_config = {"runtime_profile": {"id": "profile_a", "version": "v1"}, "hmm": {"enabled": True}}
    repository.binding_versions = [replace(active, runtime_config_json=runtime_config)]

    defaults = service.binding_defaults(program.program_id)
    applied = service.apply_binding(
        program.program_id,
        binding={"package_mode": PACKAGE_MODE_SINGLE, "package_ids": ["pkg_b"], "target_count": 5},
        activation_reason="preserve runtime config",
        expected_program_version=defaults["expected_program_version"],
        expected_binding_version_id=defaults["expected_binding_version_id"],
    )
    assert applied["binding"]["runtime_config_json"] == runtime_config

    cloned = service.clone_program(program.program_id, program_name="Runtime clone")
    cloned_binding = repository.get_active_binding_version(cloned.program_id)
    assert cloned_binding is not None
    assert cloned_binding.runtime_config_json == runtime_config

    legacy_program = _program(service)
    legacy_active = repository.get_active_binding_version(legacy_program.program_id)
    assert legacy_active is not None
    repository.binding_versions = [
        row
        if row.binding_version_id != legacy_active.binding_version_id
        else replace(row, effective_from_trade_date=None, runtime_config_json=runtime_config)
        for row in repository.binding_versions
    ]
    repaired = service.repair_legacy_binding(
        legacy_program.program_id,
        binding={"package_mode": PACKAGE_MODE_SINGLE, "package_ids": ["pkg_b"], "target_count": 5},
        repair_reason="preserve legacy runtime config",
        expected_program_version=legacy_program.version,
        expected_binding_version_id=legacy_active.binding_version_id,
    )
    assert repaired["binding"]["runtime_config_json"] == runtime_config


def test_direct_active_binding_write_is_rejected() -> None:
    service, repository = _service()
    program = _program(service)
    active = repository.get_active_binding_version(program.program_id)
    assert active is not None

    with pytest.raises(RuntimeConfigInvalidError) as excinfo:
        repository.activate_binding_version(replace(active, binding_version_id="direct_active_write"))

    assert excinfo.value.context["reason_code"] == REASON_BINDING_EXPECTED_VERSION_CONFLICT


def test_multiple_or_closed_active_bindings_fail_loudly() -> None:
    service, repository = _service()
    program = _program(service)
    active = repository.get_active_binding_version(program.program_id)
    assert active is not None
    repository.binding_versions.append(replace(active, binding_version_id="duplicate_active_binding"))

    with pytest.raises(RuntimeConfigInvalidError) as duplicate_error:
        service.active_binding(program.program_id)
    assert duplicate_error.value.context["reason_code"] == REASON_BINDING_INTERVAL_OVERLAP

    repository.binding_versions = [replace(active, effective_to_trade_date=date(2026, 6, 2))]
    with pytest.raises(RuntimeConfigInvalidError) as closed_error:
        service.active_binding(program.program_id)
    assert closed_error.value.context["reason_code"] == REASON_BINDING_INTERVAL_OVERLAP


def test_postgres_binding_replace_locks_before_mutation_and_surfaces_insert_failure() -> None:
    service, repository = _service()
    program = _program(service)
    active = repository.get_active_binding_version(program.program_id)
    assert active is not None
    updated = replace(
        program,
        version=program.version + 1,
        package_ids=["pkg_b"],
        package_weights={"pkg_b": 1.0},
    )
    successor = replace(
        active,
        binding_version_id="binding_successor",
        program_version=updated.version,
        package_ids=["pkg_b"],
        package_weights={"pkg_b": 1.0},
        effective_from_trade_date=date(2026, 6, 2),
        effective_to_trade_date=None,
    )

    connection = _RecordingPgConnection(active)
    pg_repository = AdvisoryProgramPGRepository(conn_factory=lambda: connection)
    pg_repository.replace_program_binding(
        updated,
        successor,
        expected_program_version=program.version,
        expected_binding_version_id=active.binding_version_id,
    )
    statements = connection.cursor_instance.statements
    lock_program = next(index for index, statement in enumerate(statements) if "FROM app.advisory_program" in statement and statement.endswith("FOR UPDATE"))
    lock_bindings = next(index for index, statement in enumerate(statements) if "FROM app.advisory_strategy_binding_version" in statement and statement.endswith("FOR UPDATE"))
    update_program = next(index for index, statement in enumerate(statements) if statement.startswith("UPDATE app.advisory_program"))
    retire_binding = next(index for index, statement in enumerate(statements) if statement.startswith("UPDATE app.advisory_strategy_binding_version"))
    insert_successor = next(index for index, statement in enumerate(statements) if statement.startswith("INSERT INTO app.advisory_strategy_binding_version"))
    assert lock_program < lock_bindings < update_program < retire_binding < insert_successor
    assert connection.cursor_instance.retired_binding_payload["activation_status"] == "RETIRED"
    assert connection.cursor_instance.retired_binding_payload["effective_to_trade_date"] == "2026-06-02"

    failed_connection = _RecordingPgConnection(active, fail_binding_insert=True)
    failing_repository = AdvisoryProgramPGRepository(conn_factory=lambda: failed_connection)
    with pytest.raises(RuntimeError, match="simulated binding insert failure"):
        failing_repository.replace_program_binding(
            updated,
            successor,
            expected_program_version=program.version,
            expected_binding_version_id=active.binding_version_id,
        )
    assert failed_connection.rolled_back is True


def test_postgres_binding_replace_rechecks_acquired_date_inside_transaction() -> None:
    service, repository = _service()
    program = _program(service)
    active = repository.get_active_binding_version(program.program_id)
    assert active is not None
    updated = replace(program, version=program.version + 1, package_ids=["pkg_b"], package_weights={"pkg_b": 1.0})
    successor = replace(
        active,
        binding_version_id="binding_racing_successor",
        program_version=updated.version,
        package_ids=["pkg_b"],
        package_weights={"pkg_b": 1.0},
        effective_from_trade_date=date(2026, 6, 2),
    )
    connection = _RecordingPgConnection(active, latest_acquired_date=date(2026, 6, 2))
    pg_repository = AdvisoryProgramPGRepository(conn_factory=lambda: connection)

    with pytest.raises(RuntimeConfigInvalidError) as excinfo:
        pg_repository.replace_program_binding(
            updated,
            successor,
            expected_program_version=program.version,
            expected_binding_version_id=active.binding_version_id,
        )

    assert excinfo.value.context["reason_code"] == REASON_BINDING_EFFECTIVE_DATE_IN_PAST
    assert not any(statement.startswith("UPDATE app.advisory_program") for statement in connection.cursor_instance.statements)


def test_postgres_formal_run_locks_program_and_validates_binding_interval() -> None:
    service, repository = _service()
    program = _program(service)
    active = repository.get_active_binding_version(program.program_id)
    assert active is not None
    connection = _RecordingPgConnection(active)
    pg_repository = AdvisoryProgramPGRepository(conn_factory=lambda: connection)
    review_run = AdvisoryReviewRun(
        review_run_id="formal_run_locked",
        program_id=program.program_id,
        binding_version_id=active.binding_version_id,
        trade_date=date(2026, 6, 1),
        run_type=REVIEW_RUN_TYPE_RUN,
        status=REVIEW_RUN_STATUS_FAILED,
        data_source="DB_HISTORICAL",
    )

    pg_repository.create_review_run(review_run)
    statements = connection.cursor_instance.statements
    lock_program = next(index for index, statement in enumerate(statements) if statement.startswith("SELECT program_id") and statement.endswith("FOR UPDATE"))
    lock_bindings = next(index for index, statement in enumerate(statements) if statement.startswith("SELECT * FROM app.advisory_strategy_binding_version"))
    insert_run = next(index for index, statement in enumerate(statements) if statement.startswith("INSERT INTO app.advisory_review_run"))
    assert lock_program < lock_bindings < insert_run

    mismatched = replace(review_run, review_run_id="formal_run_mismatch", binding_version_id="stale_binding")
    with pytest.raises(RuntimeConfigInvalidError) as excinfo:
        pg_repository.create_review_run(mismatched)
    assert excinfo.value.context["reason_code"] == REASON_BINDING_EXPECTED_VERSION_CONFLICT


def test_legacy_null_binding_fails_loud_without_backfill() -> None:
    service, repository = _service()
    program = _program(service)
    initial = repository.get_active_binding_version(program.program_id)
    assert initial is not None
    repository.binding_versions = [replace(initial, effective_from_trade_date=None)]

    with pytest.raises(DataUnavailableError) as excinfo:
        service.active_binding(program.program_id)

    assert excinfo.value.context["reason_code"] == REASON_LEGACY_NULL_BINDING_RESEARCH_ONLY

    repaired = service.repair_legacy_binding(
        program.program_id,
        binding={"package_mode": PACKAGE_MODE_SINGLE, "package_ids": ["pkg_b"], "target_count": 5},
        repair_reason="create prospective dated successor for legacy binding",
        expected_program_version=program.version,
        expected_binding_version_id=initial.binding_version_id,
    )
    assert repaired["binding"]["effective_from_trade_date"] == "2026-06-01"
    legacy = next(item for item in repository.list_binding_versions(program.program_id) if item.binding_version_id == initial.binding_version_id)
    assert legacy.effective_from_trade_date is None
    assert legacy.activation_status == "RETIRED"
