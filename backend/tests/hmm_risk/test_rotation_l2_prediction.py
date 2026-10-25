from __future__ import annotations

import uuid
from datetime import date

import pytest

from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.rotation_l2 import ACCEPTANCE_SCHEMA
from backend.services.hmm_risk.rotation_l2_prediction import (
    PREDICTION_COLUMNS,
    RotationL2PredictionRepository,
    RotationL2PredictionError,
    _database_parameter,
    _validate_row,
    rows_from_acceptance,
)


class _Cursor:
    def __init__(self) -> None:
        self.inserted: list[tuple] = []
        self._rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, sql: str, params: tuple | None = None) -> None:
        normalized = " ".join(sql.split()).lower()
        if normalized.startswith("insert into hmm_risk.rotation_l2_prediction"):
            assert params is not None
            self.inserted.append(params)
            return
        if "from hmm_risk.rotation_l2_prediction where run_id=%s order by" in normalized:
            self._rows = list(self.inserted)
            return
        raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchall(self) -> list[tuple]:
        return self._rows


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def cursor(self) -> _Cursor:
        return self._cursor


def _acceptance() -> dict:
    predictions = []
    for index in range(131):
        score = index / 130.0 - 0.5
        predictions.append(
            {
                "trade_date": "2026-03-31",
                "as_of_date": "2026-03-30",
                "sector_code": f"801{index:03d}.SI",
                "sector_name": f"801{index:03d}.SI",
                "rotation_score": score,
                "forecast_state": "fading" if index < 27 else "trending" if index >= 104 else "neutral",
                "feature_contributions": {"moneyflow_intensity_delta_5d_rank": score},
                "availability": "available",
                "reason_code": None,
                "structural_eligible": True,
                "feature_eligible": True,
                "outcome_status": "outcome_not_mature",
            }
        )
    acceptance = {
        "schema_version": ACCEPTANCE_SCHEMA,
        "run_id": "a" * 64,
        "model_hash": "b" * 64,
        "evaluation_contract_hash": "c" * 64,
        "input_hash": "d" * 64,
        "mapping_hash": "e" * 64,
        "quote_authority_hash": "f" * 64,
        "planned_fits": 0,
        "completed_fits": 0,
        "tail_accessed": False,
        "predictions": predictions,
        "metrics": {"overall": {"mean_daily_rank_ic": 0.03}},
        "execution_status": "COMPLETED",
        "effect_status": "DEVELOPMENT_EFFECT_QUALIFIED",
        "rotation_l2_capability_status": "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
        "forward_power_status": "UNAVAILABLE",
        "forward_confirmation": "NOT_STARTED",
        "advisory_status": "NOT_AVAILABLE",
        "validation_basis": "HISTORICAL_CAUSAL_REPLAY_ZERO_FIT",
    }
    acceptance["acceptance_sha256"] = canonical_sha256(acceptance)
    return acceptance


def test_acceptance_maps_to_complete_immutable_l2_rows() -> None:
    rows = rows_from_acceptance(_acceptance())

    assert len(rows) == 131
    assert len({row["sector_code"] for row in rows}) == 131
    assert all(row["sector_level"] == "L2" and row["research_surface_status"] == "NOT_AVAILABLE" for row in rows)
    assert all(row["revision"] == 1 and row["supersedes_prediction_id"] is None for row in rows)


def test_acceptance_hash_drift_fails_closed() -> None:
    acceptance = _acceptance()
    acceptance["effect_status"] = "BELOW_BINDING_MBE"

    with pytest.raises(RotationL2PredictionError, match="acceptance receipt is invalid"):
        rows_from_acceptance(acceptance)


def test_missing_catalog_row_fails_closed() -> None:
    acceptance = _acceptance()
    acceptance["predictions"].pop()
    acceptance["acceptance_sha256"] = canonical_sha256(
        {key: value for key, value in acceptance.items() if key != "acceptance_sha256"}
    )

    with pytest.raises(RotationL2PredictionError, match="does not contain 131"):
        rows_from_acceptance(acceptance)


def test_revision_requires_a_direct_supersedes_identity() -> None:
    row = rows_from_acceptance(_acceptance())[0]
    row["revision"] = 2
    row["prediction_id"] = None

    with pytest.raises(RotationL2PredictionError, match="revision lineage differs"):
        _validate_row(row)


def test_writer_rejects_contribution_or_zero_fit_summary_drift() -> None:
    row = rows_from_acceptance(_acceptance())[0]
    row["feature_contributions"] = {"moneyflow_intensity_delta_5d_rank": 0.0}
    with pytest.raises(RotationL2PredictionError, match="contribution differs"):
        _validate_row(row)

    row = rows_from_acceptance(_acceptance())[0]
    row["run_summary"] = {**row["run_summary"], "tail_accessed": True}
    with pytest.raises(RotationL2PredictionError, match="zero-fit no-tail"):
        _validate_row(row)


def test_writer_rejects_effect_and_capability_drift() -> None:
    row = rows_from_acceptance(_acceptance())[0]
    row["rotation_l2_capability_status"] = "NOT_AVAILABLE"

    with pytest.raises(RotationL2PredictionError, match="effect and L2 capability"):
        _validate_row(row)


@pytest.mark.parametrize("version", [None, "unknown", "hmm_risk_l2_postcalibration_effect_v1"])
def test_delta_contract_cannot_be_relabelled_as_an_explicit_hmm_version(version):
    row = rows_from_acceptance(_acceptance())[0]
    row["run_summary"] = {**row["run_summary"], "contract_version": version}
    with pytest.raises(RotationL2PredictionError):
        _validate_row(row)


def test_writer_serializes_uuid_parameters_for_plain_psycopg2_connections() -> None:
    cursor = _Cursor()
    repository = RotationL2PredictionRepository(conn_factory=lambda: _Connection(cursor))

    result = repository.write_rows(rows_from_acceptance(_acceptance()))

    prediction_id_index = PREDICTION_COLUMNS.index("prediction_id")
    supersedes_id_index = PREDICTION_COLUMNS.index("supersedes_prediction_id")
    assert result["row_count"] == 131
    assert all(isinstance(values[prediction_id_index], str) for values in cursor.inserted)
    assert all(values[supersedes_id_index] is None for values in cursor.inserted)


def test_database_parameter_serializes_both_uuid_columns_only() -> None:
    value = uuid.uuid4()

    assert _database_parameter("prediction_id", value) == str(value)
    assert _database_parameter("supersedes_prediction_id", value) == str(value)
    assert _database_parameter("run_id", value) is value
    assert _database_parameter("supersedes_prediction_id", None) is None


class _OverviewCursor(_Cursor):
    def __init__(self, dates: list[date]) -> None:
        super().__init__()
        self.dates = dates

    def execute(self, sql: str, params: tuple | None = None) -> None:
        normalized = " ".join(sql.split()).lower()
        assert params == ("a" * 64,)
        if normalized.startswith("select max(trade_date)"):
            self._rows = [(max(self.dates) if self.dates else None,)]
        elif normalized.startswith("select distinct trade_date"):
            assert "order by trade_date" in normalized
            self._rows = [(value,) for value in self.dates]
        else:
            raise AssertionError(f"unexpected overview SQL: {normalized}")

    def fetchone(self):
        return self._rows[0] if self._rows else None


def test_overview_exposes_run_scoped_historical_dates_without_recomputing(monkeypatch) -> None:
    dates = [date(2026, 3, 30), date(2026, 3, 31)]
    cursor = _OverviewCursor(dates)
    repository = RotationL2PredictionRepository(conn_factory=lambda: _Connection(cursor))
    called = []

    def read_date(trade_date, *, run_id):
        called.append((trade_date, run_id))
        return {"rows": rows_from_acceptance(_acceptance()), "trade_date": trade_date.isoformat()}

    monkeypatch.setattr(repository, "read_date", read_date)
    overview = repository.overview(run_id="a" * 64)
    assert overview["available_trade_dates"] == ["2026-03-30", "2026-03-31"]
    assert overview["trade_date"] == "2026-03-31"
    assert called == [(dates[-1], "a" * 64)]


@pytest.mark.parametrize("dates", [[], [date(2026, 3, 31), date(2026, 3, 30)], [date(2026, 3, 31)] * 2])
def test_overview_rejects_empty_or_corrupt_date_catalog(dates) -> None:
    repository = RotationL2PredictionRepository(conn_factory=lambda: _Connection(_OverviewCursor(dates)))
    with pytest.raises(RotationL2PredictionError):
        repository.overview(run_id="a" * 64)
