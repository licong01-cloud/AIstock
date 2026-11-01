"""Late data must preserve the original cohort across midnight and the open cutoff.

All packets below are isolated synthetic fixtures. No live model, database, market
outcome, training, or production scheduler is used.
"""
import pandas as pd
import pytest

from backend.services.advisory_model_first import lstm_spo_shadow_v1 as app
from backend.services.data_integrity.inputs import InputReadinessError
from backend.tests.advisory_model_first.test_lstm_spo_shadow_v1 import trial as trial


def formal_cohort(trial):
    trial.publication.update(decision_date="2026-11-02", target_date="2026-11-03",
                             created_at="2026-11-02T08:45:00+00:00")
    trial.rows[app.KEY[0]] = pd.Timestamp("2026-11-02")
    trial.rows[app.KEY[1]] = pd.Timestamp("2026-11-03")
    trial.now[0] = app.clock("2026-11-02", 20, 55)
    trial.path = trial.service.root / "days/2026-11-02/prediction.json"


def restarted(trial):
    return app.LstmSpoShadowService(root=trial.service.root.parent,
        source=trial.source, now=lambda: trial.now[0])


@pytest.mark.parametrize("ready_at", [
    ("2026-11-02", 21, 5), ("2026-11-03", 0, 15), ("2026-11-03", 9, 29),
])
@pytest.mark.parametrize("waiting_stage", ["read", "post_compute"])
def test_late_data_and_restart_keep_original_cohort_before_target_open(trial, ready_at, waiting_stage):
    formal_cohort(trial)
    ready = app.clock(*ready_at)

    def check_ready():
        if trial.now[0] < ready:
            raise InputReadinessError("actual consumed input revision is not ready")

    def inputs(_publication):
        if waiting_stage == "read":
            check_ready()
        return trial.rows, {"revision": 7}

    trial.source.inputs = inputs
    trial.source.verify_capture = lambda _receipt: check_ready()
    waiting = trial.service.run_once()
    assert waiting["results"][0]["status"] == "WAITING_DATA"
    assert not trial.path.exists()
    assert trial.service.status(program_id=app.PROGRAM)["last_waiting"] is not None

    trial.now[0] = ready
    service = restarted(trial)
    assert service.run_once()["results"][0]["status"] == "PREDICTED"
    original = trial.path.read_bytes()
    packet = app.read_packet(trial.path)
    assert packet["publication"] == trial.publication
    assert len(packet["inputs"]) == len(packet["predictions"]) == 50
    assert sorted(r["advisory_rank"] for r in packet["predictions"]) == list(range(1, 51))
    assert packet["physical_fit_count"] == 0 and packet["native_policy_changed"] is False
    assert service.status(program_id=app.PROGRAM)["last_waiting"] is None

    trial.source.inputs = lambda _p: pytest.fail("frozen prediction may not reread changed inputs")
    trial.source.verify_capture = lambda _r: pytest.fail("saved prediction must retain its original binding")
    trial.now[0] = app.clock("2026-11-03", 9, 31)
    assert restarted(trial).run_once()["results"][0]["status"] == "PREDICTION_REUSED"
    assert trial.path.read_bytes() == original


@pytest.mark.parametrize("hour,minute", [(9, 30), (10, 0)])
def test_waiting_cohort_cannot_be_recovered_as_prospective_after_open(trial, hour, minute):
    formal_cohort(trial)

    def waiting(_publication):
        raise InputReadinessError("inputs still incomplete")

    trial.source.inputs = waiting
    assert trial.service.run_once()["results"][0]["status"] == "WAITING_DATA"
    trial.now[0] = app.clock("2026-11-03", hour, minute)
    trial.source.inputs = lambda _p: pytest.fail("post-open recovery cannot read new inputs")
    service = restarted(trial)
    assert service.run_once()["results"][0]["status"] == "MISSED_CAPTURE_WINDOW"
    assert not trial.path.exists()
    missed = trial.path.parent / "missed.json"
    original = missed.read_bytes()
    assert app.read_packet(missed)["publication"] == trial.publication
    trial.now[0] = app.clock("2026-11-03", 11)
    assert restarted(trial).run_once()["results"][0]["status"] == "MISSED_CAPTURE_WINDOW"
    assert missed.read_bytes() == original


def test_revision_change_during_compute_retries_current_input_without_replacing_native(trial):
    formal_cohort(trial)
    revision, change_once = [7], [True]
    native = dict(trial.publication)
    source_rows = trial.rows.copy(deep=True)
    trial.source.inputs = lambda _p: (trial.rows, {"revision": revision[0]})

    def verify(receipt):
        if change_once[0]:
            revision[0], change_once[0] = 8, False
        if receipt["revision"] != revision[0]:
            raise InputReadinessError("actual input revision changed during compute")

    trial.source.verify_capture = verify
    assert trial.service.run_once()["results"][0]["status"] == "WAITING_DATA"
    assert not trial.path.exists()
    trial.now[0] = app.clock("2026-11-03", 0, 15)
    assert restarted(trial).run_once()["results"][0]["status"] == "PREDICTED"
    packet = app.read_packet(trial.path)
    assert packet["source_receipt"]["revision"] == 8
    assert packet["publication"] == native
    pd.testing.assert_frame_equal(trial.rows, source_rows)


def test_midnight_native_failure_in_another_program_does_not_block_exact_lstm_capture(trial):
    from backend.tests.advisory_model_first.test_forward_recovery import _cross_day_service

    formal_cohort(trial)
    trial.now[0] = app.clock("2026-11-03", 0, 15)
    service, repository, _calls = _cross_day_service(failed_program="a", clock=trial.now[0])
    service.calendar.is_trading_day = lambda _day: True
    service._evaluate_mature_model_observations = lambda *a, **kw: None
    service._retry_one_model_observation = lambda _results: None
    service._entry_price_after_baseline = lambda: {"status": "NOT_CONFIGURED"}
    service._entry_value_after_baseline = lambda: {"status": "NOT_CONFIGURED"}
    service.lstm_shadow_service = trial.service
    first = service.run_once()
    assert first["publication_due"] is False
    assert any(r["program_id"] == "a" and r["status"] == "WAITING_DATA" for r in first["results"])
    assert any(r["program_id"] == "b" and r["status"] == "PUBLISHED" for r in first["results"])
    assert repository.rows["a-28"]["publication_status"] == "WAITING_DATA"
    assert first["lstm_spo_shadow"]["results"][0]["status"] == "PREDICTED"
    original = trial.path.read_bytes()
    trial.source.inputs = lambda _p: pytest.fail("parallel baseline retry cannot replace the LSTM prediction")
    assert service.run_once()["lstm_spo_shadow"]["results"][0]["status"] == "PREDICTION_REUSED"
    assert trial.path.read_bytes() == original


def test_competing_shadow_writer_defers_without_reading_then_recovers_same_cohort(trial):
    from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

    formal_cohort(trial)
    publications = trial.source.publications
    trial.source.publications = lambda: pytest.fail("busy writer may not start a second capture")
    with app._daily_lock(trial.service.root / "run", app.EntryWorkBudget()):
        with pytest.raises(AdvisoryModelFirstError) as error:
            restarted(trial).run_once()
        assert error.value.reason_code == "ADVISORY_ENTRY_DAILY_LOCK_BUSY"
    assert not trial.path.exists()
    trial.source.publications = publications
    assert restarted(trial).run_once()["results"][0]["status"] == "PREDICTED"
    assert app.read_packet(trial.path)["publication"] == trial.publication
