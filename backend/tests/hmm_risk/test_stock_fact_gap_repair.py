from __future__ import annotations

from copy import deepcopy
from datetime import date
import json
from pathlib import Path

import pytest

from backend.services.hmm_risk import stock_fact_gap_repair as subject
from scripts.hmm_risk import repair_b3_stock_fact_gaps as cli


class _MemoryStore:
    def __init__(self, candidates):
        self.candidates = {dataset: sorted(values) for dataset, values in candidates.items()}
        self.rows = {"daily_basic": {}, "moneyflow_ts": {}}
        self.lock_count = 0

    def acquire_lock(self):
        self.lock_count += 1

    def find_candidates(self, spec):
        spec.validate()
        return {
            dataset: deepcopy(self.candidates[dataset]) if dataset in spec.datasets else []
            for dataset in subject.DATASET_ORDER
        }

    def fetch_rows(self, dataset, keys):
        return [deepcopy(self.rows[dataset][key]) for key in keys if key in self.rows[dataset]]

    def insert_missing(self, dataset, rows):
        inserted = []
        for row in rows:
            key = subject.GapKey(date.fromisoformat(row["trade_date"]), row["ts_code"])
            if key in self.rows[dataset]:
                continue
            self.rows[dataset][key] = deepcopy(dict(row))
            inserted.append(key)
        self.candidates[dataset] = [key for key in self.candidates[dataset] if key not in inserted]
        return inserted

    def delete_exact(self, dataset, rows):
        deleted = []
        for row in rows:
            key = subject.GapKey(date.fromisoformat(row["trade_date"]), row["ts_code"])
            if self.rows[dataset].get(key) != row:
                continue
            del self.rows[dataset][key]
            self.candidates[dataset].append(key)
            deleted.append(key)
        self.candidates[dataset].sort()
        return deleted


def _spec():
    return subject.RepairSpec("immutable_hmm_universe", date(2022, 1, 1), date(2024, 6, 30))


def _daily_row(trade_date="2022-01-04", ts_code="000001.SZ"):
    row = {column: None for column in subject.DAILY_BASIC_COLUMNS}
    row.update(
        {
            "trade_date": trade_date,
            "ts_code": ts_code,
            "close": 10.0,
            "total_mv": 1000.0,
            "circ_mv": 800.0,
        }
    )
    return row


def _moneyflow_row(trade_date="2022-01-04", ts_code="689009.SH"):
    row = {column: 0.0 for column in subject.MONEYFLOW_COLUMNS}
    row.update(
        {
            "trade_date": trade_date,
            "ts_code": ts_code,
            "buy_sm_amount": 1.0,
            "sell_sm_amount": 2.0,
            "buy_elg_amount": 3.0,
            "sell_elg_amount": 4.0,
            "net_mf_amount": -2.0,
        }
    )
    return row


def _store_and_plan():
    daily_key = subject.GapKey(date(2022, 1, 4), "000001.SZ")
    moneyflow_key = subject.GapKey(date(2022, 1, 4), "689009.SH")
    store = _MemoryStore({"daily_basic": [daily_key], "moneyflow_ts": [moneyflow_key]})
    return store, subject.build_plan(store, _spec())


def test_preflight_is_canonical_read_only_and_preserves_exact_keys():
    store, plan = _store_and_plan()

    assert subject.verify_plan(plan) == _spec()
    assert subject.plan_keys(plan) == store.candidates
    assert plan["candidate_counts"] == {"daily_basic": 1, "moneyflow_ts": 1}
    assert plan["selected_datasets"] == ["daily_basic", "moneyflow_ts"]
    assert plan["db_writes"] is False
    assert plan["fit_performed"] is False
    assert plan["selection_performed"] is False
    assert plan["model_ready_write_performed"] is False
    assert store.lock_count == 0


def test_preflight_can_bind_daily_basic_only_without_silently_changing_plan_scope():
    store, _ = _store_and_plan()
    spec = subject.RepairSpec(
        "immutable_hmm_universe",
        date(2022, 1, 1),
        date(2024, 6, 30),
        ("daily_basic",),
    )

    plan = subject.build_plan(store, spec)

    assert subject.verify_plan(plan) == spec
    assert plan["selected_datasets"] == ["daily_basic"]
    assert plan["candidate_counts"] == {"daily_basic": 1, "moneyflow_ts": 0}
    assert subject.plan_keys(plan)["moneyflow_ts"] == []
    tampered = deepcopy(plan)
    tampered["selected_datasets"] = ["daily_basic", "moneyflow_ts"]
    with pytest.raises(subject.StockFactGapRepairError, match="plan hash mismatch"):
        subject.verify_plan(tampered)


def test_daily_basic_only_plan_applies_without_requiring_moneyflow_provider_rows():
    store, _ = _store_and_plan()
    spec = subject.RepairSpec(
        "immutable_hmm_universe",
        date(2022, 1, 1),
        date(2024, 6, 30),
        ("daily_basic",),
    )
    plan = subject.build_plan(store, spec)

    receipt = subject.apply_plan(store, plan, {"daily_basic": [_daily_row()]})

    assert receipt["status"] == "applied"
    assert receipt["row_counts"] == {"daily_basic": 1, "moneyflow_ts": 0}
    assert store.rows["moneyflow_ts"] == {}
    assert store.candidates["moneyflow_ts"] == [subject.GapKey(date(2022, 1, 4), "689009.SH")]


def test_hashed_plan_and_receipt_still_reject_semantically_inconsistent_status_counts_and_flags():
    store, plan = _store_and_plan()
    bad_plan = deepcopy(plan)
    bad_plan["status"] = "ready"
    bad_plan["plan_sha256"] = subject.canonical_sha256(
        {key: value for key, value in bad_plan.items() if key != "plan_sha256"}
    )
    with pytest.raises(subject.StockFactGapRepairError, match="status is not planned"):
        subject.verify_plan(bad_plan)

    bad_counts = deepcopy(plan)
    bad_counts["candidate_counts"]["daily_basic"] = 2
    bad_counts["plan_sha256"] = subject.canonical_sha256(
        {key: value for key, value in bad_counts.items() if key != "plan_sha256"}
    )
    with pytest.raises(subject.StockFactGapRepairError, match="counts differ"):
        subject.verify_plan(bad_counts)

    receipt = subject.apply_plan(
        store,
        plan,
        {"daily_basic": [_daily_row()], "moneyflow_ts": [_moneyflow_row()]},
    )
    bad_receipt = deepcopy(receipt)
    bad_receipt["db_writes"] = False
    bad_receipt["receipt_sha256"] = subject.canonical_sha256(
        {key: value for key, value in bad_receipt.items() if key != "receipt_sha256"}
    )
    with pytest.raises(subject.StockFactGapRepairError, match="status and db_writes"):
        subject.verify_receipt(bad_receipt)


def test_provider_contract_fails_on_missing_nonfinite_duplicate_or_extra_keys():
    store, plan = _store_and_plan()
    keys = subject.plan_keys(plan)
    daily = _daily_row()
    moneyflow = _moneyflow_row()

    broken = dict(daily)
    del broken["circ_mv"]
    with pytest.raises(subject.StockFactGapRepairError, match="lacks columns"):
        subject.normalize_provider_rows(keys, {"daily_basic": [broken], "moneyflow_ts": [moneyflow]})

    nonfinite = dict(daily, circ_mv=float("inf"))
    with pytest.raises(subject.StockFactGapRepairError, match="not finite"):
        subject.normalize_provider_rows(keys, {"daily_basic": [nonfinite], "moneyflow_ts": [moneyflow]})

    with pytest.raises(subject.StockFactGapRepairError, match="invalid trade_date"):
        subject.normalize_provider_row("daily_basic", _daily_row(trade_date="2022-02-31"))

    with pytest.raises(subject.StockFactGapRepairError, match="duplicate daily_basic"):
        subject.normalize_provider_rows(
            keys,
            {"daily_basic": [daily, daily], "moneyflow_ts": [moneyflow]},
        )

    with pytest.raises(subject.StockFactGapRepairError, match="key set differs"):
        subject.normalize_provider_rows(
            keys,
            {"daily_basic": [_daily_row(ts_code="000002.SZ")], "moneyflow_ts": [moneyflow]},
        )


def test_apply_readback_and_second_apply_are_exact_and_idempotent():
    store, plan = _store_and_plan()
    provider = {"daily_basic": [_daily_row()], "moneyflow_ts": [_moneyflow_row()]}

    receipt = subject.apply_plan(store, plan, provider)
    readback = subject.readback_receipt(store, receipt)
    second = subject.apply_plan(store, plan, provider)

    assert receipt["status"] == "applied"
    assert receipt["db_writes"] is True
    assert receipt["row_counts"] == {"daily_basic": 1, "moneyflow_ts": 1}
    assert readback["status"] == "verified"
    assert readback["db_writes"] is False
    assert second["status"] == "already_applied"
    assert second["db_writes"] is False
    assert store.lock_count == 2


def test_apply_rejects_candidate_drift_without_partial_insert():
    store, plan = _store_and_plan()
    store.candidates["moneyflow_ts"] = []

    with pytest.raises(subject.StockFactGapRepairError, match="candidate set drifted"):
        subject.apply_plan(
            store,
            plan,
            {"daily_basic": [_daily_row()], "moneyflow_ts": [_moneyflow_row()]},
        )

    assert store.rows == {"daily_basic": {}, "moneyflow_ts": {}}


def test_guarded_rollback_deletes_only_unchanged_inserted_rows():
    store, plan = _store_and_plan()
    receipt = subject.apply_plan(
        store,
        plan,
        {"daily_basic": [_daily_row()], "moneyflow_ts": [_moneyflow_row()]},
    )
    daily_key = subject.GapKey(date(2022, 1, 4), "000001.SZ")
    store.rows["daily_basic"][daily_key]["circ_mv"] = "801"

    with pytest.raises(subject.StockFactGapRepairError, match="changed after apply"):
        subject.rollback_receipt(store, receipt)

    store.rows["daily_basic"][daily_key]["circ_mv"] = "800"
    rollback = subject.rollback_receipt(store, receipt)

    assert rollback["status"] == "rolled_back"
    assert rollback["deleted_counts"] == {"daily_basic": 1, "moneyflow_ts": 1}
    assert store.rows == {"daily_basic": {}, "moneyflow_ts": {}}


def test_guarded_rollback_rejects_no_write_idempotency_receipt():
    store, plan = _store_and_plan()
    provider = {"daily_basic": [_daily_row()], "moneyflow_ts": [_moneyflow_row()]}
    subject.apply_plan(store, plan, provider)
    no_write_receipt = subject.apply_plan(store, plan, provider)

    with pytest.raises(subject.StockFactGapRepairError, match="performed database writes"):
        subject.rollback_receipt(store, no_write_receipt)


def test_postgres_contract_is_insert_only_exact_date_and_never_updates_existing_rows():
    assert "cal.previous_trade_date" in subject.DAILY_BASIC_CANDIDATE_SQL
    assert "previous_basic.trade_date=cal.previous_trade_date" in subject.DAILY_BASIC_CANDIDATE_SQL
    assert "previous_basic.circ_mv IS NULL" in subject.DAILY_BASIC_CANDIDATE_SQL
    assert "basic_history" not in subject.DAILY_BASIC_CANDIDATE_SQL
    assert "previous_basic_date" not in subject.DAILY_BASIC_CANDIDATE_SQL
    assert "mf.trade_date=k.trade_date" in subject.MONEYFLOW_SYMBOL_CANDIDATE_SQL
    assert "count(DISTINCT to_jsonb(t))>1" in subject.SOURCE_CONFLICT_SQL
    source = Path(subject.__file__).read_text(encoding="utf-8")
    assert "ON CONFLICT DO NOTHING RETURNING trade_date,ts_code" in source
    assert "ON CONFLICT DO UPDATE" not in source


def test_cli_dev_target_uses_only_explicit_dev_database_settings(monkeypatch):
    for name, value in {
        "TDX_DB_DEV_HOST": "dev-host",
        "TDX_DB_DEV_PORT": "5433",
        "TDX_DB_DEV_USER": "dev-user",
        "TDX_DB_DEV_PASSWORD": "dev-password",
        "TDX_DB_DEV_NAME": "aistock_dev",
        "TDX_DB_HOST": "production-host",
        "TDX_DB_PORT": "5432",
        "TDX_DB_USER": "production-user",
        "TDX_DB_PASSWORD": "production-password",
        "TDX_DB_NAME": "aistock",
    }.items():
        monkeypatch.setenv(name, value)

    config = cli._db_config("dev")

    assert config["host"] == "dev-host"
    assert config["port"] == 5433
    assert config["user"] == "dev-user"
    assert config["password"] == "dev-password"
    assert config["dbname"] == "aistock_dev"


def test_cli_refuses_missing_dev_database_identity(monkeypatch):
    for name in ("HOST", "PORT", "USER", "PASSWORD", "NAME"):
        monkeypatch.delenv(f"TDX_DB_DEV_{name}", raising=False)

    with pytest.raises(subject.StockFactGapRepairError, match="lacks required TDX_DB_DEV"):
        cli._db_config("dev")


def test_mutating_receipt_can_be_fsynced_before_commit_and_atomically_published(tmp_path):
    output = tmp_path / "receipt.json"
    value = {"schema_version": "test_receipt_v1", "status": "applied", "db_writes": True}

    temporary, _text = cli._prepare_output(value, output)

    assert temporary.exists()
    assert not output.exists()
    assert json.loads(temporary.read_text(encoding="utf-8")) == value

    cli._finalize_output(temporary, output)

    assert output.exists()
    assert not temporary.exists()
    assert json.loads(output.read_text(encoding="utf-8")) == value


def test_commit_failure_is_reported_as_unknown_and_discards_uncommitted_prepared_receipt(tmp_path, monkeypatch):
    store, plan = _store_and_plan()
    plan_path = tmp_path / "plan.json"
    output = tmp_path / "receipt.json"
    env_file = tmp_path / ".env"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    env_file.write_text("", encoding="utf-8")

    class _Connection:
        autocommit = True
        rollback_count = 0

        @staticmethod
        def commit():
            raise ConnectionError("commit outcome unavailable")

        def rollback(self):
            self.rollback_count += 1

        @staticmethod
        def close():
            return None

    connection = _Connection()
    monkeypatch.setattr(cli, "_connect", lambda _target: connection)
    monkeypatch.setattr(cli, "PostgresGapStore", lambda _conn: store)
    monkeypatch.setattr(
        cli,
        "fetch_provider_rows",
        lambda _plan: {"daily_basic": [_daily_row()], "moneyflow_ts": [_moneyflow_row()]},
    )

    exit_code = cli.run(
        [
            "apply",
            "--env-file",
            str(env_file),
            "--target",
            "dev",
            "--plan",
            str(plan_path),
            "--output",
            str(output),
            "--confirm",
            subject.CONFIRM_APPLY,
        ]
    )
    error = json.loads(output.read_text(encoding="utf-8"))

    assert exit_code == 2
    assert error["database_commit_status"] == "unknown"
    assert error["db_writes"] is None
    assert error["pending_receipt_path"] is None
    assert connection.rollback_count == 1
    assert list(tmp_path.glob(".receipt.json.*.tmp")) == []


def test_moneyflow_provider_fetch_uses_day_authority_and_filters_exact_plan_keys():
    class _Frame:
        empty = False

        def __init__(self, rows):
            self.rows = rows

        def to_dict(self, *, orient):
            assert orient == "records"
            return deepcopy(self.rows)

    class _Provider:
        def __init__(self):
            self.calls = []

        def moneyflow(self, **kwargs):
            self.calls.append(kwargs)
            row = _moneyflow_row(trade_date="2022-01-04", ts_code="689009.SH")
            unrelated = _moneyflow_row(trade_date="2022-01-04", ts_code="000001.SZ")
            return _Frame([row, unrelated])

    provider = _Provider()
    keys = [subject.GapKey(date(2022, 1, 4), "689009.SH")]

    rows = cli._fetch_moneyflow(provider, keys)

    assert rows == [_moneyflow_row()]
    assert len(provider.calls) == 1
    assert provider.calls[0]["trade_date"] == "20220104"
    assert "ts_code" not in provider.calls[0]
    assert "start_date" not in provider.calls[0]
    assert "end_date" not in provider.calls[0]


def test_moneyflow_provider_does_not_silently_rewrite_a_historical_symbol_alias():
    class _Frame:
        empty = False

        def to_dict(self, *, orient):
            assert orient == "records"
            return [_moneyflow_row(ts_code="300114.SZ")]

    class _Provider:
        @staticmethod
        def moneyflow(**kwargs):
            assert kwargs["trade_date"] == "20220104"
            return _Frame()

    keys = [subject.GapKey(date(2022, 1, 4), "302132.SZ")]

    assert cli._fetch_moneyflow(_Provider(), keys) == []


def test_provider_query_retries_transient_failure_and_reports_permanent_failure(monkeypatch):
    monkeypatch.setattr(cli.time, "sleep", lambda _value: None)
    attempts = []

    def transient():
        attempts.append(len(attempts) + 1)
        if len(attempts) < 3:
            raise TimeoutError("temporary")
        return "complete"

    assert cli._provider_query("daily_basic", "2022-01-04", transient) == "complete"
    assert attempts == [1, 2, 3]

    with pytest.raises(subject.StockFactGapRepairError, match="after 3 attempts") as error:
        cli._provider_query("moneyflow", "2022-01-04", lambda: (_ for _ in ()).throw(OSError("offline")))

    assert "attempt=1:OSError:offline" in str(error.value)
    assert "attempt=3:OSError:offline" in str(error.value)
