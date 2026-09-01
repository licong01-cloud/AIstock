import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "sync_stock_namechange.py"
SPEC = importlib.util.spec_from_file_location("sync_stock_namechange", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
subject = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = subject
SPEC.loader.exec_module(subject)


def test_normalize_provider_rows_is_deterministic_and_deduplicated() -> None:
    rows = [
        {
            "ts_code": "600848.SH",
            "name": "上海临港",
            "start_date": "20151118",
            "end_date": None,
            "ann_date": "20151117",
            "change_reason": "改名",
        },
        {
            "ts_code": "600848.SH",
            "name": "上海临港",
            "start_date": "20151118",
            "end_date": None,
            "ann_date": "20151117",
            "change_reason": "改名",
        },
        {
            "ts_code": "600848.SH",
            "name": "自仪股份",
            "start_date": "20070514",
            "end_date": "20151117",
            "ann_date": "20070513",
            "change_reason": "撤销ST",
        },
    ]

    normalized = subject.normalize_provider_rows(reversed(rows))

    assert [(row.name, row.start_date.isoformat()) for row in normalized] == [
        ("自仪股份", "2007-05-14"),
        ("上海临港", "2015-11-18"),
    ]
    assert all(len(row.source_record_sha256) == 64 for row in normalized)


def test_normalize_provider_rows_rejects_invalid_or_conflicting_intervals() -> None:
    with pytest.raises(subject.StockNamechangeSyncError, match="invalid name interval"):
        subject.normalize_provider_rows(
            [
                {
                    "ts_code": "600848.SH",
                    "name": "自仪股份",
                    "start_date": "20151118",
                    "end_date": "20151117",
                }
            ]
        )

    with pytest.raises(subject.StockNamechangeSyncError, match="overlapping"):
        subject.normalize_provider_rows(
            [
                {
                    "ts_code": "600848.SH",
                    "name": "自仪股份",
                    "start_date": "20070514",
                    "end_date": "20151118",
                },
                {
                    "ts_code": "600848.SH",
                    "name": "上海临港",
                    "start_date": "20151118",
                },
            ]
        )

    with pytest.raises(subject.StockNamechangeSyncError, match="conflicting duplicate"):
        subject.normalize_provider_rows(
            [
                {
                    "ts_code": "600848.SH",
                    "name": "上海临港",
                    "start_date": "20151118",
                    "change_reason": "改名",
                },
                {
                    "ts_code": "600848.SH",
                    "name": "上海临港",
                    "start_date": "20151118",
                    "change_reason": "其他",
                },
            ]
        )


def test_normalize_provider_rows_canonicalizes_nan_as_null() -> None:
    [row] = subject.normalize_provider_rows(
        [
            {
                "ts_code": "600848.SH",
                "name": "上海临港",
                "start_date": "20151118",
                "end_date": float("nan"),
                "ann_date": float("nan"),
                "change_reason": float("nan"),
            }
        ]
    )

    assert row.end_date is None
    assert row.ann_date is None
    assert row.change_reason is None
    assert row.source_payload["end_date"] is None


def test_stable_provider_fetch_requires_two_consecutive_equal_responses(monkeypatch) -> None:
    monkeypatch.setattr(subject.time, "sleep", lambda _seconds: None)
    responses = iter(
        [
            [{"ts_code": "600848.SH", "name": "自仪股份", "start_date": "20070514"}],
            [{"ts_code": "600848.SH", "name": "上海临港", "start_date": "20151118"}],
            [{"ts_code": "600848.SH", "name": "上海临港", "start_date": "20151118"}],
        ]
    )

    rows = subject._stable_provider_fetch(lambda: next(responses), label="600848.SH")

    assert rows == [
        {
            "ts_code": "600848.SH",
            "name": "上海临港",
            "start_date": "20151118",
            "end_date": None,
            "ann_date": None,
            "change_reason": None,
        }
    ]


def test_stable_provider_fetch_fails_closed_on_persistent_drift(monkeypatch) -> None:
    monkeypatch.setattr(subject.time, "sleep", lambda _seconds: None)
    counter = iter(range(4))

    def changing_rows():
        day = 10 + next(counter)
        return [
            {
                "ts_code": "600848.SH",
                "name": "上海临港",
                "start_date": f"201511{day}",
            }
        ]

    with pytest.raises(subject.StockNamechangeSyncError, match="response is unstable"):
        subject._stable_provider_fetch(changing_rows, label="600848.SH")


def test_apply_requires_exact_database_identity(monkeypatch) -> None:
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _sql, _params=None):
            return None

        def fetchone(self):
            return ("aistock", "market.stock_namechange")

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(subject, "_db_config", lambda _prefix: {})
    monkeypatch.setattr(subject.psycopg2, "connect", lambda **_kwargs: Connection())

    with pytest.raises(subject.StockNamechangeSyncError, match="database identity mismatch"):
        subject._apply_rows([], prefix="TDX_DB_DEV_", expected_database="aistock_dev")


def test_apply_counts_changes_across_execute_values_pages(monkeypatch) -> None:
    fetches = iter(
        [
            ("aistock_dev", "market.stock_namechange"),
            (10,),
            (12,),
        ]
    )

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _sql, _params=None):
            return None

        def fetchone(self):
            return next(fetches)

        def fetchall(self):
            return []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return Cursor()

        def commit(self):
            return None

    captured = {}

    def execute_values(_cur, _sql, _rows, *, page_size, fetch):
        captured.update(page_size=page_size, fetch=fetch)
        return [("600848.SH", "上海临港", "2015-11-18"), ("600000.SH", "浦发银行", "1999-11-10")]

    [row] = subject.normalize_provider_rows(
        [{"ts_code": "600848.SH", "name": "上海临港", "start_date": "20151118"}]
    )
    monkeypatch.setattr(subject, "_db_config", lambda _prefix: {})
    monkeypatch.setattr(subject.psycopg2, "connect", lambda **_kwargs: Connection())
    monkeypatch.setattr(subject.pgx, "execute_values", execute_values)

    result = subject._apply_rows([row], prefix="TDX_DB_DEV_", expected_database="aistock_dev")

    assert result == {
        "database": "aistock_dev",
        "affected_rows": 2,
        "table_rows_before": 10,
        "table_rows": 12,
    }
    assert captured == {"page_size": 500, "fetch": True}


def test_parser_keeps_writes_opt_in() -> None:
    args = subject.build_parser().parse_args(["--ts-code", "600848.SH"])

    assert args.apply is False
    assert args.db_env_prefix == "TDX_DB_DEV_"
    assert args.max_workers == 4


def test_range_mode_reconciles_full_histories_for_affected_codes(monkeypatch, capsys) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "configured")
    monkeypatch.setattr(subject, "load_dotenv", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        subject,
        "_fetch_by_range",
        lambda _start, _end: [
            {"ts_code": "600848.SH", "name": "上海临港", "start_date": "20151118"},
            {"ts_code": "000001.SZ", "name": "平安银行", "start_date": "20200101"},
        ],
    )
    captured = {}

    def full_history(codes, max_workers):
        captured.update(codes=codes, max_workers=max_workers)
        return [{"ts_code": "000001.SZ", "name": "平安银行", "start_date": "20200101"}]

    monkeypatch.setattr(subject, "_fetch_by_code", full_history)

    assert subject.main(["--start-date", "2026-07-01", "--end-date", "2026-07-31"]) == 0

    assert captured == {"codes": ["000001.SZ", "600848.SH"], "max_workers": 4}
    receipt = __import__("json").loads(capsys.readouterr().out)
    assert receipt["request"]["mode"] == "announcement_range_then_full_code_reconcile"
    assert receipt["request"]["range_probe_row_count"] == 2
    assert receipt["request"]["affected_code_count"] == 2


def test_provider_spec_uses_unmasked_paginated_sync_engine() -> None:
    assert subject.NAMECHANGE_SPEC.tushare_api == "namechange"
    assert subject.NAMECHANGE_SPEC.fetch_params == {"limit": 5000}
    assert subject.NAMECHANGE_SPEC.row_limit == 5000
    assert subject.NAMECHANGE_SPEC.primary_keys == ["ts_code", "name", "start_date"]


def test_direct_cli_help_imports_repository_backend() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "--expected-database" in result.stdout
