import datetime as dt
import importlib.util
from collections import Counter
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "sync_eastmoney_anns_metadata.py"
SPEC = importlib.util.spec_from_file_location("sync_eastmoney_anns_metadata", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _item(codes: list[dict[str, str]]) -> dict[str, object]:
    return {
        "title_ch": "关于中弘控股股份有限公司股票终止上市的公告",
        "notice_date": "2018-11-08",
        "display_time": "2018-11-08 20:59:26",
        "art_code": "AN201811081241155794",
        "codes": codes,
    }


def test_rows_from_item_keeps_only_security_master_verified_issuer() -> None:
    audit: Counter[str] = Counter()
    identities = {
        "000001.SZ": ("平安银行", "平安银行股份有限公司"),
        "000852.SZ": ("石化机械", "中石化石油机械股份有限公司"),
        "000979.SZ": ("中弘退(退)", "中弘控股股份有限公司"),
    }

    rows = MODULE.rows_from_item(
        _item(
            [
                {"stock_code": "000001", "short_name": "中弘退"},
                {"stock_code": "000300", "short_name": "中弘退"},
                {"stock_code": "000852", "short_name": "中弘退"},
                {"stock_code": "000979", "short_name": "中弘退"},
                {"stock_code": "399001", "short_name": "中弘退"},
            ]
        ),
        dt.date(2018, 11, 8),
        identities,
        audit,
    )

    assert [row[1] for row in rows] == ["000979.SZ"]
    assert audit["issuer_candidate_count"] == 5
    assert audit["issuer_verified_count"] == 1
    assert audit["issuer_rejected_count"] == 4


def test_rows_from_item_fails_closed_when_multiple_security_identities_match() -> None:
    audit: Counter[str] = Counter()
    identities = {
        "000979.SZ": ("中弘退(退)", "中弘控股股份有限公司"),
        "001979.SZ": ("中弘退", "中弘控股股份有限公司"),
    }

    rows = MODULE.rows_from_item(
        _item(
            [
                {"stock_code": "000979", "short_name": "中弘退"},
                {"stock_code": "001979", "short_name": "中弘退"},
            ]
        ),
        dt.date(2018, 11, 8),
        identities,
        audit,
    )

    assert rows == []
    assert audit["issuer_ambiguous_document_count"] == 1
    assert audit["issuer_rejected_count"] == 2


def test_sync_one_date_reports_primary_and_rollback_failures(monkeypatch) -> None:
    class BrokenConnection:
        def rollback(self) -> None:
            raise RuntimeError("rollback failed")

    def fail_identity_load(_conn) -> None:
        raise ValueError("identity load failed")

    monkeypatch.setattr(MODULE, "get_conn", lambda **_: BrokenConnection())
    monkeypatch.setattr(MODULE, "load_security_identities", fail_identity_load)

    result = MODULE.sync_one_date(
        dt.date(2026, 7, 31),
        request_sleep=0.0,
        max_retries=1,
        bulk_session_tune=False,
    )

    assert result == {
        "ann_date": "2026-07-31",
        "status": "failed",
        "error_type": "ValueError",
        "error": "identity load failed",
        "rollback_error": {
            "error_type": "RuntimeError",
            "error": "rollback failed",
        },
    }
