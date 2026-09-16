"""Build the immutable PT-NEXT-020 corporate-action authorities.

The operator reads ``market.dividend`` in a repeatable, read-only transaction,
verifies the two known ambiguous source groups against pinned official
documents, emits the existing v6 snapshot, and binds the complete r5
factor-preflight failures into a separate typed full-scope authority.  It never
mutates the database, a dataset candidate, or runtime state.
"""

from __future__ import annotations

import argparse
import datetime as dt
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any, Mapping, Sequence
from urllib.request import Request, urlopen

import psycopg2
from dotenv import dotenv_values

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.position_timing.action_value_corporate_actions import (  # noqa: E402
    CorporateActionBook,
    SNAPSHOT_SCHEMA,
    _snapshot_payload,
)
from backend.services.position_timing.action_value_data import DailyCandidate, file_reference  # noqa: E402
from backend.services.position_timing.contracts import canonical_json_bytes, canonical_sha256  # noqa: E402


AUTHORITY_SCHEMA = "local_data_corporate_action_resolution_authority_v1"
FULL_SCOPE_AUTHORITY_SCHEMA = "local_data_corporate_action_full_scope_authority_v1"
RECEIPT_SCHEMA = "pt_next_020_corporate_action_authority_receipt_v1"
REQUEST_ID = "PT-NEXT-020"
EXPECTED_CANDIDATE_MANIFEST_SHA256 = (
    "7b5402c38b4b279140375fa6517595f88bdfb472e617faf8b032c04f0d33d1c1"
)
IMPLEMENTED_DIVIDEND = "实施"
ACCOUNT_CLASS = "GENERIC_PUBLIC_A_SHARE_ACCOUNT"
SOURCE_COLUMNS = (
    "ts_code", "end_date", "ann_date", "imp_ann_date", "ex_date",
    "stk_div", "stk_bo_rate", "stk_co_rate", "cash_div", "cash_div_tax",
    "record_date", "pay_date", "div_listdate", "base_date", "base_share",
)
TARGET_KEYS = (("002352.SZ", "2024-11-07"), ("600989.SH", "2024-07-24"))

# These groups are the complete fail-closed result of the r5 source-only
# preflight after the two economic conflicts above are resolved.  The source
# group hashes bind the exact market.dividend rows; the factor evidence binds
# the immutable r5 candidate.  Account economics are never inferred from the
# factor ratio.  In particular, restructuring conversion shares belong to the
# restructuring plan rather than ordinary holders, while 300234 has separate
# eligible-account and exchange reference-price bases because treasury shares
# do not participate in the distribution.
FULL_SCOPE_RESOLUTION_SPECS: Mapping[tuple[str, str], Mapping[str, Any]] = {
    ("000528.SZ", "2018-10-26"): {
        "classification": "DUPLICATE_SOURCE_RECORD",
        "source_row_count": 3,
        "source_rows_sha256": "ae4e21231cb21b541833b1f65eba443075dd9018ff29631673b21e2372b294d7",
        "accumulation_policy": "ONE_CANONICAL_ACCOUNT_ACTION",
        "replay_application": "APPLY_ACCOUNT_ACTION",
        "account_economics": {"quantity_multiplier": "1.3", "cash_yuan_per_share": "0"},
        "reference_price_economics": {"factor_ratio": "1.299628440464188329331616537"},
        "factor_boundary": {"previous": "2018-10-25", "current": "2018-10-26"},
        "announcement_url": "https://data.eastmoney.com/notices/detail/000528/AN201810181216185374.html",
    },
    ("000615.SZ", "2025-12-29"): {
        "classification": "SPECIAL_RESTRUCTURING_NON_PRO_RATA",
        "source_row_count": 2,
        "source_rows_sha256": "e1d18fe42fea3fa7d3c24ff219650cddc0a8e7fa9aa5d991ffc246397a53da34",
        "accumulation_policy": "DO_NOT_CREDIT_CONVERSION_SHARES_TO_ORDINARY_ACCOUNT",
        "replay_application": "REFERENCE_PRICE_ONLY",
        "account_economics": {"quantity_multiplier": "1", "cash_yuan_per_share": "0"},
        "reference_price_economics": {"factor_ratio": "1.184130609682135285696358729"},
        "factor_boundary": {"previous": "2025-12-25", "current": "2025-12-29"},
        "announcement_url": "https://data.eastmoney.com/notices/detail/000615/AN202512221805948985.html",
    },
    ("000797.SZ", "2018-08-01"): {
        "classification": "PREHISTORY_BOUNDARY_ACTION",
        "source_row_count": 1,
        "source_rows_sha256": "8ec2795e53b977cda0771e205d8498e7ed7a8fb3828d8a8c1a39055cdd82d877",
        "accumulation_policy": "RETAIN_SOURCE_DO_NOT_REPLAY",
        "replay_application": "EXCLUDE_BEFORE_FIRST_OBSERVABLE_POSITION",
        "account_economics": {"quantity_multiplier": "1.3000595", "cash_yuan_per_share": "0.0600119"},
        "reference_price_economics": None,
        "factor_boundary": {"first_valid": "2018-08-01", "first_pit_eligible": "2018-08-01"},
        "announcement_url": None,
    },
    ("000908.SZ", "2026-03-11"): {
        "classification": "SPECIAL_RESTRUCTURING_NON_PRO_RATA",
        "source_row_count": 2,
        "source_rows_sha256": "1b011f82443586ca0f4ec26c059f615fae07cb4f5ad24f6b6f59e47b528af73f",
        "accumulation_policy": "DO_NOT_CREDIT_CONVERSION_SHARES_TO_ORDINARY_ACCOUNT",
        "replay_application": "REFERENCE_PRICE_ONLY",
        "account_economics": {"quantity_multiplier": "1", "cash_yuan_per_share": "0"},
        "reference_price_economics": {"factor_ratio": "1.460954009756403478383762977"},
        "factor_boundary": {"previous": "2026-03-10", "current": "2026-03-11"},
        "announcement_url": "https://data.eastmoney.com/notices/detail/000908/AN202603031820229997.html",
    },
    ("002602.SZ", "2018-10-31"): {
        "classification": "PREHISTORY_BOUNDARY_ACTION",
        "source_row_count": 1,
        "source_rows_sha256": "65651bc81d2d7e48a88d032fe05994d0abc348a9f32165e9a0bc306e53e781ae",
        "accumulation_policy": "RETAIN_SOURCE_DO_NOT_REPLAY",
        "replay_application": "EXCLUDE_BEFORE_FIRST_OBSERVABLE_POSITION",
        "account_economics": {"quantity_multiplier": "1.6", "cash_yuan_per_share": "0.1"},
        "reference_price_economics": None,
        "factor_boundary": {"first_valid": "2018-11-07", "first_pit_eligible": "2018-11-07"},
        "announcement_url": "https://data.eastmoney.com/notices/detail/002602/AN201810241220510217.html",
    },
    ("002713.SZ", "2025-12-30"): {
        "classification": "SPECIAL_RESTRUCTURING_NON_PRO_RATA",
        "source_row_count": 2,
        "source_rows_sha256": "2db6a966933a75d737b58ffa2f2ae168c8e023dedaf3e189222d38d8a3a5b0eb",
        "accumulation_policy": "DO_NOT_CREDIT_CONVERSION_SHARES_TO_ORDINARY_ACCOUNT",
        "replay_application": "REFERENCE_PRICE_ONLY",
        "account_economics": {"quantity_multiplier": "1", "cash_yuan_per_share": "0"},
        "reference_price_economics": {"factor_ratio": "1.050359510448741950063936389"},
        "factor_boundary": {"previous": "2025-12-26", "current": "2025-12-30"},
        "announcement_url": "https://data.eastmoney.com/notices/detail/002713/AN202512231806622901.html",
    },
    ("002739.SZ", "2018-08-08"): {
        "classification": "PREHISTORY_BOUNDARY_ACTION",
        "source_row_count": 1,
        "source_rows_sha256": "017673f37493e6dce47900933ea2293a12fdcb7a9916b25c3eff1b5b8cdc3526",
        "accumulation_policy": "RETAIN_SOURCE_DO_NOT_REPLAY",
        "replay_application": "EXCLUDE_BEFORE_FIRST_OBSERVABLE_POSITION",
        "account_economics": {"quantity_multiplier": "1.5", "cash_yuan_per_share": "0.2"},
        "reference_price_economics": None,
        "factor_boundary": {"first_valid": "2018-11-05", "first_pit_eligible": "2018-11-05"},
        "announcement_url": None,
    },
    ("002742.SZ", "2025-12-10"): {
        "classification": "SPECIAL_RESTRUCTURING_NON_PRO_RATA",
        "source_row_count": 2,
        "source_rows_sha256": "17bcf5b017c8b8e00ac9411e84b5c95bc15da140372d055661e692b1e8cccca0",
        "accumulation_policy": "DO_NOT_CREDIT_CONVERSION_SHARES_TO_ORDINARY_ACCOUNT",
        "replay_application": "REFERENCE_PRICE_ONLY",
        "account_economics": {"quantity_multiplier": "1", "cash_yuan_per_share": "0"},
        "reference_price_economics": {"factor_ratio": "1.045255289274430692363587398"},
        "factor_boundary": {"previous": "2025-12-08", "current": "2025-12-10"},
        "announcement_url": "http://www.cninfo.com.cn/new/disclosure/detail?stockCode=002742&announcementId=1224850420&orgId=9900022970&announcementTime=2025-12-05",
    },
    ("300125.SZ", "2025-12-29"): {
        "classification": "SPECIAL_RESTRUCTURING_NON_PRO_RATA",
        "source_row_count": 2,
        "source_rows_sha256": "234d0ae5755a207e5059e7fd1c2a8a69dffb7f3ebec13bca48aa9c94fe56f1d5",
        "accumulation_policy": "DO_NOT_CREDIT_CONVERSION_SHARES_TO_ORDINARY_ACCOUNT",
        "replay_application": "REFERENCE_PRICE_ONLY",
        "account_economics": {"quantity_multiplier": "1", "cash_yuan_per_share": "0"},
        "reference_price_economics": {"factor_ratio": "1.263672105375504714094000955"},
        "factor_boundary": {"previous": "2025-12-25", "current": "2025-12-29"},
        "announcement_url": "https://data.eastmoney.com/notices/detail/300125/AN202512221805987762.html",
    },
    ("300234.SZ", "2020-04-21"): {
        "classification": "TREASURY_SHARE_EXCLUDED_DUAL_BASIS_DISTRIBUTION",
        "source_row_count": 1,
        "source_rows_sha256": "a50737f86d3b9043637767e7cf698cb7bcc8778356cf26c392da23da2973ac82",
        "accumulation_policy": "APPLY_ELIGIBLE_ACCOUNT_BASIS_ONCE",
        "replay_application": "APPLY_ACCOUNT_ACTION_WITH_DISTINCT_REFERENCE_BASIS",
        "account_economics": {"quantity_multiplier": "1.8125392", "cash_yuan_per_share": "0.0629717"},
        "reference_price_economics": {"quantity_multiplier": "1.766366", "cash_yuan_per_share": "0.059393", "factor_ratio": "1.774970332611693357568878109"},
        "factor_boundary": {"previous": "2020-04-20", "current": "2020-04-21"},
        "announcement_url": "https://data.eastmoney.com/notices/detail/300234/AN202004141377997624.html",
    },
    ("300506.SZ", "2025-12-22"): {
        "classification": "SPECIAL_RESTRUCTURING_NON_PRO_RATA",
        "source_row_count": 2,
        "source_rows_sha256": "de8808fdaeaaae3175c31d2fb32f1bc66708594c8943c4069b49dd2fa8cbfc6a",
        "accumulation_policy": "DO_NOT_CREDIT_CONVERSION_SHARES_TO_ORDINARY_ACCOUNT",
        "replay_application": "REFERENCE_PRICE_ONLY",
        "account_economics": {"quantity_multiplier": "1", "cash_yuan_per_share": "0"},
        "reference_price_economics": {"factor_ratio": "1.325949671464771899736514322"},
        "factor_boundary": {"previous": "2025-12-18", "current": "2025-12-22"},
        "announcement_url": "https://data.eastmoney.com/notices/detail/300506/AN202512151801046991.html",
    },
    ("300510.SZ", "2018-11-08"): {
        "classification": "PREHISTORY_BOUNDARY_ACTION",
        "source_row_count": 1,
        "source_rows_sha256": "72f8e9c712229d2c0087549820218e50fe6d16205aa22fc081939d796c789fdc",
        "accumulation_policy": "RETAIN_SOURCE_DO_NOT_REPLAY",
        "replay_application": "EXCLUDE_BEFORE_FIRST_OBSERVABLE_POSITION",
        "account_economics": {"quantity_multiplier": "1.8", "cash_yuan_per_share": "0.056"},
        "reference_price_economics": None,
        "factor_boundary": {"first_valid": "2018-12-03", "first_pit_eligible": "2018-12-03"},
        "announcement_url": None,
    },
    ("600375.SH", "2024-12-26"): {
        "classification": "SPECIAL_RESTRUCTURING_NON_PRO_RATA",
        "source_row_count": 2,
        "source_rows_sha256": "a39fe1fb379eb782d440a1fc0e0abf67a3069af7f105a7ba0229c5f7e0e25993",
        "accumulation_policy": "DO_NOT_CREDIT_CONVERSION_SHARES_TO_ORDINARY_ACCOUNT",
        "replay_application": "REFERENCE_PRICE_ONLY",
        "account_economics": {"quantity_multiplier": "1", "cash_yuan_per_share": "0"},
        "reference_price_economics": {"factor_ratio": "1.064114195482434551158691969"},
        "factor_boundary": {"previous": "2024-12-24", "current": "2024-12-26"},
        "announcement_url": "http://dataclouds.cninfo.com.cn/shgonggao/2024/2024-12-20/56a87f46bde811ef9f64fa163e26e5de.pdf",
    },
    ("600423.SH", "2018-12-18"): {
        "classification": "PREHISTORY_BOUNDARY_ACTION",
        "source_row_count": 1,
        "source_rows_sha256": "3f54082fd4323946abe1feab7c9a465a80652bb25c370bde6b05559baa5d269d",
        "accumulation_policy": "RETAIN_SOURCE_DO_NOT_REPLAY",
        "replay_application": "EXCLUDE_BEFORE_FIRST_OBSERVABLE_POSITION",
        "account_economics": {"quantity_multiplier": "2", "cash_yuan_per_share": "0"},
        "reference_price_economics": None,
        "factor_boundary": {"first_valid": "2018-12-26", "first_pit_eligible": "2021-05-20"},
        "announcement_url": "https://data.eastmoney.com/notices/detail/600423/AN201812111267469927.html",
    },
    ("600733.SH", "2018-09-19"): {
        "classification": "PREHISTORY_BOUNDARY_ACTION",
        "source_row_count": 3,
        "source_rows_sha256": "80ea1bbf652cb9499fe99c7c4ed6bffc4a687009713fd2b48c6fb2a47176afe5",
        "accumulation_policy": "RETAIN_ONE_CANONICAL_SOURCE_DO_NOT_REPLAY",
        "replay_application": "EXCLUDE_BEFORE_FIRST_OBSERVABLE_POSITION",
        "account_economics": {"quantity_multiplier": "3.5", "cash_yuan_per_share": "0"},
        "reference_price_economics": None,
        "factor_boundary": {"first_valid": "2018-09-27", "first_pit_eligible": "2018-09-27"},
        "announcement_url": "https://static.cninfo.com.cn/finalpage/2018-09-13/1205432898.PDF",
    },
}


class AuthorityBuildError(RuntimeError):
    """Fail-closed data-authority build error."""


DOCUMENTS: tuple[Mapping[str, Any], ...] = (
    {
        "document_id": "002352_2024_INTERIM_PLAN",
        "document_type": "INTERIM_DIVIDEND_PLAN",
        "official_source_url": "https://static.cninfo.com.cn/finalpage/2024-10-11/1221360553.PDF",
        "capture_url": "https://static.cninfo.com.cn/finalpage/2024-10-11/1221360553.PDF",
        "filename": "002352_2024_interim_dividend_plan.pdf",
        "source_content_sha256": "a0f329f4465abdae7016850c45993fd545058c55e0b324ae667874373cd5cd02",
        "source_file_size": 271597,
    },
    {
        "document_id": "002352_2024_SPECIAL_PLAN",
        "document_type": "SPECIAL_DIVIDEND_PLAN",
        "official_source_url": "https://disc.static.szse.cn/disc/disk03/finalpage/2024-10-11/a80885ef-9095-42f5-8579-e8b92ad05419.PDF",
        "capture_url": "https://disc.static.szse.cn/disc/disk03/finalpage/2024-10-11/a80885ef-9095-42f5-8579-e8b92ad05419.PDF",
        "filename": "002352_2024_special_dividend_plan.pdf",
        "source_content_sha256": "6628441352b6ffe4e3812b187b974cc4a590c875115f68316090edb60303ec41",
        "source_file_size": 200746,
    },
    {
        "document_id": "002352_2024_COMBINED_IMPLEMENTATION",
        "document_type": "COMBINED_DIVIDEND_IMPLEMENTATION",
        "official_source_url": "https://static.cninfo.com.cn/finalpage/2024-10-31/1221573964.PDF",
        "capture_url": "https://static.cninfo.com.cn/finalpage/2024-10-31/1221573964.PDF",
        "filename": "002352_2024_combined_dividend_implementation.pdf",
        "source_content_sha256": "338b6f3fe077d74966638cf0caf937e0d5e53aa86d06ed518b23e15c94cdc871",
        "source_file_size": 303561,
    },
    {
        "document_id": "600989_2023_DISTRIBUTION_ADJUSTMENT",
        "document_type": "DIFFERENTIAL_DIVIDEND_ADJUSTMENT",
        "official_source_url": "https://static.sse.com.cn/disclosure/listedinfo/announcement/c/new/2024-07-03/600989_20240703_QE7F.pdf",
        "capture_url": "https://file.finance.sina.com.cn/211.154.219.97:9494/MRGG/CNSESH_STOCK/2024/2024-7/2024-07-03/10309648.PDF",
        "filename": "600989_2023_dividend_adjustment.pdf",
        "source_content_sha256": "22cd9aacc8575b5e44138cfb349a9deec7f7bf86d98c2d527fe774fb01dca197",
        "source_file_size": 281019,
    },
    {
        "document_id": "600989_2023_DISTRIBUTION_IMPLEMENTATION",
        "document_type": "DIFFERENTIAL_DIVIDEND_IMPLEMENTATION",
        "official_source_url": "https://static.sse.com.cn/disclosure/listedinfo/announcement/c/new/2024-07-18/600989_20240718_2K7K.pdf",
        "capture_url": "https://file.finance.sina.com.cn/211.154.219.97:9494/MRGG/CNSESH_STOCK/2024/2024-7/2024-07-18/10334213.PDF",
        "filename": "600989_2023_dividend_implementation.pdf",
        "source_content_sha256": "d47db5c0b7d2c9bfc37dbc71cfdd2e34ed299948baaad08e7fadedb6fa2dde86",
        "source_file_size": 805142,
    },
)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _decimal_text(value: Any) -> str | None:
    if value is None:
        return None
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise AuthorityBuildError("non-finite source decimal")
    text = format(parsed.normalize(), "f")
    return "0" if text in {"-0", ""} else text


def _date_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        value = value.date()
    if isinstance(value, dt.date):
        return value.isoformat()
    return dt.date.fromisoformat(str(value)).isoformat()


def portable_row(values: Sequence[Any]) -> dict[str, Any]:
    if len(values) != len(SOURCE_COLUMNS):
        raise AuthorityBuildError("market.dividend source schema differs")
    result: dict[str, Any] = {}
    for index, name in enumerate(SOURCE_COLUMNS):
        value = values[index]
        if name in {"end_date", "ann_date", "imp_ann_date", "ex_date", "record_date", "pay_date", "div_listdate", "base_date"}:
            result[name] = _date_text(value)
        elif name == "ts_code":
            result[name] = str(value).upper()
        else:
            result[name] = _decimal_text(value)
    return result


def _signature(row: Mapping[str, Any]) -> tuple[str | None, ...]:
    return tuple(row.get(name) for name in SOURCE_COLUMNS)


EXPECTED_TARGET_ROWS = {
    _signature(row)
    for row in (
        {"ts_code":"002352.SZ","end_date":"2024-06-30","ann_date":"2024-08-29","imp_ann_date":"2024-10-31","ex_date":"2024-11-07","stk_div":"0","stk_bo_rate":None,"stk_co_rate":None,"cash_div":"0.4","cash_div_tax":"0.4","record_date":"2024-11-06","pay_date":"2024-11-07","div_listdate":None,"base_date":"2024-11-06","base_share":"479541.5625"},
        {"ts_code":"002352.SZ","end_date":"2024-10-11","ann_date":"2024-10-11","imp_ann_date":"2024-10-31","ex_date":"2024-11-07","stk_div":"0","stk_bo_rate":None,"stk_co_rate":None,"cash_div":"1","cash_div_tax":"1","record_date":"2024-11-06","pay_date":"2024-11-07","div_listdate":None,"base_date":"2024-11-06","base_share":"479541.5625"},
        {"ts_code":"002352.SZ","end_date":"2024-06-30","ann_date":"2024-10-30","imp_ann_date":"2024-10-31","ex_date":"2024-11-07","stk_div":"0","stk_bo_rate":None,"stk_co_rate":None,"cash_div":"1.4","cash_div_tax":"1.4","record_date":"2024-11-06","pay_date":"2024-11-07","div_listdate":None,"base_date":"2024-11-06","base_share":"479541.5625"},
        {"ts_code":"600989.SH","end_date":"2023-12-31","ann_date":"2024-03-22","imp_ann_date":"2024-07-18","ex_date":"2024-07-24","stk_div":"0","stk_bo_rate":None,"stk_co_rate":None,"cash_div":"0.265","cash_div_tax":"0.265","record_date":"2024-07-23","pay_date":"2024-07-24","div_listdate":None,"base_date":"2024-07-18","base_share":"516047.0063"},
        {"ts_code":"600989.SH","end_date":"2023-12-31","ann_date":"2024-06-22","imp_ann_date":"2024-07-18","ex_date":"2024-07-24","stk_div":"0","stk_bo_rate":None,"stk_co_rate":None,"cash_div":"0.3158","cash_div_tax":"0.3158","record_date":"2024-07-23","pay_date":"2024-07-24","div_listdate":None,"base_date":"2024-07-18","base_share":"217288.9937"},
    )
}


def database_config(target: str, env_file: Path) -> dict[str, Any]:
    values = dotenv_values(env_file)
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    keys = {"host":"HOST", "port":"PORT", "dbname":"NAME", "user":"USER", "password":"PASSWORD"}
    config = {key: values.get(prefix + suffix) for key, suffix in keys.items()}
    if any(value in (None, "") for value in config.values()):
        raise AuthorityBuildError(f"{target} database configuration is incomplete")
    config["port"] = int(config["port"])
    if target == "dev" and (config["dbname"], config["port"]) != ("aistock_dev", 5433):
        raise AuthorityBuildError("dev database target identity is invalid")
    if target == "production" and (config["dbname"], config["port"]) != ("aistock", 5432):
        raise AuthorityBuildError("production database target identity is invalid")
    return config


def read_source_rows(connection: Any, *, symbols: Sequence[str], start: dt.date, end: dt.date) -> tuple[tuple[Any, ...], ...]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT ts_code, end_date, ann_date, imp_ann_date, ex_date,
                   stk_div, stk_bo_rate, stk_co_rate, cash_div, cash_div_tax,
                   record_date, pay_date, div_listdate, base_date, base_share
              FROM market.dividend
             WHERE ts_code = ANY(%s)
               AND ex_date BETWEEN %s AND %s
               AND trim(div_proc) = %s
             ORDER BY ts_code, ex_date, imp_ann_date DESC NULLS LAST,
                      end_date DESC, ann_date DESC NULLS LAST
            """,
            (list(symbols), start, end, IMPLEMENTED_DIVIDEND),
        )
        return tuple(tuple(row) for row in cursor.fetchall())


def classify_target_rows(rows: Sequence[Sequence[Any]]) -> tuple[dict[str, Any], ...]:
    portable = tuple(portable_row(row) for row in rows)
    actual = {_signature(row) for row in portable}
    if len(portable) != 5 or actual != EXPECTED_TARGET_ROWS:
        raise AuthorityBuildError("PT-NEXT-020 target source rows drifted")
    classified: list[dict[str, Any]] = []
    for row in sorted(portable, key=lambda item: _signature(item)):
        symbol, cash = row["ts_code"], row["cash_div"]
        if symbol == "002352.SZ" and cash == "0.4":
            classification, accumulation, action_id = (
                "DISTINCT_SAME_DAY_ACTION", "INCLUDE_COMPONENT",
                "002352.SZ:2024-11-07:2024_INTERIM",
            )
            applicability = "ALL_ELIGIBLE_A_SHARE_HOLDERS"
        elif symbol == "002352.SZ" and cash == "1":
            classification, accumulation, action_id = (
                "DISTINCT_SAME_DAY_ACTION", "INCLUDE_COMPONENT",
                "002352.SZ:2024-11-07:SPECIAL",
            )
            applicability = "ALL_ELIGIBLE_A_SHARE_HOLDERS"
        elif symbol == "002352.SZ" and cash == "1.4":
            classification, accumulation, action_id = (
                "AGGREGATE_OF_COMPONENTS", "EXCLUDE_AGGREGATE_COMPONENTS_SELECTED",
                "002352.SZ:2024-11-07:COMBINED_AGGREGATE",
            )
            applicability = "ALL_ELIGIBLE_A_SHARE_HOLDERS"
        elif symbol == "600989.SH" and cash == "0.3158":
            classification, accumulation, action_id = (
                "FINAL_IMPLEMENTED_HOLDER_CLASS_RATE", "INCLUDE_ACCOUNT_CLASS",
                "600989.SH:2024-07-24:PUBLIC_HOLDER_RATE",
            )
            applicability = ACCOUNT_CLASS
        elif symbol == "600989.SH" and cash == "0.265":
            classification, accumulation, action_id = (
                "FINAL_IMPLEMENTED_HOLDER_CLASS_RATE", "EXCLUDE_MUTUALLY_EXCLUSIVE_HOLDER_CLASS",
                "600989.SH:2024-07-24:DESIGNATED_MAJOR_HOLDER_RATE",
            )
            applicability = "DESIGNATED_MAJOR_HOLDERS_ONLY"
        else:
            raise AuthorityBuildError("unclassified PT-NEXT-020 source row")
        classified.append(
            {
                "source_row": row,
                "source_row_sha256": canonical_sha256(row),
                "classification": classification,
                "accumulation_policy": accumulation,
                "economic_action_id": action_id,
                "holder_applicability": applicability,
            }
        )
    return tuple(classified)


def resolution_payloads(classified: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    by_symbol = {
        symbol: [entry for entry in classified if entry["source_row"]["ts_code"] == symbol]
        for symbol, _ in TARGET_KEYS
    }
    return (
        {
            "symbol": "002352.SZ", "effective_trade_date": "2024-11-07",
            "resolution_type": "COMPONENTS_WITH_NON_ACCUMULATING_AGGREGATE",
            "account_class": ACCOUNT_CLASS,
            "account_cash_yuan_per_share": "1.4",
            "reference_price_cash_yuan_per_share": "1.3939620",
            "canonical_economic_action_count": 2,
            "source_rows_sha256": canonical_sha256([entry["source_row"] for entry in by_symbol["002352.SZ"]]),
            "row_classifications": by_symbol["002352.SZ"],
            "official_document_ids": ["002352_2024_INTERIM_PLAN", "002352_2024_SPECIAL_PLAN", "002352_2024_COMBINED_IMPLEMENTATION"],
        },
        {
            "symbol": "600989.SH", "effective_trade_date": "2024-07-24",
            "resolution_type": "MUTUALLY_EXCLUSIVE_HOLDER_CLASS_RATES",
            "account_class": ACCOUNT_CLASS,
            "account_cash_yuan_per_share": "0.3158",
            "reference_price_cash_yuan_per_share": "0.28",
            "canonical_economic_action_count": 1,
            "source_rows_sha256": canonical_sha256([entry["source_row"] for entry in by_symbol["600989.SH"]]),
            "row_classifications": by_symbol["600989.SH"],
            "official_document_ids": ["600989_2023_DISTRIBUTION_ADJUSTMENT", "600989_2023_DISTRIBUTION_IMPLEMENTATION"],
        },
    )


def _stable_portable_rows(rows: Sequence[Sequence[Any]]) -> list[dict[str, Any]]:
    return sorted(
        (portable_row(row) for row in rows),
        key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
    )


def _factor_boundary_evidence(
    candidate: DailyCandidate,
    *,
    symbol: str,
    effective_trade_date: dt.date,
    expected: Mapping[str, str],
) -> dict[str, Any]:
    bars = candidate.bars(symbol)
    factors = bars["factor"]
    valid = factors.notna() & factors.map(lambda value: Decimal(str(value)).is_finite()) & factors.gt(0)
    valid_dates = tuple(item.date() for item in bars.index[valid])
    valid_values = tuple(Decimal(str(item)) for item in factors.loc[valid].tolist())
    if not valid_dates:
        raise AuthorityBuildError(f"full-scope factor history unavailable: {symbol}")
    pit_rows = candidate.spans.loc[candidate.spans.symbol.eq(symbol)]
    if pit_rows.empty:
        raise AuthorityBuildError(f"full-scope PIT span unavailable: {symbol}")
    first_pit_eligible = min(item.date() for item in pit_rows.start)
    evidence: dict[str, Any] = {
        "factor_file": candidate.references[f"{symbol}:factor"],
        "first_valid_factor_date": valid_dates[0].isoformat(),
        "first_pit_eligible_date": first_pit_eligible.isoformat(),
    }
    if "first_valid" in expected:
        if (
            valid_dates[0].isoformat() != expected["first_valid"]
            or first_pit_eligible.isoformat() != expected["first_pit_eligible"]
            or any(day < effective_trade_date for day in valid_dates)
        ):
            raise AuthorityBuildError(f"full-scope prehistory boundary drifted: {symbol}")
        evidence.update(
            {
                "boundary_type": "NO_PRE_ACTION_FACTOR_IN_CANDIDATE",
                "previous_factor_date": None,
                "current_factor_date": valid_dates[0].isoformat(),
                "observed_factor_ratio": None,
            }
        )
        return evidence

    previous = [index for index, day in enumerate(valid_dates) if day < effective_trade_date]
    following = [index for index, day in enumerate(valid_dates) if day >= effective_trade_date]
    if not previous or not following:
        raise AuthorityBuildError(f"full-scope factor interval unavailable: {symbol}")
    previous_index, current_index = previous[-1], following[0]
    observed = valid_values[current_index] / valid_values[previous_index]
    if (
        valid_dates[previous_index].isoformat() != expected["previous"]
        or valid_dates[current_index].isoformat() != expected["current"]
    ):
        raise AuthorityBuildError(f"full-scope factor interval drifted: {symbol}")
    evidence.update(
        {
            "boundary_type": "MATERIAL_FACTOR_INTERVAL",
            "previous_factor_date": valid_dates[previous_index].isoformat(),
            "current_factor_date": valid_dates[current_index].isoformat(),
            "observed_factor_ratio": str(observed),
        }
    )
    return evidence


def build_full_scope_authority(
    rows: Sequence[Sequence[Any]],
    *,
    candidate: DailyCandidate,
    candidate_manifest: Mapping[str, Any],
    captured_at: dt.datetime,
    specs: Mapping[tuple[str, str], Mapping[str, Any]] = FULL_SCOPE_RESOLUTION_SPECS,
) -> dict[str, Any]:
    if captured_at.tzinfo is None:
        raise AuthorityBuildError("captured_at must be timezone-aware")
    grouped: dict[tuple[str, str], list[Sequence[Any]]] = {key: [] for key in specs}
    for row in rows:
        key = (str(row[0]).upper(), str(_date_text(row[4])))
        if key in grouped:
            grouped[key].append(row)
    resolutions: list[dict[str, Any]] = []
    for key, spec in sorted(specs.items()):
        symbol, effective_text = key
        stable_rows = _stable_portable_rows(grouped[key])
        source_rows_sha256 = canonical_sha256(stable_rows)
        if (
            len(stable_rows) != int(spec["source_row_count"])
            or source_rows_sha256 != spec["source_rows_sha256"]
        ):
            raise AuthorityBuildError(f"full-scope source rows drifted: {symbol}/{effective_text}")
        factor_evidence = _factor_boundary_evidence(
            candidate,
            symbol=symbol,
            effective_trade_date=dt.date.fromisoformat(effective_text),
            expected=spec["factor_boundary"],
        )
        reference = spec.get("reference_price_economics")
        if reference is not None and reference.get("factor_ratio") is not None:
            if factor_evidence["observed_factor_ratio"] != reference["factor_ratio"]:
                raise AuthorityBuildError(f"full-scope factor ratio drifted: {symbol}")
        row_classifications = [
            {
                "source_row": row,
                "source_row_sha256": canonical_sha256(row),
                "classification": spec["classification"],
                "may_accumulate_as_separate_account_action": False,
            }
            for row in stable_rows
        ]
        resolutions.append(
            {
                "symbol": symbol,
                "effective_trade_date": effective_text,
                "classification": spec["classification"],
                "accumulation_policy": spec["accumulation_policy"],
                "replay_application": spec["replay_application"],
                "account_class": ACCOUNT_CLASS,
                "account_economics": spec["account_economics"],
                "reference_price_economics": reference,
                "source_row_count": len(stable_rows),
                "source_rows_sha256": source_rows_sha256,
                "row_classifications": row_classifications,
                "factor_boundary_evidence": factor_evidence,
                "announcement_url": spec.get("announcement_url"),
                "source_record_key": f"market.dividend:{symbol}:{effective_text}",
            }
        )
    classification_counts: dict[str, int] = {}
    for resolution in resolutions:
        classification = resolution["classification"]
        classification_counts[classification] = classification_counts.get(classification, 0) + 1
    identity = {
        "schema_version": FULL_SCOPE_AUTHORITY_SCHEMA,
        "request_id": REQUEST_ID,
        "candidate_manifest": dict(candidate_manifest),
        "account_class": ACCOUNT_CLASS,
        "scope": {
            "resolution_count": len(resolutions),
            "symbols": sorted({item["symbol"] for item in resolutions}),
            "start": candidate.calendar[0].date().isoformat(),
            "end": candidate.calendar[-1].date().isoformat(),
        },
        "classification_counts": classification_counts,
        "resolutions": resolutions,
        "captured_at": captured_at.isoformat(),
        "consumer_contract": {
            "legacy_snapshot_schema": SNAPSHOT_SCHEMA,
            "typed_authority_reader_required": True,
            "account_economics_must_not_be_inferred_from_factor": True,
            "prehistory_actions_must_not_be_applied_to_new_window_positions": True,
        },
        "safety": {
            "database_write_performed": False,
            "candidate_write_performed": False,
            "adj_factor_write_performed": False,
            "outcomes_read": False,
            "runtime_action_performed": False,
        },
    }
    return {**identity, "canonical_sha256": canonical_sha256(identity)}


def _synthetic_rows() -> tuple[tuple[Any, ...], ...]:
    return (
        ("002352.SZ", dt.date(2024, 6, 30), dt.date(2024, 10, 31), dt.date(2024, 10, 31), dt.date(2024, 11, 7), Decimal("0"), None, None, Decimal("1.4"), Decimal("1.3939620"), dt.date(2024, 11, 6), dt.date(2024, 11, 7), None, dt.date(2024, 11, 6), Decimal("479541.5625")),
        ("600989.SH", dt.date(2023, 12, 31), dt.date(2024, 7, 18), dt.date(2024, 7, 18), dt.date(2024, 7, 24), Decimal("0"), None, None, Decimal("0.3158"), Decimal("0.28"), dt.date(2024, 7, 23), dt.date(2024, 7, 24), None, dt.date(2024, 7, 18), Decimal("217288.9937")),
    )


def build_snapshot(rows: Sequence[Sequence[Any]], *, symbols: Sequence[str], start: dt.date, end: dt.date, authority_sha256: str, resolutions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    target_rows = [row for row in rows if (str(row[0]).upper(), _date_text(row[4])) in TARGET_KEYS]
    classify_target_rows(target_rows)
    retained = [row for row in rows if (str(row[0]).upper(), _date_text(row[4])) not in TARGET_KEYS]
    payload = _snapshot_payload((*retained, *_synthetic_rows()), symbols=symbols, start=start, end=end)
    if payload.get("schema_version") != SNAPSHOT_SCHEMA:
        raise AuthorityBuildError("corporate-action snapshot schema differs")
    resolution_by_key = {(item["symbol"], item["effective_trade_date"]): item for item in resolutions}
    for action in payload["actions"]:
        resolution = resolution_by_key.get((action["symbol"], action["effective_trade_date"]))
        if resolution is not None:
            action["source_row_count"] = len(resolution["row_classifications"])
            action["source_rows_sha256"] = resolution["source_rows_sha256"]
            action["source_economic_action_count"] = resolution["canonical_economic_action_count"]
    payload["raw_source_row_count"] = len(rows)
    payload["canonical_economic_action_count"] += 1
    payload["combined_same_day_economic_action_count"] += 1
    payload["resolution_authority_canonical_sha256"] = authority_sha256
    payload["source_query"] = {
        "table": "market.dividend",
        "filter": "trim(div_proc)=IMPLEMENTED AND ex_date BETWEEN start AND end AND ts_code IN symbols",
        "resolution_contract": AUTHORITY_SCHEMA,
        "account_class": ACCOUNT_CLASS,
        "database_writes": False,
    }
    identity = {key: value for key, value in payload.items() if key != "snapshot_sha256"}
    payload["snapshot_sha256"] = canonical_sha256(identity)
    return payload


def download_documents(
    folder: Path,
    *,
    source_document_root: Path | None = None,
) -> tuple[dict[str, Any], ...]:
    folder.mkdir()
    if source_document_root is not None:
        source_document_root = source_document_root.resolve(strict=True)
        if not source_document_root.is_dir():
            raise AuthorityBuildError("source document root is not a directory")
    references: list[dict[str, Any]] = []
    for spec in DOCUMENTS:
        if source_document_root is None:
            request = Request(str(spec["capture_url"]), headers={"User-Agent": "AIstock-local-data-authority/1.0"})
            with urlopen(request, timeout=60) as response:
                body = response.read()
        else:
            source_path = source_document_root / str(spec["filename"])
            if not source_path.is_file() or source_path.is_symlink():
                raise AuthorityBuildError(f"pinned source document unavailable: {spec['document_id']}")
            body = source_path.read_bytes()
        if not body.startswith(b"%PDF-"):
            raise AuthorityBuildError(f"source document is not PDF: {spec['document_id']}")
        if len(body) != spec["source_file_size"] or sha256_bytes(body) != spec["source_content_sha256"]:
            raise AuthorityBuildError(f"source document identity differs: {spec['document_id']}")
        path = folder / str(spec["filename"])
        with path.open("xb") as handle:
            handle.write(body)
        references.append({**dict(spec), "artifact_relative_path": f"sources/{spec['filename']}"})
    return tuple(references)


def _write_canonical_exclusive(path: Path, payload: Mapping[str, Any]) -> None:
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(payload) + b"\n")


def build_artifact(
    *,
    connection: Any,
    target: str,
    candidate_root: Path,
    output_root: Path,
    source_document_root: Path | None = None,
    captured_at: dt.datetime | None = None,
) -> Mapping[str, Any]:
    candidate_root = candidate_root.resolve(strict=True)
    output_root = output_root.absolute()
    if not candidate_root.is_dir() or not output_root.is_absolute() or output_root.is_relative_to(REPOSITORY_ROOT.resolve()):
        raise AuthorityBuildError("artifact paths are invalid")
    if output_root.exists():
        raise AuthorityBuildError("output root already exists")
    manifest_path = candidate_root / "qe_dataset_manifest.json"
    manifest_reference = file_reference(manifest_path)
    if manifest_reference["sha256"] != EXPECTED_CANDIDATE_MANIFEST_SHA256:
        raise AuthorityBuildError("r5 candidate manifest identity differs")
    candidate = DailyCandidate.open(candidate_root)
    symbols = candidate.symbols
    start, end = candidate.calendar[0].date(), candidate.calendar[-1].date()
    if (start, end, len(candidate.calendar)) != (dt.date(2018, 8, 1), dt.date(2026, 8, 31), 1961):
        raise AuthorityBuildError("r5 calendar identity differs")
    rows = read_source_rows(connection, symbols=symbols, start=start, end=end)
    target_rows = [row for row in rows if (str(row[0]).upper(), _date_text(row[4])) in TARGET_KEYS]
    classified = classify_target_rows(target_rows)
    resolutions = resolution_payloads(classified)
    captured = captured_at or dt.datetime.now(dt.timezone.utc)
    if captured.tzinfo is None:
        raise AuthorityBuildError("captured_at must be timezone-aware")
    parent = output_root.parent
    parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_root.name}.staging-", dir=parent))
    try:
        documents = download_documents(
            staging / "sources",
            source_document_root=source_document_root,
        )
        unsigned_authority = {
            "schema_version": AUTHORITY_SCHEMA,
            "request_id": REQUEST_ID,
            "candidate_manifest": manifest_reference,
            "scope": {"symbols": list(symbols), "start": start.isoformat(), "end": end.isoformat()},
            "database_source": {
                "target": target,
                "database_name": connection.get_dsn_parameters().get("dbname"),
                "transaction_isolation": "REPEATABLE READ",
                "transaction_read_only": True,
                "query_row_count": len(rows),
                "ordered_rows_sha256": canonical_sha256([portable_row(row) for row in rows]),
            },
            "account_class": ACCOUNT_CLASS,
            "official_documents": list(documents),
            "resolutions": list(resolutions),
            "captured_at": captured.isoformat(),
            "safety": {"database_write_performed": False, "candidate_write_performed": False, "outcomes_read": False, "runtime_action_performed": False},
        }
        authority_sha256 = canonical_sha256(unsigned_authority)
        authority = {**unsigned_authority, "canonical_sha256": authority_sha256}
        authority_path = staging / "corporate_action_resolution_authority.json"
        _write_canonical_exclusive(authority_path, authority)
        full_scope_authority = build_full_scope_authority(
            rows,
            candidate=candidate,
            candidate_manifest=manifest_reference,
            captured_at=captured,
        )
        full_scope_authority_path = staging / "corporate_action_full_scope_authority.json"
        _write_canonical_exclusive(full_scope_authority_path, full_scope_authority)
        snapshot = build_snapshot(rows, symbols=symbols, start=start, end=end, authority_sha256=authority_sha256, resolutions=resolutions)
        snapshot_path = staging / "corporate_action_snapshot_v6.json"
        _write_canonical_exclusive(snapshot_path, snapshot)
        opened = CorporateActionBook.open(snapshot_path)
        expected = {("002352.SZ", dt.date(2024, 11, 7)): (Decimal("1.4"), Decimal("1.3939620")), ("600989.SH", dt.date(2024, 7, 24)): (Decimal("0.3158"), Decimal("0.28"))}
        for key, economics in expected.items():
            action = opened.on(*key)
            if action is None or (action.cashflow_yuan_per_share, action.reference_price_cash_yuan_per_share) != economics:
                raise AuthorityBuildError("resolved snapshot consumer readback differs")
        receipt = {
            "schema_version": RECEIPT_SCHEMA,
            "artifact_root": output_root.as_posix(),
            "candidate_manifest_sha256": manifest_reference["sha256"],
            "authority_canonical_sha256": authority_sha256,
            "authority_file_sha256": sha256_file(authority_path),
            "full_scope_authority_schema_version": FULL_SCOPE_AUTHORITY_SCHEMA,
            "full_scope_authority_canonical_sha256": full_scope_authority["canonical_sha256"],
            "full_scope_authority_file_sha256": sha256_file(full_scope_authority_path),
            "full_scope_resolution_count": len(full_scope_authority["resolutions"]),
            "full_scope_classification_counts": full_scope_authority["classification_counts"],
            "position_timing_typed_authority_reader_required": True,
            "snapshot_sha256": snapshot["snapshot_sha256"],
            "snapshot_file_sha256": sha256_file(snapshot_path),
            "source_document_count": len(documents),
            "source_rows": len(rows),
            "resolved_conflict_groups": len(resolutions),
            "resolved_source_rows": len(classified),
            "account_class": ACCOUNT_CLASS,
            "corporate_action_economic_conflict": 0,
            "unresolved_revision": 0,
            "aggregate_component_double_count": 0,
            "database_write_performed": False,
            "candidate_write_performed": False,
            "active_profile_write_performed": False,
            "outcomes_read": False,
            "runtime_action_performed": False,
        }
        _write_canonical_exclusive(staging / "receipt.json", receipt)
        os.replace(staging, output_root)
        return receipt
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("dev", "production"), required=True)
    parser.add_argument("--env-file", type=Path, default=REPOSITORY_ROOT / ".env")
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--source-document-root",
        type=Path,
        help="Optional directory of pinned official PDFs; every file is reverified by size and SHA-256.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    config = database_config(args.target, args.env_file)
    connection = psycopg2.connect(**config)
    try:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ", autocommit=False)
        receipt = build_artifact(
            connection=connection,
            target=args.target,
            candidate_root=args.candidate_root,
            output_root=args.output_root,
            source_document_root=args.source_document_root,
        )
        connection.rollback()
    finally:
        connection.close()
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
