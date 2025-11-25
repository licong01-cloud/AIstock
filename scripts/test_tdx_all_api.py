#!/usr/bin/env python3
"""TDX API comprehensive smoke test.

Covers all documented endpoints in tdx_doc/API_接口文档.md, verifying response
structure compatibility with ingestion scripts and measuring basic availability.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional

import requests

DEFAULT_CODES = ["000001", "600000", "601318"]
DEFAULT_INDEX = "sh000001"
DEFAULT_TIMEOUT = 10.0


class TestFailure(Exception):
    """Raised when an API endpoint does not satisfy expected contract."""


class ApiTester:
    def __init__(
        self,
        base_url: str,
        codes: List[str],
        index_code: str,
        trade_date: dt.date,
        start_date: dt.date,
        end_date: dt.date,
        timeout: float,
        bulk_timeout: Optional[float],
        verbose: bool,
        include_tasks: bool,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.codes = codes
        self.index_code = index_code
        self.trade_date = trade_date
        self.start_date = start_date
        self.end_date = end_date
        self.timeout = timeout
        self.bulk_timeout = bulk_timeout
        self.verbose = verbose
        self.include_tasks = include_tasks
        self.results: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------
    def _get(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        effective_timeout = self._resolve_timeout(timeout)
        response = requests.get(url, params=params, timeout=effective_timeout)
        response.raise_for_status()
        data = response.json()
        self._ensure_standard_response(path, data)
        return data

    def _post(
        self,
        path: str,
        *,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        expect_ok: bool = True,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        effective_timeout = self._resolve_timeout(timeout)
        response = requests.post(url, params=params, json=json_body, timeout=effective_timeout)
        response.raise_for_status()
        data = response.json()
        if expect_ok:
            self._ensure_standard_response(path, data)
        return data

    def _resolve_timeout(self, timeout: Optional[float]) -> Optional[float]:
        if timeout is None:
            return self.timeout
        if timeout <= 0:
            return None
        return timeout

    @staticmethod
    def _ensure_standard_response(path: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            raise TestFailure(f"{path}: response is not JSON dict")
        if payload.get("code") != 0:
            raise TestFailure(f"{path}: code={payload.get('code')} message={payload.get('message')}" )

    def _record(self, name: str, success: bool, detail: str, extra: Optional[Dict[str, Any]] = None) -> None:
        item = {"name": name, "success": success, "detail": detail}
        if extra:
            item.update(extra)
        self.results.append(item)
        if self.verbose or not success:
            status = "OK" if success else "FAIL"
            print(f"[{status:<4}] {name}: {detail}")

    # ------------------------------------------------------------------
    # Individual endpoint tests
    # ------------------------------------------------------------------
    def _test_quote(self) -> None:
        path = "/api/quote"
        single = self._get(path, params={"code": self.codes[0]})
        data = single.get("data")
        if not isinstance(data, list) or not data:
            raise TestFailure("/api/quote single: empty list")
        multi = self._get(path, params={"code": ",".join(self.codes[:2])})
        mdata = multi.get("data")
        if not isinstance(mdata, list) or len(mdata) < 2:
            raise TestFailure("/api/quote multi: insufficient rows")
        self._record("GET /api/quote", True, f"single={len(data)} multi={len(mdata)}")

    def _test_kline(self) -> None:
        payload = self._get("/api/kline", params={"code": self.codes[0], "type": "day"})
        block = payload.get("data", {})
        if not isinstance(block, dict) or "List" not in block:
            raise TestFailure("/api/kline: missing List")
        self._record("GET /api/kline", True, f"items={len(block['List'])}")

    def _test_minute(self) -> None:
        payload = self._get(
            "/api/minute",
            params={"code": self.codes[0], "date": self.trade_date.strftime("%Y%m%d")},
        )
        block = payload.get("data", {})
        if not isinstance(block, dict) or "List" not in block:
            raise TestFailure("/api/minute: missing List")
        self._record(
            "GET /api/minute",
            True,
            f"date={block.get('date')} items={len(block.get('List', []))}",
        )

    def _test_trade(self) -> None:
        payload = self._get(
            "/api/trade",
            params={"code": self.codes[0], "date": self.trade_date.strftime("%Y%m%d")},
        )
        block = payload.get("data", {})
        if not isinstance(block, dict) or "List" not in block:
            raise TestFailure("/api/trade: missing List")
        self._record(
            "GET /api/trade",
            True,
            f"items={len(block.get('List', []))}",
        )

    def _test_search(self) -> None:
        payload = self._get("/api/search", params={"keyword": self.codes[0][:3]})
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            raise TestFailure("/api/search: no results")
        self._record("GET /api/search", True, f"matches={len(data)}")

    def _test_stock_info(self) -> None:
        payload = self._get("/api/stock-info", params={"code": self.codes[0]})
        data = payload.get("data", {})
        if not all(k in data for k in ("quote", "kline_day", "minute")):
            raise TestFailure("/api/stock-info: missing sections")
        self._record("GET /api/stock-info", True, "contains quote/kline_day/minute")

    def _test_codes(self) -> None:
        payload = self._get("/api/codes")
        data = payload.get("data", {})
        codes = data.get("codes") if isinstance(data, dict) else None
        if not isinstance(codes, list) or not codes:
            raise TestFailure("/api/codes: empty codes")
        self._record("GET /api/codes", True, f"total={data.get('total')} sample={codes[0]['code']}")

    def _test_codes_exchange(self) -> None:
        payload = self._get("/api/codes", params={"exchange": "sh"})
        data = payload.get("data", {})
        codes = data.get("codes") if isinstance(data, dict) else None
        if not isinstance(codes, list) or not codes:
            raise TestFailure("/api/codes?exchange=sh: empty list")
        sample = codes[0]
        if sample.get("exchange") != "sh":
            raise TestFailure("/api/codes?exchange=sh: wrong exchange")
        self._record("GET /api/codes?exchange=sh", True, f"count={len(codes)}")

    def _test_batch_quote(self) -> None:
        payload = self._post("/api/batch-quote", json_body={"codes": self.codes[:3]})
        data = payload.get("data")
        if not isinstance(data, list) or len(data) != len(self.codes[:3]):
            raise TestFailure("/api/batch-quote: unexpected rows")
        self._record("POST /api/batch-quote", True, f"items={len(data)}")

    def _test_kline_history(self) -> None:
        payload = self._get(
            "/api/kline-history",
            params={
                "code": self.codes[0],
                "type": "day",
                "start_date": self.start_date.strftime("%Y%m%d"),
                "end_date": self.end_date.strftime("%Y%m%d"),
                "limit": 50,
            },
        )
        data = payload.get("data", {})
        if not isinstance(data, dict) or "List" not in data:
            raise TestFailure("/api/kline-history: missing List")
        self._record("GET /api/kline-history", True, f"items={len(data['List'])}")

    def _test_index(self) -> None:
        payload = self._get("/api/index", params={"code": self.index_code, "type": "day"})
        data = payload.get("data", {})
        if not isinstance(data, dict) or "List" not in data:
            raise TestFailure("/api/index: missing List")
        self._record("GET /api/index", True, f"items={len(data['List'])}")

    def _test_server_status(self) -> None:
        payload = self._get("/api/server-status")
        data = payload.get("data", {})
        if data.get("status") != "running":
            raise TestFailure("/api/server-status: status not running")
        self._record("GET /api/server-status", True, "status=running")

    def _test_etf(self) -> None:
        payload = self._get("/api/etf", params={"limit": 5})
        data = payload.get("data", {})
        if not isinstance(data, dict) or not data.get("list"):
            raise TestFailure("/api/etf: empty list")
        self._record("GET /api/etf", True, f"items={len(data['list'])}")

    def _test_trade_history(self) -> None:
        payload = self._get(
            "/api/trade-history",
            params={
                "code": self.codes[0],
                "date": self.trade_date.strftime("%Y%m%d"),
                "start": 0,
                "count": 10,
            },
        )
        data = payload.get("data", {})
        if not isinstance(data, dict) or "List" not in data:
            raise TestFailure("/api/trade-history: missing List")
        self._record("GET /api/trade-history", True, f"items={len(data['List'])}")

    def _test_minute_trade_all(self) -> None:
        payload = self._get(
            "/api/minute-trade-all",
            params={"code": self.codes[0], "date": self.trade_date.strftime("%Y%m%d")},
        )
        data = payload.get("data", {})
        if not isinstance(data, dict) or "List" not in data:
            raise TestFailure("/api/minute-trade-all: missing List")
        self._record("GET /api/minute-trade-all", True, f"items={len(data['List'])}")

    def _test_workday(self) -> None:
        payload = self._get("/api/workday", params={"date": self.trade_date.strftime("%Y%m%d"), "count": 2})
        data = payload.get("data", {})
        if not isinstance(data, dict) or "is_workday" not in data:
            raise TestFailure("/api/workday: invalid payload")
        self._record("GET /api/workday", True, f"is_workday={data.get('is_workday')}")

    def _test_market_count(self) -> None:
        payload = self._get("/api/market-count")
        data = payload.get("data", {})
        if not isinstance(data, dict) or "exchanges" not in data:
            raise TestFailure("/api/market-count: missing exchanges")
        self._record("GET /api/market-count", True, f"total={data.get('total')}")

    def _test_stock_codes(self) -> None:
        payload = self._get("/api/stock-codes", params={"limit": 10, "prefix": True})
        data = payload.get("data", {})
        lst = data.get("list") if isinstance(data, dict) else None
        if not isinstance(lst, list) or not lst:
            raise TestFailure("/api/stock-codes: empty list")
        self._record("GET /api/stock-codes", True, f"items={len(lst)}")

    def _test_etf_codes(self) -> None:
        payload = self._get("/api/etf-codes", params={"limit": 10})
        data = payload.get("data", {})
        lst = data.get("list") if isinstance(data, dict) else None
        if not isinstance(lst, list) or not lst:
            raise TestFailure("/api/etf-codes: empty list")
        self._record("GET /api/etf-codes", True, f"items={len(lst)}")

    def _test_kline_all(self) -> None:
        start = time.perf_counter()
        payload = self._get(
            "/api/kline-all",
            params={"code": self.codes[0], "type": "day", "limit": 200},
            timeout=self.bulk_timeout,
        )
        elapsed = time.perf_counter() - start
        data = payload.get("data")
        if isinstance(data, dict):
            data_list = data.get("list")
        else:
            data_list = data
        if not isinstance(data_list, list) or not data_list:
            raise TestFailure("/api/kline-all: empty list")
        self._record("GET /api/kline-all", True, f"items={len(data_list)} time={elapsed:.2f}s")

    def _test_index_all(self) -> None:
        start = time.perf_counter()
        payload = self._get(
            "/api/index/all",
            params={"code": self.index_code, "type": "day", "limit": 200},
            timeout=self.bulk_timeout,
        )
        elapsed = time.perf_counter() - start
        data = payload.get("data")
        if isinstance(data, dict):
            data_list = data.get("list")
        else:
            data_list = data
        if not isinstance(data_list, list) or not data_list:
            raise TestFailure("/api/index/all: empty list")
        self._record("GET /api/index/all", True, f"items={len(data_list)} time={elapsed:.2f}s")

    def _test_trade_history_full(self) -> None:
        start = time.perf_counter()
        payload = self._get(
            "/api/trade-history/full",
            params={"code": self.codes[0], "before": self.end_date.strftime("%Y%m%d"), "limit": 300},
            timeout=self.bulk_timeout,
        )
        elapsed = time.perf_counter() - start
        data = payload.get("data")
        if isinstance(data, dict):
            data_list = data.get("list")
        else:
            data_list = data
        if not isinstance(data_list, list) or not data_list:
            raise TestFailure("/api/trade-history/full: empty or invalid data")
        self._record("GET /api/trade-history/full", True, f"items={len(data_list)} time={elapsed:.2f}s")

    def _test_workday_range(self) -> None:
        payload = self._get(
            "/api/workday/range",
            params={
                "start": (self.end_date - dt.timedelta(days=10)).strftime("%Y%m%d"),
                "end": self.end_date.strftime("%Y%m%d"),
            },
        )
        data = payload.get("data")
        if isinstance(data, dict):
            data_list = data.get("list")
        else:
            data_list = data
        if not isinstance(data_list, list) or not data_list:
            raise TestFailure("/api/workday/range: empty list")
        self._record("GET /api/workday/range", True, f"days={len(data_list)}")

    def _test_income(self) -> None:
        payload = self._get(
            "/api/income",
            params={
                "code": self.codes[0],
                "start_date": self.start_date.strftime("%Y-%m-%d"),
                "days": "5,10,20",
            },
        )
        data = payload.get("data", {})
        lst = data.get("list") if isinstance(data, dict) else None
        if not isinstance(lst, list) or not lst:
            raise TestFailure("/api/income: empty list")
        self._record("GET /api/income", True, f"items={len(lst)}")

    def _test_tasks(self) -> None:
        snapshot = self._get("/api/tasks")
        existing = snapshot.get("data")
        if existing is None:
            raise TestFailure("/api/tasks: invalid response")
        self._record("GET /api/tasks", True, f"count={len(existing) if isinstance(existing, list) else 'unknown'}")

        created_ids: List[str] = []
        try:
            kline_resp = self._post(
                "/api/tasks/pull-kline",
                json_body={
                    "codes": self.codes[:1],
                    "tables": ["day"],
                    "limit": 1,
                    "start_date": self.start_date.strftime("%Y-%m-%d"),
                },
            )
            task_id = self._extract_task_id(kline_resp, "pull_kline")
            created_ids.append(task_id)
            self._record("POST /api/tasks/pull-kline", True, f"task_id={task_id}")

            trade_resp = self._post(
                "/api/tasks/pull-trade",
                json_body={
                    "code": self.codes[0],
                    "start_year": self.start_date.year,
                    "end_year": self.end_date.year,
                },
            )
            trade_task = self._extract_task_id(trade_resp, "pull_trade")
            created_ids.append(trade_task)
            self._record("POST /api/tasks/pull-trade", True, f"task_id={trade_task}")

            for task_id in created_ids:
                detail = self._get(f"/api/tasks/{task_id}")
                self._record(f"GET /api/tasks/{task_id}", True, detail.get("data", {}).get("status", "unknown"))

                cancel = self._post(f"/api/tasks/{task_id}/cancel", expect_ok=False)
                code = cancel.get("code")
                if code == 0:
                    status = cancel.get("message", "cancelled")
                    self._record(f"POST /api/tasks/{task_id}/cancel", True, status)
                else:
                    msg = cancel.get("message", "")
                    if "不存在" in msg or "已结束" in msg:
                        self._record(
                            f"POST /api/tasks/{task_id}/cancel",
                            True,
                            f"already finished ({msg})",
                        )
                    else:
                        raise TestFailure(f"/api/tasks/{task_id}/cancel: {msg}")

        finally:
            # Give backend a moment to process cancellations to avoid lingering tasks.
            if created_ids:
                time.sleep(1.0)

    @staticmethod
    def _extract_task_id(payload: Dict[str, Any], label: str) -> str:
        data = payload.get("data", {})
        task_id = data.get("task_id") if isinstance(data, dict) else None
        if not task_id:
            raise TestFailure(f"task response ({label}) missing task_id: {payload}")
        return task_id

    # ------------------------------------------------------------------
    def run(self) -> None:
        tests = [
            self._test_quote,
            self._test_kline,
            self._test_minute,
            self._test_trade,
            self._test_search,
            self._test_stock_info,
            self._test_codes,
            self._test_codes_exchange,
            self._test_batch_quote,
            self._test_kline_history,
            self._test_index,
            self._test_server_status,
            self._test_etf,
            self._test_trade_history,
            self._test_minute_trade_all,
            self._test_workday,
            self._test_market_count,
            self._test_stock_codes,
            self._test_etf_codes,
            self._test_kline_all,
            self._test_index_all,
            self._test_trade_history_full,
            self._test_workday_range,
            self._test_income,
        ]

        if self.include_tasks:
            tests.append(self._test_tasks)

        for func in tests:
            name = func.__name__
            try:
                func()
            except requests.RequestException as exc:
                self._record(name, False, f"HTTP error: {exc}")
            except TestFailure as exc:
                self._record(name, False, str(exc))
            except Exception as exc:  # noqa: BLE001 - capture unexpected
                self._record(name, False, f"unexpected error: {exc}")

    # ------------------------------------------------------------------
    def summary(self) -> Dict[str, Any]:
        total = len(self.results)
        success = sum(1 for item in self.results if item["success"])
        return {"total": total, "success": success, "failed": total - success}

    def dump_results(self, path: Optional[str]) -> None:
        if not path:
            return
        with open(path, "w", encoding="utf-8") as fout:
            json.dump({"results": self.results, "summary": self.summary()}, fout, ensure_ascii=False, indent=2)


# ----------------------------------------------------------------------

def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test all documented TDX API endpoints")
    parser.add_argument("--base-url", default=os.getenv("TDX_API_BASE", "http://localhost:8080"))
    parser.add_argument("--codes", default=",".join(DEFAULT_CODES), help="Comma-separated stock codes")
    parser.add_argument("--index-code", default=DEFAULT_INDEX, help="Index code for index endpoints")
    parser.add_argument("--date", default=dt.date.today().strftime("%Y-%m-%d"), help="Trade date YYYY-MM-DD")
    parser.add_argument(
        "--start-date",
        default=(dt.date.today() - dt.timedelta(days=30)).strftime("%Y-%m-%d"),
        help="Historical start date YYYY-MM-DD",
    )
    parser.add_argument("--end-date", default=dt.date.today().strftime("%Y-%m-%d"), help="Historical end date YYYY-MM-DD")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument(
        "--bulk-timeout",
        type=float,
        default=None,
        help="Optional timeout override (seconds) for bulk endpoints; <=0 disables timeout",
    )
    parser.add_argument("--no-tasks", action="store_true", help="Skip task creation/cancellation endpoints")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--output", help="Optional JSON report path")
    return parser.parse_args(argv)


def to_date(text: str) -> dt.date:
    try:
        return dt.date.fromisoformat(text)
    except ValueError:
        raise SystemExit(f"Invalid date format: {text}. Expected YYYY-MM-DD") from None


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    if not codes:
        raise SystemExit("At least one stock code must be provided")

    trade_date = to_date(args.date)
    start_date = to_date(args.start_date)
    end_date = to_date(args.end_date)

    tester = ApiTester(
        base_url=args.base_url,
        codes=codes,
        index_code=args.index_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        timeout=args.timeout,
        bulk_timeout=args.bulk_timeout,
        verbose=args.verbose,
        include_tasks=not args.no_tasks,
    )

    tester.run()
    summary = tester.summary()

    print("\nSummary: {} passed / {} failed".format(summary["success"], summary["failed"]))
    tester.dump_results(args.output)

    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
