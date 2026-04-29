from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


DEFAULT_QE_SOURCES = ("qe_20260416_002701", "qe_20260413_084216", "qe_20260416_082012")
LIVE_SETTLED_STATUSES = {"LIVE_WAITING_FOR_BAR", "LIVE_WAITING_NEXT_TRADING_DAY", "SUCCEEDED", "FAILED", "STOPPED"}


class ValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class HttpResult:
    status: int
    payload: dict[str, Any]


class ApiClient:
    def __init__(self, base_url: str, *, timeout: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> HttpResult:
        body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
                parsed = json.loads(raw.decode("utf-8")) if raw else {}
                return HttpResult(status=response.status, payload=parsed)
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            parsed: dict[str, Any]
            try:
                parsed = json.loads(raw.decode("utf-8")) if raw else {}
            except json.JSONDecodeError:
                parsed = {"detail": raw.decode("utf-8", errors="replace")}
            raise ValidationError(f"{method} {path} failed with HTTP {exc.code}: {json.dumps(parsed, ensure_ascii=False)[:2000]}") from exc
        except urllib.error.URLError as exc:
            raise ValidationError(f"{method} {path} connection failed: {exc.reason}") from exc


def _date_today() -> dt.date:
    return dt.datetime.now().date()


def _json_date(value: Any) -> dt.date:
    return dt.date.fromisoformat(str(value))


def _assert(condition: bool, message: str, context: dict[str, Any] | None = None) -> None:
    if not condition:
        suffix = f" context={json.dumps(context, ensure_ascii=False)}" if context else ""
        raise ValidationError(message + suffix)


def probe_services(api: ApiClient, tdx_base_url: str, tdx_probe_code: str) -> dict[str, Any]:
    openapi_base = api.base_url.removesuffix("/api/v1")
    openapi_url = f"{openapi_base}/openapi.json"
    tdx_url = f"{tdx_base_url.rstrip('/')}/api/kline-all/tdx?{urllib.parse.urlencode({'code': tdx_probe_code, 'type': 'minute1'})}"
    probes: dict[str, Any] = {}
    for name, url in {"backend_openapi": openapi_url, "tdx_minute": tdx_url}.items():
        try:
            with urllib.request.urlopen(url, timeout=15) as response:
                body = response.read(4096)
                _assert(200 <= response.status < 300 and bool(body.strip()), f"{name} probe returned an empty or non-2xx response")
                probes[name] = {"url": url, "status": response.status, "bytes": len(body)}
        except Exception as exc:  # noqa: BLE001 - fail-fast diagnostic wrapper.
            raise ValidationError(f"{name} probe failed for {url}: {exc}") from exc
    return probes


def choose_package_and_policy(api: ApiClient, source_ids: list[str]) -> tuple[dict[str, Any], dict[str, Any]]:
    packages = api.request("GET", "/strategy-packages?limit=500").payload.get("packages") or []
    for source_id in source_ids:
        package = next((item for item in packages if item.get("source_id") == source_id or item.get("package_name") == source_id), None)
        if not package:
            continue
        policies = api.request("GET", f"/strategy-packages/{package['package_id']}/execution-policies").payload.get("execution_policies") or []
        policy = next((item for item in policies if item.get("algo_code") == "V25_TWO_STAGE" and item.get("paper_enabled")), None)
        if policy:
            return package, policy
    raise ValidationError(
        "no existing StrategyPackage with a paper-enabled V25_TWO_STAGE policy was found; this validation does not create or mutate StrategyPackage assets",
        {"source_ids": source_ids},
    )


def choose_replay_start(api: ApiClient, live_date: dt.date, *, lookback_trading_days: int) -> tuple[dt.date, dict[str, Any]]:
    historical_as_of = live_date - dt.timedelta(days=1)
    defaults = api.request(
        "GET",
        f"/paper-v2/trading-days/defaults?{urllib.parse.urlencode({'lookback_trading_days': lookback_trading_days, 'as_of_date': historical_as_of.isoformat(), 'require_minute_data': 'true'})}",
    ).payload
    latest = _json_date(defaults["latest_trading_day"])
    replay_start = _json_date(defaults["replay_start_date"])
    _assert(latest < live_date, "live validation replay source must be before live_date", {
        "live_date": live_date.isoformat(),
        "latest_historical_trading_day": latest.isoformat(),
        "defaults": defaults,
    })
    return replay_start, defaults


def runtime_config(top_k: int) -> dict[str, Any]:
    return {
        "paper_v2_session": {"signal_data_source": "DB_HISTORICAL", "manual_tick_only": True},
        "selection_artifact_config": {"auto_generate": True, "inference_backend": "wsl"},
        "runtime_profile": {
            "selection": {"top_k": top_k},
            "tradability": {"exclude_suspended": True},
            "industry_blacklist": [],
            "hmm": {"enabled": False},
        },
    }


def wait_progress(api: ApiClient, session_id: str, *, timeout_seconds: int, poll_seconds: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    latest: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        latest = api.request("GET", f"/paper-v2/sessions/{session_id}/progress").payload["progress"]
        status = str(latest.get("session", {}).get("status") or "").upper()
        if status in LIVE_SETTLED_STATUSES:
            return latest
        time.sleep(poll_seconds)
    raise ValidationError("session did not reach a live settled status before timeout", {"session_id": session_id, "last_progress": latest})


def run_catchup_live(args: argparse.Namespace) -> dict[str, Any]:
    api = ApiClient(args.api_base, timeout=args.http_timeout)
    probes = probe_services(api, args.tdx_base_url, args.tdx_probe_code)
    live_date = dt.date.fromisoformat(args.live_date) if args.live_date else _date_today()
    package, policy = choose_package_and_policy(api, args.source_id)
    replay_start, defaults = choose_replay_start(api, live_date, lookback_trading_days=args.replay_lookback_trading_days)
    _assert(replay_start < live_date, "catch-up live validation requires a completed historical trading day before live_date", {
        "replay_start": replay_start.isoformat(),
        "live_date": live_date.isoformat(),
        "defaults": defaults,
    })

    portfolio_payload = {
        "package_id": package["package_id"],
        "portfolio_name": f"LiveValidation-{package.get('source_id') or package['package_id']}-{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "initial_cash": args.initial_cash,
        "start_date": replay_start.isoformat(),
        "data_source": "DB_HISTORICAL",
        "execution_policy": {"validated_execution_policy_id": policy["policy_id"]},
    }
    portfolio = api.request("POST", "/paper-v2/portfolios", portfolio_payload).payload["portfolio"]
    session_payload = {
        "mode": "REPLAY_ONLY",
        "start_date": replay_start.isoformat(),
        "end_date": live_date.isoformat(),
        "historical_data_source": "DB_HISTORICAL",
        "live_data_source": "TDX_REALTIME",
        "runtime_config": runtime_config(args.top_k),
        "rerun_policy": "reject_existing",
        "auto_switch_to_live": True,
        "created_by": "paper_v2_live_validation",
    }
    session = api.request("POST", f"/paper-v2/portfolios/{portfolio['portfolio_id']}/sessions", session_payload).payload["session"]
    tick_payload: dict[str, Any] = {"allow_paused": True}
    if args.as_of_time:
        tick_payload["as_of_time"] = args.as_of_time
    tick_result = api.request("POST", f"/paper-v2/sessions/{session['session_id']}/tick", tick_payload).payload["progress"]
    status = str(tick_result.get("session", {}).get("status") or "").upper()
    progress = tick_result if status in LIVE_SETTLED_STATUSES else wait_progress(
        api,
        session["session_id"],
        timeout_seconds=args.wait_timeout,
        poll_seconds=args.poll_seconds,
    )

    final_status = str(progress.get("session", {}).get("status") or "").upper()
    _assert(final_status != "FAILED", "catch-up live session failed", {"progress": progress})
    _assert(progress.get("session", {}).get("mode") == "CATCHUP_THEN_LIVE", "auto_switch_to_live did not create a catch-up live session", progress)

    runs = api.request("GET", f"/paper-v2/portfolios/{portfolio['portfolio_id']}/runs?limit=100").payload.get("runs") or []
    orders = api.request("GET", f"/paper-v2/portfolios/{portfolio['portfolio_id']}/orders?limit=1000").payload.get("orders") or []
    fills = api.request("GET", f"/paper-v2/portfolios/{portfolio['portfolio_id']}/fills?limit=1000").payload.get("fills") or []
    errors = api.request("GET", f"/paper-v2/portfolios/{portfolio['portfolio_id']}/errors?limit=1000").payload.get("errors") or []
    live_run = next((item for item in runs if item.get("trade_date") == live_date.isoformat()), None)
    _assert(not errors, "paper v2 live validation produced persisted errors", {"errors": errors[:5]})
    _assert(any(item.get("trade_date") == replay_start.isoformat() for item in runs), "historical catch-up did not create a replay run", {"runs": runs})
    _assert(live_run is not None, "live tick did not create a current-date live run", {"live_date": live_date.isoformat(), "runs": runs})
    _assert(len(orders) > 0, "live/catch-up validation produced no orders", {"portfolio_id": portfolio["portfolio_id"]})
    if args.require_live_bars:
        _assert(progress.get("latest_available_bar_time"), "no live minute bar was observed by Paper v2", {"progress": progress})
        _assert(progress.get("last_processed_bar_time"), "live minute bars were available but not processed", {"progress": progress})
    if args.require_fills:
        _assert(len(fills) > 0, "live validation produced no fills while --require-fills was requested", {"orders": orders[:5], "progress": progress})

    return {
        "ok": True,
        "mode": "catchup_then_live",
        "probes": probes,
        "package": {"package_id": package["package_id"], "package_name": package.get("package_name"), "source_id": package.get("source_id")},
        "policy": {"policy_id": policy["policy_id"], "algo_code": policy.get("algo_code")},
        "portfolio_id": portfolio["portfolio_id"],
        "session_id": session["session_id"],
        "replay_start": replay_start.isoformat(),
        "live_date": live_date.isoformat(),
        "final_status": final_status,
        "latest_available_bar_time": progress.get("latest_available_bar_time"),
        "last_processed_bar_time": progress.get("last_processed_bar_time"),
        "run_count": len(runs),
        "order_count": len(orders),
        "fill_count": len(fills),
        "error_count": len(errors),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Paper v2 catch-up-to-live validation against local dev services.")
    parser.add_argument("--api-base", default=os.environ.get("PAPER_V2_API_BASE") or os.environ.get("NEXT_PUBLIC_API_BASE") or "http://127.0.0.1:8012/api/v1")
    parser.add_argument("--tdx-base-url", default=os.environ.get("TDX_BASE_URL", "http://127.0.0.1:19080"))
    parser.add_argument("--tdx-probe-code", default=os.environ.get("TDX_PROBE_CODE", "SZ000001"))
    parser.add_argument("--source-id", action="append", default=list(DEFAULT_QE_SOURCES), help="Existing QE source id to try; may be repeated.")
    parser.add_argument("--live-date", help="Live trading date, default is local today.")
    parser.add_argument("--as-of-time", help="Optional ISO datetime passed to the session tick endpoint.")
    parser.add_argument("--initial-cash", type=float, default=1_000_000)
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument("--replay-lookback-trading-days", type=int, default=1, help="Number of completed historical trading days to replay before switching to live.")
    parser.add_argument("--http-timeout", type=float, default=900.0)
    parser.add_argument("--wait-timeout", type=int, default=900)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--require-live-bars", action="store_true", help="Require TDX bars to be observed and processed for live_date.")
    parser.add_argument("--require-fills", action="store_true", help="Require at least one live/replay fill.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.top_k < 1 or args.top_k > 50:
        raise SystemExit("--top-k must be between 1 and 50")
    if args.replay_lookback_trading_days < 1 or args.replay_lookback_trading_days > 20:
        raise SystemExit("--replay-lookback-trading-days must be between 1 and 20")
    try:
        result = run_catchup_live(args)
    except ValidationError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
