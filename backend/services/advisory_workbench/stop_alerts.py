"""Read-only minute stop reminders for the Advisory workbench."""
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
import logging
from threading import Lock

from requests import RequestException

from backend.data_service.tdx_adapter import fetch_minute_kline_tdx
from backend.services.position_timing.intraday.contracts import CHINA, sessions, utc
from backend.services.position_timing.intraday.inputs import normalize_tdx_bars

from .projection import WorkbenchError, daily_projection, identity
from .repository import ReadRepository, VERSION_COLUMNS

LOG = logging.getLogger(__name__)
CLOSE_DELAY = timedelta(seconds=5)
ONE_MINUTE = timedelta(minutes=1)
SCAN_LOCK = Lock()
SCAN_TIMEOUT = 18


def _positive(value):
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _expected_ends(day, cutoff, after=None):
    return {
        left + ONE_MINUTE * n
        for left, right in sessions(day)
        for n in range(1, int((min(cutoff, right) - left) // ONE_MINUTE) + 1)
        if after is None or left + ONE_MINUTE * (n - 1) >= after
    }


def _pending(reason):
    return dict(status="UNVERIFIED", reason_code=reason, triggered_at=None,
                trigger_price=None, observed_through=None, observed_bars=0, coverage="INCOMPLETE")


def minute_stop(rows, *, day, cutoff, received_at, entry_price, stop_bps, after=None):
    """Inspect only the closed, visible prefix for this request."""
    ends = _expected_ends(day, cutoff, after)
    if not ends:
        return _pending("NO_CLOSED_MINUTES")
    bars = [
        bar for bar in normalize_tdx_bars(rows, trade_date=day, received_at=received_at)
        if bar.end in ends
    ]
    seen = {bar.end for bar in bars}
    complete = seen == ends
    observed = dict(observed_bars=len(bars), expected_bars=len(ends),
                    observed_through=bars[-1].end.astimezone(CHINA) if bars else None,
                    coverage="COMPLETE" if complete else "INCOMPLETE")
    threshold = entry_price * (1 - Decimal(stop_bps) / 10000)
    hit = next((bar for bar in bars if bar.volume_shares > 0 and bar.close <= threshold), None)
    if hit is not None:
        return dict(status="TRIGGERED", reason_code="STOP_LOSS", **observed,
                    triggered_at=hit.end.astimezone(CHINA), trigger_price=hit.close,
                    return_bps=(hit.close / entry_price - 1) * 10000,
                    first_trigger_verified=all(end in seen for end in ends if end < hit.end))
    if not complete or not any(bar.volume_shares > 0 for bar in bars):
        return {**_pending("MINUTE_COVERAGE_INCOMPLETE" if not complete else "NO_TRADED_MINUTES"), **observed}
    return dict(status="NOT_TRIGGERED", reason_code=None, triggered_at=None, trigger_price=None, **observed)


class MinuteStopAlerts:
    def __init__(self, repository=None, clock=None, fetch=None):
        self.repository = repository if repository is not None else ReadRepository()
        self.clock = clock if clock is not None else lambda: datetime.now(CHINA)
        self.fetch = fetch if fetch is not None else fetch_minute_kline_tdx

    def _snapshot(self, program_id, day, list_version_id):
        with self.repository.transaction() as repo:
            program = repo.program(program_id)
            if program["status"] != "ENABLED":
                return {"status": "PROGRAM_DISABLED"}
            cal = [row for row in repo.calendar(day, day) if row["cal_date"] == day]
            if len(cal) != 1 or not isinstance(cal[0]["is_trading"], bool):
                return {"status": "CALENDAR_UNVERIFIED"}
            if not cal[0]["is_trading"]:
                return {"status": "NON_TRADING_DAY"}

            versions = repo.rows(f"""SELECT {VERSION_COLUMNS}
                FROM app.advisory_recommendation_list_version
                WHERE program_id=%s AND trade_date=%s AND version_status='PUBLISHED'
                ORDER BY created_at DESC,list_version_id DESC LIMIT 1""", (program_id, day))
            if not versions:
                return {"status": "WAITING_LIST"}
            current = versions[0]
            if current["list_version_id"] != list_version_id:
                raise WorkbenchError("STOP_ALERT_LIST_CHANGED", "已发布名单已变化，请刷新资料。", 409)
            active = [b for b in repo.bindings(program_id) if b["activation_status"] == "ACTIVE"]
            if len(active) != 1 or active[0]["binding_version_id"] != current["binding_version_id"]:
                return {"status": "BINDING_UNVERIFIED"}
            binding = current["binding_version_id"]
            payload = program.get("program_payload_json") or program
            policy = payload.get("review_policy", {})
            stop_bps = policy.get("stop_loss_bps") if isinstance(policy, dict) else None
            if type(stop_bps) is not int or stop_bps < 0:
                return {"status": "RULE_UNVERIFIED"}
            if stop_bps == 0:
                return {"status": "RULE_DISABLED"}

            ids = [list_version_id]
            prev = current["previous_list_version_id"]
            if prev:
                verified = repo.rows("""SELECT list_version_id FROM app.advisory_recommendation_list_version
                    WHERE program_id=%s AND list_version_id=%s AND binding_version_id=%s
                      AND trade_date<%s AND version_status='PUBLISHED'""", (program_id, prev, binding, day))
                if verified:
                    ids.append(prev)
            entries = [row for row in repo.items(program_id, ids)
                       if row["binding_version_id"] == binding]
            main = [r for r in entries if r["list_version_id"] == list_version_id]
            previous = [r for r in entries if r["list_version_id"] == prev]
            rows, _ = daily_projection(main, previous, prior_known=len(ids) == 2)
            eligible_groups = {"IN_SCOPE", "OUTSIDE_PENDING"}
            links = {}
            for link in repo.entry_links(program_id, current, [
                r["symbol"] for r in rows if r["membership"] == "IN_SCOPE" and not r.get("episode_id")
            ]):
                links.setdefault(link["symbol"], set()).add(link["episode_id"])
            selected = {}
            for row in rows:
                if row["membership"] not in eligible_groups:
                    continue
                declared = row.get("episode_id")
                fallback = links.get(row["symbol"], set())
                episode_id = declared or (next(iter(fallback)) if len(fallback) == 1 else None)
                if episode_id:
                    selected.setdefault(episode_id, []).append(row)
            selected = {episode_id: matches[0] for episode_id, matches in selected.items() if len(matches) == 1}
            if not selected:
                return {"status": "READY", "binding_version_id": binding, "stop_loss_bps": stop_bps, "entries": []}

            episodes, _ = repo.episodes(program_id, {
                "episode_ids": sorted(selected), "binding": binding, "source": "FORWARD", "status": "ACTIVE",
            })
            candidates = []
            for row in episodes:
                item = selected[row["episode_id"]]
                entry_day = row["effective_entry_date"]
                if (row["symbol"] != item["symbol"] or row["episode_status"] != "ACTIVE"
                        or row["exit_signal_date"] or row["effective_exit_date"]
                        or entry_day is None or entry_day > day):
                    continue
                after = None
                if entry_day == day:
                    # For a new entry, the ledger timestamp prevents backdating a reminder
                    # to an earlier minute while the entry was still awaiting confirmation.
                    confirmed = row.get("created_at")
                    if confirmed is not None and confirmed.tzinfo is not None and confirmed.astimezone(CHINA).date() == day:
                        after = utc(confirmed)
                price = _positive(row["entry_price"])
                candidates.append(dict(
                    episode_id=row["episode_id"], symbol=row["symbol"], stock_name=item.get("stock_name"),
                    membership=item["membership"], entry_price=price, effective_entry_date=entry_day,
                    after=after, side="T1_RESTRICTED" if entry_day == day else "UNVERIFIED",
                ))
            symbols = sorted({row["symbol"] for row in candidates})
            suspensions = repo.rows("""SELECT DISTINCT ts_code FROM market.suspend_d
                WHERE trade_date=%s AND ts_code=ANY(%s) AND suspend_type='S'""", (day, symbols)) if symbols else []
            suspended = {row["ts_code"] for row in suspensions}
            for entry in candidates:
                if entry["symbol"] in suspended and entry["side"] != "T1_RESTRICTED":
                    entry["side"] = "SUSPENSION_RECORDED"
            return {"status": "READY", "binding_version_id": binding, "stop_loss_bps": stop_bps,
                    "entries": sorted(candidates, key=lambda x: (x["symbol"], x["episode_id"]))}

    def _observe(self, symbol, day):
        try:
            return self.fetch(symbol, day), utc(self.clock())
        except (RequestException, ValueError, RuntimeError):
            LOG.warning("Advisory stop alert minute source unavailable for %s", symbol, exc_info=True)
            return None

    def _scan(self, symbols, day):
        if not SCAN_LOCK.acquire(blocking=False):
            return {}, "SCAN_BUSY"
        try:
            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = {pool.submit(self._observe, symbol, day): symbol for symbol in symbols}
                done, pending = wait(futures, timeout=SCAN_TIMEOUT)
                for future in pending:
                    future.cancel()
                return {futures[future]: future.result() for future in done}, "SCAN_NOT_COMPLETED"
        finally:
            SCAN_LOCK.release()

    def get(self, program_id, *, target_date: date, list_version_id: str):
        now = utc(self.clock())
        day = now.astimezone(CHINA).date()
        result = dict(schema_version="advisory_minute_stop_alerts_v1", program_id=program_id,
                      target_date=target_date, list_version_id=list_version_id,
                      checked_at=now.astimezone(CHINA), items=[], eligible_count=0, evaluated_count=0)
        if day != target_date:
            return {**result, "status": "NOT_CURRENT_DAY"}
        snapshot = self._snapshot(program_id, day, list_version_id)
        if snapshot["status"] != "READY":
            return {**result, "status": snapshot["status"]}
        entries = snapshot["entries"]
        result.update(eligible_count=len(entries), stop_loss_bps=snapshot["stop_loss_bps"],
                      binding_version_id=snapshot["binding_version_id"])
        if not entries:
            return {**result, "status": "NO_ELIGIBLE_EPISODES"}
        cutoff = now - CLOSE_DELAY
        if not _expected_ends(day, cutoff):
            return {**result, "status": "WAITING_SESSION"}
        symbols = sorted({
            row["symbol"] for row in entries if row["entry_price"] is not None
            and (row["effective_entry_date"] != day or row["after"] is not None)
        })
        observations, missing_reason = self._scan(symbols, day) if symbols else ({}, "SCAN_NOT_COMPLETED")
        if (utc(self.clock()).astimezone(CHINA).date() != day
                or self._snapshot(program_id, day, list_version_id) != snapshot):
            raise WorkbenchError("STOP_ALERT_SNAPSHOT_CHANGED", "荐股名单或跟踪状态已变化，请刷新资料。", 409)
        for row in entries:
            if row["entry_price"] is None:
                observed = _pending("ENTRY_PRICE_UNVERIFIED")
            elif row["effective_entry_date"] == day and row["after"] is None:
                observed = _pending("ENTRY_TIME_UNVERIFIED")
            elif row["symbol"] not in observations:
                observed = _pending(missing_reason)
            elif observations[row["symbol"]] is None:
                observed = _pending("MINUTE_SOURCE_UNAVAILABLE")
            else:
                raw, received_at = observations[row["symbol"]]
                try:
                    observed = minute_stop(raw, day=day, cutoff=cutoff, received_at=received_at,
                                           entry_price=row["entry_price"], stop_bps=snapshot["stop_loss_bps"],
                                           after=row["after"])
                except (ValueError, TypeError, KeyError, InvalidOperation):
                    LOG.warning("Advisory stop alert rejected minute data for %s", row["symbol"], exc_info=True)
                    observed = _pending("MINUTE_DATA_INVALID")
            result["items"].append(dict(
                **{k: row[k] for k in ("episode_id", "symbol", "stock_name", "membership", "entry_price")},
                alert_id=identity((program_id, snapshot["binding_version_id"], row["episode_id"], day)),
                sellability=row["side"], **observed,
            ))
        result["evaluated_count"] = sum(item["status"] != "UNVERIFIED" for item in result["items"])
        result["status"] = "READY"
        return result
