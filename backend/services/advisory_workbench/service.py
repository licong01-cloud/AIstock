"""Operator reads over immutable lists and current reference-price observations."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
import logging
from zoneinfo import ZoneInfo

from backend.services.advisory_program import (
    AdvisoryProgramService, _episode_from_row, _json_ready, _program_from_row, episode_to_dict,
)
from backend.services.advisory_historical_range.program_performance import (
    episode_performance, performance_metrics, reporting_cutoff,
)
from .projection import (
    WorkbenchError, calendar_context, coverage, daily_projection, identity, membership, run_state, streak_counts,
)
from .repository import ReadRepository

LOG = logging.getLogger(__name__)
CN = ZoneInfo("Asia/Shanghai")
SCHEMA = "advisory_workbench_v1"
EPISODE_FIELDS = ("episode_id", "program_id", "symbol", "stock_name", "status", "signal_date",
    "effective_entry_date", "effective_exit_date", "entry_price", "exit_price", "entry_rank", "current_rank",
    "exit_signal_date", "exit_reason", "holding_trading_days", "max_drawdown_bps", "price_quality_status")


class WorkbenchService:
    def __init__(self, repository=None, clock=None):
        self.repository = repository or ReadRepository()
        self.clock = clock or (lambda: datetime.now(CN))

    def _context(self, repo, program_id, target=None, expected=None):
        now = self.clock()
        program = repo.program(program_id)
        versions = repo.versions(program_id)
        bindings = repo.bindings(program_id)
        start = min([v["trade_date"] for v in versions] + [now.date() - timedelta(days=30)])
        calendar = repo.calendar(start, max(now.date(), date.fromisoformat(target) if target else now.date()))
        default = calendar_context(calendar, now.astimezone(CN).date())
        target = target or default
        selected = next((v for v in versions if str(v["trade_date"]) == target and v["version_status"] == "PUBLISHED"), None)
        published = [v for v in versions if v["version_status"] == "PUBLISHED"]
        active = next((b for b in bindings if b["activation_status"] == "ACTIVE"), None)
        # Explicitly tied to all read sources; changes invalidate a paged view.
        stamp = identity({"program": program, "versions": versions, "bindings": bindings, "calendar": calendar,
            "watermark": repo.watermark(program_id, reporting_cutoff(now)), "target": target,
            "reporting_cutoff": reporting_cutoff(now),
            "clock_phase": (now.astimezone(CN).date(), now.astimezone(CN).hour >= 15,
                now.astimezone(CN).hour * 60 + now.astimezone(CN).minute >= 570)})
        if expected and stamp != expected:
            raise WorkbenchError("SNAPSHOT_CHANGED", "读取期间数据已更新，请刷新后继续查看。", 409)
        return {"program": program, "versions": versions, "bindings": bindings, "active": active,
                "calendar": calendar, "target": target, "default_target": default, "selected": selected,
                "latest": published[0] if published else None, "now": now, "snapshot": stamp}

    @staticmethod
    def _envelope(ctx, **payload):
        return _json_ready({"schema_version": SCHEMA, "program_id": ctx["program"]["program_id"],
            "binding_version_id": (ctx["selected"] or ctx["active"] or {}).get("binding_version_id"),
            "target_date": ctx["target"], "list_version_id": (ctx["selected"] or {}).get("list_version_id"),
            "snapshot_id": ctx["snapshot"], "generated_at": ctx["now"], **payload})

    def _metric_rows(self, ctx, records):
        service = AdvisoryProgramService(now_provider=lambda: ctx["now"])
        program = _program_from_row(ctx["program"])
        episodes = [_episode_from_row(row) for row in records]
        episodes = service._with_settled_episode_metric_prices(episodes, cutoff=reporting_cutoff(ctx["now"]))
        episodes = service._with_latest_open_episode_metric_marks(program, episodes, cutoff=reporting_cutoff(ctx["now"]))
        result = []
        for original, episode in zip(records, episodes):
            raw = episode_to_dict(episode)
            view = episode_performance(raw, now=ctx["now"])
            row = {key: raw.get(key) for key in EPISODE_FIELDS}
            mark = (raw.get("evidence_json") or {}).get("open_mark_to_market") or {}
            row.update(view, source=original["origin_source"], binding_version_id=original["origin_binding_id"],
                mark_date=mark.get("trade_date"), mark_price=mark.get("mark_price"),
                return_bps=view["realized_gross_return_bps"] if view["performance_state"] == "SETTLED" else view["unrealized_gross_return_bps"],
                return_kind="REALIZED" if view["performance_state"] == "SETTLED" else "UNREALIZED")
            if view["performance_state"] != "SETTLED" and raw["status"] == "EXITED":
                row["exit_price"] = None
            row["progress"] = ("ENDED" if view["performance_state"] == "SETTLED" else "PENDING_EXIT"
                if raw["status"] == "EXITED" else "PENDING_ENTRY" if view["performance_state"] == "PLANNED"
                else "TRACKING" if view["performance_state"] == "EXECUTED" else "UNKNOWN")
            result.append(row)
        # Name lookup is descriptive only. Missing names retain the real security code.
        result = service._enrich_display_names(result)
        metrics = performance_metrics(program.__dict__, [episode_to_dict(e) for e in episodes], now=ctx["now"])
        return result, metrics

    @staticmethod
    def _filters(ctx, filters):
        filters = {k: v for k, v in filters.items() if v is not None and v != ""}
        if filters.get("start") and filters.get("end") and str(filters["start"]) > str(filters["end"]):
            raise WorkbenchError("INVALID_DATE_RANGE", "开始日期不能晚于结束日期。")
        if filters.get("binding") == "CURRENT":
            filters["binding"] = (ctx["active"] or {}).get("binding_version_id") or "__NO_ACTIVE_BINDING__"
        return filters

    def _summary(self, repo, ctx, filters):
        rows, _ = repo.episodes(ctx["program"]["program_id"], self._filters(ctx, filters))
        _, metrics = self._metric_rows(ctx, rows)
        runs = repo.forward(ctx["program"]["program_id"], ctx["target"])
        latest_attempt = repo.forward(ctx["program"]["program_id"])
        state = run_state(runs[0] if runs else (latest_attempt[0] if latest_attempt and not ctx["selected"] else None),
                          has_target=ctx["selected"] is not None, configured=ctx["program"]["status"])
        latest = ctx["latest"]
        return self._envelope(ctx,
            program={k: ctx["program"].get(k) for k in ("program_id", "program_name", "status", "enabled_since", "target_count", "package_ids")},
            active_binding=ctx["active"], bindings=ctx["bindings"], metrics=metrics, runtime=state,
            package_models=repo.package_models((ctx["active"] or ctx["program"]).get("package_ids") or []),
            selected=ctx["selected"], latest=latest, default_target_date=ctx["default_target"],
            source_counts=dict(Counter(row["origin_source"] for row in rows)), stats_filter=filters,
            coverage=coverage(ctx["versions"], ctx["calendar"], ctx["target"]),
            date_options=[str(r["cal_date"]) for r in ctx["calendar"] if r["is_trading"]],
            versions_count=len(ctx["versions"]))

    def overview(self, *, status="ENABLED", **filters):
        with self.repository.transaction() as repo:
            programs = repo.programs(status)
            cards = [self._summary(repo, self._context(repo, row["program_id"]), filters) for row in programs]
        priority = {"ASSET_BLOCKED": 0, "FAILED": 1, "WAITING_DATA": 2, "NO_CURRENT_LIST": 3, "PUBLISHED": 4}
        cards.sort(key=lambda row: (priority.get(row["runtime"]["state"], 3),
            -date.fromisoformat((row["latest"] or {}).get("trade_date") or "0001-01-01").toordinal(), row["program"]["program_name"]))
        return {"schema_version": SCHEMA, "generated_at": self.clock().isoformat(), "items": cards,
                "count": len(cards), "status_counts": dict(Counter(c["runtime"]["state"] for c in cards))}

    def program(self, program_id, *, target_date=None, **filters):
        with self.repository.transaction() as repo:
            ctx = self._context(repo, program_id, target_date)
            return self._summary(repo, ctx, filters)

    def daily(self, program_id, *, target_date=None, group="IN_SCOPE", change=None, q=None, limit=20, offset=0, snapshot_id=None):
        with self.repository.transaction() as repo:
            ctx = self._context(repo, program_id, target_date, snapshot_id)
            version = ctx["selected"]
            if not version:
                return self._envelope(ctx, status="NO_CURRENT_LIST", items=[], total_count=0, summary=None,
                                      latest=ctx["latest"], limit=limit, offset=offset)
            versions = [v for v in ctx["versions"] if v["version_status"] == "PUBLISHED" and v["trade_date"] <= version["trade_date"]]
            by_version = defaultdict(list)
            for row in repo.items(program_id, [v["list_version_id"] for v in versions]):
                by_version[row["list_version_id"]].append(row)
            previous = next((v for v in versions if v["list_version_id"] == version["previous_list_version_id"]), None)
            first = not any(v["trade_date"] < version["trade_date"] for v in versions)
            prior_known = bool((first and not version["previous_list_version_id"]) or (previous and previous["binding_version_id"] == version["binding_version_id"]))
            streaks = streak_counts(version, versions, by_version, ctx["calendar"])
            rows, summary = daily_projection(by_version[version["list_version_id"]],
                by_version.get(previous["list_version_id"], []) if previous else [], prior_known=prior_known, streaks=streaks)
            context = version.get("date_context") or {}
            data_date = context.get("selection_as_of_trade_date") or context.get("decision_as_of_trade_date")
            marks = repo.marks([r["symbol"] for r in rows], data_date)
            for row in rows:
                mark = marks.get(row["symbol"], {})
                row.update(reference_price=mark.get("price"), reference_date=mark.get("trade_date"))
            filtered = [r for r in rows if (group == "ALL" or (r["change"] == "REMOVED" if group == "REMOVED" else r["membership"] == group))
                and (not change or r["change"] == change)
                and (not q or q.lower() in (r["symbol"] + " " + str(r.get("stock_name") or "")).lower())]
            page = filtered[offset:offset + limit]
            links = defaultdict(set)
            for link in repo.entry_links(program_id, version, [r['symbol'] for r in page if not r.get('episode_id')]):
                links[link['symbol']].add(link['episode_id'])
            for row in page:
                if not row.get('episode_id') and links[row['symbol']]:
                    candidates = links[row['symbol']]
                    row['episode_link_state'] = 'LINKED_ENTER' if len(candidates) == 1 else 'AMBIGUOUS_ENTER'
                    if len(candidates) == 1:
                        row['episode_id'] = next(iter(candidates))
            episode_ids = [r.get("episode_id") for r in page if r.get("episode_id")]
            # Batch page-specific episode reads, never one price query per stock.
            records, _ = repo.episodes(program_id, {"episode_ids": episode_ids}) if episode_ids else ([], 0)
            views, _ = self._metric_rows(ctx, records)
            by_episode = {view["episode_id"]: view for view in views}
            for row in page:
                view = by_episode.get(row.get('episode_id'))
                if row.get('episode_link_state') == 'LINKED_ENTER' and (not view
                        or view['symbol'] != row['symbol'] or view['program_id'] != program_id
                        or view['binding_version_id'] != version['binding_version_id']):
                    row.update(episode_id=None, episode_link_state='EPISODE_IDENTITY_MISMATCH')
                    continue
                if view:
                    row["episode"] = view
                    row["progress"] = row["episode"]["progress"]
            page = AdvisoryProgramService()._enrich_display_names(page)
            return self._envelope(ctx, status="PUBLISHED", items=page, total_count=len(filtered),
                summary=summary, data_date=data_date, limit=limit, offset=offset)

    def episodes(self, program_id, *, target_date=None, limit=20, offset=0, snapshot_id=None, **filters):
        with self.repository.transaction() as repo:
            ctx = self._context(repo, program_id, target_date, snapshot_id)
            filters = self._filters(ctx, filters)
            if filters.get("status") == "UNSETTLED":
                # Financial settlement is not the ledger EXITED flag. Filter using
                # the canonical reconciled performance state, then paginate in SQL.
                filters.pop("status")
                candidates, _ = repo.episodes(program_id, filters)
                views, _ = self._metric_rows(ctx, candidates)
                filters["episode_ids"] = [v["episode_id"] for v in views if v["performance_state"] != "SETTLED"]
            records, total = repo.episodes(program_id, filters, limit=limit, offset=offset)
            rows, _ = self._metric_rows(ctx, records)
            self._memberships(repo, ctx, rows)
            return self._envelope(ctx, items=rows, total_count=total, limit=limit, offset=offset, filters=filters)

    @staticmethod
    def _memberships(repo, ctx, rows):
        selected = ctx["selected"]
        items = repo.items(ctx["program"]["program_id"], [selected["list_version_id"]]) if selected else []
        later = defaultdict(set)
        if selected:
            for link in repo.entry_links(ctx['program']['program_id'], selected,
                                         [i['symbol'] for i in items if not i.get('episode_id')]):
                later[link['symbol']].add(link['episode_id'])
        for row in rows:
            matches = [i for i in items if i.get("episode_id") == row["episode_id"]]
            candidates = later[row['symbol']]
            if (not matches and candidates == {row['episode_id']}
                    and row.get('program_id') == ctx['program']['program_id']
                    and row.get('binding_version_id') == selected['binding_version_id']):
                matches = [i for i in items if i['symbol'] == row['symbol']]
            row["selected_membership"] = ("NO_CURRENT_LIST" if not selected else
                membership(matches[0]) if len(matches) == 1 else "UNKNOWN" if matches else "OUTSIDE_LIST")

    def episode(self, program_id, episode_id, *, target_date=None, limit=20, offset=0, snapshot_id=None):
        with self.repository.transaction() as repo:
            ctx = self._context(repo, program_id, target_date, snapshot_id)
            records, _ = repo.episodes(program_id, {"episode_id": episode_id}, limit=20)
            if not records:
                raise WorkbenchError("EPISODE_NOT_FOUND", "该荐股过程不属于当前策略或已不可用。", 404)
            rows, _ = self._metric_rows(ctx, records)
            self._memberships(repo, ctx, rows)
            events, total = repo.decisions(program_id, {"episode_id": episode_id}, limit=limit, offset=offset)
            return self._envelope(ctx, episode=rows[0], items=events, total_count=total, limit=limit, offset=offset)

    def versions(self, program_id, *, target_date=None, limit=20, offset=0, snapshot_id=None, start=None, end=None):
        with self.repository.transaction() as repo:
            ctx = self._context(repo, program_id, target_date, snapshot_id)
            self._filters(ctx, {"start": start, "end": end})
            # Metadata is needed for full-period coverage; paging is still applied in SQL.
            if start or end:
                where, params = ["program_id=%s"], [program_id]
                for key, op in ((start, ">="), (end, "<=")):
                    if key:
                        where.append(f"trade_date{op}%s")
                        params.append(key)
                from .repository import VERSION_COLUMNS
                clause = " AND ".join(where)
                total = repo.rows(f"SELECT count(*) AS n FROM app.advisory_recommendation_list_version WHERE {clause}", params)[0]["n"]
                rows = repo.rows(f"SELECT {VERSION_COLUMNS} FROM app.advisory_recommendation_list_version WHERE {clause} "
                    "ORDER BY trade_date DESC,created_at DESC,list_version_id LIMIT %s OFFSET %s", [*params, limit, offset])
            else:
                rows, total = repo.versions(program_id, limit, offset), len(ctx["versions"])
            return self._envelope(ctx, items=rows, total_count=total, limit=limit, offset=offset)

    def decisions(self, program_id, *, target_date=None, limit=20, offset=0, snapshot_id=None, **filters):
        with self.repository.transaction() as repo:
            ctx = self._context(repo, program_id, target_date, snapshot_id)
            rows, total = repo.decisions(program_id, self._filters(ctx, filters), limit=limit, offset=offset)
            return self._envelope(ctx, items=rows, total_count=total, limit=limit, offset=offset)

    def readiness(self, program_id, data_date=None):
        from backend.services.data_integrity.service import DailyIntegrityService
        with self.repository.transaction() as repo:
            program = repo.program(program_id)
            bindings = repo.bindings(program_id)
            active = next((b for b in bindings if b["activation_status"] == "ACTIVE"), None)
        packages = (active or program).get("package_ids") or []
        data_date = data_date or self.clock().astimezone(CN).date().isoformat()
        results, errors = [], []
        service = DailyIntegrityService()
        try:
            sync = service.day(data_date)
        except Exception as exc:
            LOG.warning("Advisory shared daily summary unavailable: %s", type(exc).__name__)
            sync = {}
        for package_id in packages:
            try:
                raw = service.readiness(data_date=data_date, consumer="ADVISORY", binding_id=package_id)
                blockers = raw.get("blockers") or []
                if blockers:
                    from collections import Counter
                    counts = Counter(row["state"] for row in blockers)
                    dates = [row["fact_date"] for row in blockers]
                    raw["reason"] = (f"等待验证：{len(blockers)} 个输入分区（{min(dates)} 至 {max(dates)}）；" +
                        " / ".join(f"{key} {count}" for key, count in sorted(counts.items())) +
                        "。涉及 " + "、".join(sorted({row["dataset"] for row in blockers})) + "。")
                results.append({k: raw.get(k) for k in ("binding_id", "consumer_ready", "prediction_ready", "state", "reason")}
                    | {"all_sync_complete": sync.get("all_sync_complete"), "blocker_count": len(blockers)})
            except Exception as exc:
                LOG.warning("Advisory workbench readiness unavailable: %s", type(exc).__name__)
                errors.append({"reason_code": type(exc).__name__, "message": "每日数据就绪合同暂不可读，请核对数据管理状态。"})
        return _json_ready({"schema_version": SCHEMA, "program_id": program_id, "data_date": data_date,
            "generated_at": self.clock(), "status": "UNAVAILABLE" if errors or not packages else "AVAILABLE",
            "items": results, "errors": errors})
