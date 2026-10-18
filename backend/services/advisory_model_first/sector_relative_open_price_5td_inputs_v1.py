"""Role-projected immutable files; no DB, current profile or selection rerun."""
from collections import OrderedDict
from copy import deepcopy
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import json

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from backend.data_service.frozen_dataset import read_h5_window, raw_price_li
from backend.services.advisory_historical_range.outcome_file_source import (
    FileHistoricalRangeOutcomeSourceProvider, OutcomeFileVerificationSession,
)
from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import build_shared_daily_features
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_labels_v1 import PRICE_FIELDS, REFERENCE_FIELDS
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import build_generic_price_5td_valuation_v2
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_inputs_v1 import (
    checked_source, clocks_v1, validate_roster_v1,
)
from backend.services.advisory_model_first.sector_relative_open_price_5td_contracts_v1 import (
    CHUNK_DAYS, CONTEXT_COLUMNS, DAY_META, FEATURES, KEY, META, PEER_POLICY, ROSTER,
    ROSTER_KEY, VALUATION_POLICY_SHA256, node_path, sha,
)


def frame_sha256(frame):
    normalized = frame.astype(object).where(frame.notna(), None).astype(str)
    return sha(dict(columns=[str(c) for c in frame.columns], rows=normalized.to_numpy().tolist()))


def _positive(value):
    return _number(value, positive=True)


class SectorOpenFileProjectionV1(FileHistoricalRangeOutcomeSourceProvider):
    """Reuse native pins/PIT membership, never invoke its full OHLC constructor.

    Locator translation does not mutate the frozen binding or its authority.
    Numeric caches belong to this consumer/process, never another scene's run.
    """
    def __init__(self, binding, *, verification_session=None):
        body = {k: v for k, v in binding.items() if k != "binding_hash"}
        if body.get("schema_version") != "advisory_outcome_frozen_files_v1" or sha(body) != binding.get("binding_hash"):
            raise ValueError("frozen input binding changed")
        self.binding_hash = binding["binding_hash"]
        self._root = node_path(body["profile"]["controller_paths"]["candidate_root"])
        self._files, self._stamps = deepcopy(body["files"]), {}
        self._verification_session = verification_session or OutcomeFileVerificationSession()
        self._paths = {k: self._verify_file(k) for k in ("day_calendar", "day_meta_export",
            "day_selection_universe", "factor_meta", "sector_membership_spans", "sector_code_map")}
        self._load_industry_context()
        self._pool = tuple(tuple(line.split()) for line in self._paths["day_selection_universe"].read_text(
            encoding="utf-8-sig").splitlines() if line.strip())
        self._calendar = tuple(date.fromisoformat(v.strip()) for v in self._paths["day_calendar"].read_text(
            encoding="utf-8-sig").splitlines() if v.strip())
        if not self._calendar or self._calendar != tuple(sorted(set(self._calendar))):
            raise ValueError("file calendar must be ordered and unique")
        self._calendar_positions = {v: i for i, v in enumerate(self._calendar)}
        meta = json.loads(self._paths["factor_meta"].read_text(encoding="utf-8-sig"))
        if meta.get("schema_version") != "qe_direct_factor_h5_static_v2":
            raise ValueError("unrecognized daily price coordinate schema")
        self._daily_root = self._paths["day_meta_export"].parent
        self._cache = OrderedDict()
        self.read_receipts, self.bin_receipts = [], []
        self._index_metadata = None
        self._suspension_metadata = None
        self.assert_unchanged()

    def _path(self, key):
        if key not in self._paths:
            self._paths[key] = self._verify_file(key)
        self.assert_unchanged()
        return self._paths[key]

    def _remember(self, key, loader):
        self.assert_unchanged()
        if key not in self._cache:
            self._cache[key] = loader()
            if len(self._cache) > 8:
                self._cache.popitem(last=False)
        self._cache.move_to_end(key)
        return self._cache[key].copy(deep=True)

    def daily(self, *, start, end, columns, members, role, cutoff):
        start, end, cutoff = (pd.Timestamp(v).date() for v in (start, end, cutoff))
        allowed = {"D_HISTORY": {"open", "high", "low", "close", "volume", "factor"},
            "PEER_D_CLOSE": {"close", "factor"}, "T_OPEN": {"open", "factor"},
            "TRAIN_FINANCE": {"open", "high", "low", "close", "factor"},
            "EVALUATION_FINANCE": {"open", "high", "low", "close", "factor"}}
        if (role not in allowed or set(columns) != allowed[role] or start > end or end > cutoff
                or start < self._calendar[0] or end > self._calendar[-1]):
            raise ValueError("illegal role/field/date projection")
        names = tuple(sorted(set(members)))
        key = (role, start, end, tuple(columns), names)
        def load():
            path = self._path("daily_pv")
            frame = read_h5_window(path, start=start, end=end, columns=list(columns), members=names)
            self.read_receipts.append(dict(role=role, start=str(start), end=str(end), fields=list(columns),
                members_sha256=sha(names), rows=len(frame), values_sha256=frame_sha256(frame.reset_index())))
            self.assert_unchanged()
            return frame
        return self._remember(key, load)

    def index(self, *, dates, field):
        """Fixed-HDF object block contains ONLY keys; slice numeric block first.

        pandas.read_hdf on this asset would decode all future OHLC. Its metadata
        keys are permissible; only the requested numeric column/rows are read.
        """
        dates = tuple(sorted(set(pd.Timestamp(d).normalize() for d in dates)))
        if field not in ("open", "close") or not dates:
            raise ValueError("index projection needs explicit dates and open/close")
        path = self._path("index_daily")
        with pd.HDFStore(path, mode="r") as store:
            s = store.get_storer("data")
            if s.is_table:
                frame = store.select("data", where="trade_date in dates", columns=["trade_date", "ts_code", field])
                frame = frame.loc[frame.ts_code.eq("000300.SH") & pd.to_datetime(frame.trade_date).isin(dates)]
                out = frame.assign(trade_date=pd.to_datetime(frame.trade_date)).set_index("trade_date")[field]
            else:
                group = s.group
                blocks = {}
                for name, child in group._v_children.items():
                    if name.startswith("block") and name.endswith("_items"):
                        labels = [v.decode() if isinstance(v, bytes) else str(v) for v in child[:]]
                        blocks[name[:-6]] = labels
                metadata = [k for k, v in blocks.items() if set(v) == {"ts_code", "trade_date"}]
                numeric_block = [k for k, v in blocks.items() if field in v]
                if len(metadata) != 1 or len(numeric_block) != 1:
                    raise ValueError("fixed index keys/numeric block are ambiguous")
                labels = blocks[metadata[0]]
                if self._index_metadata is None:
                    self._index_metadata = pd.DataFrame(group._f_get_child(metadata[0]+"_values")[0], columns=labels)
                    self._index_metadata["trade_date"] = pd.to_datetime(self._index_metadata.trade_date)
                meta = self._index_metadata
                positions = np.flatnonzero(meta.ts_code.eq("000300.SH") & meta.trade_date.isin(dates))
                numeric = blocks[numeric_block[0]]
                node = group._f_get_child(numeric_block[0]+"_values")
                values = node[positions.tolist(), numeric.index(field)] if len(positions) else []
                out = pd.Series(np.asarray(values).reshape(-1), index=pd.DatetimeIndex(meta.iloc[positions].trade_date), dtype=float)
        if out.index.has_duplicates or not set(out.index).issubset(dates):
            raise ValueError("index projection returned duplicate/foreign dates")
        out = out.map(_positive)
        self.read_receipts.append(dict(role="INDEX_"+field.upper(), fields=[field],
            dates=[str(d.date()) for d in dates], rows=len(out), values_sha256=frame_sha256(out.reset_index())))
        self.assert_unchanged()
        return out

    def suspension_keys(self, dates):
        # Lifecycle metadata is never a T-open peer availability oracle. It is
        # consumed only by post-H valuation, following the native full-day rule.
        if self._suspension_metadata is None:
            p = self._path("suspend_data")
            frame = pd.read_parquet(p, columns=["ts_code", "trade_date", "suspend_type", "suspend_timing"])
            full = frame.suspend_type.eq("S") & (frame.suspend_timing.isna() | frame.suspend_timing.astype(str).str.strip().eq(""))
            self._suspension_metadata = {(str(s).upper(), pd.Timestamp(d).date())
                for s, d in frame.loc[full, ["ts_code", "trade_date"]].itertuples(index=False, name=None)}
        wanted = {pd.Timestamp(d).date() for d in dates}
        return {(s, d) for s, d in self._suspension_metadata if d in wanted}

    def limits(self, symbol, dates):
        """Decode requested float slices, not the future tail of an entire bin."""
        from backend.data_service.frozen_dataset import inside
        out = {}
        for field in ("up_limit_price", "down_limit_price"):
            relative = (self._daily_root.relative_to(self._root)/"features"/symbol.lower()/(field+".day.bin")).as_posix()
            path = inside(self._root, relative)
            before = self._stamp(path)
            if path in self._stamps and self._stamps[path] != before:
                raise ValueError("limit bin changed during the operation")
            values, blocks = {}, []
            with path.open("rb") as stream:
                header = stream.read(4)
                start = float(np.frombuffer(header, dtype="<f4")[0]) if len(header) == 4 else float("nan")
                if not np.isfinite(start) or start != int(start):
                    raise ValueError("invalid limit bin start")
                for day in dates:
                    d = pd.Timestamp(day).date()
                    offset = self._calendar_positions[d]-int(start)+1
                    data = b""
                    if offset >= 1 and 4*(offset+1) <= before[2]:
                        stream.seek(4*offset)
                        data = stream.read(4)
                    v = float(np.frombuffer(data, dtype="<f4")[0]) if data else np.nan
                    number = _positive(v)
                    values[d] = None if number is None else float(Decimal(str(number)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP))
                    blocks.append([d.isoformat(), data.hex()])
            if before != self._stamp(path):
                raise ValueError("limit file changed during projected read")
            self._stamps[path] = before
            self.bin_receipts.append(dict(path=relative, slice_sha256=sha([header.hex(), blocks]),
                pin_authority="CONSUMER_OBSERVED_SLICE_NOT_PRODUCER_PIN"))
            out[field] = values
        return out

    def receipt(self):
        from importlib.metadata import version
        import platform
        self.assert_unchanged()
        return dict(binding_hash=self.binding_hash, source_evidence="FROZEN_FILES_NON_VINTAGE_NATIVE_UNPROVEN",
            projections=self.read_receipts, projected_limit_bins=self.bin_receipts,
            consumer_python_version=platform.python_version(),
            consumer_library_versions={n: version(n) for n in ("numpy", "pandas", "pyarrow", "tables")},
            sql_queries=0, network_calls=0, new_native_receipt=False, source_vintage_upgraded=False)


def load_original_metadata_v1(plan):
    root, header = checked_source(plan)
    calendar = json.loads((root/"calendar.json").read_bytes())
    predicate = (ds.field(KEY[0]) >= pd.Timestamp(plan.train_start)) & (ds.field(KEY[0]) <= pd.Timestamp(plan.evaluation_end))
    roster = ds.dataset(root/"rows.parquet").to_table(columns=list(META), filter=predicate).to_pandas()
    days = ds.dataset(root/"days.parquet").to_table(columns=list(DAY_META)).to_pandas()
    validate_roster_v1(roster, days, plan, calendar)
    for s in plan.sources:
        group = roster.loc[roster.package_id.eq(s.package_id)]
        if not group.manifest_sha256.eq(s.manifest_sha256).all() or not group.run_id.eq(s.run_id).all() or not group.source_id.eq(s.source_id).all():
            raise ValueError("original source identity differs")
    return roster, days, calendar, header


def _raw_daily(frame):
    out = frame.reset_index().rename(columns={"datetime": "trade_date"})
    factor = out.factor.map(_positive)
    values = out[[n for n in ("open", "high", "low", "close", "volume") if n in out]].map(_number)
    result = out[["trade_date", "instrument"]].copy()
    result["adj_factor"] = factor
    for name in ("open", "high", "low", "close"):
        if name in values:
            result["raw_"+name+"_cny"] = [np.nan if pd.isna(v) or pd.isna(f) else float(raw_price_li(v, Decimal(str(f)))/1000)
                for v, f in zip(values[name], factor, strict=True)]
    if "volume" in values:
        result["volume_hand"] = values.volume*factor/100
    return result


def build_peer_panel_v1(*, membership, pool, d_close, t_open, candidates, decision_date, target_date):
    """Every D member retained; unavailable quotes never infer a later halt."""
    d, t = pd.Timestamp(decision_date), pd.Timestamp(target_date)
    if d >= t or membership.instrument.duplicated().any() or len(set(pool)) != len(pool):
        raise ValueError("peer identity/date or membership conflicts")
    if not set(membership.instrument).issubset(pool):
        raise ValueError("peer mapping contains a foreign pool member")
    panel = pd.DataFrame({"instrument": list(pool)}).merge(membership, on="instrument", how="left", validate="one_to_one")
    def coordinate(frame, names, day):
        if frame.duplicated(["trade_date", "instrument"]).any() or not frame.trade_date.eq(day).all():
            raise ValueError("peer projection contains duplicate or foreign quotes")
        return frame.set_index("instrument").loc[:, names]
    close = coordinate(d_close, ["raw_close_cny", "adj_factor"], d)
    opened = coordinate(t_open, ["raw_open_cny", "adj_factor"], t)
    panel = panel.join(close.rename(columns={"adj_factor": "factor_d"}), on="instrument").join(
        opened.rename(columns={"adj_factor": "factor_t"}), on="instrument")
    nums = panel[["raw_close_cny", "raw_open_cny", "factor_d", "factor_t"]].map(_positive)
    known = nums.notna().all(axis=1)
    panel["quote_status"] = np.where(known, "OBSERVED_T_OPEN", "QUOTE_UNOBSERVED_STATE_NOT_INFERRED")
    panel["adjusted_overnight_mark_bps"] = np.nan
    panel.loc[known, "adjusted_overnight_mark_bps"] = 10000*(nums.loc[known, "raw_open_cny"]*nums.loc[known, "factor_t"]
        /(nums.loc[known, "raw_close_cny"]*nums.loc[known, "factor_d"])-1)
    if not np.isfinite(panel.loc[known, "adjusted_overnight_mark_bps"]).all():
        raise ValueError("peer coordinate arithmetic is invalid")
    rows = []
    for symbol in candidates:
        own = panel.loc[panel.instrument.eq(symbol)]
        industry = None if own.empty or pd.isna(own.iloc[0].l2_code_id) else own.iloc[0].l2_code_id
        peers = panel.loc[panel.l2_code_id.eq(industry) & panel.instrument.ne(symbol)] if industry is not None else panel.iloc[:0]
        obs = peers.adjusted_overnight_mark_bps.dropna()
        enough = len(obs) >= PEER_POLICY["minimum_quotes"]
        rows.append({KEY[0]: d, KEY[1]: t, "instrument": symbol,
            "peer_open_median_bps": float(obs.median()) if enough else np.nan,
            "peer_up_fraction": float(obs.gt(0).mean()) if enough else np.nan,
            "peer_quote_coverage": len(obs)/len(peers) if len(peers) else np.nan,
            "peer_count": len(peers), "peer_observed_count": len(obs),
            "peer_status": "KNOWN" if enough else "UNKNOWN_PEER_SUPPORT",
            "peer_members_sha256": sha(peers.instrument.tolist())})
    return pd.DataFrame(rows), panel


def build_sector_context_inputs_v1(*, roster, reader, calendar, cutoff):
    """Original Top5 inputs only; original Top50 metadata is kept separately."""
    sessions = [pd.Timestamp(v).date() for v in calendar]
    if not set(sessions).issubset(reader._calendar):
        raise ValueError("original and file calendar identities do not align")
    for d, t in roster.loc[:, KEY[:2]].drop_duplicates().itertuples(index=False, name=None):
        i = reader._calendar_positions[pd.Timestamp(d).date()]
        if i+1 >= len(reader._calendar) or reader._calendar[i+1] != pd.Timestamp(t).date():
            raise ValueError("file and original D/T clocks differ")
    dates = sorted(roster[KEY[0]].unique())
    output, panels = [], []
    for first in range(0, len(dates), CHUNK_DAYS):
        selected = dates[first:first+CHUNK_DAYS]
        original = roster.loc[roster[KEY[0]].isin(selected)].copy()
        symbols = sorted(set(original.instrument))
        position = sessions.index(pd.Timestamp(selected[0]).date())
        if position < 19:
            raise ValueError("original D lacks its required twenty-session history")
        low = sessions[position-19]
        high = pd.Timestamp(selected[-1]).date()
        raw = _raw_daily(reader.daily(start=low, end=high, columns=["open", "high", "low", "close", "volume", "factor"],
            members=symbols, role="D_HISTORY", cutoff=cutoff))
        history_dates = [d for d in sessions if low <= d <= high]
        index = reader.index(dates=history_dates, field="close").rename("close").rename_axis("trade_date").reset_index()
        index["instrument"] = "000300.SH"
        features, refs = build_shared_daily_features(rosters=original, calendar=sessions, daily=raw, index_daily=index)
        keep = [*KEY, "reference_cny", "d_anchor_factor", "reference_visible_through"]
        features = features.merge(refs[keep], on=list(KEY), validate="many_to_one", sort=False)
        lawful = [(pd.Timestamp(d), pd.Timestamp(sessions[sessions.index(pd.Timestamp(d).date())+1])) for d in selected
            if sessions[sessions.index(pd.Timestamp(d).date())+1] <= pd.Timestamp(cutoff).date()]
        pools = {d: reader.load_pit_eligible_symbols(universe_key=PEER_POLICY["universe"], decision_trade_date=d.date())
            for d, _ in lawful}
        if lawful:
            all_peers = sorted({s for pool, _ in pools.values() for s in pool})
            close_block = _raw_daily(reader.daily(start=lawful[0][0], end=lawful[-1][0], columns=["close", "factor"],
                members=all_peers, role="PEER_D_CLOSE", cutoff=lawful[-1][0]))
            open_block = _raw_daily(reader.daily(start=lawful[0][1], end=lawful[-1][1], columns=["open", "factor"],
                members=all_peers, role="T_OPEN", cutoff=cutoff))
            market_closes = reader.index(dates=[d for d, _ in lawful], field="close")
            market_opens = reader.index(dates=[t for _, t in lawful], field="open")
        for d in selected:
            d = pd.Timestamp(d)
            t = pd.Timestamp(sessions[sessions.index(d.date())+1])
            at = features.loc[features[KEY[0]].eq(d)].copy()
            if t.date() > pd.Timestamp(cutoff).date():
                for name in (*CONTEXT_COLUMNS, "observed_gap_bps"):
                    at[name] = np.nan
                at["peer_status"] = "T_OUTSIDE_DEVELOPMENT_WINDOW"
                output.append(at)
                continue
            pool, pool_hash = pools[d]
            spans = reader._memberships
            membership = spans.loc[(spans.start_date <= d.date()) & (spans.end_date >= d.date())
                & spans.instrument.str.upper().isin(pool), ["instrument", "l2_code_id"]].copy()
            membership["instrument"] = membership.instrument.str.upper()
            if membership.groupby("instrument").l2_code_id.nunique().gt(1).any() or not set(membership.l2_code_id).issubset(reader._sector_codes):
                raise ValueError("D membership is conflicting or unmapped")
            membership = membership.drop_duplicates()
            close = close_block.loc[close_block.trade_date.eq(d) & close_block.instrument.isin(pool)]
            opened = open_block.loc[open_block.trade_date.eq(t) & open_block.instrument.isin(pool)]
            peer, panel = build_peer_panel_v1(membership=membership, pool=pool, d_close=close, t_open=opened,
                candidates=sorted(set(at.instrument)), decision_date=d, target_date=t)
            panel[KEY[0]], panel[KEY[1]], panel["pool_hash"] = d, t, pool_hash
            panels.append(panel)
            at = at.merge(peer, on=list(KEY), validate="many_to_one", sort=False)
            c, o = market_closes.get(d), market_opens.get(t)
            at["market_open_gap_bps"] = np.nan if c is None or o is None or pd.isna(c) or pd.isna(o) else 10000*(o/c-1)
            observed = opened.set_index("instrument")
            f = at.instrument.map(observed.adj_factor)
            opened_price = at.instrument.map(observed.raw_open_cny)
            at["observed_gap_bps"] = 10000*(opened_price*f/(at.reference_cny*at.d_anchor_factor)-1)
            at["peer_pool_hash"] = pool_hash
            output.append(at)
    result = pd.concat(output, ignore_index=True) if output else roster.copy()
    # Restore the exact original order after D batching; no selection is performed.
    result = roster[list(ROSTER_KEY)].merge(result, on=list(ROSTER_KEY), validate="one_to_one", sort=False)
    if result.groupby(list(KEY))[[*FEATURES, *CONTEXT_COLUMNS, "reference_cny", "observed_gap_bps"]].nunique(dropna=False).gt(1).any().any():
        raise ValueError("same stock/D input conflicts across source packages")
    return result, pd.concat(panels, ignore_index=True) if panels else pd.DataFrame()


def measure_sector_finance_v1(*, inputs, reader, calendar, cutoff, role):
    """Only mature keys reach the existing V2 valuation, once per stock/D."""
    if role not in ("TRAIN_FINANCE", "EVALUATION_FINANCE"):
        raise ValueError("unknown financial projection role")
    sessions = [pd.Timestamp(v) for v in calendar]
    cap = pd.Timestamp(cutoff)
    positions = {d: i for i, d in enumerate(sessions)}
    rows = inputs.copy()
    rows["label_information_end"] = rows[KEY[1]].map(lambda t: sessions[positions[t]+4]
        if t in positions and positions[t]+4 < len(sessions) else pd.NaT)
    mature = rows.label_information_end.notna() & rows.label_information_end.le(cap)
    rows["valuation_status"], rows["valuation_policy_sha256"] = "IMMATURE", VALUATION_POLICY_SHA256
    rows["exit_execution_status"], rows["label_status"] = "IMMATURE", "IMMATURE"
    rows["valuation_reason"], rows["label_reason"] = "H_BEYOND_LEGAL_WINDOW", "H_BEYOND_LEGAL_WINDOW"
    rows["verified_suspension_sessions"], rows["carried_valuation_sessions"] = 0, 0
    financial = ["valuation_gross_terminal_ratio", "valuation_path_min_ratio", "hypothetical_liquidation_net_bps", "mark_to_market_net_bps"]
    for field in financial:
        rows[field] = np.nan
    selected_dates = sorted(rows.loc[mature, KEY[0]].unique())
    for first in range(0, len(selected_dates), CHUNK_DAYS):
        dates = selected_dates[first:first+CHUNK_DAYS]
        block = rows.loc[mature & rows[KEY[0]].isin(dates)].copy()
        unique = block.drop_duplicates(list(KEY)).sort_values(list(KEY)).copy()
        unique["selection_effective_rank"] = unique.groupby(KEY[0]).cumcount()+1
        unique["candidate_group_size"] = unique.groupby(KEY[0]).instrument.transform("size")
        low, high = unique[KEY[1]].min(), unique.label_information_end.max()
        raw = _raw_daily(reader.daily(start=low, end=high, columns=["open", "high", "low", "close", "factor"],
            members=sorted(set(unique.instrument)), role=role, cutoff=cap))
        raw_map = raw.set_index(["trade_date", "instrument"]).to_dict("index")
        requested = sorted({v for t in unique[KEY[1]] for v in sessions[positions[t]:positions[t]+5]})
        suspended = reader.suspension_keys(requested)
        limits = {s: reader.limits(s, requested) for s in sorted(set(unique.instrument))}
        prices = []
        for r in unique.to_dict("records"):
            d, t, symbol = (r[n] for n in KEY)
            anchor_factor = _positive(r["d_anchor_factor"])
            for day in sessions[positions[t]:positions[t]+5]:
                raw_row = raw_map.get((day, symbol), {})
                is_suspended = (symbol, day.date()) in suspended
                upper, lower = (limits[symbol][name].get(day.date()) for name in ("up_limit_price", "down_limit_price"))
                factor = _positive(raw_row.get("adj_factor"))
                prices.append({KEY[0]: d, "trade_date": day, "instrument": symbol,
                    **{n: raw_row.get("raw_"+n+"_cny", np.nan) for n in ("open", "high", "low", "close")},
                    "d_anchor_factor": np.nan if factor is None or anchor_factor is None else factor/anchor_factor,
                    "suspended": bool(is_suspended), "tradability_unknown": bool(not is_suspended and (upper is None or lower is None)),
                    "up_limit": upper, "down_limit": lower})
        prices = pd.DataFrame(prices, columns=PRICE_FIELDS)
        references = unique.loc[:, REFERENCE_FIELDS]
        labels, _ = build_generic_price_5td_valuation_v2(candidates=unique.loc[:, ROSTER],
            decision_dates=[pd.Timestamp(d).date() for d in dates], calendar=[d.date() for d in sessions if d <= cap],
            prices=prices, references=references,
            source_context=dict(calendar_sha256=sha(calendar), prices_sha256=frame_sha256(prices),
                references_sha256=frame_sha256(references), label_price_basis="D_REFERENCE_POLICY_RATIO",
                source_evidence="FROZEN_FILES_NON_VINTAGE_NATIVE_UNPROVEN"))
        fields = ["valuation_status", "exit_execution_status", "label_status", "valuation_reason", "label_reason",
            "verified_suspension_sessions", "carried_valuation_sessions", *financial]
        measured = block.loc[:, ROSTER_KEY].merge(labels[[*KEY, *fields]], on=list(KEY), validate="many_to_one", sort=False)
        lookup = measured.set_index(list(ROSTER_KEY))
        mask = rows[KEY[0]].isin(dates) & mature
        indices = pd.MultiIndex.from_frame(rows.loc[mask, list(ROSTER_KEY)])
        for field in fields:
            rows.loc[mask, field] = lookup[field].reindex(indices).to_numpy()
    rows["label_cluster"] = [sha([str(d.date()), str(t.date()), s]) for d, t, s in rows[list(KEY)].itertuples(index=False, name=None)]
    rows["cluster_mass"] = 1/rows.groupby("label_cluster").label_cluster.transform("size")
    rows["net_return_bps"] = rows.hypothetical_liquidation_net_bps
    rows["supervision_status"] = np.where(rows.valuation_status.eq("AVAILABLE"), "AVAILABLE", "UNKNOWN_OR_IMMATURE")
    return rows


def prepare_sector_inputs_v1(*, plan, reader=None):
    from backend.services.advisory_model_first.sector_relative_open_price_5td_model_v1 import fit_encoding_v1
    roster, days, calendar, header = load_original_metadata_v1(plan)
    reader = reader or SectorOpenFileProjectionV1(plan.file_source_binding)
    if reader.binding_hash != plan.file_source_binding["binding_hash"]:
        raise ValueError("common file reader does not match the preregistered binding")
    training = roster.loc[roster[KEY[0]].le(pd.Timestamp(plan.train_end)) & roster.selection_effective_rank.le(5)].copy()
    contexts, panel = build_sector_context_inputs_v1(roster=training, reader=reader, calendar=calendar, cutoff=plan.train_end)
    domain = measure_sector_finance_v1(inputs=contexts, reader=reader, calendar=calendar, cutoff=plan.train_end, role="TRAIN_FINANCE")
    clocks = clocks_v1(plan, calendar)
    encoding = fit_encoding_v1(domain, clocks)
    return dict(roster=roster, days=days, calendar=calendar, domain=domain, encoding=encoding,
        peer_panel=panel, source_receipt=reader.receipt(), source_stage_sha256=header["stage_sha256"])
