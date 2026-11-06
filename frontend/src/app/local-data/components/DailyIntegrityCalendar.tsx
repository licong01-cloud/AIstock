"use client";

import React, { useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import styles from "./dailyIntegrity.module.css";

type State = "NOT_TRADING" | "UNVERIFIED" | "IN_PROGRESS" | "PARTIAL" | "ERROR" | "COMPLETE";
type Count = { unit: string; expected: number | null; valid: number | null; legal_na: number | null;
  missing: number | null; invalid: number | null; unverified: number | null };
type Day = { fact_date: string; state: State; completion_kind: string | null; as_of: string;
  future?: boolean;
  watermark: string; view_watermark?: string;
  revision: number | null; all_sync_complete: boolean; release_contains_revision: boolean | null;
  release_coverage_state: string; release_read_error?: string; partitions?: Partition[] };
type Partition = { dataset: string; state: string; data_revision?: number; validated_revision?: number;
  due_at: string | null; next_retry_at?: string | null; metadata?: { validation?: { counts?: Count[]; reason?: string } } };
type Calendar = { month: string; as_of: string; days: Day[]; counts: Record<State, number>;
  trading_day_count?: number; recovered_day_count?: number };
type GapPage = { dataset: string; groups: Record<string, unknown>[]; next_cursor: string | null; group_count: number };

const labels: Record<State, string> = { NOT_TRADING: "休市", UNVERIFIED: "待核验", IN_PROGRESS: "同步或等待",
  PARTIAL: "部分完成", ERROR: "检查异常", COMPLETE: "完整" };
const kinds: Record<string, string> = { RECOVERED: "补齐", HISTORICAL_VERIFIED: "历史补验", SOURCE_REVISED: "已修订" };
const todayCN = () => new Intl.DateTimeFormat("sv-SE", { timeZone: "Asia/Shanghai" }).format(new Date());

async function read<T>(base: string, path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${base.replace(/\/$/, "")}/api/v1/local-data/${path}`, { signal, cache: "no-store" });
  if (!response.ok) throw new Error(`读取失败（HTTP ${response.status}）`);
  return response.json();
}

function moveMonth(value: string, offset: number) {
  const [year, month] = value.split("-").map(Number);
  const result = new Date(Date.UTC(year, month - 1 + offset, 1));
  return result.toISOString().slice(0, 7);
}

export function DailyIntegrityCalendar({ backendBase }: { backendBase: string }) {
  const [month, setMonth] = useState(() => todayCN().slice(0, 7));
  const [calendar, setCalendar] = useState<Calendar | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<Day | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [gaps, setGaps] = useState<GapPage | null>(null);
  const [gapError, setGapError] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);
  const gapRequest = useRef<AbortController | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let busy = false;
    async function load() {
      if (document.hidden || busy || controller.signal.aborted) return;
      busy = true;
      try {
        const result = await read<Calendar>(backendBase, `integrity/calendar?month=${month}`, controller.signal);
        if (!Array.isArray(result.days) || !result.counts || result.month !== month) throw new Error("状态台账合同无效");
        if (!controller.signal.aborted) { setCalendar(result); setError(null); }
      } catch (cause) {
        if (!controller.signal.aborted) setError(cause instanceof Error ? cause.message : "读取失败");
      } finally {
        busy = false;
        if (!controller.signal.aborted) timer = setTimeout(load, 15000);
      }
    }
    const visible = () => {
      if (timer) clearTimeout(timer);
      if (!document.hidden) void load();
    };
    void load();
    document.addEventListener("visibilitychange", visible);
    return () => { controller.abort(); if (timer) clearTimeout(timer); document.removeEventListener("visibilitychange", visible); };
  }, [backendBase, month, refresh]);

  useEffect(() => {
    if (!selected) { setDetail(null); return; }
    const controller = new AbortController();
    setDetail(null); setDetailError(null); setGaps(null); setGapError(null);
    gapRequest.current?.abort();
    void read<Day>(backendBase, `integrity/days/${selected}`, controller.signal).then(result => {
      if (!controller.signal.aborted) setDetail(result);
    }).catch(cause => { if (!controller.signal.aborted) setDetailError(String(cause.message || cause)); });
    return () => controller.abort();
  }, [backendBase, selected, calendar?.days.find(day => day.fact_date === selected)?.view_watermark
      || calendar?.days.find(day => day.fact_date === selected)?.watermark]);

  useEffect(() => () => gapRequest.current?.abort(), []);

  function selectMonth(value: string) {
    if (!/^\d{4}-\d{2}$/.test(value)) return;
    setMonth(value); setSelected(null); setDetail(null); setCalendar(null); setGaps(null); setError(null);
    gapRequest.current?.abort();
  }
  async function loadGaps(dataset: string, cursor?: string) {
    gapRequest.current?.abort();
    const controller = new AbortController(); gapRequest.current = controller;
    setGapError(null);
    try {
      const query = new URLSearchParams({ dataset, limit: "100", ...(cursor ? { cursor } : {}) });
      const result = await read<GapPage>(backendBase, `integrity/days/${selected}/gaps?${query}`, controller.signal);
      if (!controller.signal.aborted) setGaps(result);
    } catch (cause) {
      if (!controller.signal.aborted) { setGaps(null); setGapError("缺口读取失败或版本已变化，请重新选择数据集。"); }
    }
  }
  const offset = (new Date(`${month}-01T00:00:00Z`).getUTCDay() + 6) % 7;
  return <Card className={styles.root} data-testid="daily-integrity-calendar">
    <CardHeader><CardTitle>交易日数据完整性</CardTitle></CardHeader>
    <CardContent>
      <div className={styles.toolbar}>
        <Button variant="outline" size="sm" onClick={() => selectMonth(moveMonth(month, -1))} aria-label="上个月">←</Button>
        <input type="month" aria-label="完整性月份" value={month} onChange={event => selectMonth(event.target.value)} />
        <Button variant="outline" size="sm" onClick={() => selectMonth(moveMonth(month, 1))} aria-label="下个月">→</Button>
        <Button variant="outline" size="sm" onClick={() => { selectMonth(todayCN().slice(0, 7)); setSelected(todayCN()); }}>今天</Button>
        <Button variant="outline" size="sm" onClick={() => setRefresh(value => value + 1)}>刷新状态</Button>
      </div>
      {error && <p role="alert" className={styles.error}>无法获取最新状态：{error}。下方如有数据，仅为上次读回。</p>}
      <p className={styles.note}>事实完整性与任务执行状态、发布文件覆盖分别显示；未知不代表成功。</p>
      {calendar && <p aria-live="polite" className={styles.note}>最后读回：{calendar.as_of} · {Object.entries(calendar.counts).map(([state, count]) => `${labels[state as State]} ${count}`).join(" · ")}</p>}
      {calendar && <p className={styles.note}>交易日 {calendar.trading_day_count ?? "未知"} · 补齐 {calendar.recovered_day_count ?? "未知"}</p>}
      <div className={styles.grid} aria-label={`${month}完整性日历`}>
        {["一", "二", "三", "四", "五", "六", "日"].map(day => <span key={day} className={styles.weekday}>{day}</span>)}
        {Array.from({ length: offset }, (_, index) => <span key={`blank-${index}`} />)}
        {(calendar?.days || []).map(day => <button type="button" key={day.fact_date}
          className={`${styles.day} ${styles[day.state]} ${day.completion_kind === "RECOVERED" ? styles.recovered : ""}`}
          onClick={() => setSelected(day.fact_date)} aria-pressed={selected === day.fact_date}
          aria-label={`${day.fact_date} ${day.future ? "未到期" : labels[day.state]} ${kinds[day.completion_kind || ""] || ""}`}>
          <span>{Number(day.fact_date.slice(-2))}</span><span>{day.future ? "未到期" : labels[day.state]}</span>
          {day.completion_kind && kinds[day.completion_kind] && <small>{kinds[day.completion_kind]}</small>}
        </button>)}
      </div>
      {!calendar && !error && <p role="status">正在读取状态台账…</p>}
      <p className={styles.legend} aria-label="状态图例">灰：待核验/休市　蓝：同步或等待　黄：部分完成　红：检查异常　绿：完整　青绿：补齐</p>
      {selected && <section aria-label={`${selected}详情`} className={styles.detail}>
        <h3>{selected} 数据详情</h3>
        {detailError && <p role="alert">无法读取详情：{detailError}</p>}
        {!detail && !detailError && <p role="status">正在读取…</p>}
        {detail && <>
          <p>源完整：{detail.all_sync_complete ? "是" : "否"} · 已包含于活动发布：{detail.release_coverage_state === "OUTSIDE_CUTOFF" ? "未覆盖" : detail.release_contains_revision === null ? "未建立对应关系" : detail.release_contains_revision ? "是" : "否"}</p>
          {detail.release_read_error && <p role="alert">活动发布身份暂不可读，不能确认是否包含本次源修订；源完整性结论不因此改变。</p>}
          <div className={styles.tableScroll}><table><thead><tr><th>数据集/状态</th><th>应到时间</th><th>已验/当前修订</th><th>检查计数（未知不填0）</th><th>缺口</th></tr></thead>
            <tbody>{detail.partitions?.map(partition => <tr key={partition.dataset}>
              <td>{partition.dataset}<br />{partition.state}<br />{partition.metadata?.validation?.reason || ""}
                {partition.next_retry_at && <small>下次重试：{partition.next_retry_at}</small>}</td><td>{partition.due_at || "待确认日历"}</td>
              <td>{partition.validated_revision ?? "未知"} / {partition.data_revision ?? "未知"}</td>
              <td>{partition.metadata?.validation?.counts?.map((count, index) => <div key={index}>{count.unit}：应有 {count.expected ?? "未知"}，有效 {count.valid ?? "未知"}，合法NA {count.legal_na ?? "未知"}，缺失 {count.missing ?? "未知"}，无效 {count.invalid ?? "未知"}，未验 {count.unverified ?? "未知"}</div>) || "尚未验收"}</td>
              <td><Button variant="outline" size="sm" onClick={() => void loadGaps(partition.dataset)}>查看缺口</Button></td>
            </tr>)}</tbody></table></div>
          {gapError && <p role="alert">{gapError}</p>}
          {gaps && <section aria-label="分页缺口"><h4>{gaps.dataset}：共 {gaps.group_count} 个缺口组</h4>
            <pre className={styles.gaps}>{JSON.stringify(gaps.groups, null, 2)}</pre>
            {gaps.next_cursor && <Button variant="outline" onClick={() => void loadGaps(gaps.dataset, gaps.next_cursor || undefined)}>下一页缺口</Button>}
          </section>}
        </>}
      </section>}
    </CardContent>
  </Card>;
}
