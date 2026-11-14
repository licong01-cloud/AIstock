"use client";
import Link from "next/link";
import { useState } from "react";
import { path } from "@/lib/api/advisoryWorkbench";
import type { BriefOverview } from "@/lib/api/advisoryComparison";
import Comparison from "./Comparison";
import { Badge, day, Empty, ErrorNotice, Metric, number, text, time, useRead } from "./shared";
import styles from "./workbench.module.css";

export default function Overview({ refresh, onRefresh }: { refresh: number; onRefresh: () => void }) {
  const [status, setStatus] = useState("ENABLED");
  const [query, setQuery] = useState("");
  const { data, error, loading } = useRead<BriefOverview>(path("/overview", { status, mode: "brief" }), refresh);
  const today = new Date().toLocaleDateString("en-CA", { timeZone: "Asia/Shanghai" });
  const integrity = useRead<{ fact_date: string; all_sync_complete: boolean; state?: string }>(`/local-data/integrity/days/${today}`, refresh);
  const cards = data?.items.filter(row => row.program.program_name.toLowerCase().includes(query.toLowerCase())) || [];
  return <>
    <header className={styles.header}><div><h1>荐股总览</h1><p>按策略查看推荐、跟踪状态与历史表现</p></div><div className={styles.actions}><a href="#strategy-comparison">收益对比 ↓</a><button onClick={onRefresh} disabled={loading}>刷新</button><Link className={styles.primary} href="/paper-v2/advisory/manage">任务管理</Link></div></header>
    <div className={styles.toolbar}><span>最近读取：{time(data?.generated_at)}</span><span>先显示发布概况 · 收益单独按所选区间比较</span></div>
    <ErrorNotice error={error} stale={!!data} />
    <div className={styles.metrics}>
      <Metric label="所选任务" value={data ? number(data.count) : "—"} note={status === "ENABLED" ? "当前已启用" : text(status)} />
      <Metric label="当前目标日已发布" value={data ? number(data.status_counts.PUBLISHED || 0) : "—"} />
      <Metric label="发布受阻" value={data ? number((data.status_counts.ASSET_BLOCKED || 0) + (data.status_counts.FAILED || 0)) : "—"} />
      <Metric label="等待数据或名单" value={data ? number((data.status_counts.WAITING_DATA || 0) + (data.status_counts.NO_CURRENT_LIST || 0)) : "—"} />
    </div>
    <div className={styles.notice}><span>{today} 数据同步：{integrity.error ? "完成状态未能确认" : integrity.data?.all_sync_complete === true ? "全部同步已完成" : integrity.data ? "仍有数据待完成或待验收" : "正在读取权威台账"}。各策略输入就绪另按依赖核对。</span><Link href="/local-data">查看数据管理 →</Link></div>
    <div className={styles.toolbar}><input aria-label="搜索策略" placeholder="搜索策略名称" value={query} onChange={e => setQuery(e.target.value)} /><div className={styles.filters}><select aria-label="任务状态" value={status} onChange={e => setStatus(e.target.value)}>{["ENABLED", "PAUSED", "DRAFT", "ARCHIVED", "ALL"].map(s => <option key={s} value={s}>{text(s)}</option>)}</select><span>异常优先 · 按任务分别统计</span></div></div>
    {loading && !data ? <Empty>正在读取策略概况…</Empty> : !error && data && !cards.length ? <Empty>当前筛选下没有荐股任务。</Empty> : null}
    <section className={styles.cardGrid} aria-label="策略荐股概况">
      {cards.map(card => <article key={card.program.program_id} className={`${styles.strategyCard} ${card.runtime.state === "ASSET_BLOCKED" ? styles.blocked : ""}`}>
        <header><h2><Link href={`/paper-v2/advisory?program_id=${encodeURIComponent(card.program.program_id)}`}>{card.program.program_name}</Link></h2><Badge value={card.runtime.state} /></header>
        <p className={styles.muted}>{text(card.program.status)} · 启用 {day(card.program.enabled_since)}</p>
        <p className={styles.muted}>模型：{[...new Set(card.package_models?.flatMap(p => p.model_types) || [])].join(" / ") || "类别待核验"}</p>
        <div className={styles.dateRow}><div>首次发布目标日<strong>{day(card.coverage.first_published_target_date)}</strong></div><div>最新推荐<strong>{day(card.latest?.trade_date)}</strong><small>输入截止 {day(card.latest?.date_context?.selection_as_of_trade_date || card.latest?.date_context?.decision_as_of_trade_date)}</small></div></div>
        <div className={styles.cardMetrics}>
          <Metric label="最新名单范围" value={number(card.latest_in_scope_count)} note={card.latest && card.latest_in_scope_count == null ? "成员身份待核验" : undefined} /><Metric label="已保存版本" value={number(card.versions_count)} />
        </div>
        <p className={styles.muted}>生成于 {time(card.latest?.created_at)}</p>
        {card.runtime.state === "ASSET_BLOCKED" ? <div className={styles.inlineWarning}>冻结模型资产绑定不完整，后续发布受阻。</div> : <p className={styles.muted}>目标日结算：{text(card.runtime.settlement, "未记录")}</p>}
        <footer><span>推荐与持续跟踪</span><Link href={`/paper-v2/advisory?program_id=${encodeURIComponent(card.program.program_id)}`}>查看详情 →</Link></footer>
      </article>)}
    </section>
    {!!data?.items.length && <Comparison key={status} programs={data.items.map(c => c.program)} status={status} refresh={refresh} />}
  </>;
}
