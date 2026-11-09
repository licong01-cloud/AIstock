"use client";
import Link from "next/link";
import { useState } from "react";
import type { ComparisonResponse, ComparisonRow, Distribution } from "@/lib/api/advisoryComparison";
import { bps, day, gain, number, percent, text } from "./shared";
import styles from "./workbench.module.css";

const numeric = (v: number | null) => v == null ? "—" : v.toFixed(2);
const reasonText: Record<string, string> = { EVALUABLE: "可评价", IMMATURE: "未满5交易日", UNKNOWN: "行情待齐", DUPLICATE_PUBLICATION: "同日版本冲突", MEMBERSHIP_UNKNOWN: "成员身份待核验", LIST_IDENTITY_UNKNOWN: "名单身份待核验", CALENDAR_UNKNOWN: "日历不足", NOT_EVALUATED_EMPTY_SCOPE: "无推荐，不评价", PRICE_OR_ADJUSTMENT_MISSING: "缺少价格或调整因子", LIMIT_DATA_MISSING: "缺少涨跌停数据", EXIT_NOT_EXECUTABLE: "无法按期退出", SUSPENDED: "停牌估值待核验", PUBLICATION_SOURCE_UNKNOWN: "发布来源待核验", NO_PUBLICATIONS_IN_SCOPE: "所选范围没有发布记录", NO_TRADING_DAYS: "没有交易日" };
const reason = (value: string) => reasonText[value] || "证据待核验";
function DistributionDetail({ label, data }: { label: string; data: Distribution }) {
  return <div><h4>{label} · {data.sample_count} 个</h4><dl className={styles.detailMetrics}>
    <div><dt>均值 / 中位</dt><dd>{bps(data.avg_return_bps)} / {bps(data.median_return_bps)}</dd></div>
    <div><dt>胜率</dt><dd>{percent(data.win_rate)}</dd></div><div><dt>平均盈利 / 亏损</dt><dd>{bps(data.avg_win_bps)} / {bps(data.avg_loss_bps)}</dd></div>
    <div><dt>盈亏比 / 盈利因子</dt><dd>{numeric(data.payoff_ratio)} / {numeric(data.profit_factor)}</dd></div>
  </dl></div>;
}
function Detail({ row }: { row: ComparisonRow }) {
  const stats = row.fixed_5td, cover = row.coverage, track = row.tracking;
  return <div className={styles.comparisonDetail}>
    <div><h4>固定5TD · 原名单等权</h4><p>毛收益 {bps(stats.gross.avg_return_bps)} · 最差 {bps(stats.worst_bps)} · 最差10%均值 {bps(stats.tail_10pct_bps)}</p><p>超额95%描述区间 {stats.excess_interval_bps ? `[${bps(stats.excess_interval_bps[0])}, ${bps(stats.excess_interval_bps[1])}]` : "样本不足5日，暂不计算"}</p><p>已校正4阶序列相关；区间不是独立经济确认。</p></div>
    <DistributionDetail label="实际过程 · 已结束毛收益" data={track.gross} /><DistributionDetail label="实际过程 · 已结束净收益" data={track.net} />
    <div><h4>跟踪与估值</h4><p>浮动毛收益 {bps(track.floating_gross.avg_return_bps)} · {track.floating_gross.sample_count} 个</p><p>假设清算净收益 {bps(track.floating_liquidation_net.avg_return_bps)} · 估值 {day(track.mark_date)}</p><p>平均持有 {numeric(track.avg_holding_days)} 交易日 · 待结算 {track.pending_exit_count} · 缺失估值 {track.missing_mark_count}</p><p>退出原因：{Object.entries(track.exit_reasons).map(([reason, count]) => `${text(reason, "其他退出")} ${count}`).join(" · ") || "暂无已结束样本"}</p></div>
    <div><h4>样本与覆盖</h4><p>已发布 {cover.published_count} · 成熟 {cover.mature_count} · 可评价 {cover.evaluable_count} · 本次比较 {cover.compared_count}</p><p>原始槽位 {cover.original_slots} · 已知无法买入留现金 {cover.cash_slots} · 未知 {cover.unknown_slots}</p><p>名单缺口 {cover.missing_publication_count} 日 · 来源筛选排除 {number(cover.source_excluded_date_count)} 日 · {Object.entries(cover.source_counts).map(([s, n]) => `${text(s)} ${n}`).join(" · ")}</p><p>过程来源/绑定待核验 {number(cover.unverified_origin_episode_count)} 个</p><p>状态：{Object.entries(cover.status_counts).map(([s, n]) => `${reason(s)} ${n}`).join(" · ") || "无名单"}</p>{cover.missing_publication_dates.length > 0 && <details><summary>查看缺失目标日期</summary><p className={styles.missingDates}>{cover.missing_publication_dates.join("、")}</p></details>}<small>绑定 {row.binding_version_id || "待核验"}</small></div>
    <div><h4>参考组合 · 非账户收益</h4><p>现金 {percent(row.portfolio.cash_fraction)} · 投入 {percent(row.portfolio.invested_fraction)} · 累计费用 / 初始资金 {bps(row.portfolio.cost_drag_bps)}</p><p>沪深300同批次参考收益 {bps(row.benchmark_portfolio.total_return_bps)} · 回撤 {bps(row.benchmark_portfolio.max_drawdown_bps)}</p>{row.portfolio.status !== "READY" && <p>净值待核验：{reason(row.portfolio.reason || "UNKNOWN")} · 自 {day(row.portfolio.unknown_from)}</p>}<p>每天最多投入前日净值20%，同日卖出资金次日可用；未知持仓估值后续净值留空。</p></div>
  </div>;
}
export default function ComparisonTable({ data }: { data: ComparisonResponse }) {
  const [sort, setSort] = useState("excess");
  const value = (r: ComparisonRow) => sort === "return" ? r.portfolio.total_return_bps : sort === "risk" ? r.portfolio.max_drawdown_bps : r.fixed_5td.excess.avg_return_bps;
  const rows = [...data.items].sort((a, b) => (value(b) ?? -Infinity) - (value(a) ?? -Infinity));
  return <><div className={styles.toolbar}><span className={styles.muted}>净收益减同期沪深300毛收益 = 净超额 · 点击展开查看全部指标</span><label>排序 <select aria-label="对比排序" value={sort} onChange={e => setSort(e.target.value)}><option value="excess">5TD净超额</option><option value="return">参考组合收益</option><option value="risk">最大回撤更小</option></select></label></div>
    <div className={styles.tableWrap}><table className={styles.comparisonTable}><thead><tr><th>策略包</th><th>比较日数</th><th>5TD净均值</th><th>5TD净中位</th><th>5TD胜率</th><th>5TD净超额</th><th>参考组合收益</th><th>组合最大回撤</th><th>详情</th></tr></thead><tbody>{rows.map(row => <ComparisonTableRow key={row.arm_id} row={row} />)}</tbody></table></div>
    <p className={styles.explanation}>实际过程按各自参考进入日选样，按原始进入/退出时点及未复权参考价格计算；固定5TD和参考组合使用复权价格。共同日期约束仅作用于固定5TD。参考组合使用同一资金规则和区间，保留各策略发布缺口。详情名单的累计涨跌另按连续荐股期间计算。</p>
  </>;
}
function ComparisonTableRow({ row }: { row: ComparisonRow }) {
  const [open, setOpen] = useState(false), fixed = row.fixed_5td;
  return <><tr><td><Link href={`/paper-v2/advisory?program_id=${encodeURIComponent(row.program_id)}`}>{row.name}</Link><small>{row.binding_version_id?.slice(-10) || "绑定待核验"}</small></td><td>{number(row.coverage.compared_count)}<small>各自可评 {row.coverage.evaluable_count} 日</small></td><td className={gain(fixed.net.avg_return_bps)}>{bps(fixed.net.avg_return_bps)}</td><td className={gain(fixed.net.median_return_bps)}>{bps(fixed.net.median_return_bps)}</td><td>{percent(fixed.net.win_rate)}</td><td className={gain(fixed.excess.avg_return_bps)}><strong>{bps(fixed.excess.avg_return_bps)}</strong></td><td className={gain(row.portfolio.total_return_bps)}>{bps(row.portfolio.total_return_bps)}</td><td className={gain(row.portfolio.max_drawdown_bps)}>{bps(row.portfolio.max_drawdown_bps)}</td><td><button aria-label={`展开${row.name}指标`} aria-expanded={open} onClick={() => setOpen(v => !v)}>{open ? "收起" : "全部指标"}</button></td></tr>{open && <tr><td colSpan={9}><Detail row={row} /></td></tr>}</>;
}
