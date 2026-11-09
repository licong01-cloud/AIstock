"use client";
import type { ComparisonResponse, NavPoint, SeriesResponse } from "@/lib/api/advisoryComparison";
import { bps, Empty, ErrorNotice, useRead } from "./shared";
import styles from "./workbench.module.css";

const colors = ["#2563eb", "#7c3aed", "#0891b2", "#d97706", "#db2777", "#059669", "#475569", "#9333ea"];
const color = (i: number) => colors[i] || `hsl(${i * 137.5 % 360} 65% 43%)`;
function BarChart({ data }: { data: ComparisonResponse }) {
  const known = data.items.filter(r => r.fixed_5td.excess.avg_return_bps != null);
  const span = Math.max(1, ...known.map(r => Math.abs(r.fixed_5td.excess.avg_return_bps!)));
  return <div className={styles.barChart}>{known.length ? known.map(r => { const v = r.fixed_5td.excess.avg_return_bps!; return <div className={styles.barRow} key={r.arm_id}><span title={r.name}>{r.name}</span><div className={styles.barTrack}><i className={styles.zeroLine} /><span style={{ left: `${v < 0 ? 50 + v / span * 46 : 50}%`, width: `${Math.abs(v) / span * 46}%`, background: color(data.items.findIndex(item => item.arm_id === r.arm_id)) }} title={`${r.name}：${bps(v)}`} /></div><strong>{bps(v)}</strong></div>; }) : <Empty>没有共同可评价样本。可调整时间或选择参与对比的策略。</Empty>}</div>;
}
function NavChart({ data }: { data: SeriesResponse }) {
  const sets = data.series.flatMap((s, i) => [{ name: s.name, points: s.nav, dashed: false, i }, { name: `${s.name} · 沪深300同批次`, points: s.benchmark, dashed: true, i }]);
  const dates = [...new Set(sets.flatMap(s => s.points.map(p => p[0])))].sort();
  const first = Date.parse(dates[0] || "1970-01-01"), duration = Date.parse(dates.at(-1) || "1970-01-01") - first;
  const x = new Map(dates.map(d => [d, 45 + (Date.parse(d) - first) / Math.max(1, duration) * 650]));
  const values = sets.flatMap(s => s.points.map(p => p[1]).filter((v): v is number => v != null));
  const low = Math.min(.99, ...values), high = Math.max(1.01, ...values), spread = high - low;
  const y = (value: number) => 205 - (value - low) / spread * 175;
  const line = (points: NavPoint[]) => { let pen = false; return points.map(([d, v]) => { if (v == null) { pen = false; return ""; } const part = `${pen ? "L" : "M"}${x.get(d)},${y(v)}`; pen = true; return part; }).join(" "); };
  return <><svg className={styles.chartSvg} viewBox="0 0 720 245" role="img" aria-label="参考组合与沪深300净值曲线，初始净值为1">
    {[0, .5, 1].map(t => <g key={t}><line x1="45" x2="695" y1={y(low + spread * t)} y2={y(low + spread * t)} stroke="#e2e8f0" /><text x="4" y={y(low + spread * t) + 4}>{(low + spread * t).toFixed(3)}</text></g>)}
    <line x1="45" x2="695" y1={y(1)} y2={y(1)} stroke="#94a3b8" strokeDasharray="3 4" />
    {sets.map(s => <path key={s.name + s.i} d={line(s.points)} fill="none" stroke={color(s.i)} strokeWidth={s.dashed ? 1.2 : 2.2} strokeDasharray={s.dashed ? "5 4" : undefined}><title>{s.name}</title></path>)}
    <text x="45" y="235">{dates[0]}</text><text x="695" y="235" textAnchor="end">{dates.at(-1)}</text>
  </svg><div className={styles.chartLegend}>{data.series.map((s, i) => <span key={s.arm_id}><i style={{ background: color(i) }} />{s.name}</span>)}</div><p className={styles.explanation}>实线为策略，虚线为同一发布日程的沪深300毛收益参考；各线从相同区间、初始资金1开始。空缺保持断线。曲线采样不影响指标计算。</p></>;
}
function Heatmap({ data }: { data: SeriesResponse }) {
  const months = [...new Set(data.series.flatMap(s => s.monthly.map(r => r.month)))].sort();
  return <div className={styles.tableWrap}><table className={styles.heatmap}><thead><tr><th>平均5TD净超额</th>{months.map(m => <th key={m}>{m}</th>)}</tr></thead><tbody>{data.series.map(s => <tr key={s.arm_id}><td>{s.name}</td>{months.map(m => { const r = s.monthly.find(r => r.month === m); return <td key={m} style={r ? { background: r.excess_bps >= 0 ? "#fff1f2" : "#ecfdf5", color: r.excess_bps >= 0 ? "#be123c" : "#047857" } : undefined} title={r ? `${r.count} 个可评价日` : "无可评价日"}>{r ? bps(r.excess_bps) : "—"}</td>; })}</tr>)}</tbody></table>{!months.length && <Empty>当前时间范围暂无月度样本。</Empty>}</div>;
}
function Scatter({ data }: { data: ComparisonResponse }) {
  const rows = data.items.filter(r => r.portfolio.total_return_bps != null && r.portfolio.max_drawdown_bps != null);
  const minX = Math.min(-1, ...rows.map(r => r.portfolio.max_drawdown_bps!));
  const minY = Math.min(-1, ...rows.map(r => r.portfolio.total_return_bps!)), maxY = Math.max(1, ...rows.map(r => r.portfolio.total_return_bps!));
  return <><svg viewBox="0 0 720 260" className={styles.chartSvg} role="img" aria-label="参考组合收益与最大回撤散点图，越靠右上收益风险表现越好"><line x1="80" x2="680" y1="210" y2="210" stroke="#94a3b8" /><line x1="80" x2="80" y1="30" y2="210" stroke="#94a3b8" /><text x="80" y="235">{bps(minX)}</text><text x="680" y="235" textAnchor="end">回撤0%</text><text x="8" y="40">{bps(maxY)}</text><text x="8" y="210">{bps(minY)}</text>{rows.map((r, i) => <circle key={r.arm_id} cx={80 + (r.portfolio.max_drawdown_bps! - minX) / -minX * 590} cy={205 - (r.portfolio.total_return_bps! - minY) / (maxY - minY) * 170} r={7} fill={color(data.items.findIndex(item => item.arm_id === r.arm_id))} stroke="white" strokeWidth="2"><title>{r.name}：收益{bps(r.portfolio.total_return_bps)}，回撤{bps(r.portfolio.max_drawdown_bps)}</title></circle>)}</svg><div className={styles.chartLegend}>{rows.map((r, i) => <span key={r.arm_id}><i style={{ background: color(data.items.findIndex(item => item.arm_id === r.arm_id)) }} />{r.name}</span>)}</div>{!rows.length && <Empty>缺少完整净值，暂不绘制风险散点。</Empty>}</>;
}
export default function ComparisonCharts({ summary, url, refresh }: { summary: ComparisonResponse; url: string; refresh: number }) {
  const result = useRead<SeriesResponse>(url, refresh);
  const consistent = result.data?.source_version === summary.source_version;
  return <div className={styles.chartGrid}><section className={styles.chartPanel}><h3>固定5TD · 净超额收益</h3><BarChart data={summary} /></section><section className={styles.chartPanel}><h3>参考组合 · 收益与风险</h3><Scatter data={summary} /></section>
    <section className={styles.chartWide}><h3>参考组合净值 · 含费用与浮动估值</h3><ErrorNotice error={result.error} stale={false} />{result.loading && <Empty>正在按需读取净值曲线…</Empty>}{consistent && result.data ? <NavChart data={result.data} /> : !result.loading && !result.error ? <Empty>来源版本已变化，请刷新对比后重新查看图表。</Empty> : null}</section>
    <section className={styles.chartWide}><h3>月度稳定性 · 按推荐目标日分组</h3>{consistent && result.data && <Heatmap data={result.data} />}</section>
  </div>;
}
