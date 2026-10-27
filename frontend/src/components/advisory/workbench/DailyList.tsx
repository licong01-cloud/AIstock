"use client";
import { useState } from "react";
import { path, programPath, type DailyItem, type DailyPage, type Summary } from "@/lib/api/advisoryWorkbench";
import { Badge, bps, day, Empty, ErrorNotice, gain, Metric, number, Pager, text, useRead } from "./shared";
import styles from "./workbench.module.css";

type Props = { summary: Summary; refresh: number; onStock: (item: DailyItem) => void; onDate: (date: string) => void };
function Rows({ items, onStock }: { items: DailyItem[]; onStock: Props["onStock"] }) {
  return <div className={styles.tableWrap}><table><thead><tr><th>排名</th><th>股票</th><th>今日变化</th><th>连续推荐</th><th>跟踪状态</th><th>参考价</th><th>参考收益</th></tr></thead><tbody>{items.map((row, i) => <tr key={`${row.symbol}-${i}`} className={row.change === "NEW" ? styles.newRow : ""}>
    <td>{number(row.rank)}</td><td><button className={styles.stockLink} onClick={() => onStock(row)}>{row.stock_name || row.symbol}</button><small>{row.symbol}</small></td>
    <td><Badge value={row.change === "UNKNOWN" && row.membership !== "IN_SCOPE" ? row.membership : row.change} /></td><td>{row.consecutive_days == null ? "—" : row.consecutive_days === 1 ? "首日" : `${row.consecutive_days} 日`}</td>
    <td><Badge value={row.progress} />{row.membership !== "IN_SCOPE" && <small>{text(row.reason_code, "原因待核验")}</small>}</td>
    <td>{row.reference_price == null ? "待确认" : `¥${number(row.reference_price, 2)}`}<small>{row.reference_date ? day(row.reference_date) : "缺少参考日期"}</small></td>
    <td className={gain(row.episode?.return_bps)}>{bps(row.episode?.return_bps)}{row.episode?.return_bps != null && <small>{row.episode.return_kind === "REALIZED" ? "已结束参考收益" : "浮动参考收益"} · {day(row.episode.mark_date || row.episode.metric_as_of_trade_date)}</small>}</td>
  </tr>)}</tbody></table></div>;
}
function OtherGroup({ summary, refresh, group, title, onStock }: Omit<Props, "onDate"> & { group: string; title: string }) {
  const [page, setPage] = useState(1), [limit, setLimit] = useState(20);
  const result = useRead<DailyPage>(path(programPath(summary.program_id, "/daily"), { target_date: summary.target_date, snapshot_id: summary.snapshot_id, group, limit, offset: (page - 1) * limit }), refresh, summary.program_id);
  return <section className={styles.panel}><header className={styles.panelHeader}><h3>{title} · {number(result.data?.total_count)} 只</h3></header><ErrorNotice error={result.error} stale={!!result.data} />
    {result.data && <><Rows items={result.data.items} onStock={onStock} />{!result.data.items.length && <Empty>当前没有此类记录。</Empty>}<Pager total={result.data.total_count} limit={limit} page={page} onChange={setPage} onLimit={n => { setLimit(n); setPage(1); }} /></>}
  </section>;
}

export default function DailyList({ summary, refresh, onStock, onDate }: Props) {
  const [page, setPage] = useState(1), [limit, setLimit] = useState(20), [change, setChange] = useState("");
  const [query, setQuery] = useState("");
  const result = useRead<DailyPage>(path(programPath(summary.program_id, "/daily"), { target_date: summary.target_date, snapshot_id: summary.snapshot_id, change, q: query, limit, offset: (page - 1) * limit }), refresh, summary.program_id);
  const counts = result.data?.summary;
  const scopeLabel = summary.target_date === summary.default_target_date ? "当日" : "所选日";
  return <>
    <div className={styles.metrics}><Metric label={`${scopeLabel}荐股范围`} value={`${number(counts?.in_scope)} 只`} note="按已发布名单的进入合同" /><Metric label="新加入" value={`${number(counts?.comparison_known ? counts.new : null)} 只`} note="相较上次名单新增" /><Metric label="继续推荐" value={`${number(counts?.comparison_known ? counts.continuing : null)} 只`} note="相较上次名单保留" /><Metric label={`${scopeLabel}移出`} value={`${number(counts?.comparison_known ? counts.removed : null)} 只`} note="退出确认另外跟踪" /></div>
    <ErrorNotice error={result.error} stale={!!result.data} />
    {result.loading && !result.data && <Empty>正在读取所选交易日名单…</Empty>}
    {result.data?.status === "NO_CURRENT_LIST" && <div className={styles.notice}>所选目标日 {summary.target_date} 尚无有效推荐名单。{summary.runtime.state === "ASSET_BLOCKED" ? "冻结资产绑定不完整，发布受阻。" : "请查看下方运行与数据状态。"}{summary.latest && <button onClick={() => onDate(summary.latest!.trade_date)}>查看最近有效名单（{summary.latest.trade_date}）</button>}</div>}
    {counts && <>
      <div className={styles.dailyGrid}><section className={styles.panel}><header className={styles.panelHeader}><h2>{scopeLabel}推荐名单</h2><Badge value="IN_SCOPE">范围内 · {counts.in_scope} 只</Badge></header>
        <div className={styles.toolbar}><div className={styles.filters}>{[["", "全部"], ["NEW", "新加入"], ["CONTINUING", "继续推荐"], ["UNKNOWN", "变化待核验"]].map(([key, label]) => <button key={key} aria-pressed={change === key} className={change === key ? styles.selected : ""} onClick={() => { setChange(key); setPage(1); }}>{label}</button>)}</div><input aria-label="搜索当日股票" placeholder="搜索股票" value={query} onChange={e => { setQuery(e.target.value); setPage(1); }} /></div>
        <Rows items={result.data!.items} onStock={onStock} />{!result.data!.items.length && <Empty>当前筛选下没有推荐股票。</Empty>}<Pager total={result.data!.total_count} limit={limit} page={page} onChange={setPage} onLimit={n => { setLimit(n); setPage(1); }} />
        <p className={styles.explanation}>推荐成员身份与进入/退出进度分别展示；参考价来自输入日收盘，不是成交价。未知连续天数不会以前序缺失记录补齐。</p>
      </section><aside className={styles.panel}><header className={styles.panelHeader}><h3>所选日名单变化</h3></header><div className={styles.changeFlow}><span>上次<strong>{number(counts.previous_count)} 只</strong></span><span>→</span><span>当次<strong>{counts.in_scope} 只</strong></span></div><div className={styles.changeTags}><Badge value="CONTINUING">保留 {number(counts.comparison_known ? counts.continuing : null)}</Badge><Badge value="REMOVED">移出 {number(counts.comparison_known ? counts.removed : null)}</Badge><Badge value="NEW">新增 {number(counts.comparison_known ? counts.new : null)}</Badge></div>
        <p className={styles.explanation}>{counts.comparison_known ? "按同一绑定的前后已发布名单对比。" : "前序身份不足，变化暂不能完整确认。"}</p>{counts.unknown_change > 0 && <p className={styles.explanation}>变化待核验 {counts.unknown_change} 只</p>}<p className={styles.explanation}>点击股票查看推荐过程、参考价格与后续跟踪记录。</p>
      </aside></div>
      {counts.comparison_known ? <OtherGroup summary={summary} refresh={refresh} group="REMOVED" title={`${scopeLabel === "当日" ? "今日" : scopeLabel}移出 · 已不在所选日荐股范围`} onStock={onStock} /> : <div className={styles.notice}>前序名单身份不足，尚不能确认新加入、继续推荐和移出集合。</div>}
      {(counts.outside_exit || 0) > 0 && <details className={styles.panel}><summary>退出建议 · {counts.outside_exit} 只 · 结算另外核对</summary><OtherGroup summary={summary} refresh={refresh} group="OUTSIDE_EXIT" title="已产生退出建议" onStock={onStock} /></details>}
      <details className={styles.panel}><summary>范围外仍待复评 · {counts.outside_pending} 只</summary><OtherGroup summary={summary} refresh={refresh} group="OUTSIDE_PENDING" title="范围外待复评" onStock={onStock} /></details>
      <details className={styles.panel}><summary>观察候选 · {counts.observation} 只 · 未计入当日推荐</summary><OtherGroup summary={summary} refresh={refresh} group="OBSERVATION" title="观察候选" onStock={onStock} /></details>
      {counts.unknown > 0 && <OtherGroup summary={summary} refresh={refresh} group="UNKNOWN" title="成员身份待核验" onStock={onStock} />}
    </>}
  </>;
}
