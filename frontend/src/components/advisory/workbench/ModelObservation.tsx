"use client";
import { useState } from "react";
import { type Shadow } from "@/lib/api/advisoryWorkbench";
import { Badge, bps, Empty, ErrorNotice, Metric, number, text, time, useRead } from "./shared";
import { QuoteCell } from "../LiveQuotes";
import styles from "./workbench.module.css";

const changes: Record<string, { label: string; color: string }> = {
  ENTERED: { label: "换入辅助Top10", color: styles.blue }, EXITED: { label: "移出辅助Top10", color: styles.bad },
  RETAINED: { label: "两榜均入选", color: styles.continuing }, OUTSIDE: { label: "两榜范围外", color: styles.mutedBadge },
};
const targetText = (value?: string) => value === "POINT_ESTIMATE_AT_OR_ABOVE_TARGET" ? "当前点估计达到参考目标" : value === "POINT_ESTIMATE_BELOW_TARGET" ? "当前点估计未达参考目标" : "尚无完整配对评价";

export default function ModelObservation({ programId, target, refresh }: { programId: string; target: string; refresh: number }) {
  const { data, error, loading } = useRead<Shadow>(`/advisory/programs/${encodeURIComponent(programId)}/reranking-shadow?target_trade_date=${target}`, refresh, programId);
  const [view, setView] = useState("model");
  const review = data?.selected?.business_review, summary = data?.business_summary;
  const rows = (review?.rows ?? data?.selected?.predictions ?? []).filter(row => view === "all" || (view === "model" ? row.advisory_rank : row.selection_effective_rank)! <= 10)
    .slice().sort((a, b) => Number(view === "model" ? a.advisory_rank : a.selection_effective_rank) - Number(view === "model" ? b.advisory_rank : b.selection_effective_rank));
  return <section className={styles.panel}><header className={styles.panelHeader}><h2>重排序模型观察</h2>{data && <Badge value={data.status} />}</header><ErrorNotice error={error} stale={!!data} />
    <p className={styles.explanation}>精确LSTM包的Top10辅助观察，比较同一Top50内的模型Top10与原生Top10。生产Top20保持独立，影子结果不产生买卖指令。</p>
    <p className={styles.explanation}>本轮实用价值参考目标：每个5交易日批次净增量100bps（1个百分点）。以下已扣既定费用，不是账户收益或真实成交；达到点估计目标不等于统计确认或自动上线。当前最新价单独展示，不参与固定5TD结果。</p>
    {loading && !data && <Empty>正在读取持久化观察结果…</Empty>}
    {data && <>
      {summary ? <div data-testid="reranking-business-summary"><div className={styles.metrics}><Metric label="已成熟 / 已捕获交易日" value={`${number(data.complete_days)} / ${number(data.captured_days)}`} /><Metric label="原生Top10批次均值" value={bps(summary.native_mean_net_bps)} /><Metric label="辅助Top10批次均值" value={bps(summary.specialist_mean_net_bps)} /><Metric label="配对平均差值" value={bps(summary.mean_delta_bps)} note="不等于独立经济确认" /></div><p className={styles.explanation}>仅同一组完整配对批次 {number(summary.complete_pairs)} / 捕获 {number(summary.captured_days)}；未知评价批次 {number(summary.unknown_days)}，未评价批次 {number(summary.unevaluated_days)}。{targetText(summary.target_status)}；缺失批次保留，不用于正式确认。</p></div>
        : data.status === "SHADOW_ONLY" && <><div className={styles.metrics}><Metric label="已捕获交易日" value={number(data.captured_days)} /><Metric label="已成熟评价日" value={number(data.complete_days)} /><Metric label="配对平均差值" value={bps(data.mean_delta_bps)} note="不等于独立经济确认" /></div><div className={styles.notice}>业务收益视图尚未加载，保留原观察结果。</div></>}
      {!data.selected ? <Empty>{["NOT_APPLICABLE", "NOT_CONFIGURED"].includes(data.status) ? text(data.status) : "所选日期没有已保存的重排序预测。"}</Empty> : <>
        <p className={styles.explanation}>目标日 {data.selected.target_date} · 捕获于 {time(data.selected.observed_at)}</p>
        {review && <div data-testid="reranking-selected-review"><div className={styles.metrics}><Metric label="所选批次原生Top10" value={bps(review.native_net_bps)} /><Metric label="所选批次辅助Top10" value={bps(review.specialist_net_bps)} /><Metric label="所选批次净增量" value={bps(review.delta_bps)} /><Metric label="所选批次评价" value={text(review.status)} /></div>
          <p className={styles.explanation}>固定T开盘至T+4收盘，十槽等权。原生/辅助未知槽 {number(review.native_unknown_slots)} / {number(review.specialist_unknown_slots)}；未执行现金槽 {number(review.native_cash_slots)} / {number(review.specialist_cash_slots)}。{targetText(review.target_status)}。</p>
          <details><summary>查看增量来源与损益分解</summary><div className={styles.metrics}><Metric label="新增盈利贡献" value={bps(review.components.added_profit_bps)} /><Metric label="错失盈利贡献" value={bps(review.components.missed_profit_bps)} /><Metric label="避免损失贡献" value={bps(review.components.avoided_loss_bps)} /><Metric label="新增损失贡献" value={bps(review.components.added_loss_bps)} /></div><p className={styles.explanation}>净增量＝新增盈利−错失盈利＋避免损失−新增损失；未成熟或影响结果的未知项保持“—”。</p></details>
        </div>}
        <div className={styles.filters} aria-label="辅助观察名单"><button aria-pressed={view === "model"} onClick={() => setView("model")}>辅助Top10</button><button aria-pressed={view === "native"} onClick={() => setView("native")}>原生Top10</button><button aria-pressed={view === "all"} onClick={() => setView("all")}>全部原始候选</button></div>
        <div className={styles.tableWrap}><table data-testid="reranking-business-rows"><thead><tr><th>股票</th><th>当前最新价</th><th>原生排名</th><th>重排排名</th><th>两榜关系</th><th>5TD参考净收益</th><th>批次增量贡献</th></tr></thead><tbody>{rows.map((row, i) => <tr key={row.instrument || i}><td>{String(row.symbol || row.ts_code || row.instrument || row.stock_code || "证券名称待核验")}</td><td><QuoteCell symbol={String(row.symbol || row.ts_code || row.instrument || row.stock_code || "")} /></td><td>{number(row.selection_effective_rank ?? row.native_rank ?? row.parent_rank ?? row.rank)}</td><td>{number(row.advisory_rank ?? row.policy_rank ?? row.candidate_rank)}</td><td><span className={`${styles.badge} ${changes[row.change || ""]?.color || styles.mutedBadge}`}>{changes[row.change || ""]?.label || "待核验"}</span></td><td>{bps(row.net_bps)}{row.valuation_status === "CASH_ENTRY_NOT_EXECUTABLE" && <small>未执行现金槽</small>}</td><td title="按十槽等权计算对批次差值的贡献">{bps(row.delta_contribution_bps)}</td></tr>)}</tbody></table></div>
      </>}
      {!!data.days?.length && <div className={styles.tableWrap}><table data-testid="reranking-history"><thead><tr><th>目标日</th><th>评价状态</th><th>原生Top10参考净收益</th><th>辅助Top10参考净收益</th><th>配对差值</th></tr></thead><tbody>{data.days.map(row => <tr key={row.target_date}><td>{row.target_date}</td><td>{text(row.evaluation.status)}</td><td>{bps(row.evaluation.native_net_bps)}</td><td>{bps(row.evaluation.specialist_net_bps)}</td><td>{bps(row.evaluation.delta_bps)}</td></tr>)}</tbody></table></div>}
      {data.last_waiting && <div className={styles.notice}>等待输入数据完成验证。{data.last_waiting.results.filter(row => row.status === "WAITING_DATA").map((row, i) => <p key={i}>决策日 {row.decision_date || "待确认"} · {row.message} {row.context?.dataset && `（${row.context.dataset} ${row.context.fact_date || ""}）`}</p>)}就绪后由每日任务重试；超过目标日开盘截止时间将记为错过捕获。</div>}
      {data.last_failure && <div className={styles.notice}>最近一次模型观察失败；已有预测不会被失败结果覆盖，请核对运行记录。</div>}
    </>}
  </section>;
}
