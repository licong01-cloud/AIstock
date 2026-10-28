"use client";
import { type Shadow } from "@/lib/api/advisoryWorkbench";
import { Badge, bps, Empty, ErrorNotice, Metric, number, text, time, useRead } from "./shared";
import styles from "./workbench.module.css";

export default function ModelObservation({ programId, target, refresh }: { programId: string; target: string; refresh: number }) {
  const { data, error, loading } = useRead<Shadow>(`/advisory/programs/${encodeURIComponent(programId)}/reranking-shadow?target_trade_date=${target}`, refresh, programId);
  return <section className={styles.panel}><header className={styles.panelHeader}><h2>重排序模型观察</h2>{data && <Badge value={data.status} />}</header><ErrorNotice error={error} stale={!!data} />
    <p className={styles.explanation}>读取已保存的原生与重排序预测及自然成熟评价。原生推荐名单保持独立，影子结果不产生买卖指令。</p>
    {loading && !data && <Empty>正在读取持久化观察结果…</Empty>}
    {data && <><div className={styles.metrics}><Metric label="已捕获交易日" value={number(data.captured_days)} /><Metric label="已成熟评价日" value={number(data.complete_days)} /><Metric label="配对平均差值" value={bps(data.mean_delta_bps)} note="不等于独立经济确认" /></div>
      {!data.selected ? <Empty>{["NOT_APPLICABLE", "NOT_CONFIGURED"].includes(data.status) ? text(data.status) : "所选日期没有已保存的重排序预测。"}</Empty> : <>
        <p className={styles.explanation}>目标日 {data.selected.target_date} · 捕获于 {time(data.selected.observed_at)}</p>
        <div className={styles.tableWrap}><table><thead><tr><th>股票</th><th>原生排名</th><th>重排排名</th></tr></thead><tbody>{data.selected.predictions.map((row, i) => <tr key={i}><td>{String(row.symbol || row.ts_code || row.instrument || row.stock_code || "证券名称待核验")}</td><td>{number(row.selection_effective_rank ?? row.native_rank ?? row.parent_rank ?? row.rank)}</td><td>{number(row.advisory_rank ?? row.policy_rank ?? row.candidate_rank)}</td></tr>)}</tbody></table></div>
      </>}
      {!!data.days?.length && <div className={styles.tableWrap}><table><thead><tr><th>目标日</th><th>评价状态</th><th>配对差值</th></tr></thead><tbody>{data.days.map(row => <tr key={row.target_date}><td>{row.target_date}</td><td>{text(row.evaluation.status)}</td><td>{bps(row.evaluation.delta_bps)}</td></tr>)}</tbody></table></div>}
      {data.last_waiting && <div className={styles.notice}>等待输入数据完成验证。{data.last_waiting.results.filter(row => row.status === "WAITING_DATA").map((row, i) => <p key={i}>决策日 {row.decision_date || "待确认"} · {row.message} {row.context?.dataset && `（${row.context.dataset} ${row.context.fact_date || ""}）`}</p>)}就绪后由每日任务重试；超过目标日开盘截止时间将记为错过捕获。</div>}
      {data.last_failure && <div className={styles.notice}>最近一次模型观察失败；已有预测不会被失败结果覆盖，请核对运行记录。</div>}
    </>}
  </section>;
}
