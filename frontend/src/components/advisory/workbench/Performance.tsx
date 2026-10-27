import { type Summary } from "@/lib/api/advisoryWorkbench";
import { bps, day, gain, Metric, number, percent, text } from "./shared";
import styles from "./workbench.module.css";

export default function Performance({ summary }: { summary: Summary }) {
  const m = summary.metrics;
  return <section className={styles.panel}><header className={styles.panelHeader}><h2>效果统计</h2><span>{text(m.metric_status)}</span></header>
    <p className={styles.explanation}>按当前筛选的入选日期、来源与绑定统计；收益观察截至 {day(m.metric_as_of_trade_date)}。参考价格、费用前，不代表账户收益。</p>
    <div className={styles.metrics}>
      <Metric label="已结束有效样本" value={number(m.metric_evaluable_count)} /><Metric label="已结束胜率" value={percent(m.win_rate)} />
      <Metric label="平均参考收益" value={bps(m.avg_return_bps)} className={gain(m.avg_return_bps)} /><Metric label="中位数参考收益" value={bps(m.median_return_bps)} className={gain(m.median_return_bps)} />
      <Metric label="单次最差回撤" value={bps(m.max_drawdown_bps)} note="不是组合净值回撤" /><Metric label="平均跟踪交易日" value={number(m.avg_holding_days, 1)} />
      <Metric label="止盈 / 止损" value={`${number(m.take_profit_count)} / ${number(m.stop_loss_count)}`} /><Metric label="到期结算覆盖" value={percent(m.coverage?.settled_coverage)} note={`${number(m.coverage?.settled_count)} / ${number(m.coverage?.due_count)} 个到期过程`} />
    </div>
    <h3 className={styles.subheading}>未结束参考收益</h3><div className={styles.metrics}><Metric label="跟踪中" value={number(m.active_count)} /><Metric label="平均浮动参考收益" value={bps(m.unrealized?.gross?.avg_return_bps)} className={gain(m.unrealized?.gross?.avg_return_bps)} /><Metric label="缺少估值" value={number(m.missing_open_mark_count)} /><Metric label="最新估值日期" value={day(m.metric_mark_trade_date)} /></div>
    <p className={styles.explanation}>来源：日常前向 {number(summary.source_counts.FORWARD || 0)} · 历史回放 {number(summary.source_counts.REPLAY || 0)} · 待核验 {number(summary.source_counts.UNKNOWN || 0)}。缺失价格和无样本指标保持未知。</p>
  </section>;
}
