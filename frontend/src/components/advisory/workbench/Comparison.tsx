"use client";
import dynamic from "next/dynamic";
import { useEffect, useRef, useState } from "react";
import { defaultComparison, type ComparisonOptions, type ComparisonResponse } from "@/lib/api/advisoryComparison";
import { path, read, type Program } from "@/lib/api/advisoryWorkbench";
import ComparisonFilters from "./ComparisonFilters";
import ComparisonTable from "./ComparisonTable";
import { Empty, ErrorNotice, time } from "./shared";
import styles from "./workbench.module.css";

const Charts = dynamic(() => import("./ComparisonCharts"), { ssr: false, loading: () => <Empty>正在加载图表…</Empty> });
function useComparison(url: string, refresh: number, forceToken: number) {
  const forced = useRef(forceToken), sequence = useRef(0);
  const [state, setState] = useState<{ url: string; data: ComparisonResponse | null; error: string | null; loading: boolean }>({ url, data: null, error: null, loading: true });
  useEffect(() => {
    const seq = ++sequence.current, controller = new AbortController();
    const force = forceToken !== forced.current;
    forced.current = forceToken;
    const timer = setTimeout(() => controller.abort("timeout"), 45000);
    setState(old => ({ url, data: old.url === url ? old.data : null, error: null, loading: true }));
    read<ComparisonResponse>(url + (force ? "&force_refresh=true" : ""), controller.signal).then(data => {
      if (seq !== sequence.current || controller.signal.aborted) return;
      if (!data || data.schema_version !== "advisory_native_comparison_v1" || !Array.isArray(data.items) || !/^[a-f0-9]{64}$/.test(data.source_version)) throw new Error("收益对比身份不可用，请刷新重试。");
      setState({ url, data, error: null, loading: false });
    }).catch(error => {
      if (seq !== sequence.current || (controller.signal.aborted && controller.signal.reason !== "timeout")) return;
      setState(old => ({ ...old, loading: false, error: controller.signal.reason === "timeout" ? "收益对比读取超时，请重试。" : error instanceof Error ? error.message : "收益对比读取失败。" }));
    }).finally(() => clearTimeout(timer));
    return () => { controller.abort(); clearTimeout(timer); };
  }, [url, refresh, forceToken]);
  return state.url === url ? state : { url, data: null, error: null, loading: true };
}
export default function Comparison({ programs, status, refresh }: { programs: Program[]; status: string; refresh: number }) {
  const [options, setOptions] = useState(defaultComparison), [charts, setCharts] = useState(false), [generation, setGeneration] = useState(0);
  const result = useComparison(path("/comparison", { ...options, status }), refresh, generation);
  const data = result.data;
  const apply = (next: ComparisonOptions) => { setOptions(next); setCharts(false); setGeneration(0); };
  return <section id="strategy-comparison" className={`${styles.panel} ${styles.comparisonSection}`} aria-label="策略包收益横向对比">
    <header className={styles.panelHeader}><div><h2>策略包收益对比</h2><p className={styles.muted}>从净收益、相对市场表现和风险判断策略效果</p></div><button disabled={result.loading} onClick={() => { setGeneration(v => v + 1); setCharts(false); }}>重新读取数据</button></header>
    <ComparisonFilters programs={programs} loading={result.loading} onApply={apply} />
    <ErrorNotice error={result.error} stale={!!data} />
    {result.loading && !data && <div className={styles.comparisonSkeleton} aria-busy="true"><Empty>正在读取发布名单并计算可比收益…</Empty></div>}
    {data && <><div className={styles.comparisonMeta}><span>{data.range.start} — {data.range.end}</span><span>{data.filters.sample === "COMMON" ? `共同可评价 ${data.common_count} 日` : "各自样本 · 样本不同，不作为公平排名"}</span><span>买入 {data.cost.buy_cost_bps.toFixed(2)} / 卖出 {data.cost.sell_cost_bps.toFixed(2)} bps（含滑点）</span><span>读取 {time(data.generated_at)}</span></div>
      {data.status === "NO_COMPARABLE_SAMPLE" && <div className={styles.notice}>当前选择没有可比较的完整成熟样本。请展开“选择策略与费用假设”调整参与策略，或改用各自样本查看已有记录。缺失数据保留未知。</div>}
      {data.filters.source === "ALL" && <div className={styles.notice}>当前混合正式前向与历史回放；同日版本冲突不参与收益比较。</div>}
      {options.period === "CUSTOM" && options.end > data.range.end && <div className={styles.notice}>收益评价截至 {data.range.end}，此后的价格尚未纳入。</div>}
      <ComparisonTable data={data} />
      <div className={styles.toolbar}><p className={styles.muted}>沪深300：相同目标日开盘至第5交易日收盘的价格收益。</p><button aria-expanded={charts} onClick={() => setCharts(v => !v)}>{charts ? "收起图表" : "查看横向对比图表"}</button></div>
      {charts && <Charts summary={data} url={path("/comparison/series", { ...options, status })} refresh={refresh + generation} />}
      <p className={styles.footnote}>固定5TD保留发布名单原始槽位；已知无法买入留现金，未知不按零收益补齐。参考组合采用有限资金逐日跟踪，不能将重叠5日收益连乘。所有结果均为参考测算，不代表实际成交或账户收益。</p>
    </>}
  </section>;
}
