"use client";
import { createContext, useCallback, useContext, useEffect, useId, useRef, useState, type ReactNode } from "react";
import { comparisonMetrics, type ComparisonMetric } from "./comparisonMetricDefinitions";
import styles from "./workbench.module.css";

const HelpContext = createContext<((metric?: ComparisonMetric) => void) | null>(null);
function useMetricHelp() {
  const open = useContext(HelpContext);
  if (!open) throw new Error("Comparison metric help requires its provider.");
  return open;
}

export function ComparisonMetricHelpProvider({ children }: { children: ReactNode }) {
  const [guide, setGuide] = useState<{ metric?: ComparisonMetric } | null>(null);
  const opener = useRef<HTMLElement | null>(null);
  const open = useCallback((metric?: ComparisonMetric) => {
    opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    setGuide({ metric });
  }, []);
  const close = useCallback(() => { setGuide(null); opener.current?.focus(); }, []);
  return <HelpContext.Provider value={open}>{children}{guide && <MetricDialog metric={guide.metric} onClose={close} />}</HelpContext.Provider>;
}

export function MetricLabel({ metric, children }: { metric: ComparisonMetric; children?: ReactNode }) {
  const open = useMetricHelp(), definition = comparisonMetrics[metric];
  return <span className={styles.metricLabel}><span>{children ?? definition.label}</span><button type="button" className={styles.metricHelpButton} aria-label={`查看${definition.label}说明`} title={definition.meaning} onClick={() => open(metric)}>?</button></span>;
}

export function MetricGuideButton() {
  const open = useMetricHelp();
  return <button type="button" onClick={() => open()}>指标中文说明</button>;
}

function MetricDialog({ metric, onClose }: { metric?: ComparisonMetric; onClose: () => void }) {
  const ref = useRef<HTMLDialogElement>(null), titleId = useId(), descriptionId = useId();
  const [query, setQuery] = useState(metric ? comparisonMetrics[metric].label : "");
  useEffect(() => {
    const dialog = ref.current;
    if (!dialog) return;
    dialog.showModal();
    return () => dialog.close();
  }, []);
  const term = query.trim().toLowerCase();
  const entries = Object.entries(comparisonMetrics).filter(([, value]) => !term || `${value.label} ${value.group} ${value.meaning} ${value.calculation} ${value.boundary}`.toLowerCase().includes(term));
  const groups = [...new Set(entries.map(([, definition]) => definition.group))];
  const close = () => { ref.current?.close(); onClose(); };
  return <dialog ref={ref} className={styles.metricDialog} aria-labelledby={titleId} aria-describedby={descriptionId} onCancel={event => { event.preventDefault(); close(); }}>
    <header className={styles.metricDialogHeader}><h2 id={titleId}>指标中文说明</h2><button type="button" aria-label="关闭指标说明" onClick={close}>关闭</button></header>
    <p id={descriptionId} className={styles.metricDialogIntro}>5TD 表示5个交易日；STD 表示标准差，本页未展示标准差。收益空缺“—”表示证据或样本不足，不等于0。</p>
    <div className={styles.metricSearch}><label>查找指标<input autoFocus type="search" aria-label="查找指标" placeholder="输入均值、胜率、回撤等" value={query} onChange={event => setQuery(event.target.value)} /></label>{query && <button type="button" onClick={() => setQuery("")}>查看全部指标</button>}<span role="status" aria-live="polite">共 {entries.length} 项说明</span></div>
    <div className={styles.metricGuideBody}>
      {groups.map(group => <section key={group} aria-label={group}><h3>{group}</h3><div className={styles.metricGuideGrid}>{entries.filter(([, definition]) => definition.group === group).map(([key, definition]) => <article key={key} className={styles.metricDefinition} data-metric={key}><h4>{definition.label}</h4><p>{definition.meaning}</p><dl><div><dt>计算口径</dt><dd>{definition.calculation}</dd></div><div><dt>阅读提示</dt><dd>{definition.boundary}</dd></div></dl></article>)}</div></section>)}
      {!entries.length && <p className={styles.empty}>没有匹配的指标，请换个关键词或查看全部指标。</p>}
    </div>
  </dialog>;
}
