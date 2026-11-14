"use client";
import { useState } from "react";
import { defaultComparison, type ComparisonOptions } from "@/lib/api/advisoryComparison";
import type { Program } from "@/lib/api/advisoryWorkbench";
import styles from "./workbench.module.css";

type Props = { programs: Program[]; loading: boolean; onApply: (value: ComparisonOptions) => void };
export default function ComparisonFilters({ programs, loading, onApply }: Props) {
  const [draft, setDraft] = useState(defaultComparison);
  const [selected, setSelected] = useState<string[] | null>(null);
  const ids = selected ?? programs.map(p => p.program_id);
  const set = <K extends keyof ComparisonOptions>(key: K, value: ComparisonOptions[K]) => setDraft(v => ({ ...v, [key]: value }));
  const invalid = !ids.length || (draft.period === "CUSTOM" && (!draft.start || !draft.end || draft.start > draft.end));
  return <form className={styles.comparisonFilters} onSubmit={e => { e.preventDefault(); if (!invalid) onApply({ ...draft, program_ids: ids.join(",") }); }}>
    <div className={styles.periodButtons} role="group" aria-label="对比时间范围">{[["5", "近5交易日"], ["10", "近10交易日"], ["20", "近20交易日"], ["60", "近60交易日"], ["120", "近120交易日"], ["YTD", "今年"], ["ALL", "全部历史"], ["CUSTOM", "自定义"]].map(([value, label]) => <button type="button" key={value} aria-pressed={draft.period === value} className={draft.period === value ? styles.selected : ""} onClick={() => set("period", value)}>{label}</button>)}</div>
    <div className={styles.filters}>
      {draft.period === "CUSTOM" && <><label>开始 <input aria-label="对比开始日期" type="date" value={draft.start} onChange={e => set("start", e.target.value)} required /></label><label>结束 <input aria-label="对比结束日期" type="date" min={draft.start} value={draft.end} onChange={e => set("end", e.target.value)} required /></label></>}
      <label>样本 <select aria-label="对比样本" value={draft.sample} onChange={e => set("sample", e.target.value)}><option value="COMMON">共同可评价日期</option><option value="OWN">各自全部样本</option></select></label>
      <label>来源 <select aria-label="对比来源" value={draft.source} onChange={e => set("source", e.target.value)}><option value="FORWARD">正式前向</option><option value="REPLAY">历史回放</option><option value="ALL">全部来源（混合）</option><option value="UNKNOWN">来源待核验</option></select></label>
      <label>绑定 <select aria-label="对比绑定" value={draft.binding} onChange={e => set("binding", e.target.value)}><option value="CURRENT">当前绑定</option><option value="ALL">全部绑定（分开显示）</option></select></label>
      <button className={styles.primary} type="submit" disabled={loading || invalid}>{loading ? "正在计算…" : "应用对比"}</button>
    </div>
    <details className={styles.comparisonAdvanced}><summary>选择策略与费用假设 · 已选 {ids.length} 个</summary>
      <div className={styles.programChecks}>{programs.map(p => <label key={p.program_id}><input type="checkbox" checked={ids.includes(p.program_id)} onChange={e => setSelected(e.target.checked ? [...ids, p.program_id] : ids.filter(id => id !== p.program_id))} />{p.program_name}</label>)}</div>
      <div className={styles.filters}>{([["buy_bps", "买入费用"], ["sell_bps", "卖出费用"], ["slippage_bps", "单边滑点"]] as const).map(([key, label]) => <label key={key}>{label} <input aria-label={label} type="number" min={0} max={1000} step={.01} value={draft[key]} onChange={e => set(key, Number(e.target.value))} required /> bps</label>)}</div>
    </details>
  </form>;
}
