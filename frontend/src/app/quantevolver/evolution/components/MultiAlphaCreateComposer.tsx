"use client";

import React, { useMemo, useState } from "react";
import { Plus, Play, RotateCcw, Trash2, X } from "lucide-react";
import {
  MultiAlphaCreateRequest,
  MultiAlphaCreateResult,
  MultiAlphaLegDraft,
  MultiAlphaScenarioDraft,
  submitMultiAlphaScenarios,
} from "./multiAlphaEvolutionAdapter";

const WEIGHTING_SCHEMES = ["equal", "orthogonality_aware", "ic_weighted", "risk_parity", "rank_fusion_rrf", "rank_fusion_borda"];

type ComposerDraft = {
  task_id: string;
  oos_start: string;
  oos_end: string;
  weighting_schemes: string[];
  normalize_method: string;
  walk_forward_enabled: boolean;
  walk_forward_window: number;
  walk_forward_min_periods: number;
  walk_forward_expanding: boolean;
  rank_fusion_json: string;
  backtest_advanced_json: string;
  baseline_leg_id: string;
  min_date_coverage: number;
  run_async: boolean;
  scheme_timeout_seconds: number;
  run_timeout_seconds: number;
  wait_timeout_seconds: number;
};

type Props = {
  apiBase: string;
  onClose: () => void;
  onSubmitted: (results: MultiAlphaCreateResult[]) => void | Promise<void>;
};

const inputStyle: React.CSSProperties = {
  width: "100%",
  boxSizing: "border-box",
  padding: "7px 9px",
  border: "1px solid #cbd5e1",
  borderRadius: 6,
  background: "#fff",
  color: "#0f172a",
  fontSize: 12,
};

const labelStyle: React.CSSProperties = { display: "grid", gap: 4, color: "#475569", fontSize: 11, fontWeight: 700 };

function newScenario(index: number): MultiAlphaScenarioDraft {
  return {
    scenario_id: `scenario_${Date.now()}_${index}`,
    scenario_name: `capital_10000000_topk${index === 0 ? 20 : 25}`,
    initial_cash: 10_000_000,
    topk: index === 0 ? 20 : 25,
    node_id: "wsl2-5080",
    node_parallelism: 2,
    n_drop: 5,
    max_n_drop: 5,
    min_n_drop: 0,
    hold_thresh: 0,
  };
}

function positiveInteger(value: unknown, label: string, minimum = 1): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < minimum) throw new Error(`${label} 必须是大于等于 ${minimum} 的整数`);
  return parsed;
}

function parseObject(text: string, label: string): Record<string, unknown> {
  const value = text.trim();
  if (!value) return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch (error) {
    throw new Error(`${label} 不是有效 JSON：${error instanceof Error ? error.message : String(error)}`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error(`${label} 必须是 JSON object`);
  return parsed as Record<string, unknown>;
}

export function buildMultiAlphaScenarioPayloads(
  draft: ComposerDraft,
  legs: MultiAlphaLegDraft[],
  scenarios: MultiAlphaScenarioDraft[],
): Array<{ scenario: MultiAlphaScenarioDraft; payload: MultiAlphaCreateRequest }> {
  if (legs.length < 2) throw new Error("组合至少需要两条 Alpha 腿");
  const normalizedLegs = legs.map((leg, index) => {
    const legId = leg.leg_id.trim();
    const seedRunIds = leg.seed_run_ids.map((item) => item.trim()).filter(Boolean);
    if (!legId) throw new Error(`第 ${index + 1} 条腿缺少 leg_id`);
    if (!seedRunIds.length) throw new Error(`${legId} 至少需要一个 seed run id`);
    return { leg_id: legId, seed_run_ids: seedRunIds, metadata: leg.metadata || {} };
  });
  if (new Set(normalizedLegs.map((leg) => leg.leg_id)).size !== normalizedLegs.length) throw new Error("leg_id 必须唯一");
  if (!normalizedLegs.some((leg) => leg.leg_id === draft.baseline_leg_id)) throw new Error("baseline_leg_id 必须引用当前 roster 中的一条腿");
  if (!draft.oos_start || !draft.oos_end || draft.oos_end < draft.oos_start) throw new Error("OOS 日期范围无效");
  if (!draft.weighting_schemes.length) throw new Error("至少选择一种 weighting scheme");
  if (!scenarios.length) throw new Error("至少需要一个回测场景");
  if (!(draft.min_date_coverage > 0 && draft.min_date_coverage <= 1)) throw new Error("min_date_coverage 必须在 (0, 1] 范围内");

  const rankFusion = parseObject(draft.rank_fusion_json, "rank_fusion");
  const advanced = parseObject(draft.backtest_advanced_json, "backtest_config 高级字段");
  const seenScenarioNames = new Set<string>();

  return scenarios.map((scenario, index) => {
    const scenarioName = scenario.scenario_name.trim();
    if (!scenarioName) throw new Error(`第 ${index + 1} 个场景缺少名称`);
    if (seenScenarioNames.has(scenarioName)) throw new Error(`场景名称重复：${scenarioName}`);
    seenScenarioNames.add(scenarioName);
    const topk = positiveInteger(scenario.topk, `${scenarioName}.topk`);
    const nDrop = positiveInteger(scenario.n_drop, `${scenarioName}.n_drop`, 0);
    const maxNDrop = positiveInteger(scenario.max_n_drop, `${scenarioName}.max_n_drop`, 0);
    const minNDrop = positiveInteger(scenario.min_n_drop, `${scenarioName}.min_n_drop`, 0);
    const holdThresh = positiveInteger(scenario.hold_thresh, `${scenarioName}.hold_thresh`, 0);
    if (minNDrop > maxNDrop) throw new Error(`${scenarioName}: min_n_drop 不能大于 max_n_drop`);
    if (nDrop > topk || maxNDrop > topk) throw new Error(`${scenarioName}: n_drop/max_n_drop 不能大于 topk`);
    const nodeId = scenario.node_id.trim();
    if (!nodeId) throw new Error(`${scenarioName}: node_id 不能为空`);
    const nodeParallelism = positiveInteger(scenario.node_parallelism, `${scenarioName}.node_parallelism`);
    const advancedStrategy = advanced.strategy_kwargs && typeof advanced.strategy_kwargs === "object" && !Array.isArray(advanced.strategy_kwargs)
      ? advanced.strategy_kwargs as Record<string, unknown>
      : {};
    const backtestConfig = {
      ...advanced,
      node_id: nodeId,
      node_parallelism: { ...(advanced.node_parallelism && typeof advanced.node_parallelism === "object" ? advanced.node_parallelism as Record<string, unknown> : {}), [nodeId]: nodeParallelism },
      initial_cash: positiveInteger(scenario.initial_cash, `${scenarioName}.initial_cash`),
      topk,
      scenario_name: scenarioName,
      strategy_kwargs: { ...advancedStrategy, n_drop: nDrop, max_n_drop: maxNDrop, min_n_drop: minNDrop, hold_thresh: holdThresh },
    };
    return {
      scenario,
      payload: {
        task_id: draft.task_id.trim() || null,
        roster: normalizedLegs,
        oos_start: scenario.oos_start || draft.oos_start,
        oos_end: scenario.oos_end || draft.oos_end,
        weighting_schemes: [...draft.weighting_schemes],
        normalize_method: draft.normalize_method,
        walk_forward: {
          enabled: draft.walk_forward_enabled,
          window: positiveInteger(draft.walk_forward_window, "walk_forward.window"),
          min_periods: positiveInteger(draft.walk_forward_min_periods, "walk_forward.min_periods"),
          expanding: draft.walk_forward_expanding,
        },
        rank_fusion: rankFusion,
        backtest_config: backtestConfig,
        baseline_leg_id: draft.baseline_leg_id,
        topk,
        min_date_coverage: draft.min_date_coverage,
        run_async: draft.run_async,
        scheme_timeout_seconds: scenario.scheme_timeout_seconds || positiveInteger(draft.scheme_timeout_seconds, "scheme_timeout_seconds"),
        run_timeout_seconds: scenario.run_timeout_seconds || positiveInteger(draft.run_timeout_seconds, "run_timeout_seconds"),
        wait_timeout_seconds: draft.run_async ? null : positiveInteger(draft.wait_timeout_seconds, "wait_timeout_seconds"),
      },
    };
  });
}

export default function MultiAlphaCreateComposer({ apiBase, onClose, onSubmitted }: Props) {
  const [draft, setDraft] = useState<ComposerDraft>({
    task_id: "",
    oos_start: "2024-07-01",
    oos_end: "2026-06-29",
    weighting_schemes: ["equal", "orthogonality_aware", "ic_weighted", "risk_parity"],
    normalize_method: "zscore",
    walk_forward_enabled: true,
    walk_forward_window: 60,
    walk_forward_min_periods: 20,
    walk_forward_expanding: false,
    rank_fusion_json: "{}",
    backtest_advanced_json: "{}",
    baseline_leg_id: "trend_leg",
    min_date_coverage: 0.8,
    run_async: true,
    scheme_timeout_seconds: 7200,
    run_timeout_seconds: 28800,
    wait_timeout_seconds: 60,
  });
  const [legs, setLegs] = useState<MultiAlphaLegDraft[]>([
    { leg_id: "trend_leg", seed_run_ids: [""], metadata: { family: "trend" } },
    { leg_id: "sector_leg", seed_run_ids: [""], metadata: { family: "sector" } },
  ]);
  const [legMetadataTexts, setLegMetadataTexts] = useState(["{\"family\":\"trend\"}", "{\"family\":\"sector\"}"]);
  const [scenarios, setScenarios] = useState<MultiAlphaScenarioDraft[]>([newScenario(0)]);
  const [results, setResults] = useState<MultiAlphaCreateResult[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const preview = useMemo(() => {
    try {
      const parsedLegs = legs.map((leg, index) => ({ ...leg, metadata: parseObject(legMetadataTexts[index] || "{}", `${leg.leg_id || `leg ${index + 1}`} metadata`) }));
      return { rows: buildMultiAlphaScenarioPayloads(draft, parsedLegs, scenarios), error: null };
    } catch (caught) {
      return { rows: [], error: caught instanceof Error ? caught.message : String(caught) };
    }
  }, [draft, legMetadataTexts, legs, scenarios]);
  const advancedConflictKeys = useMemo(() => {
    try {
      const advanced = parseObject(draft.backtest_advanced_json, "backtest_config 高级字段");
      return ["node_id", "node_parallelism", "initial_cash", "topk", "scenario_name", "strategy_kwargs"].filter((key) => key in advanced);
    } catch {
      return [];
    }
  }, [draft.backtest_advanced_json]);

  const patchLeg = (index: number, patch: Partial<MultiAlphaLegDraft>) => setLegs((current) => current.map((item, itemIndex) => itemIndex === index ? { ...item, ...patch } : item));
  const patchScenario = (index: number, patch: Partial<MultiAlphaScenarioDraft>) => setScenarios((current) => current.map((item, itemIndex) => itemIndex === index ? { ...item, ...patch } : item));

  const submitRows = async (rows: Array<{ scenario: MultiAlphaScenarioDraft; payload: MultiAlphaCreateRequest }>) => {
    if (!rows.length) return;
    setSubmitting(true);
    setError(null);
    const next: MultiAlphaCreateResult[] = [];
    try {
      const submitted = await submitMultiAlphaScenarios(apiBase, rows, (result) => {
        next.push(result);
        setResults([...next]);
      });
      setResults(submitted);
      await onSubmitted(submitted);
    } finally {
      setSubmitting(false);
    }
  };

  const retryFailed = () => {
    const failedIds = new Set(results.filter((item) => item.status === "failed").map((item) => item.scenario_id));
    void submitRows(preview.rows.filter((item) => failedIds.has(item.scenario.scenario_id)));
  };

  return (
    <div role="dialog" aria-modal="true" aria-label="创建多 Alpha 组合回测" style={{ position: "fixed", inset: 0, zIndex: 1000, background: "rgba(15,23,42,.48)", display: "grid", placeItems: "center", padding: 24 }}>
      <div style={{ width: "min(1180px, 96vw)", maxHeight: "92vh", overflow: "auto", background: "#f8fafc", borderRadius: 12, border: "1px solid #cbd5e1", boxShadow: "0 24px 64px rgba(15,23,42,.25)" }}>
        <div style={{ position: "sticky", top: 0, zIndex: 2, display: "flex", justifyContent: "space-between", alignItems: "center", padding: "14px 18px", background: "#fff", borderBottom: "1px solid #e2e8f0" }}>
          <div><strong style={{ color: "#0f172a" }}>创建多 Alpha 组合回测</strong><div style={{ marginTop: 3, color: "#64748b", fontSize: 11 }}>复用既有 prediction；每个场景创建独立 run，不触发模型训练。</div></div>
          <button onClick={onClose} aria-label="关闭创建器" style={{ border: 0, background: "transparent", cursor: "pointer", color: "#64748b" }}><X size={18} /></button>
        </div>

        <div style={{ padding: 18, display: "grid", gap: 16 }}>
          <section style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 10, padding: 14 }}>
            <h3 style={{ margin: "0 0 12px", fontSize: 14 }}>任务、窗口与融合</h3>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4,minmax(0,1fr))", gap: 10 }}>
              <label style={labelStyle}>已有 task_id（可空）<input style={inputStyle} value={draft.task_id} onChange={(e) => setDraft({ ...draft, task_id: e.target.value })} /></label>
              <label style={labelStyle}>OOS 开始<input type="date" style={inputStyle} value={draft.oos_start} onChange={(e) => setDraft({ ...draft, oos_start: e.target.value })} /></label>
              <label style={labelStyle}>OOS 结束<input type="date" style={inputStyle} value={draft.oos_end} onChange={(e) => setDraft({ ...draft, oos_end: e.target.value })} /></label>
              <label style={labelStyle}>归一化<select style={inputStyle} value={draft.normalize_method} onChange={(e) => setDraft({ ...draft, normalize_method: e.target.value })}><option value="zscore">zscore</option><option value="rank">rank</option></select></label>
              <label style={labelStyle}>覆盖率<input type="number" min="0.01" max="1" step="0.01" style={inputStyle} value={draft.min_date_coverage} onChange={(e) => setDraft({ ...draft, min_date_coverage: Number(e.target.value) })} /></label>
              <label style={labelStyle}>scheme 超时（秒）<input type="number" style={inputStyle} value={draft.scheme_timeout_seconds} onChange={(e) => setDraft({ ...draft, scheme_timeout_seconds: Number(e.target.value) })} /></label>
              <label style={labelStyle}>run 超时（秒）<input type="number" style={inputStyle} value={draft.run_timeout_seconds} onChange={(e) => setDraft({ ...draft, run_timeout_seconds: Number(e.target.value) })} /></label>
              <label style={labelStyle}>同步等待超时（秒）<input type="number" disabled={draft.run_async} style={inputStyle} value={draft.wait_timeout_seconds} onChange={(e) => setDraft({ ...draft, wait_timeout_seconds: Number(e.target.value) })} /></label>
            </div>
            <div style={{ marginTop: 10, display: "flex", gap: 12, flexWrap: "wrap", fontSize: 12 }}>
              {WEIGHTING_SCHEMES.map((scheme) => <label key={scheme} style={{ display: "inline-flex", gap: 5 }}><input type="checkbox" checked={draft.weighting_schemes.includes(scheme)} onChange={(e) => setDraft({ ...draft, weighting_schemes: e.target.checked ? [...draft.weighting_schemes, scheme] : draft.weighting_schemes.filter((item) => item !== scheme) })} />{scheme}</label>)}
              <label style={{ display: "inline-flex", gap: 5 }}><input type="checkbox" checked={draft.run_async} onChange={(e) => setDraft({ ...draft, run_async: e.target.checked })} />异步执行</label>
            </div>
            <div style={{ marginTop: 10, display: "grid", gridTemplateColumns: "repeat(4,minmax(0,1fr))", gap: 10 }}>
              <label style={labelStyle}>Walk-forward window<input type="number" style={inputStyle} value={draft.walk_forward_window} onChange={(e) => setDraft({ ...draft, walk_forward_window: Number(e.target.value) })} /></label>
              <label style={labelStyle}>min_periods<input type="number" style={inputStyle} value={draft.walk_forward_min_periods} onChange={(e) => setDraft({ ...draft, walk_forward_min_periods: Number(e.target.value) })} /></label>
              <label style={{ ...labelStyle, alignContent: "end" }}><span><input type="checkbox" checked={draft.walk_forward_enabled} onChange={(e) => setDraft({ ...draft, walk_forward_enabled: e.target.checked })} /> 启用 walk-forward</span></label>
              <label style={{ ...labelStyle, alignContent: "end" }}><span><input type="checkbox" checked={draft.walk_forward_expanding} onChange={(e) => setDraft({ ...draft, walk_forward_expanding: e.target.checked })} /> expanding</span></label>
            </div>
          </section>

          <section style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 10, padding: 14 }}>
            <div style={{ display: "flex", justifyContent: "space-between" }}><h3 style={{ margin: 0, fontSize: 14 }}>Alpha roster</h3><button onClick={() => { setLegs([...legs, { leg_id: `leg_${legs.length + 1}`, seed_run_ids: [""], metadata: {} }]); setLegMetadataTexts([...legMetadataTexts, "{}"]); }} style={{ ...inputStyle, width: "auto", cursor: "pointer" }}><Plus size={12} /> 添加腿</button></div>
            <div style={{ marginTop: 10, display: "grid", gap: 8 }}>
              {legs.map((leg, index) => <div key={index} style={{ display: "grid", gridTemplateColumns: "180px 1fr 260px 32px", gap: 8, alignItems: "start" }}>
                <label style={labelStyle}>leg_id<input style={inputStyle} value={leg.leg_id} onChange={(e) => patchLeg(index, { leg_id: e.target.value })} /></label>
                <label style={labelStyle}>seed run ids（逗号或换行）<textarea style={{ ...inputStyle, minHeight: 58 }} value={leg.seed_run_ids.join("\n")} onChange={(e) => patchLeg(index, { seed_run_ids: e.target.value.split(/[\n,]/) })} /></label>
                <label style={labelStyle}>metadata JSON<textarea style={{ ...inputStyle, minHeight: 58 }} value={legMetadataTexts[index] || ""} onChange={(e) => setLegMetadataTexts((current) => current.map((item, itemIndex) => itemIndex === index ? e.target.value : item))} /></label>
                <button disabled={legs.length <= 2} onClick={() => { setLegs(legs.filter((_, i) => i !== index)); setLegMetadataTexts(legMetadataTexts.filter((_, i) => i !== index)); }} style={{ marginTop: 20, border: 0, background: "transparent", color: legs.length <= 2 ? "#cbd5e1" : "#dc2626", cursor: legs.length <= 2 ? "not-allowed" : "pointer" }}><Trash2 size={15} /></button>
              </div>)}
            </div>
            <label style={{ ...labelStyle, marginTop: 10, maxWidth: 260 }}>Baseline 腿<select style={inputStyle} value={draft.baseline_leg_id} onChange={(e) => setDraft({ ...draft, baseline_leg_id: e.target.value })}>{legs.map((leg) => <option key={leg.leg_id} value={leg.leg_id}>{leg.leg_id || "(未命名)"}</option>)}</select></label>
          </section>

          <section style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 10, padding: 14 }}>
            <div style={{ display: "flex", justifyContent: "space-between" }}><h3 style={{ margin: 0, fontSize: 14 }}>回测场景</h3><button onClick={() => setScenarios([...scenarios, newScenario(scenarios.length)])} style={{ ...inputStyle, width: "auto", cursor: "pointer" }}><Plus size={12} /> 添加场景</button></div>
            <div style={{ marginTop: 10, overflowX: "auto" }}><table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}><thead><tr>{["名称","资金","TopK","节点","并行","n_drop","max","min","hold","OOS开始","OOS结束","scheme超时","run超时","操作"].map((item) => <th key={item} style={{ padding: 5, textAlign: "left", color: "#64748b" }}>{item}</th>)}</tr></thead><tbody>
              {scenarios.map((scenario, index) => <tr key={scenario.scenario_id}>{[
                ["scenario_name", scenario.scenario_name, "text"], ["initial_cash", scenario.initial_cash, "number"], ["topk", scenario.topk, "number"], ["node_id", scenario.node_id, "text"], ["node_parallelism", scenario.node_parallelism, "number"], ["n_drop", scenario.n_drop, "number"], ["max_n_drop", scenario.max_n_drop, "number"], ["min_n_drop", scenario.min_n_drop, "number"], ["hold_thresh", scenario.hold_thresh, "number"], ["oos_start", scenario.oos_start || "", "date"], ["oos_end", scenario.oos_end || "", "date"], ["scheme_timeout_seconds", scenario.scheme_timeout_seconds || "", "number"], ["run_timeout_seconds", scenario.run_timeout_seconds || "", "number"],
              ].map(([key, value, type]) => <td key={String(key)} style={{ padding: 4 }}><input type={String(type)} style={{ ...inputStyle, minWidth: key === "scenario_name" ? 170 : key === "node_id" ? 110 : 70 }} value={String(value)} onChange={(e) => patchScenario(index, { [String(key)]: type === "number" ? Number(e.target.value) : e.target.value })} /></td>)}<td><button disabled={scenarios.length <= 1} onClick={() => setScenarios(scenarios.filter((_, i) => i !== index))} style={{ border: 0, background: "transparent", color: scenarios.length <= 1 ? "#cbd5e1" : "#dc2626" }}><Trash2 size={14} /></button></td></tr>)}
            </tbody></table></div>
          </section>

          <section style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 10, padding: 14, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
            <label style={labelStyle}>rank_fusion JSON<textarea style={{ ...inputStyle, minHeight: 80, fontFamily: "monospace" }} value={draft.rank_fusion_json} onChange={(e) => setDraft({ ...draft, rank_fusion_json: e.target.value })} /></label>
            <label style={labelStyle}>backtest_config 高级 JSON（结构化场景字段最终覆盖）<textarea style={{ ...inputStyle, minHeight: 80, fontFamily: "monospace" }} value={draft.backtest_advanced_json} onChange={(e) => setDraft({ ...draft, backtest_advanced_json: e.target.value })} />{advancedConflictKeys.length > 0 && <span style={{ color: "#92400e", fontWeight: 600 }}>以下高级键将被上方可见场景字段覆盖：{advancedConflictKeys.join(", ")}</span>}</label>
          </section>

          {(error || preview.error) && <div style={{ padding: 10, border: "1px solid #fecaca", background: "#fef2f2", color: "#991b1b", borderRadius: 8, fontSize: 12 }}>{error || preview.error}</div>}

          <details style={{ background: "#0f172a", color: "#dbeafe", borderRadius: 8, padding: 10 }}><summary style={{ cursor: "pointer", fontSize: 12, fontWeight: 700 }}>完整 payload preview（{preview.rows.length} 个场景）</summary><pre style={{ whiteSpace: "pre-wrap", fontSize: 10, maxHeight: 260, overflow: "auto" }}>{JSON.stringify(preview.rows, null, 2)}</pre></details>

          {results.length > 0 && <section style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 10, padding: 12 }}><strong style={{ fontSize: 12 }}>逐场景提交结果</strong><div style={{ display: "grid", gap: 6, marginTop: 8 }}>{results.map((result) => <div key={result.scenario_id} style={{ padding: 8, borderRadius: 6, background: result.status === "succeeded" ? "#ecfdf5" : "#fef2f2", color: result.status === "succeeded" ? "#065f46" : "#991b1b", fontSize: 11 }}><b>{result.scenario_name}</b> · {result.status}{result.error ? ` · ${result.error.reason_code}: ${result.error.message}` : ` · ${JSON.stringify(result.data)}`}</div>)}</div></section>}

          <div style={{ position: "sticky", bottom: 0, display: "flex", justifyContent: "flex-end", gap: 8, padding: 10, background: "rgba(248,250,252,.95)", borderTop: "1px solid #e2e8f0" }}>
            {results.some((item) => item.status === "failed") && <button disabled={submitting} onClick={retryFailed} style={{ ...inputStyle, width: "auto", cursor: "pointer" }}><RotateCcw size={13} /> 仅重试失败场景</button>}
            <button disabled={submitting || Boolean(preview.error)} onClick={() => void submitRows(preview.rows)} style={{ ...inputStyle, width: "auto", background: "#2563eb", color: "#fff", borderColor: "#2563eb", cursor: submitting ? "wait" : "pointer" }}><Play size={13} /> {submitting ? "逐场景提交中…" : `创建 ${preview.rows.length} 个场景 run`}</button>
          </div>
        </div>
      </div>
    </div>
  );
}
