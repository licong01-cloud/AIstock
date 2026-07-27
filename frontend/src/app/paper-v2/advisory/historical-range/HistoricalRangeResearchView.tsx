"use client";

import { Database, Eye, Play, Plus, RefreshCw, RotateCcw, Square } from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import type { HistoricalRangeCreatePayload, HistoricalRangeProgramSpec, HistoricalRangeRecord } from "@/lib/api/advisory";
import { useHistoricalRangeResearch } from "./useHistoricalRangeResearch";

type Props = { prefillProgramId?: string };
type ResearchRow = { id: string; name: string; packageId: string; targetCount: number };

function value(row: HistoricalRangeRecord | null, key: string, fallback = "-"): string {
  const result = row?.[key];
  return result === null || result === undefined || result === "" ? fallback : String(result);
}

function statusClass(status: unknown): string {
  const normalized = String(status || "UNKNOWN").toLowerCase().replaceAll("_", "-");
  return `ahr-status ahr-status-${normalized}`;
}

function record(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function nestedRows(row: HistoricalRangeRecord, containerKey: string, rowsKey: string): Record<string, unknown>[] {
  const container = record(row[containerKey]);
  const rows = container?.[rowsKey];
  return Array.isArray(rows) ? rows.map(record).filter((item): item is Record<string, unknown> => item !== null) : [];
}

function structuredValue(input: unknown): string {
  if (input === null || input === undefined || input === "") return "-";
  if (typeof input === "object") return JSON.stringify(input);
  return String(input);
}

function metricText(metric: Record<string, unknown>): string {
  const name = metric.metric_name || metric.metric || metric.projection || metric.name || "metric";
  const result = metric.value ?? metric.metric_value ?? metric.projection_value_decimal ?? metric.status ?? "unavailable";
  return `${String(name)} = ${structuredValue(result)}`;
}

function ErrorPanel({ error }: { error: ReturnType<typeof useHistoricalRangeResearch>["error"] }) {
  if (!error) return null;
  return (
    <div className="ahr-error" role="alert" data-testid="historical-range-error">
      <strong>{error.reason_code || error.error_code}</strong>
      <span>{error.message}</span>
      {error.retryable ? <span>该错误可重试；不确定网络结果会复用原幂等键。</span> : null}
      {error.correlation_id ? <code>{error.correlation_id}</code> : null}
      {Object.keys(error.context).length ? <dl className="ahr-structured-facts">{Object.entries(error.context).map(([key, item]) => <div key={key}><dt>{key}</dt><dd>{structuredValue(item)}</dd></div>)}</dl> : null}
    </div>
  );
}

export function HistoricalRangeResearchView({ prefillProgramId }: Props) {
  const model = useHistoricalRangeResearch();
  const [existingIds, setExistingIds] = useState<string[]>(prefillProgramId ? [prefillProgramId] : []);
  const [researchRows, setResearchRows] = useState<ResearchRow[]>([]);
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [labelAsOfTradeDate, setLabelAsOfTradeDate] = useState("");

  useEffect(() => {
    if (prefillProgramId) setExistingIds((current) => current.includes(prefillProgramId) ? current : [...current, prefillProgramId]);
  }, [prefillProgramId]);

  const selectedBatch = model.selectedBatch;
  const successfulDays = Number(selectedBatch?.successful_day_count || 0);
  const recoverable = Boolean(selectedBatch?.planning_recoverable) || Number(selectedBatch?.recoverable_program_count || 0) > 0;
  const batchStatus = value(selectedBatch, "status");
  const waitingRuns = model.runs.filter((run) => String(run.status) === "WAITING_INPUT").length;
  const retryableRuns = model.runs.filter((run) => String(run.status) === "RETRYABLE_FAILED").length;
  const batchFinished = new Set(["COMPLETED", "FAILED", "CANCELLED", "DEDUPLICATED"]).has(value(selectedBatch, "status"));
  const canCancel = selectedBatch && !batchFinished && !["CANCELLING", "CANCELLED"].includes(value(selectedBatch, "status"));
  const canBridge = selectedBatch && !recoverable && successfulDays > 0;
  const availableHorizons = Array.from(new Set([
    ...(model.options?.outcome_catalog.default_horizons || []),
    ...(model.options?.outcome_catalog.long_trend_horizons || []),
  ])).sort((left, right) => left - right);

  const submitPayload = useMemo<HistoricalRangeCreatePayload | null>(() => {
    if (!model.options || !startDate || !endDate) return null;
    const specs: HistoricalRangeProgramSpec[] = [];
    for (const programId of existingIds) {
      const option = model.options.existing_programs.find((item) => item.program_id === programId);
      if (!option) continue;
      specs.push({
        source_kind: "EXISTING_PROGRAM",
        program_id: option.program_id,
        expected_program_version: option.version,
        expected_binding_version_id: option.active_binding_version_id,
      });
    }
    for (const row of researchRows) {
      if (!row.name.trim() || !row.packageId) continue;
      specs.push({
        source_kind: "RESEARCH_PROGRAM_SPEC",
        program_name: row.name.trim(),
        package_id: row.packageId,
        target_count: row.targetCount,
        review_policy: {},
        runtime_config: {},
        entry_price_basis: "next_open_executable",
        exit_price_basis: "next_open_executable",
        style_profile_ref: null,
        style_profile_hash: null,
      });
    }
    return specs.length ? { program_specs: specs, start_trade_date: startDate, end_trade_date: endDate } : null;
  }, [endDate, existingIds, model.options, researchRows, startDate]);

  return (
    <div className="ahr-shell" data-testid="historical-range-view">
      <section className="ahr-section ahr-create">
        <div className="ahr-section-head">
          <div><span className="ahr-eyebrow">PHASE 1R / RETROSPECTIVE</span><h2>创建历史验证范围</h2></div>
          <button className="pv2-button-primary ahr-icon-button" disabled={!submitPayload || model.mutating} onClick={() => submitPayload && void model.create(submitPayload)} type="button">
            <Play size={16} aria-hidden /> {model.mutating ? "正在持久化..." : "开始历史验证"}
          </button>
        </div>
        <p className="ahr-context">只生成历史研究证据，不运行账户、订单、模拟盘或当前荐股。一个 batch 内的 Program 独立执行，失败不会覆盖其他 Program 的成功事实。</p>
        <ErrorPanel error={model.error} />
        {model.loading ? <div className="ahr-state" data-testid="historical-range-loading">正在读取 Program、已准入策略包与任务...</div> : null}
        <div className="ahr-create-grid">
          <fieldset className="ahr-fieldset">
            <legend>Existing Program（可多选）</legend>
            <div className="ahr-option-list">
              {(model.options?.existing_programs || []).map((program) => (
                <label className="ahr-check" key={program.program_id}>
                  <input type="checkbox" checked={existingIds.includes(program.program_id)} onChange={(event) => setExistingIds((current) => event.target.checked ? [...current, program.program_id] : current.filter((id) => id !== program.program_id))} />
                  <span><strong>{program.name}</strong><small>v{program.version} / binding {program.active_binding_version_id} / {program.package_id}</small></span>
                </label>
              ))}
              {!model.options?.existing_programs.length && !model.loading ? <div className="ahr-state">没有可用的 Existing Program。</div> : null}
            </div>
          </fieldset>
          <fieldset className="ahr-fieldset">
            <legend>Research-only Program</legend>
            {researchRows.map((row, index) => (
              <div className="ahr-research-row" key={row.id}>
                <input className="pv2-input" aria-label={`研究配置 ${index + 1} 名称`} placeholder="研究配置名称" value={row.name} onChange={(event) => setResearchRows((current) => current.map((item) => item.id === row.id ? { ...item, name: event.target.value } : item))} />
                <select className="pv2-select" aria-label={`研究配置 ${index + 1} 策略包`} value={row.packageId} onChange={(event) => setResearchRows((current) => current.map((item) => item.id === row.id ? { ...item, packageId: event.target.value } : item))}>
                  <option value="">选择一个已准入包</option>
                  {(model.options?.admitted_packages || []).map((pkg) => <option value={pkg.package_id} key={pkg.package_id}>{pkg.name} / {pkg.alpha_mode} / {pkg.component_count} legs</option>)}
                </select>
                <input className="pv2-input" aria-label={`研究配置 ${index + 1} 目标数量`} type="number" min={1} max={100} value={row.targetCount} onChange={(event) => setResearchRows((current) => current.map((item) => item.id === row.id ? { ...item, targetCount: Number(event.target.value) } : item))} />
                <button className="pv2-button-ghost" onClick={() => setResearchRows((current) => current.filter((item) => item.id !== row.id))} type="button">移除</button>
              </div>
            ))}
            <button className="pv2-button ahr-icon-button" onClick={() => setResearchRows((current) => [...current, { id: crypto.randomUUID(), name: "", packageId: "", targetCount: 5 }])} type="button"><Plus size={15} aria-hidden />添加研究配置</button>
          </fieldset>
        </div>
        <div className="ahr-date-row">
          <label>开始交易日<input className="pv2-input" type="date" value={startDate} onChange={(event) => setStartDate(event.target.value)} /></label>
          <label>结束交易日<input className="pv2-input" type="date" value={endDate} onChange={(event) => setEndDate(event.target.value)} /></label>
          <span>日期必须是数据库已完成历史交易日；不强制使用最新交易日。</span>
        </div>
      </section>

      <section className="ahr-section">
        <div className="ahr-section-head"><div><span className="ahr-eyebrow">BATCH QUEUE</span><h2>范围任务</h2></div><span className="ahr-count">{model.batches.length} loaded</span></div>
        <div className="ahr-table-wrap"><table className="ahr-table"><thead><tr><th>Batch</th><th>日期</th><th>Programs</th><th>状态</th><th>日进度</th><th>恢复项</th><th>创建时间</th><th>操作</th></tr></thead><tbody>
          {model.batches.map((batch) => <tr key={String(batch.batch_id)} className={model.selectedBatch?.batch_id === batch.batch_id ? "is-selected" : ""}>
            <td><code>{String(batch.batch_id)}</code></td><td>{String(batch.start_trade_date)} → {String(batch.end_trade_date)}</td><td>{String(batch.program_count)}</td><td><span className={statusClass(batch.status)}>{String(batch.status)}</span></td><td>{String(batch.successful_day_count || 0)} / {String(batch.planned_day_count || 0)}</td><td>{String(batch.recoverable_program_count || 0)}</td><td>{String(batch.created_at || "-")}</td><td><button className="ahr-row-button" title="查看" aria-label="查看任务" onClick={() => void model.selectBatch(String(batch.batch_id))} type="button"><Eye size={16} /></button></td>
          </tr>)}
          {!model.batches.length && !model.loading ? <tr><td colSpan={8}><div className="ahr-state">尚无历史验证 batch；合法空列表不是请求失败。</div></td></tr> : null}
        </tbody></table></div>
        {model.batchPage.has_more ? <button className="pv2-button" onClick={() => void model.loadMoreBatches()} type="button">加载更多任务</button> : null}
      </section>

      {selectedBatch ? <section className="ahr-section ahr-detail" data-testid="historical-range-detail">
        <div className="ahr-section-head"><div><span className="ahr-eyebrow">SELECTED BATCH</span><h2>{value(selectedBatch, "batch_id")}</h2></div><div className="ahr-actions">
          <button className="ahr-action" disabled={!recoverable || model.mutating} title="恢复" onClick={() => void model.mutate("resume")} type="button"><RotateCcw size={16} />恢复</button>
          <button className="ahr-action" disabled={!canCancel || model.mutating} title="取消" onClick={() => void model.mutate("cancel")} type="button"><Square size={15} />取消</button>
          <button className="ahr-action" disabled={successfulDays < 1 || !labelAsOfTradeDate || !model.selectedHorizons.length || model.mutating} title="刷新收益" onClick={() => void model.mutate("refresh", labelAsOfTradeDate)} type="button"><RefreshCw size={16} />刷新收益</button>
          <button className="ahr-action" disabled={!canBridge || !model.selectedHorizons.length || model.mutating} title="构建 retrospective dataset bridge" onClick={() => void model.mutate("bridge")} type="button"><Database size={16} />构建数据桥</button>
        </div></div>
        {batchStatus === "WAITING_INPUT" || waitingRuns ? <div className="ahr-domain-state is-waiting">WAITING_INPUT：输入或权威来源尚未闭合；已完成事实仍可查看，恢复命令不会伪造成功。</div> : null}
        {batchStatus === "PARTIAL" ? <div className="ahr-domain-state is-partial">{recoverable ? "部分结果；存在可恢复项，各 Program 的成功事实保持独立。" : "部分结果，当前无可恢复项；已完成 Program/日期证据仍是有效事实。"}</div> : null}
        {retryableRuns ? <div className="ahr-domain-state is-retryable">RETRYABLE_FAILED：{retryableRuns} 个 Program 可按原身份恢复，不会重复成功事实。</div> : null}
        {batchStatus === "FAILED" ? <div className="ahr-domain-state is-failed">FAILED：范围任务失败；错误 receipt 与各 Program 已完成事实不会被聚合状态覆盖。</div> : null}
        <div className="ahr-metrics"><div><small>范围状态</small><strong>{value(selectedBatch, "status")}</strong></div><div><small>Catalog</small><strong>{value(selectedBatch, "catalog_phase")}</strong></div><div><small>成功日</small><strong>{successfulDays}</strong></div><div><small>失败日</small><strong>{value(selectedBatch, "terminal_failed_day_count", "0")}</strong></div><div><small>Row version</small><strong>{value(selectedBatch, "row_version")}</strong></div></div>
        <fieldset className="ahr-horizon-picker"><legend>Outcome / Dataset horizons</legend>{availableHorizons.map((horizon) => <label key={horizon}><input type="checkbox" checked={model.selectedHorizons.includes(horizon)} onChange={(event) => model.setSelectedHorizons((current) => event.target.checked ? [...current, horizon].sort((left, right) => left - right) : current.filter((item) => item !== horizon))} /><span>{horizon}D</span></label>)}<label className="ahr-label-date">Outcome label-as-of<input className="pv2-input" type="date" value={labelAsOfTradeDate} onChange={(event) => setLabelAsOfTradeDate(event.target.value)} /></label></fieldset>
        {model.activeOperation ? <div className="ahr-operation"><span>本次命令</span><code>{value(model.activeOperation, "operation_id")}</code><strong className={statusClass(model.activeOperation.status)}>{value(model.activeOperation, "status")}</strong>{model.activeOperation.result_status ? <span>Receipt: <strong>{String(model.activeOperation.result_status)}</strong></span> : null}{model.activeOperation.result_ref ? <span>证据: {structuredValue(record(model.activeOperation.result_ref)?.semantic_content_hash || model.activeOperation.result_ref)}</span> : null}{model.activeOperation.error_json ? <div className="ahr-operation-error"><strong>{String(record(model.activeOperation.error_json)?.reason_code || "命令失败")}</strong><span>{String(record(model.activeOperation.error_json)?.message || "失败 receipt 已持久化；batch 主状态不会覆盖此错误。")}</span>{record(model.activeOperation.error_json)?.context ? <span>{structuredValue(record(model.activeOperation.error_json)?.context)}</span> : null}</div> : null}</div> : null}
        <div className="ahr-split">
          <div><h3>Program runs</h3><div className="ahr-table-wrap"><table className="ahr-table"><thead><tr><th>Program / package</th><th>模式</th><th>状态</th><th>日进度</th><th></th></tr></thead><tbody>{model.runs.map((run) => <tr key={String(run.range_run_id)}><td><strong>{String(run.research_program_id)}</strong><small>{String(run.package_id)}@{String(run.package_version)}</small></td><td>{String(run.alpha_mode)}</td><td><span className={statusClass(run.status)}>{String(run.status)}</span></td><td>{String(run.completed_day_count || 0)} / {String(run.total_day_count || 0)}</td><td><button className="ahr-row-button" aria-label="查看 Program run" onClick={() => void model.selectRun(run)} type="button"><Eye size={16} /></button></td></tr>)}</tbody></table></div></div>
          <div><h3>Operations</h3><div className="ahr-operation-list">{model.operations.map((operation) => <button className={model.activeOperation?.operation_id === operation.operation_id ? "is-selected" : ""} key={String(operation.operation_id)} onClick={() => void model.selectOperation(String(operation.operation_id))} type="button"><span>{String(operation.operation_type)}</span><strong className={statusClass(operation.status)}>{String(operation.status)}</strong><small>{String(operation.updated_at || operation.created_at)}</small></button>)}</div></div>
        </div>
      </section> : null}

      {model.selectedRun ? <section className="ahr-section">
        <div className="ahr-section-head"><div><span className="ahr-eyebrow">PROGRAM EVIDENCE</span><h2>{value(model.selectedRun, "research_program_id")}</h2></div><span className={statusClass(model.selectedRun.status)}>{value(model.selectedRun, "status")}</span></div>
        <div className="ahr-evidence-grid">
          <div><h3>逐日决策证据（T 日可用）</h3><div className="ahr-table-wrap"><table className="ahr-table"><thead><tr><th>序号</th><th>交易日</th><th>状态</th><th>候选</th><th>ENTER/HOLD/EXIT/WATCH</th><th></th></tr></thead><tbody>{model.days.map((day) => <tr key={String(day.day_run_id)} className={model.selectedDay?.day_run_id === day.day_run_id ? "is-selected" : ""}><td>{String(day.ordinal)}</td><td>{String(day.decision_trade_date)}</td><td><span className={statusClass(day.status)}>{String(day.status)}</span></td><td>{String(day.candidate_count || 0)}</td><td>{String(day.enter_count || 0)} / {String(day.hold_count || 0)} / {String(day.exit_count || 0)} / {String(day.watch_count || 0)}</td><td><button className="ahr-row-button" aria-label={`查看 ${String(day.decision_trade_date)} 证据`} onClick={() => void model.selectDay(day)} type="button"><Eye size={16} /></button></td></tr>)}</tbody></table></div>{model.dayPage.has_more ? <button className="pv2-button" onClick={() => void model.loadMoreDays()} type="button">加载更多交易日</button> : null}</div>
          <div><h3>Episode 与 Outcome</h3><p className="ahr-warning">以下为未来成熟后的历史结果，不是决策日可用信息，也不是回测或模拟盘收益。</p><div className="ahr-table-wrap"><table className="ahr-table ahr-outcome-table"><thead><tr><th>主体</th><th>投影 / 周期</th><th>成熟度 / label-as-of</th><th>历史收益 / MFE / MAE</th><th>成本 / benchmark 证据</th><th>版本</th></tr></thead><tbody>{model.outcomes.map((outcome) => { const calculations = nestedRows(outcome, "outcome_json", "calculation_results"); return <tr key={String(outcome.outcome_version_id)}><td>{String(outcome.subject_type)} / {String(outcome.subject_id)}</td><td>{String(outcome.projection)} / {String(outcome.horizon_trade_days)}D</td><td><span className={statusClass(outcome.maturity_status)}>{String(outcome.maturity_status)}</span><small>{String(outcome.label_as_of_trade_date || outcome.next_refresh_trade_date || "-")}</small></td><td>{calculations.length ? calculations.map((item, index) => <small key={`${String(item.projection)}:${index}`}>{String(item.projection || "result")}: {structuredValue(item.projection_value_decimal ?? item.projection_event_code ?? item.maturity_status)}</small>) : <small>尚无成熟计算值，不按 0 展示。</small>}</td><td><small>cost: {String(outcome.cost_policy_hash || "unavailable")}</small><small>benchmark: {String(outcome.benchmark_hash || "unavailable")}</small></td><td>{String(outcome.outcome_version)}</td></tr>; })}{!model.outcomes.length ? <tr><td colSpan={6}><div className="ahr-state">尚无 outcome；这表示未刷新或尚未成熟，不等同于零收益。</div></td></tr> : null}</tbody></table></div>{model.outcomePage.has_more ? <button className="pv2-button" onClick={() => void model.loadMoreOutcomes()} type="button">加载更多 Outcome</button> : null}</div>
        </div>
        {model.selectedDay ? <div className="ahr-day-detail" data-testid="historical-range-day-detail">
          <div className="ahr-section-head"><div><span className="ahr-eyebrow">DAY / LIST / EPISODE</span><h3>{value(model.selectedDay, "decision_trade_date")}</h3></div><span className={statusClass(model.selectedDay.status)}>{value(model.selectedDay, "status")}</span></div>
          <div className="ahr-evidence-grid">
            <div><h3>候选与排名证据</h3><div className="ahr-table-wrap"><table className="ahr-table"><thead><tr><th>Symbol</th><th>原始排名</th><th>有效排名</th><th>成员状态</th><th>分数</th></tr></thead><tbody>{model.candidates.map((candidate) => <tr key={String(candidate.candidate_id)}><td><strong>{String(candidate.symbol)}</strong></td><td>{String(candidate.selection_raw_rank ?? "-")}</td><td>{String(candidate.selection_effective_rank ?? "-")}</td><td>{String(candidate.membership_status ?? "-")}</td><td>{String(candidate.selection_score ?? "-")}</td></tr>)}{!model.candidates.length ? <tr><td colSpan={5}><div className="ahr-state">VALID_NO_CANDIDATE 表示当日完成了有效计算但没有候选，不是加载失败。</div></td></tr> : null}</tbody></table></div>{model.candidatePage.has_more ? <button className="pv2-button" onClick={() => void model.loadMoreCandidates()} type="button">加载更多候选</button> : null}</div>
            <div><h3>List 与 Episode 状态</h3>{model.listVersion ? <div className="ahr-list-meta"><span>List {value(model.listVersion, "list_version_id")}</span><span>Active {value(model.listVersion, "active_count", "0")}</span></div> : null}<div className="ahr-table-wrap"><table className="ahr-table"><thead><tr><th>Symbol</th><th>动作</th><th>排名</th><th>前序排名</th><th>理由</th><th>Episode</th><th>执行指引</th></tr></thead><tbody>{model.listItems.map((item) => <tr key={`${String(item.list_version_id)}:${String(item.symbol)}`}><td><strong>{String(item.symbol)}</strong></td><td><span className={statusClass(item.action)}>{String(item.action)}</span></td><td>{String(item.rank ?? "-")}</td><td>{String(item.previous_rank ?? "-")}</td><td>{structuredValue(item.reason_codes)}</td><td>{String(item.recommendation_state ?? item.episode_id ?? "-")}</td><td>{structuredValue(item.intended_execution_basis ?? item.rule_guidance_json)}</td></tr>)}{!model.listItems.length ? <tr><td colSpan={7}><div className="ahr-state">该日列表为空；空列表仍保留完整 list version 证据。</div></td></tr> : null}</tbody></table></div>{model.listItemPage.has_more ? <button className="pv2-button" onClick={() => void model.loadMoreListItems()} type="button">加载更多 List 项</button> : null}</div>
          </div>
        </div> : null}
        <div className="ahr-summary"><h3>Summary 与 Dataset</h3>{model.summaries.length ? model.summaries.map((summary) => { const metrics = nestedRows(summary, "summary_json", "metrics"); const unavailable = nestedRows(summary, "summary_json", "unavailable_metrics"); return <div className="ahr-summary-card" key={String(summary.summary_id)}><strong>Summary v{String(summary.summary_version)}</strong><span>Outcome set {String(summary.covered_outcome_set_hash)}</span><div className="ahr-summary-metrics">{metrics.map((item, index) => <span key={`${metricText(item)}:${index}`}>{metricText(item)}</span>)}{unavailable.map((item, index) => <span className="is-unavailable" key={`unavailable:${index}`}>{metricText(item)}</span>)}</div></div>; }) : <div className="ahr-state">暂无 Summary。Dataset 的 VALID_EMPTY 会作为有效 receipt 显示，不会伪装成失败。</div>}</div>
        <details className="ahr-debug"><summary>高级调试：原始 JSON</summary><pre>{JSON.stringify({ batch: model.selectedBatch, run: model.selectedRun, operation: model.activeOperation }, null, 2)}</pre></details>
      </section> : null}
    </div>
  );
}
