"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { simulationRuntimeApi } from "@/lib/paper-v2/api";
import { formatCompact, shortHash, statusLabel } from "@/lib/paper-v2/format";
import type {
  JsonObject,
  SimulationRuntimePlanSummary,
  SimulationRuntimeRunDetail,
  SimulationRuntimeRunSummary,
  SimulationRuntimeRunsResponse,
  SimulationRuntimeSchedulerStatus,
} from "@/lib/paper-v2/types";

function todayIso(): string {
  const now = new Date();
  return new Date(now.getTime() - now.getTimezoneOffset() * 60_000).toISOString().slice(0, 10);
}

function textValue(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  return String(value);
}

function numberValue(value: unknown): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function mapCount(row: JsonObject | null | undefined, key: string): number {
  if (!row) return 0;
  return numberValue(row[key]);
}

function itemValue(row: JsonObject | null | undefined, key: string): unknown {
  return row ? row[key] : undefined;
}

function arrayValue(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function objectValue(value: unknown): JsonObject | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : null;
}

function hashLabel(value: unknown, size = 10): string {
  const text = textValue(value);
  return text === "-" ? "-" : shortHash(text, size);
}

function displayValue(row: SimulationRuntimeRunSummary | null | undefined, key: string): string {
  const display = objectValue(itemValue(row, "display"));
  return textValue(itemValue(display, key));
}

function runStatusLabel(value: unknown): string {
  const status = String(value || "").toUpperCase();
  const labels: Record<string, string> = {
    PLANNING_EXECUTION: "执行计划已生成",
    INTRADAY_RUNNING: "盘中运行中",
    SUCCEEDED: "已完成 / 无待处理错误",
    FAILED_RETRYABLE: "当日失败 / 可重试",
    FAILED_TERMINAL: "终止失败",
    CANCELLED: "已取消",
  };
  return labels[status] || statusLabel(value);
}

function brokerLabel(row: SimulationRuntimeRunSummary): string {
  const display = displayValue(row, "broker_label");
  if (display !== "-") return display;
  if (row.broker_backend === "minqmt_sim") return "MiniQMT 模拟盘";
  if (row.broker_backend === "local_sim") return "LocalSim 本地模拟";
  return textValue(row.broker_backend);
}

function readableIdentifier(value: unknown): string {
  const raw = textValue(value);
  if (raw === "-") return "-";
  const ignored = new Set(["strategy", "simrun", "srr", "simbind", "dse", "plan", "pkg", "ag", "slot"]);
  const words = raw.split(/[_-]+/).filter(Boolean).filter((part) => !ignored.has(part.toLowerCase())).map((part) => {
    const lower = part.toLowerCase();
    if (lower === "local") return "Local";
    if (lower === "miniqmt" || lower === "minqmt") return "MiniQMT";
    if (lower === "qmt") return "QMT";
    if (lower === "sim") return "SIM";
    if (lower === "ops") return "Ops";
    if (/^(19|20)\d{6}$/.test(part)) return `${part.slice(0, 4)}-${part.slice(4, 6)}-${part.slice(6)}`;
    return part.slice(0, 1).toUpperCase() + part.slice(1);
  });
  return words.length ? words.join(" ") : hashLabel(raw, 8);
}

function strategyLabel(row: SimulationRuntimeRunSummary): string {
  const display = displayValue(row, "strategy_label");
  return display !== "-" ? display : readableIdentifier(row.strategy_id);
}

function accountSlotLabel(row: SimulationRuntimeRunSummary): string {
  const display = displayValue(row, "account_slot_label");
  if (display !== "-") return display;
  const account = row.account_group_id ? readableIdentifier(row.account_group_id) : "本地模拟账户";
  const slot = row.strategy_slot_id ? readableIdentifier(row.strategy_slot_id) : "默认策略槽";
  return `${account} / ${slot}`;
}

function selectionLabel(row: SimulationRuntimeRunSummary): string {
  const display = displayValue(row, "selection_label");
  if (display !== "-") return display;
  return `选出 ${mapCount(row.stage_counts, "target_count")} 只候选`;
}

function executionPlanLabel(row: SimulationRuntimeRunSummary): string {
  const display = displayValue(row, "execution_plan_label");
  if (display !== "-") return display;
  const counts = row.stage_counts || {};
  const intents = mapCount(counts, "execution_plan_intent_count") || mapCount(counts, "order_intent_count");
  return `交易意图 ${intents} / 已提交 ${mapCount(counts, "submitted_intents")} / 失败 ${mapCount(counts, "failed_intents")}`;
}

function orderResultLabel(row: SimulationRuntimeRunSummary): string {
  const orderCount = projectionRows(row, "orders").length || brokerOrderCount(row);
  const fillCount = projectionRows(row, "fills").length;
  const errorCount = projectionRows(row, "errors").length;
  if (errorCount) return `错误 ${errorCount} / 订单 ${orderCount} / 成交同步 ${fillCount}`;
  return `订单 ${orderCount} / 成交同步 ${fillCount}`;
}

function secondaryId(label: string, value: unknown) {
  return <div className="pv2-muted pv2-mono" title={textValue(value)}>{label}: {hashLabel(value, 8)}</div>;
}

function stageSummary(row: SimulationRuntimeRunSummary): string {
  const counts = row.stage_counts || {};
  const target = mapCount(counts, "target_count");
  const intents = mapCount(counts, "execution_plan_intent_count") || mapCount(counts, "order_intent_count");
  const submitted = mapCount(counts, "submitted_intents");
  const failed = mapCount(counts, "failed_intents");
  return `选股 ${target} / 意图 ${intents} / 已提交 ${submitted} / 失败 ${failed}`;
}

function brokerOrderCount(row: SimulationRuntimeRunSummary | null | undefined): number {
  if (!row) return 0;
  const handles = itemValue(row.broker_context, "broker_order_handles");
  const batch = objectValue(itemValue(row.broker_context, "qmt_batch_result"));
  const batchResults = batch ? arrayValue(batch.results) : [];
  return arrayValue(handles).length + batchResults.length;
}

function miniQmtRuntimeId(row: SimulationRuntimeRunSummary | null | undefined): string | undefined {
  const batch = objectValue(itemValue(row?.broker_context, "qmt_batch_result"));
  const evidence = objectValue(itemValue(batch, "runtime_evidence"));
  const runtimeId = textValue(itemValue(evidence, "runtime_id"));
  return runtimeId === "-" ? undefined : runtimeId;
}

function reconciliationIssueCount(row: SimulationRuntimeRunSummary | null | undefined): number {
  if (!row) return 0;
  return numberValue(itemValue(row.reconciliation_context, "issue_count"));
}

function projectionRows(row: SimulationRuntimeRunSummary | null | undefined, key: "orders" | "fills" | "errors"): JsonObject[] {
  if (!row) return [];
  return arrayValue(row[key]).map(objectValue).filter((item): item is JsonObject => Boolean(item));
}

const OPERATOR_COMMAND_OPTIONS = [
  "CANCEL_ALL_OPEN_ORDERS",
  "FLATTEN_ALL_POSITIONS",
  "FLATTEN_STRATEGY_SLOT",
  "RESET_STRATEGY_SLOT",
  "REPLACE_ALPHA_SIGNAL_BOOK",
] as const;

export default function SimulationRuntimeOpsPage() {
  const [scheduler, setScheduler] = useState<SimulationRuntimeSchedulerStatus | null>(null);
  const [runsPayload, setRunsPayload] = useState<SimulationRuntimeRunsResponse>({ summary: {}, runs: [] });
  const [selectedRun, setSelectedRun] = useState<SimulationRuntimeRunDetail | null>(null);
  const [selectedPlan, setSelectedPlan] = useState<SimulationRuntimePlanSummary | null>(null);
  const [tradeDate, setTradeDate] = useState(todayIso());
  const [brokerBackend, setBrokerBackend] = useState("");
  const [status, setStatus] = useState("");
  const [strategyId, setStrategyId] = useState("");
  const [loading, setLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [operatorCommand, setOperatorCommand] = useState<(typeof OPERATOR_COMMAND_OPTIONS)[number]>("CANCEL_ALL_OPEN_ORDERS");
  const [operatorReason, setOperatorReason] = useState("");
  const [operatorConfirmText, setOperatorConfirmText] = useState("");
  const [operatorAlphaSignalBookId, setOperatorAlphaSignalBookId] = useState("");
  const [operatorSubmitting, setOperatorSubmitting] = useState(false);
  const [operatorResult, setOperatorResult] = useState<JsonObject | null>(null);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextScheduler, nextRuns] = await Promise.all([
        simulationRuntimeApi.schedulerStatus(),
        simulationRuntimeApi.listRuns({
          tradeDate: tradeDate || undefined,
          brokerBackend: brokerBackend || undefined,
          status: status || undefined,
          strategyId: strategyId || undefined,
          limit: 100,
        }),
      ]);
      setScheduler(nextScheduler);
      setRunsPayload(nextRuns);
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [brokerBackend, status, strategyId, tradeDate]);

  useEffect(() => { load(); }, [load]);

  const runs = useMemo(() => runsPayload.runs || [], [runsPayload.runs]);
  const summary = useMemo(() => runsPayload.summary || {}, [runsPayload.summary]);
  const byStatus = useMemo(() => {
    const raw = summary.by_status;
    return raw && typeof raw === "object" && !Array.isArray(raw) ? raw as JsonObject : {};
  }, [summary]);
  const byBackend = useMemo(() => {
    const raw = summary.by_broker_backend;
    return raw && typeof raw === "object" && !Array.isArray(raw) ? raw as JsonObject : {};
  }, [summary]);
  const schedulerWindows = useMemo(
    () => arrayValue(scheduler?.schedule_windows).map(objectValue).filter((item): item is JsonObject => Boolean(item)),
    [scheduler],
  );

  async function showRun(row: SimulationRuntimeRunSummary) {
    setDetailLoading(true);
    setError(null);
    try {
      const detail = await simulationRuntimeApi.getRun(row.run_id);
      setSelectedRun(detail);
      setSelectedPlan(detail.execution_plan || null);
    } catch (exc) {
      setError(exc);
    } finally {
      setDetailLoading(false);
    }
  }

  function clearSelectedRun() {
    setSelectedRun(null);
    setSelectedPlan(null);
    setOperatorResult(null);
  }

  async function executeOperatorCommand() {
    if (!selectedRun) return;
    setOperatorSubmitting(true);
    setError(null);
    setOperatorResult(null);
    try {
      const result = await simulationRuntimeApi.executeMiniQmtOperatorCommand({
        command_type: operatorCommand,
        account_group_id: String(selectedRun.run.account_group_id || ""),
        strategy_slot_id: selectedRun.run.strategy_slot_id ? String(selectedRun.run.strategy_slot_id) : undefined,
        alpha_signal_book_id: operatorAlphaSignalBookId.trim() || undefined,
        trade_date: selectedRun.run.trade_date || tradeDate,
        runtime_config_hash: String(selectedRun.run.execution_plan_hash || selectedRun.run.binding_hash || selectedRun.run.release_hash || ""),
        runtime_id: miniQmtRuntimeId(selectedRun.run),
        reason: operatorReason.trim(),
        confirm_text: operatorConfirmText.trim(),
        requested_by: "paper-v2-simulation-runtime-ui",
        payload: {
          selected_run_id: selectedRun.run.run_id,
          source: "simulation_runtime_ops_page",
        },
      });
      setOperatorResult(result as unknown as JsonObject);
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setOperatorSubmitting(false);
    }
  }

  return (
    <main>
      <ErrorPanel error={error} title="模拟盘运行态加载失败" />
      <NoticePanel title="受控运维入口" tone="info">
        本页展示 StrategyRuntimeRelease、SimulationReleaseBinding、DailySelectionEvidence、ExecutionPlan 和 SimulationDailyRun 的真实后端状态；手动 tick / start / stop 属于受控 API，默认 submit=false，不会默认向 LocalSim 或 MiniQMT 提交订单。
      </NoticePanel>

      <div className="pv2-grid pv2-grid-4">
        <div data-testid="sim-runtime-total-runs"><MetricCard label="运行记录" value={numberValue(summary.run_count)} hint={`active ${numberValue(summary.active_run_count)} / terminal ${numberValue(summary.terminal_run_count)}`} tone="info" /></div>
        <div data-testid="sim-runtime-local-count"><MetricCard label="LocalSim" value={mapCount(byBackend, "local_sim")} hint="按统一 execution plan 运行" tone="success" /></div>
        <div data-testid="sim-runtime-miniqmt-count"><MetricCard label="MiniQMT SIM" value={mapCount(byBackend, "minqmt_sim")} hint="托管订单默认 preview / dry-run" tone="warning" /></div>
        <div data-testid="sim-runtime-submit-default"><MetricCard label="Scheduler 默认提交" value={scheduler?.default_submit ? "ON" : "OFF"} hint={scheduler?.autostart ? "autostart enabled" : "autostart disabled"} tone={scheduler?.default_submit || scheduler?.autostart ? "danger" : "success"} /></div>
      </div>

      <SectionCard title="运行筛选" eyebrow="SimulationDailyRun" action={<button className="pv2-button" data-testid="sim-runtime-refresh" onClick={load} disabled={loading} type="button">{loading ? "刷新中..." : "刷新"}</button>}>
        <div className="pv2-form-grid">
          <div className="pv2-field"><label>交易日</label><input className="pv2-input" data-testid="sim-runtime-trade-date" type="date" value={tradeDate} onChange={(event) => { clearSelectedRun(); setTradeDate(event.target.value); }} /></div>
          <div className="pv2-field"><label>Backend</label><select className="pv2-select" data-testid="sim-runtime-backend-filter" value={brokerBackend} onChange={(event) => { clearSelectedRun(); setBrokerBackend(event.target.value); }}><option value="">全部</option><option value="local_sim">LocalSim</option><option value="minqmt_sim">MiniQMT SIM</option></select></div>
          <div className="pv2-field"><label>状态</label><select className="pv2-select" data-testid="sim-runtime-status-filter" value={status} onChange={(event) => { clearSelectedRun(); setStatus(event.target.value); }}><option value="">全部</option><option value="PLANNING_EXECUTION">计划已生成</option><option value="INTRADAY_RUNNING">盘中运行</option><option value="SUCCEEDED">成功/无调仓</option><option value="FAILED_RETRYABLE">可重试失败</option><option value="FAILED_TERMINAL">终止失败</option></select></div>
          <div className="pv2-field"><label>strategy_id</label><input className="pv2-input" data-testid="sim-runtime-strategy-filter" value={strategyId} onChange={(event) => { clearSelectedRun(); setStrategyId(event.target.value); }} placeholder="可留空" /></div>
        </div>
      </SectionCard>

      <SectionCard title="运行列表" eyebrow="人类可读的运行摘要；ID/Hash 仅作为次要追踪信息">
        <PaperTable
          rows={runs}
          empty="暂无符合条件的统一模拟盘运行记录。"
          columns={[
            {
              key: "summary",
              header: "运行摘要",
              render: (row) => <div><strong>{row.trade_date} - {runStatusLabel(row.status)}</strong>{secondaryId("run", row.run_id)}<div className="pv2-muted">{stageSummary(row)}</div></div>,
            },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "backend", header: "Broker", render: (row) => brokerLabel(row) },
            {
              key: "strategyAccount",
              header: "策略 / 账户",
              render: (row) => <div data-testid={`sim-runtime-slot-${row.run_id}`}><strong>策略实例：{strategyLabel(row)}</strong><div className="pv2-muted" title={`${textValue(row.account_group_id)} / ${textValue(row.strategy_slot_id)}`}>账户槽：{accountSlotLabel(row)}</div></div>,
            },
            {
              key: "runtimeConfig",
              header: "运行配置",
              render: (row) => <div><strong>{displayValue(row, "package_label") !== "-" ? displayValue(row, "package_label") : `策略包 ${readableIdentifier(row.package_id)}`}</strong>{secondaryId("release", row.release_hash)}{secondaryId("binding", row.binding_hash)}</div>,
            },
            {
              key: "selection",
              header: "选股结果",
              render: (row) => <div><strong>{selectionLabel(row)}</strong>{secondaryId("evidence", row.selection_evidence_id || row.selection_artifact_hash)}</div>,
            },
            {
              key: "plan",
              header: "执行计划",
              render: (row) => <div><strong>{executionPlanLabel(row)}</strong>{secondaryId("plan", row.execution_plan_id || row.execution_plan_hash)}</div>,
            },
            { key: "orders", header: "订单 / 错误", render: (row) => orderResultLabel(row) },
            { key: "action", header: "详情", render: (row) => <button className="pv2-link-button" data-testid={`sim-runtime-run-detail-${row.run_id}`} onClick={() => showRun(row)} disabled={detailLoading} type="button">查看链路</button> },
          ]}
        />
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="Scheduler 安全状态" eyebrow="controlled operations">
          <div className="pv2-readable-panel" data-testid="sim-runtime-scheduler-status">
            <div className="pv2-readable-table">
              <div className="pv2-readable-row"><div className="pv2-readable-key">调度器</div><div className="pv2-readable-value">{scheduler?.scheduler || "-"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">自动启动</div><div className="pv2-readable-value"><StatusBadge status={scheduler?.autostart ? "ENABLED" : "DISABLED"} /></div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">默认提交订单</div><div className="pv2-readable-value"><StatusBadge status={scheduler?.default_submit ? "ENABLED" : "DISABLED"} /></div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">手动 tick API</div><div className="pv2-readable-value"><StatusBadge status={scheduler?.manual_tick_endpoint_enabled ? "ENABLED" : "DISABLED"} /></div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">受控 start/stop API</div><div className="pv2-readable-value"><StatusBadge status={scheduler?.scheduler_control_api_enabled ? "ENABLED" : "DISABLED"} /></div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">Context Provider</div><div className="pv2-readable-value" data-testid="sim-runtime-provider-mode">{textValue(scheduler?.context_provider_mode || objectValue(scheduler?.context_provider)?.provider_mode) || "-"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">Recovery mode</div><div className="pv2-readable-value" data-testid="sim-runtime-restart-recovery-mode">{scheduler?.restart_recovery_mode || "-"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">准入状态</div><div className="pv2-readable-value">{(scheduler?.approval_states || []).join(", ") || "-"}</div></div>
            </div>
          </div>
        </SectionCard>

        <SectionCard title="状态分布" eyebrow="business summary">
          <div className="pv2-readable-panel">
            <div className="pv2-readable-table">
              {Object.keys(byStatus).length ? Object.entries(byStatus).map(([key, value]) => (
                <div className="pv2-readable-row" key={key}><div className="pv2-readable-key">{key}</div><div className="pv2-readable-value">{formatCompact(numberValue(value))}</div></div>
              )) : <div className="pv2-readable-row"><div className="pv2-readable-key">状态</div><div className="pv2-readable-value">暂无记录</div></div>}
            </div>
          </div>
        </SectionCard>
      </div>

      <SectionCard title="Scheduler Windows" eyebrow="unattended lifecycle windows">
        <PaperTable
          rows={schedulerWindows}
          empty="Scheduler has not returned trading-window state."
          columns={[
            { key: "window", header: "Window", render: (row) => textValue(row.label || row.window_id) },
            { key: "time", header: "Time", render: (row) => `${textValue(row.start)} - ${textValue(row.end)}` },
            { key: "action", header: "Action", render: (row) => textValue(row.action) },
            { key: "state", header: "State", render: (row) => <StatusBadge status={textValue(row.state)} /> },
          ]}
        />
      </SectionCard>

      <SectionCard title="MiniQMT Operator Command" eyebrow="runtime command">
        <NoticePanel title="必须进入 MiniQMTExecutionRuntime" tone="warning">
          清仓、撤单、重置策略槽和换 alpha 信号只允许通过受控 runtime 命令执行；页面不会自动触发，破坏性命令必须输入确认文本。
        </NoticePanel>
        <div className="pv2-form-grid" data-testid="sim-runtime-operator-command-panel">
          <div className="pv2-field">
            <label>command</label>
            <select className="pv2-select" data-testid="sim-runtime-operator-command-type" value={operatorCommand} onChange={(event) => setOperatorCommand(event.target.value as (typeof OPERATOR_COMMAND_OPTIONS)[number])}>
              {OPERATOR_COMMAND_OPTIONS.map((item) => <option key={item} value={item}>{item}</option>)}
            </select>
          </div>
          <div className="pv2-field">
            <label>reason</label>
            <input className="pv2-input" data-testid="sim-runtime-operator-reason" value={operatorReason} onChange={(event) => setOperatorReason(event.target.value)} placeholder="必须填写运维原因" />
          </div>
          <div className="pv2-field">
            <label>confirm_text</label>
            <input className="pv2-input" data-testid="sim-runtime-operator-confirm" value={operatorConfirmText} onChange={(event) => setOperatorConfirmText(event.target.value)} placeholder={`EXECUTE ${operatorCommand}`} />
          </div>
          <div className="pv2-field">
            <label>alpha_signal_book_id</label>
            <input className="pv2-input" data-testid="sim-runtime-operator-alpha-book" value={operatorAlphaSignalBookId} onChange={(event) => setOperatorAlphaSignalBookId(event.target.value)} placeholder="REPLACE_ALPHA_SIGNAL_BOOK 必填" />
          </div>
        </div>
        <div className="pv2-readable-panel">
          <div className="pv2-readable-table">
            <div className="pv2-readable-row"><div className="pv2-readable-key">目标运行</div><div className="pv2-readable-value pv2-mono" data-testid="sim-runtime-operator-selected-run">{selectedRun?.run.run_id || "请先选择 MiniQMT SIM 运行记录"}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">account group / slot</div><div className="pv2-readable-value pv2-mono">{textValue(selectedRun?.run.account_group_id)} / {textValue(selectedRun?.run.strategy_slot_id)}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">确认文本</div><div className="pv2-readable-value pv2-mono">EXECUTE {operatorCommand}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">last result</div><div className="pv2-readable-value" data-testid="sim-runtime-operator-result">{textValue(itemValue(objectValue(operatorResult?.result), "status") || itemValue(operatorResult, "ok"))}</div></div>
          </div>
        </div>
        <button className="pv2-button pv2-button-danger" data-testid="sim-runtime-operator-submit" type="button" onClick={executeOperatorCommand} disabled={!selectedRun || selectedRun.run.broker_backend !== "minqmt_sim" || operatorSubmitting || !operatorReason.trim()}>
          {operatorSubmitting ? "执行中..." : "提交 runtime 命令"}
        </button>
      </SectionCard>

      <SectionCard title="选中运行链路" eyebrow="operator detail">
        {selectedRun ? (
          <div className="pv2-grid pv2-grid-2" data-testid="sim-runtime-selected-run">
            <div>
              <h3>业务摘要</h3>
              <div className="pv2-readable-panel">
                <div className="pv2-readable-table">
                  <div className="pv2-readable-row"><div className="pv2-readable-key">run</div><div className="pv2-readable-value pv2-mono" data-testid="sim-runtime-selected-run-id">{selectedRun.run.run_id}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">strategy</div><div className="pv2-readable-value pv2-mono">{selectedRun.run.strategy_id}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">backend</div><div className="pv2-readable-value">{brokerLabel(selectedRun.run)}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">status</div><div className="pv2-readable-value"><StatusBadge status={selectedRun.run.status} /></div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">last stage</div><div className="pv2-readable-value">{selectedRun.run.last_stage || "-"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">account group / slot</div><div className="pv2-readable-value pv2-mono" data-testid="sim-runtime-selected-account-slot">{textValue(selectedRun.run.account_group_id)} / {textValue(selectedRun.run.strategy_slot_id)}</div></div>
                </div>
              </div>
            </div>
            <div>
              <h3>链路证据</h3>
              <div className="pv2-readable-panel">
                <div className="pv2-readable-table">
                  <div className="pv2-readable-row"><div className="pv2-readable-key">selection evidence</div><div className="pv2-readable-value pv2-mono" data-testid="sim-runtime-selected-evidence-id">{itemValue(selectedRun.selection_evidence, "evidence_id") ? String(itemValue(selectedRun.selection_evidence, "evidence_id")) : "-"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">selection hash</div><div className="pv2-readable-value pv2-mono">{hashLabel(itemValue(selectedRun.selection_evidence, "artifact_hash") || selectedRun.run.selection_artifact_hash, 16)}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">execution plan</div><div className="pv2-readable-value pv2-mono" data-testid="sim-runtime-selected-plan-id">{selectedPlan?.plan_id || "-"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">plan hash</div><div className="pv2-readable-value pv2-mono">{hashLabel(selectedPlan?.plan_hash || selectedRun.run.execution_plan_hash, 16)}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">release / binding</div><div className="pv2-readable-value pv2-mono">{hashLabel(selectedRun.run.release_id)} / {hashLabel(selectedRun.run.binding_id)}</div></div>
                </div>
              </div>
            </div>
            <div>
              <h3>执行计划</h3>
              <div className="pv2-readable-panel">
                <div className="pv2-readable-table">
                  <div className="pv2-readable-row"><div className="pv2-readable-key">intent</div><div className="pv2-readable-value" data-testid="sim-runtime-selected-intent-counts">BUY {selectedPlan?.buy_intent_count ?? 0} / SELL {selectedPlan?.sell_intent_count ?? 0} / total {selectedPlan?.intent_count ?? 0}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">symbols</div><div className="pv2-readable-value">{(selectedPlan?.symbols || []).join(", ") || "-"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">execution policy</div><div className="pv2-readable-value pv2-mono">{selectedPlan?.execution_policy_version_id || "-"} / {hashLabel(selectedPlan?.execution_policy_sha256)}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">tail policy</div><div className="pv2-readable-value pv2-mono">{selectedPlan?.tail_policy_version_id || "-"} / {hashLabel(selectedPlan?.tail_policy_sha256)}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">trading rule decisions</div><div className="pv2-readable-value">{selectedPlan?.trading_rule_decision_count ?? 0}</div></div>
                </div>
              </div>
            </div>
            <div>
              <h3>Broker 与审计</h3>
              <div className="pv2-readable-panel">
                <div className="pv2-readable-table">
                  <div className="pv2-readable-row"><div className="pv2-readable-key">broker called</div><div className="pv2-readable-value"><StatusBadge status={itemValue(selectedRun.run.broker_context, "broker_called") ? "YES" : "NO"} /></div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">qmt batch</div><div className="pv2-readable-value pv2-mono">{textValue(itemValue(selectedRun.run.broker_context, "qmt_batch_id"))} / {textValue(itemValue(selectedRun.run.broker_context, "qmt_batch_status"))}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">orders / fills</div><div className="pv2-readable-value">{projectionRows(selectedRun.run, "orders").length || brokerOrderCount(selectedRun.run)} order records / {projectionRows(selectedRun.run, "fills").length} fill sync rows</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">reconciliation</div><div className="pv2-readable-value">{textValue(itemValue(selectedRun.run.reconciliation_context, "status"))} / {reconciliationIssueCount(selectedRun.run)} issues</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">stage counts</div><div className="pv2-readable-value">{stageSummary(selectedRun.run)}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">created / updated</div><div className="pv2-readable-value">{textValue(itemValue(selectedRun.run.audit, "created_at"))} / {textValue(itemValue(selectedRun.run.audit, "updated_at"))}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">candidate / excluded</div><div className="pv2-readable-value">{numberValue(itemValue(selectedRun.selection_evidence, "candidate_count"))} / {numberValue(itemValue(selectedRun.selection_evidence, "excluded_count"))}</div></div>
                </div>
              </div>
            </div>
            <div>
              <h3>策略收益</h3>
              <div className="pv2-readable-panel">
                <div className="pv2-readable-table">
                  <div className="pv2-readable-row"><div className="pv2-readable-key">NAV</div><div className="pv2-readable-value" data-testid="sim-runtime-selected-nav">{textValue(itemValue(selectedRun.run.strategy_performance, "nav"))}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">total equity</div><div className="pv2-readable-value">{textValue(itemValue(selectedRun.run.strategy_performance, "total_equity"))}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">realized / unrealized</div><div className="pv2-readable-value">{textValue(itemValue(selectedRun.run.strategy_performance, "realized_pnl"))} / {textValue(itemValue(selectedRun.run.strategy_performance, "unrealized_pnl"))}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">market value / cash</div><div className="pv2-readable-value">{textValue(itemValue(selectedRun.run.strategy_performance, "market_value"))} / {textValue(itemValue(selectedRun.run.strategy_performance, "cash"))}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">positions</div><div className="pv2-readable-value">{arrayValue(itemValue(selectedRun.run.strategy_performance, "positions")).map((item) => textValue(itemValue(objectValue(item), "symbol"))).join(", ") || "-"}</div></div>
                </div>
              </div>
            </div>
            <div>
              <h3>订单 / 成交 / 错误</h3>
              <div className="pv2-readable-panel" data-testid="sim-runtime-selected-order-fill-errors">
                <div className="pv2-readable-table">
                  <div className="pv2-readable-row"><div className="pv2-readable-key">orders</div><div className="pv2-readable-value">{projectionRows(selectedRun.run, "orders").slice(0, 5).map((order) => `${textValue(order.source)}:${textValue(order.intent_id || order.qmt_order_id || order.handle_id)}`).join(", ") || "-"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">fills</div><div className="pv2-readable-value">{projectionRows(selectedRun.run, "fills").slice(0, 3).map((fill) => `trades ${textValue(fill.trades_seen)} / cash entries ${textValue(fill.cash_entries_appended)}`).join(", ") || "-"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">errors</div><div className="pv2-readable-value">{projectionRows(selectedRun.run, "errors").slice(0, 5).map((err) => `${textValue(err.source)}:${textValue(err.code)}`).join(", ") || "-"}</div></div>
                </div>
              </div>
            </div>
            <div>
              <h3>MiniQMT 对账</h3>
              <div className="pv2-readable-panel">
                <div className="pv2-readable-table">
                  <div className="pv2-readable-row"><div className="pv2-readable-key">同股多策略</div><div className="pv2-readable-value">{arrayValue(itemValue(selectedRun.run.reconciliation_context, "overlap_symbols")).map(String).join(", ") || "-"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">未归因订单 / 成交</div><div className="pv2-readable-value">{textValue(itemValue(selectedRun.run.reconciliation_context, "unattributed_orders"))} / {textValue(itemValue(selectedRun.run.reconciliation_context, "unattributed_trades"))}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">broker symbols</div><div className="pv2-readable-value">{Object.keys(objectValue(itemValue(selectedRun.run.reconciliation_context, "broker_quantities")) || {}).join(", ") || "-"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">issue preview</div><div className="pv2-readable-value">{arrayValue(itemValue(selectedRun.run.reconciliation_context, "issues")).slice(0, 3).map((issue) => textValue(itemValue(objectValue(issue), "issue_type"))).join(", ") || "-"}</div></div>
                </div>
              </div>
            </div>
          </div>
        ) : (
          <NoticePanel title="未选择运行记录" tone="info">点击运行列表中的“查看链路”，可查看当日 selection evidence、execution plan、intent 统计和审计字段。</NoticePanel>
        )}
      </SectionCard>
    </main>
  );
}
