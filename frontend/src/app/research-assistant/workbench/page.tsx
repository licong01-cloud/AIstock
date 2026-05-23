"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, asObject, display } from "@/components/research-assistant/AssistantShared";
import {
  LOCAL_DATA_MANAGEMENT_CAPABILITY,
  LOCAL_DATA_MANAGEMENT_PHASES,
  isLocalDataManagementTool,
  localDataRiskLabel,
  localDataToolPhase,
  localDataToolTitle,
  researchAssistantApi,
  type AssistantMcpTool,
  type AssistantTask,
  type JsonObject,
  type LocalDataPhaseKey,
} from "@/lib/research-assistant/api";

function summarizePreflight(preflight: unknown) {
  const data = asObject(preflight);
  const traceEvent = asObject(data.trace_event);
  const deepLinks = Array.isArray(data.deep_links) ? data.deep_links : Array.isArray(data.deep_link_refs) ? data.deep_link_refs : [];
  return {
    passed: Boolean(data.passed),
    approvalRequired: Boolean(data.approval_required ?? data.requires_approval),
    missingConfirmations: Array.isArray(data.missing_confirmations) ? data.missing_confirmations : [],
    toolEvent: traceEvent.event_type || data.event_type || data.status || "preflight_result",
    deepLinks,
  };
}

function payloadRows(payload: JsonObject | null): Array<[string, string]> {
  if (!payload) return [["参数状态", "JSON 格式错误，禁止提交"]];
  const entries = Object.entries(payload).slice(0, 6);
  if (!entries.length) return [["参数状态", "空参数，将按工具默认值预检"]];
  return entries.map(([key, value]) => {
    if (Array.isArray(value)) return [key, value.length ? `${value.length} 项` : "空列表"];
    if (value && typeof value === "object") return [key, "已配置对象，审计详情可展开查看"];
    return [key, display(value)];
  });
}

function workbenchPhaseStatus(
  key: LocalDataPhaseKey,
  options: {
    selectedTool: AssistantMcpTool | null;
    parsedPayload: JsonObject | null;
    preflightSummary: ReturnType<typeof summarizePreflight> | null;
    preflighting: boolean;
    dryRunning: boolean;
    dryRunResult: unknown;
  },
): string {
  if (key === "check") {
    if (options.preflighting) return "current";
    return options.preflightSummary ? "done" : "idle";
  }
  if (key === "plan") return options.selectedTool && options.parsedPayload ? "done" : "idle";
  if (key === "confirm") {
    if (!options.selectedTool) return "locked";
    if (options.preflightSummary?.approvalRequired || options.selectedTool.requires_approval) return "current";
    return options.preflightSummary ? "done" : "idle";
  }
  if (key === "execute") {
    if (options.dryRunning) return "current";
    return options.dryRunResult ? "done" : "locked";
  }
  if (key === "review") return options.dryRunResult ? "current" : "locked";
  return "idle";
}

export default function ResearchAssistantWorkbenchPage() {
  const [tools, setTools] = useState<AssistantMcpTool[]>([]);
  const [tasks, setTasks] = useState<AssistantTask[]>([]);
  const [selectedTool, setSelectedTool] = useState<AssistantMcpTool | null>(null);
  const [selectedTaskId, setSelectedTaskId] = useState("");
  const [payloadText, setPayloadText] = useState("{\n  \"title\": \"候选 Issue\",\n  \"problem_statement\": \"示例：业务流程不符合设计\"\n}");
  const [preflight, setPreflight] = useState<unknown>(null);
  const [dryRunResult, setDryRunResult] = useState<unknown>(null);
  const [error, setError] = useState<unknown>(null);
  const [preflightError, setPreflightError] = useState<unknown>(null);
  const [dryRunError, setDryRunError] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);
  const [preflighting, setPreflighting] = useState(false);
  const [dryRunning, setDryRunning] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [toolPage, taskPage] = await Promise.all([researchAssistantApi.mcpTools({ limit: 200 }), researchAssistantApi.tasks({ limit: 100 })]);
      setTools(toolPage.items);
      setTasks(taskPage.items);
      setSelectedTool((current) => current || toolPage.items[0] || null);
      setSelectedTaskId((current) => current || taskPage.items[0]?.task_id || "");
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const parsedPayload = useMemo<JsonObject | null>(() => {
    try {
      const parsed = JSON.parse(payloadText) as unknown;
      return asObject(parsed);
    } catch {
      return null;
    }
  }, [payloadText]);

  const preflightSummary = preflight ? summarizePreflight(preflight) : null;
  const localDataTools = useMemo(() => tools.filter(isLocalDataManagementTool), [tools]);
  const selectedToolPhase = selectedTool ? localDataToolPhase(selectedTool.tool_name) : undefined;
  const selectedIsLocalData = selectedTool ? isLocalDataManagementTool(selectedTool) : false;
  const payloadSummaryRows = useMemo(() => payloadRows(parsedPayload), [parsedPayload]);
  const phaseRows = LOCAL_DATA_MANAGEMENT_PHASES.map((phase) => ({
    ...phase,
    status: workbenchPhaseStatus(phase.key, { selectedTool, parsedPayload, preflightSummary, preflighting, dryRunning, dryRunResult }),
  }));

  async function runPreflight() {
    if (!selectedTool || !parsedPayload) return;
    setPreflighting(true);
    setPreflightError(null);
    setPreflight(null);
    try {
      const result = await researchAssistantApi.preflightMcpTool({
        task_id: selectedTaskId || undefined,
        server_key: selectedTool.server_key,
        tool_name: selectedTool.tool_name,
        payload_json: parsedPayload,
        idempotency_key: `${selectedTool.tool_name}-ui-dry-run`,
      });
      setPreflight(result);
    } catch (exc) {
      setPreflightError(exc);
    } finally {
      setPreflighting(false);
    }
  }

  async function runDryRunExecute() {
    if (!selectedTool || !parsedPayload) return;
    setDryRunning(true);
    setDryRunError(null);
    setDryRunResult(null);
    try {
      const result = await researchAssistantApi.dryRunExecuteTool({
        task_id: selectedTaskId || undefined,
        server_key: selectedTool.server_key,
        tool_name: selectedTool.tool_name,
        payload_json: parsedPayload,
        idempotency_key: `${selectedTool.tool_name}-ui-dry-run-execute`,
        deep_link: `/research-assistant/workbench?server_key=${selectedTool.server_key}&tool=${selectedTool.tool_name}`,
      });
      setDryRunResult(result);
      setPreflight(result.preflight || result);
    } catch (exc) {
      setDryRunError(exc);
    } finally {
      setDryRunning(false);
    }
  }

  return (
    <main>
      <ApiErrorBox error={error} />
      <div className="ra-two-column">
        <SectionCard title="本地数据 MCP 工作台" eyebrow="local_data_management / check-plan-confirm">
          <div className="pv2-readable-panel" data-testid="ra-local-data-workbench-card">
            <div className="pv2-readable-table">
              <div className="pv2-readable-row"><div className="pv2-readable-key">能力</div><div className="pv2-readable-value">{LOCAL_DATA_MANAGEMENT_CAPABILITY.displayName}（{LOCAL_DATA_MANAGEMENT_CAPABILITY.capabilityKey}）</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">Gateway module</div><div className="pv2-readable-value">{LOCAL_DATA_MANAGEMENT_CAPABILITY.gatewayModule}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">执行原则</div><div className="pv2-readable-value">只读检查和计划可先做；写控制面、启动同步任务或刷新缓存必须先确认。</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">当前目录</div><div className="pv2-readable-value">{localDataTools.length ? `已读取 ${localDataTools.length} 个本地数据工具` : "尚未读取到 local_data 工具，不使用静态假工具冒充可执行能力"}</div></div>
            </div>
          </div>
          <div className="pv2-readable-list" style={{ marginTop: 12 }}>
            {phaseRows.map((phase) => (
              <div className="pv2-readable-item" key={phase.key}>
                <strong>{phase.title}</strong> <StatusBadge status={phase.status === "idle" ? "pending" : phase.status} />
                <p className="pv2-muted">{phase.description}</p>
                <span className="pv2-chip">{localDataRiskLabel(phase.riskLevel)}</span>
                {phase.requiresConfirmation ? <span className="pv2-chip">需要确认</span> : <span className="pv2-chip">无需确认</span>}
              </div>
            ))}
          </div>
        </SectionCard>
        <SectionCard title="选择工具与人类可读计划" eyebrow="tool / plan / audit details">
          <label className="pv2-field" htmlFor="ra-tool-select">
            <span>选择 MCP 工具</span>
            <select className="pv2-select" id="ra-tool-select" value={selectedTool?.tool_id || ""} onChange={(event) => setSelectedTool(tools.find((tool) => tool.tool_id === event.target.value) || null)}>
              {tools.map((tool) => <option key={tool.tool_id} value={tool.tool_id}>{tool.server_key} / {tool.tool_name}</option>)}
            </select>
          </label>
          <label className="pv2-field" htmlFor="ra-task-select" style={{ marginTop: 12 }}>
            <span>关联任务</span>
            <select className="pv2-select" id="ra-task-select" value={selectedTaskId} onChange={(event) => setSelectedTaskId(event.target.value)}>
              <option value="">不关联任务</option>
              {tasks.map((task) => <option key={task.task_id} value={task.task_id}>{task.title}</option>)}
            </select>
          </label>
          <div className="pv2-readable-panel" style={{ marginTop: 12 }} data-testid="ra-local-data-tool-summary">
            <div className="pv2-readable-table">
              <div className="pv2-readable-row"><div className="pv2-readable-key">工具</div><div className="pv2-readable-value">{selectedTool ? localDataToolTitle(selectedTool) : "未选择工具"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">所属能力</div><div className="pv2-readable-value">{selectedIsLocalData ? "local_data_management" : "非本地数据工具，请切换到 local_data 工具后再操作"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">阶段</div><div className="pv2-readable-value">{selectedToolPhase?.title || "未匹配到固定阶段"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">风险</div><div className="pv2-readable-value">{localDataRiskLabel(selectedTool?.risk_level || selectedToolPhase?.riskLevel)}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">确认</div><div className="pv2-readable-value">{selectedTool?.requires_approval || selectedToolPhase?.requiresConfirmation ? "需要确认后才能执行" : "只读或计划步骤无需确认"}</div></div>
            </div>
          </div>
          <div className="pv2-readable-panel" style={{ marginTop: 12 }} data-testid="ra-local-data-payload-summary">
            <div className="pv2-readable-table">
              {payloadSummaryRows.map(([key, value]) => (
                <div className="pv2-readable-row" key={key}><div className="pv2-readable-key">{key}</div><div className="pv2-readable-value">{value}</div></div>
              ))}
            </div>
          </div>
          <details className="ra-detail-drawer">
            <summary>展开审计参数草稿（JSON）</summary>
            <label className="pv2-field" htmlFor="ra-payload" style={{ marginTop: 12 }}>
              <span>仅用于审计和精确调试的参数草稿</span>
              <textarea className="pv2-textarea" id="ra-payload" value={payloadText} onChange={(event) => setPayloadText(event.target.value)} />
            </label>
          </details>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button-primary" type="button" onClick={() => void runPreflight()} disabled={!selectedTool || !parsedPayload || preflighting}>{preflighting ? "preflight 中..." : "执行 preflight"}</button>
            <button className="pv2-button-ghost" type="button" onClick={() => void runDryRunExecute()} disabled={!selectedTool || !parsedPayload || dryRunning}>{dryRunning ? "dry-run 中..." : "执行 dry-run"}</button>
            <button className="pv2-button-ghost" type="button" onClick={() => void load()} disabled={loading}>{loading ? "刷新中..." : "刷新目录"}</button>
            {!parsedPayload ? <span className="pv2-error-meta">JSON 格式错误，禁止提交。</span> : null}
          </div>
          {selectedTool ? <DetailDrawer title="审计详情：工具 schema / risk / required confirmations" data={selectedTool} /> : <EmptyState title="无 MCP 工具" />}
        </SectionCard>
        <SectionCard title="本地数据预检与复查结果" eyebrow="check / review / no hidden execution">
          <ApiErrorBox error={preflightError} title="Preflight 失败" />
          {dryRunResult ? <DetailDrawer title="审计详情：dry-run 返回与 deep link" data={dryRunResult} /> : null}
          {preflightSummary ? (
            <>
              <div className="pv2-readable-panel">
                <div className="pv2-readable-table">
                  <div className="pv2-readable-row"><div className="pv2-readable-key">预检结论</div><div className="pv2-readable-value"><StatusBadge status={preflightSummary.passed ? "passed" : "blocked"} /></div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">确认要求</div><div className="pv2-readable-value">{preflightSummary.approvalRequired ? "需要审批或确认口令" : "无需审批"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">缺少确认</div><div className="pv2-readable-value">{preflightSummary.missingConfirmations.length ? preflightSummary.missingConfirmations.map(display).join(" / ") : "-"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">事件</div><div className="pv2-readable-value"><span className="pv2-mono">{display(preflightSummary.toolEvent)}</span></div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">复查入口</div><div className="pv2-readable-value">{preflightSummary.deepLinks.length ? preflightSummary.deepLinks.map(display).join(" / ") : "后端未返回 deep link"}</div></div>
                </div>
              </div>
              <DetailDrawer title="审计详情：完整 preflight payload" data={preflight} />
            </>
          ) : !preflightError ? <EmptyState title="尚未执行 preflight" hint="阶段一只执行预检查和审批登记，不直接运行长任务或高风险工具。" /> : null}
        </SectionCard>
      </div>
      <SectionCard title="本地数据工具目录" eyebrow="real catalog / readable cards">
        {localDataTools.length ? (
          <div className="pv2-readable-list" data-testid="ra-local-data-tool-cards">
            {localDataTools.map((tool) => {
              const phase = localDataToolPhase(tool.tool_name);
              return (
                <div className="pv2-readable-item" key={tool.tool_id}>
                  <strong>{localDataToolTitle(tool)}</strong>
                  <p className="pv2-muted">{phase?.description || tool.description || "本地数据管理工具，具体入参和 trace 保留在审计详情中。"}</p>
                  <span className="pv2-chip">{phase?.title || "未分配阶段"}</span>
                  <span className="pv2-chip">{localDataRiskLabel(tool.risk_level || phase?.riskLevel)}</span>
                  <span className="pv2-chip">{tool.requires_approval || phase?.requiresConfirmation ? "需要确认" : "无需确认"}</span>
                </div>
              );
            })}
          </div>
        ) : (
          <EmptyState title="尚未读取到 local_data 工具" hint="请等待后端 Capability Registry / MCP Catalog 写入真实目录；页面不会用静态假工具冒充可执行能力。" />
        )}
        <PaperTable
          rows={tools}
          empty="暂无 MCP 工具；请先通过后端 catalog seed 写入真实目录。"
          columns={[
            { key: "tool", header: "工具", render: (row) => <><span className="ra-title">{isLocalDataManagementTool(row) ? localDataToolTitle(row) : row.title || row.tool_name}</span><br /><span className="pv2-muted pv2-mono">{row.server_key}/{row.tool_name}</span></> },
            { key: "phase", header: "阶段", render: (row) => localDataToolPhase(row.tool_name)?.title || "-" },
            { key: "risk", header: "风险", render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "approval", header: "审批", render: (row) => row.requires_approval ? "需要审批" : "无需审批" },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
