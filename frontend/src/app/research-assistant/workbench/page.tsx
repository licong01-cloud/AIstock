"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import JsonPanel from "@/components/paper-v2/JsonPanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, asObject, display } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantMcpTool, type AssistantTask, type JsonObject } from "@/lib/research-assistant/api";

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
        <SectionCard title="MCP 执行工作台" eyebrow="draft / diff / preflight">
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
          <label className="pv2-field" htmlFor="ra-payload" style={{ marginTop: 12 }}>
            <span>配置草稿 JSON</span>
            <textarea className="pv2-textarea" id="ra-payload" value={payloadText} onChange={(event) => setPayloadText(event.target.value)} />
          </label>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button-primary" type="button" onClick={() => void runPreflight()} disabled={!selectedTool || !parsedPayload || preflighting}>{preflighting ? "preflight 中..." : "执行 preflight"}</button>
            <button className="pv2-button-ghost" type="button" onClick={() => void runDryRunExecute()} disabled={!selectedTool || !parsedPayload || dryRunning}>{dryRunning ? "dry-run 中..." : "执行 dry-run"}</button>
            <button className="pv2-button-ghost" type="button" onClick={() => void load()} disabled={loading}>{loading ? "刷新中..." : "刷新目录"}</button>
            {!parsedPayload ? <span className="pv2-error-meta">JSON 格式错误，禁止提交。</span> : null}
          </div>
          {selectedTool ? <DetailDrawer title="工具 schema / risk / required confirmations" data={selectedTool} /> : <EmptyState title="无 MCP 工具" />}
        </SectionCard>
        <SectionCard title="Preflight 结果" eyebrow="no execution">
          <ApiErrorBox error={preflightError} title="Preflight 失败" />
          {dryRunResult ? <DetailDrawer title="dry-run tool result / deep link" data={dryRunResult} /> : null}
          {preflightSummary ? (
            <>
              <div className="pv2-readable-panel">
                <div className="pv2-readable-table">
                  <div className="pv2-readable-row"><div className="pv2-readable-key">Passed</div><div className="pv2-readable-value"><StatusBadge status={preflightSummary.passed ? "passed" : "blocked"} /></div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">Approval Requirement</div><div className="pv2-readable-value">{preflightSummary.approvalRequired ? "需要审批" : "无需审批"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">Missing Confirmations</div><div className="pv2-readable-value">{preflightSummary.missingConfirmations.length ? preflightSummary.missingConfirmations.map(display).join(" / ") : "-"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">Tool Event</div><div className="pv2-readable-value"><span className="pv2-mono">{display(preflightSummary.toolEvent)}</span></div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">Deep Link</div><div className="pv2-readable-value">{preflightSummary.deepLinks.length ? preflightSummary.deepLinks.map(display).join(" / ") : "后端未返回 deep link"}</div></div>
                </div>
              </div>
              <DetailDrawer title="完整 preflight payload" data={preflight} />
            </>
          ) : !preflightError ? <EmptyState title="尚未执行 preflight" hint="阶段一只执行预检查和审批登记，不直接运行长任务或高风险工具。" /> : null}
        </SectionCard>
      </div>
      <SectionCard title="工具目录" eyebrow="real catalog">
        <PaperTable
          rows={tools}
          empty="暂无 MCP 工具；请先通过后端 catalog seed 写入真实目录。"
          columns={[
            { key: "tool", header: "工具", render: (row) => <><span className="ra-title">{row.title || row.tool_name}</span><br /><span className="pv2-muted pv2-mono">{row.server_key}/{row.tool_name}</span></> },
            { key: "risk", header: "风险", render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "approval", header: "审批", render: (row) => row.requires_approval ? "需要审批" : "无需审批" },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
