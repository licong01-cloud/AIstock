"use client";

import { useState } from "react";

import JsonPanel from "@/components/paper-v2/JsonPanel";
import SectionCard from "@/components/paper-v2/SectionCard";
import { DetailDrawer } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantTask } from "@/lib/research-assistant/api";

export default function ResearchAssistantChatPage() {
  const [prompt, setPrompt] = useState("请帮我规划一个 QE 10 loop 实验，先生成计划和审批草稿，不执行。");
  const [task, setTask] = useState<AssistantTask | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function createPlanTask() {
    setLoading(true);
    setError(null);
    try {
      const created = await researchAssistantApi.createTask({
        title: prompt.slice(0, 80) || "研究助理对话计划",
        task_type: "chat_plan",
        risk_level: "medium",
        input_json: { prompt, phase1_boundary: "plan_only_no_execution" },
        created_by: "user",
      });
      await researchAssistantApi.addTaskEvent(created.task_id, {
        event_type: "planned",
        message: "已从对话生成计划草稿；阶段一不会直接执行高风险操作。",
        payload_json: { prompt },
      });
      setTask(created);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setLoading(false);
    }
  }

  return (
    <main>
      <div className="ra-two-column">
        <SectionCard title="主对话入口" eyebrow="plan first / approval later">
          <label className="pv2-field" htmlFor="ra-chat-input">
            <span>需求或研究命令</span>
            <textarea className="pv2-textarea" id="ra-chat-input" value={prompt} onChange={(event) => setPrompt(event.target.value)} />
          </label>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button-primary" type="button" onClick={() => void createPlanTask()} disabled={loading}>{loading ? "生成中..." : "生成计划任务"}</button>
          </div>
          {error ? <p className="pv2-error-meta">{error}</p> : null}
          {task ? <DetailDrawer title="已创建的 Task Ledger 记录" data={task} /> : null}
        </SectionCard>
        <SectionCard title="阶段一交互边界" eyebrow="no silent action">
          <div className="pv2-readable-panel">
            <div className="pv2-readable-table">
              <div className="pv2-readable-row"><div className="pv2-readable-key">计划</div><div className="pv2-readable-value">可以从对话生成 Task Ledger 和事件。</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">执行</div><div className="pv2-readable-value">高风险 MCP 执行必须去工作台 preflight，再进入审批中心。</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">记忆</div><div className="pv2-readable-value">低价模型和临时反馈先写 Temp Memory，主模型审核后才能升格长期记忆。</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">Issue</div><div className="pv2-readable-value">只能生成候选 Issue；正式 GitHub 同步需人工确认。</div></div>
            </div>
          </div>
          <JsonPanel value={{ supported_now: ["task ledger", "event stream", "context pack", "approval queue"], not_in_phase1: ["voice", "multi-window chat", "auto long-running experiment"] }} />
        </SectionCard>
      </div>
    </main>
  );
}
