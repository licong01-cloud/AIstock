"use client";

import { useEffect, useMemo, useState } from "react";

const SCHEDULER_BASE = process.env.NEXT_PUBLIC_SCHEDULER_BASE || "http://localhost:5000";
const ENV_KEY_DISPLAY: Record<string, string> = {
  LITELLM_API_KEY: "LiteLLM API Key",
  OPENAI_API_KEY: "OpenAI API Key",
  QLIB_DATA_DIR: "Qlib 数据目录",
  COSTEER_KB_PATH: "CoSTEER KB Path",
};

type Task = {
  name?: string;
  id?: string | number;
  status?: string;
  loop_n?: number;
  all_duration?: string;
  evolving_mode?: string;
};

type Dataset = {
  name?: string;
  provider_uri?: string;
};

type ResultItem = {
  returncode?: number;
  workdir?: string;
  cmd?: string[];
  log_path?: string;
  result_files?: string[];
  summary?: Record<string, unknown>;
};

async function fetchJSON<T>(path: string, options?: RequestInit): Promise<T> {
  const resp = await fetch(`${SCHEDULER_BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    cache: "no-store",
    ...options,
  });
  if (!resp.ok) throw new Error(await resp.text());
  return resp.json();
}

export default function SchedulerPage() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [taskId, setTaskId] = useState("");
  const [log, setLog] = useState("");
  const [results, setResults] = useState<ResultItem[]>([]);
  const [creating, setCreating] = useState(false);
  const [envText, setEnvText] = useState("");
  const [envSaving, setEnvSaving] = useState(false);

  const baseUrlLabel = useMemo(() => SCHEDULER_BASE.replace(/\/$/, ""), []);

  const loadTasks = async () => {
    try {
      const data = await fetchJSON<{ items: Task[] }>("/tasks");
      setTasks(data.items || []);
    } catch (e) {
      console.error(e);
    }
  };

  const loadEnv = async () => {
    try {
      const data = await fetchJSON<{ content: string }>("/config/env");
      setEnvText(data.content || "");
    } catch (e) {
      console.error(e);
    }
  };

  const saveEnv = async () => {
    setEnvSaving(true);
    try {
      await fetchJSON("/config/env", { method: "POST", body: JSON.stringify({ content: envText }) });
    } catch (e) {
      console.error(e);
    } finally {
      setEnvSaving(false);
    }
  };

  const parseEnvKV = useMemo(() => {
    const lines = envText.split("\n");
    const kv: Record<string, string> = {};
    lines.forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#") || !trimmed.includes("=")) return;
      const [k, ...rest] = trimmed.split("=");
      kv[k] = rest.join("=");
    });
    return kv;
  }, [envText]);

  useEffect(() => {
    loadEnv();
  }, []);

  const loadDatasets = async () => {
    try {
      const data = await fetchJSON<{ items: Dataset[] }>("/datasets");
      setDatasets(data.items || []);
    } catch (e) {
      console.error(e);
    }
  };

  const loadLog = async () => {
    if (!taskId) return;
    try {
      const data = await fetchJSON<{ log: string }>(`/tasks/${taskId}/logs`);
      setLog(data.log || "");
    } catch (e) {
      console.error(e);
    }
  };

  const loadResults = async () => {
    if (!taskId) return;
    try {
      const data = await fetchJSON<{ items: ResultItem[] }>(`/tasks/${taskId}/results`);
      setResults(data.items || []);
    } catch (e) {
      console.error(e);
    }
  };

  useEffect(() => {
    loadTasks();
    loadDatasets();
  }, []);

  const handleCreateTask = async (formData: FormData) => {
    setCreating(true);
    try {
      const payload = {
        name: formData.get("name") || "",
        loop_n: Number(formData.get("loop_n") || 1),
        all_duration: formData.get("all_duration") || "1:00:00",
        evolving_mode: formData.get("evolving_mode") || "llm",
      };
      await fetchJSON("/tasks", { method: "POST", body: JSON.stringify(payload) });
      await loadTasks();
    } catch (e) {
      console.error(e);
    } finally {
      setCreating(false);
    }
  };

  const handleCreateDataset = async (formData: FormData) => {
    try {
      const payload = {
        name: formData.get("ds_name") || "",
        provider_uri: formData.get("provider_uri") || "",
      };
      await fetchJSON("/datasets", { method: "POST", body: JSON.stringify(payload) });
      await loadDatasets();
    } catch (e) {
      console.error(e);
    }
  };

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">🗓️ RD-Agent 调度</h1>
          <p className="text-sm text-gray-500">后端：{baseUrlLabel}</p>
        </div>
        <button
          className="btn"
          onClick={() => {
            loadTasks();
            loadDatasets();
            if (taskId) {
              loadLog();
              loadResults();
            }
          }}
        >
          刷新
        </button>
      </div>

      <section className="grid md:grid-cols-2 gap-6">
        <div className="card p-4 space-y-3">
          <h2 className="text-lg font-semibold">新建任务</h2>
          <form
            className="space-y-3"
            onSubmit={(e) => {
              e.preventDefault();
              const fd = new FormData(e.currentTarget);
              handleCreateTask(fd);
            }}
          >
            <div className="space-y-1">
              <label className="block text-sm font-medium">任务名</label>
              <input name="name" className="input" placeholder="task-1" defaultValue="task-1" />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-1">
                <label className="block text-sm font-medium">loop_n</label>
                <input
                  name="loop_n"
                  type="number"
                  min={1}
                  className="input"
                  defaultValue={1}
                  placeholder="loop_n"
                  aria-label="loop_n"
                />
              </div>
              <div className="space-y-1">
                <label className="block text-sm font-medium">all_duration</label>
                <input
                  name="all_duration"
                  className="input"
                  defaultValue="1:00:00"
                  placeholder="all_duration"
                  aria-label="all_duration"
                />
              </div>
            </div>
            <div className="space-y-1">
              <label className="block text-sm font-medium">evolving_mode</label>
              <select name="evolving_mode" className="input" aria-label="evolving_mode">
                <option value="llm">llm</option>
                <option value="fixed">fixed</option>
              </select>
            </div>
            <button type="submit" className="btn primary w-full" disabled={creating}>
              {creating ? "创建中..." : "创建并运行"}
            </button>
          </form>
        </div>

        <div className="card p-4 space-y-3">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold">任务列表</h2>
            <button className="btn" onClick={loadTasks}>
              刷新
            </button>
          </div>
          <div className="max-h-64 overflow-auto border rounded-md">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50">
                  <th className="px-3 py-2 text-left">ID/Name</th>
                  <th className="px-3 py-2 text-left">状态</th>
                  <th className="px-3 py-2 text-left">loop_n</th>
                  <th className="px-3 py-2 text-left">时长</th>
                </tr>
              </thead>
              <tbody>
                {tasks.map((t) => (
                  <tr key={String(t.id || t.name)}>
                    <td className="px-3 py-2">{t.name || t.id}</td>
                    <td className="px-3 py-2">{t.status}</td>
                    <td className="px-3 py-2">{t.loop_n}</td>
                    <td className="px-3 py-2">{t.all_duration}</td>
                  </tr>
                ))}
                {!tasks.length && (
                  <tr>
                    <td className="px-3 py-2 text-gray-400" colSpan={4}>
                      暂无任务
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <section className="card p-4 space-y-3">
        <div className="flex items-center gap-2">
          <input
            className="input flex-1"
            placeholder="输入任务 ID"
            value={taskId}
            onChange={(e) => setTaskId(e.target.value)}
          />
          <button className="btn" onClick={loadLog}>
            查看日志
          </button>
          <button className="btn" onClick={loadResults}>
            查看结果
          </button>
        </div>
        <div className="grid md:grid-cols-2 gap-4">
          <div>
            <h3 className="font-semibold mb-2">日志</h3>
            <pre className="bg-black text-green-200 text-xs p-3 rounded-md h-64 overflow-auto whitespace-pre-wrap">
              {log || "无日志"}
            </pre>
          </div>
          <div>
            <h3 className="font-semibold mb-2">结果</h3>
            <pre className="bg-gray-900 text-gray-100 text-xs p-3 rounded-md h-64 overflow-auto whitespace-pre-wrap">
              {results.length ? JSON.stringify(results, null, 2) : "无结果"}
            </pre>
          </div>
        </div>
      </section>

      <section className="card p-4 space-y-3">
        <div className="flex items-center justify-between">
          <h2 className="text-lg font-semibold">数据集管理</h2>
          <button className="btn" onClick={loadDatasets}>
            刷新
          </button>
        </div>
        <form
          className="grid md:grid-cols-3 gap-3"
          onSubmit={(e) => {
            e.preventDefault();
            const fd = new FormData(e.currentTarget);
            handleCreateDataset(fd);
            e.currentTarget.reset();
          }}
        >
          <label className="flex flex-col text-sm font-medium gap-1">
            数据集名称
            <input name="ds_name" className="input" placeholder="数据集名称" required />
          </label>
          <label className="flex flex-col text-sm font-medium gap-1">
            provider_uri
            <input name="provider_uri" className="input" placeholder="provider_uri" required />
          </label>
          <button className="btn primary" type="submit">
            新建数据集
          </button>
        </form>
        <div className="max-h-48 overflow-auto border rounded-md">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50">
                <th className="px-3 py-2 text-left">名称</th>
                <th className="px-3 py-2 text-left">provider_uri</th>
              </tr>
            </thead>
            <tbody>
              {datasets.map((d) => (
                <tr key={String(d.name)}>
                  <td className="px-3 py-2">{d.name}</td>
                  <td className="px-3 py-2">{d.provider_uri}</td>
                </tr>
              ))}
              {!datasets.length && (
                <tr>
                  <td className="px-3 py-2 text-gray-400" colSpan={2}>
                    暂无数据集
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
