"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type PackItem = {
  id: string;
  description?: string | null;
  source?: string | null;
  status?: string | null;
  base_pack_id?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

type ListResponse = {
  page: number;
  page_size: number;
  total: number;
  items: PackItem[];
};

type ActiveResponse = {
  active_pack_id?: string | null;
  updated_at?: string | null;
  updated_by?: string | null;
};

type ValidateResponse = {
  ok: boolean;
  pack_id?: string | null;
  meta_ok: boolean;
  meta_error?: string | null;
  required_files_count: number;
  files_count: number;
  missing_files: string[];
  extra_files: string[];
  yaml_parse_errors: Array<{ rel_path: string; error: string }>;
};

type ImportResponse = {
  pack_id: string;
  status: string;
  checksum?: string;
  files_count?: number;
};

type PublishResponse = {
  pack_id: string;
  from_status: string;
  to_status: string;
  validation_run_id: number;
};

type SetActiveResponse = {
  active_pack_id: string;
  prev_active_pack_id?: string | null;
};

type DiffResponse = {
  from: string;
  to: string;
  summary: {
    added: number;
    removed: number;
    modified: number;
    unchanged: number;
  };
  files: Array<{ rel_path: string; change: string }>;
  diffs: Record<string, string>;
};

async function apiRequest<T>(
  method: string,
  path: string,
  body?: any,
): Promise<T> {
  const url = `${API_BASE.replace(/\/$/, "")}${path}`;
  const res = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    let text = "";
    try {
      text = await res.text();
    } catch {
      text = "";
    }
    let parsed: any = null;
    try {
      parsed = text ? JSON.parse(text) : null;
    } catch {
      parsed = null;
    }
    const be = parsed?.detail?.error;
    if (be?.code || be?.message) {
      const code = be.code ? String(be.code) : "";
      const msg = be.message ? String(be.message) : `HTTP ${res.status}`;
      throw new Error(code ? `${code}: ${msg}` : msg);
    }
    throw new Error(
      `请求失败: HTTP ${res.status} ${res.statusText}${text ? ` | ${text}` : ""}`,
    );
  }
  const text = await res.text();
  if (!text) return {} as T;
  return JSON.parse(text) as T;
}

function formatDateTime(value?: string | null): string {
  if (!value) return "—";
  try {
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return String(value);
    return d
      .toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      })
      .replace(/\//g, "-");
  } catch {
    return String(value);
  }
}

export default function PromptPacksPage() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const [active, setActive] = useState<ActiveResponse | null>(null);
  const [packs, setPacks] = useState<PackItem[]>([]);

  const [importDir, setImportDir] = useState("");
  const [validateResult, setValidateResult] = useState<ValidateResponse | null>(
    null,
  );
  const [importing, setImporting] = useState(false);
  const [overwrite, setOverwrite] = useState(false);

  const [publishMsg, setPublishMsg] = useState("");
  const [setActiveMsg, setSetActiveMsg] = useState("");

  const [diffFrom, setDiffFrom] = useState<string>("active");
  const [diffTo, setDiffTo] = useState<string>("");
  const [diffLoading, setDiffLoading] = useState(false);
  const [diffChangedOnly, setDiffChangedOnly] = useState(true);
  const [diffData, setDiffData] = useState<DiffResponse | null>(null);

  const activePackId = active?.active_pack_id || null;

  const activeLabel = useMemo(() => {
    if (!activePackId) return "当前未设置 Active Pack";
    return `当前 Active Pack: ${activePackId}`;
  }, [activePackId]);

  async function loadActive() {
    const data = await apiRequest<ActiveResponse>("GET", "/prompt-packs/active");
    setActive(data);
  }

  async function loadPacks() {
    const data = await apiRequest<ListResponse>("GET", "/prompt-packs?page=1&page_size=50");
    setPacks(data.items || []);
  }

  async function refreshAll() {
    setLoading(true);
    setError(null);
    setNotice(null);
    try {
      await Promise.all([loadActive(), loadPacks()]);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refreshAll();
  }, []);

  useEffect(() => {
    if (!diffTo && packs.length) {
      setDiffTo(packs[0].id);
    }
  }, [packs, diffTo]);

  async function handleValidateImport() {
    setError(null);
    setNotice(null);
    setValidateResult(null);
    try {
      const dir = importDir.trim();
      if (!dir) throw new Error("请输入导入目录路径");
      const res = await apiRequest<ValidateResponse>(
        "POST",
        "/prompt-packs/validate-import-dir",
        { dir },
      );
      setValidateResult(res);
      setNotice(res.ok ? "预校验通过" : "预校验未通过（请查看缺失/解析错误）");
    } catch (e: any) {
      setError(e?.message || "预校验失败");
    }
  }

  async function handleImport() {
    setError(null);
    setNotice(null);
    setImporting(true);
    try {
      const dir = importDir.trim();
      if (!dir) throw new Error("请输入导入目录路径");
      const res = await apiRequest<ImportResponse>(
        "POST",
        "/prompt-packs/import-from-dir",
        { dir, overwrite },
      );
      await refreshAll();
      setValidateResult(null);
      setDiffData(null);
      setPublishMsg("");
      setSetActiveMsg("");
      setNotice(`导入成功: ${res.pack_id}`);
    } catch (e: any) {
      setError(e?.message || "导入失败");
    } finally {
      setImporting(false);
    }
  }

  async function handlePublish(packId: string) {
    setError(null);
    setNotice(null);
    try {
      const ok =
        typeof window === "undefined" ||
        window.confirm(
          `确认发布 pack ${packId} 吗？发布将执行校验（allowlist + YAML parse）。`,
        );
      if (!ok) return;
      const msg = (publishMsg || "publish").trim();
      const res = await apiRequest<PublishResponse>(
        "POST",
        `/prompt-packs/${encodeURIComponent(packId)}/publish`,
        { message: msg },
      );
      await refreshAll();
      setNotice(`发布成功: ${res.pack_id}`);
    } catch (e: any) {
      setError(e?.message || "发布失败");
    }
  }

  async function handleSetActive(packId: string) {
    setError(null);
    setNotice(null);
    try {
      const ok =
        typeof window === "undefined" ||
        window.confirm(`确认将 pack ${packId} 设置为 Active 吗？`);
      if (!ok) return;
      const msg = (setActiveMsg || "set active").trim();
      const res = await apiRequest<SetActiveResponse>(
        "POST",
        `/prompt-packs/${encodeURIComponent(packId)}/set-active`,
        { message: msg },
      );
      await refreshAll();
      setNotice(`已生效: ${res.active_pack_id}`);
    } catch (e: any) {
      setError(e?.message || "设置生效失败");
    }
  }

  async function handleDiffVsActive(packId: string) {
    setError(null);
    setNotice(null);
    setDiffData(null);
    try {
      if (!activePackId) throw new Error("当前没有 Active Pack，无法对比");
      setDiffFrom("active");
      setDiffTo(packId);
      await handleRunDiff("active", packId);
    } catch (e: any) {
      setError(e?.message || "对比失败");
    }
  }

  async function handleRunDiff(from: string, to: string) {
    setDiffLoading(true);
    setError(null);
    setNotice(null);
    setDiffData(null);
    try {
      const resolvedFrom = from === "active" ? activePackId : from;
      const resolvedTo = to === "active" ? activePackId : to;
      if (!resolvedFrom || !resolvedTo) {
        throw new Error("请选择有效的对比对象（Active 为空或 pack_id 为空）");
      }
      const res = await apiRequest<DiffResponse>(
        "GET",
        `/prompt-packs/diff?from=${encodeURIComponent(resolvedFrom)}&to=${encodeURIComponent(resolvedTo)}`,
      );
      setDiffData(res);
      setNotice(`已生成 diff：${res.from} -> ${res.to}`);
    } catch (e: any) {
      setError(e?.message || "对比失败");
    } finally {
      setDiffLoading(false);
    }
  }

  return (
    <div className="page">
      <h2>RD-agent提示词模板管理</h2>

      <div style={{ marginTop: 8, marginBottom: 8 }}>
        <button onClick={refreshAll} disabled={loading}>
          刷新
        </button>
      </div>

      <div style={{ marginBottom: 8, color: "#555" }}>{activeLabel}</div>
      <div style={{ marginBottom: 12, color: "#777" }}>
        Active 更新时间: {formatDateTime(active?.updated_at)}
      </div>

      {notice ? (
        <div style={{ marginBottom: 12, color: "#0b7" }}>{notice}</div>
      ) : null}

      {error ? (
        <div style={{ marginBottom: 12, color: "#b00020" }}>{error}</div>
      ) : null}

      <section style={{ marginBottom: 24 }}>
        <h3>从目录导入（Phase 0）</h3>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <input
            style={{ minWidth: 520 }}
            value={importDir}
            onChange={(e) => setImportDir(e.target.value)}
            placeholder="输入 pack 目录（包含 meta.yaml 与 files/）"
          />
          <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <input
              type="checkbox"
              checked={overwrite}
              onChange={(e) => setOverwrite(e.target.checked)}
            />
            overwrite
          </label>
          <button onClick={handleValidateImport}>预校验</button>
          <button onClick={handleImport} disabled={importing}>
            确认导入
          </button>
        </div>

        {validateResult ? (
          <div style={{ marginTop: 12, padding: 12, border: "1px solid #ddd" }}>
            <div>ok: {String(validateResult.ok)}</div>
            <div>pack_id: {validateResult.pack_id || "—"}</div>
            <div>meta_ok: {String(validateResult.meta_ok)}</div>
            {validateResult.meta_error ? (
              <div style={{ color: "#b00020" }}>meta_error: {validateResult.meta_error}</div>
            ) : null}
            <div>files: {validateResult.files_count} / required {validateResult.required_files_count}</div>
            <div>missing_files: {validateResult.missing_files.length}</div>
            <div>extra_files: {validateResult.extra_files.length}</div>
            <div>yaml_parse_errors: {validateResult.yaml_parse_errors.length}</div>
            {(validateResult as any).meta ? (
              <div style={{ marginTop: 8, color: "#555" }}>
                meta: {JSON.stringify((validateResult as any).meta)}
              </div>
            ) : null}
          </div>
        ) : null}
      </section>

      <section style={{ marginBottom: 24 }}>
        <h3>操作参数</h3>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <input
            style={{ minWidth: 320 }}
            value={publishMsg}
            onChange={(e) => setPublishMsg(e.target.value)}
            placeholder="发布说明（publish message）"
          />
          <input
            style={{ minWidth: 320 }}
            value={setActiveMsg}
            onChange={(e) => setSetActiveMsg(e.target.value)}
            placeholder="应用说明（set-active message）"
          />
        </div>
      </section>

      <section style={{ marginBottom: 24 }}>
        <h3>Packs</h3>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              <th style={{ textAlign: "left", borderBottom: "1px solid #ddd" }}>ID</th>
              <th style={{ textAlign: "left", borderBottom: "1px solid #ddd" }}>状态</th>
              <th style={{ textAlign: "left", borderBottom: "1px solid #ddd" }}>更新</th>
              <th style={{ textAlign: "left", borderBottom: "1px solid #ddd" }}>操作</th>
            </tr>
          </thead>
          <tbody>
            {packs.map((p) => {
              const isActive = p.id === activePackId;
              return (
                <tr key={p.id}>
                  <td style={{ padding: "6px 4px", verticalAlign: "top" }}>
                    <div style={{ fontWeight: 600 }}>
                      <Link href={`/rd-agent/prompt-packs/${encodeURIComponent(p.id)}`}>
                        {p.id}
                      </Link>
                    </div>
                    {p.description ? (
                      <div style={{ color: "#666", marginTop: 4 }}>{p.description}</div>
                    ) : null}
                    {isActive ? (
                      <div style={{ color: "#0b7", marginTop: 4 }}>（Active）</div>
                    ) : null}
                  </td>
                  <td style={{ padding: "6px 4px", verticalAlign: "top" }}>{p.status || "—"}</td>
                  <td style={{ padding: "6px 4px", verticalAlign: "top" }}>{formatDateTime(p.updated_at)}</td>
                  <td style={{ padding: "6px 4px", verticalAlign: "top" }}>
                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                      <button onClick={() => handleDiffVsActive(p.id)} disabled={!activePackId || isActive}>
                        Diff vs Active
                      </button>
                      <button onClick={() => handlePublish(p.id)} disabled={p.status === "published"}>
                        Publish
                      </button>
                      <button onClick={() => handleSetActive(p.id)} disabled={p.status !== "published"}>
                        Set Active
                      </button>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </section>

      <section style={{ marginBottom: 24 }}>
        <h3>Diff</h3>

        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
          <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
            from
            <select value={diffFrom} onChange={(e) => setDiffFrom(e.target.value)}>
              <option value="active">active</option>
              {packs.map((p) => (
                <option key={`from-${p.id}`} value={p.id}>
                  {p.id}
                </option>
              ))}
            </select>
          </label>
          <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
            to
            <select value={diffTo} onChange={(e) => setDiffTo(e.target.value)}>
              <option value="">请选择</option>
              <option value="active">active</option>
              {packs.map((p) => (
                <option key={`to-${p.id}`} value={p.id}>
                  {p.id}
                </option>
              ))}
            </select>
          </label>
          <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <input
              type="checkbox"
              checked={diffChangedOnly}
              onChange={(e) => setDiffChangedOnly(e.target.checked)}
            />
            changed-only
          </label>
          <button
            onClick={() => handleRunDiff(diffFrom, diffTo)}
            disabled={diffLoading || !diffTo}
          >
            生成 Diff
          </button>
        </div>

        {diffData ? (
          <div style={{ marginTop: 12 }}>
            <div style={{ color: "#555", marginBottom: 8 }}>
              from={diffData.from}, to={diffData.to}, added={diffData.summary.added}, removed={diffData.summary.removed}, modified={diffData.summary.modified}, unchanged={diffData.summary.unchanged}
            </div>

            {diffData.files
              .filter((f) => (diffChangedOnly ? f.change !== "unchanged" : true))
              .map((f) => {
                const d = diffData.diffs[f.rel_path] || "";
                return (
                  <details
                    key={f.rel_path}
                    open={f.change !== "unchanged"}
                    style={{ border: "1px solid #ddd", padding: 8, marginBottom: 8 }}
                  >
                    <summary style={{ cursor: "pointer" }}>
                      {f.rel_path} ({f.change})
                    </summary>
                    <pre style={{ marginTop: 8, whiteSpace: "pre-wrap" }}>{d || "(no diff)"}</pre>
                  </details>
                );
              })}
          </div>
        ) : (
          <div style={{ marginTop: 12, color: "#777" }}>
            请选择对比对象并点击“生成 Diff”。
          </div>
        )}
      </section>

      <div style={{ color: "#666" }}>
        后端地址：{API_BASE}
      </div>
    </div>
  );
}
