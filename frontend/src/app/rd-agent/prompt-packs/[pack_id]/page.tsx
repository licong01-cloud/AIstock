"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type PackDetail = {
  id: string;
  description?: string | null;
  usage_scene?: string | null;
  requirements?: string | null;
  limitations?: string | null;
  tags?: any;
  source?: string | null;
  status?: string | null;
  created_by?: string | null;
  base_pack_id?: string | null;
  checksum?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

type ActiveResponse = {
  active_pack_id?: string | null;
  updated_at?: string | null;
  updated_by?: string | null;
};

async function apiRequest<T>(method: string, path: string): Promise<T> {
  const url = `${API_BASE.replace(/\/$/, "")}${path}`;
  const res = await fetch(url, { method, cache: "no-store" });
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

export default function PromptPackDetailPage({
  params,
}: {
  params: { pack_id: string };
}) {
  const packId = params.pack_id;

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pack, setPack] = useState<PackDetail | null>(null);
  const [active, setActive] = useState<ActiveResponse | null>(null);

  const isActive = useMemo(() => {
    return !!active?.active_pack_id && active.active_pack_id === packId;
  }, [active?.active_pack_id, packId]);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const [p, a] = await Promise.all([
        apiRequest<PackDetail>("GET", `/prompt-packs/${encodeURIComponent(packId)}`),
        apiRequest<ActiveResponse>("GET", "/prompt-packs/active"),
      ]);
      setPack(p);
      setActive(a);
    } catch (e: any) {
      setError(e?.message || "加载失败");
      setPack(null);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, [packId]);

  return (
    <div className="page">
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        <h2 style={{ margin: 0 }}>Pack 详情</h2>
        <Link href="/rd-agent/prompt-packs">返回列表</Link>
        <button onClick={load} disabled={loading}>
          刷新
        </button>
      </div>

      <div style={{ marginTop: 8, color: "#555" }}>
        pack_id: <span style={{ fontWeight: 600 }}>{packId}</span>
        {isActive ? <span style={{ marginLeft: 8, color: "#0b7" }}>（Active）</span> : null}
      </div>

      {error ? (
        <div style={{ marginTop: 12, color: "#b00020" }}>{error}</div>
      ) : null}

      {!pack ? (
        <div style={{ marginTop: 12, color: "#777" }}>暂无数据</div>
      ) : (
        <div style={{ marginTop: 12 }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <tbody>
              <tr>
                <td style={{ width: 180, padding: "6px 4px", color: "#555" }}>status</td>
                <td style={{ padding: "6px 4px" }}>{pack.status || "—"}</td>
              </tr>
              <tr>
                <td style={{ padding: "6px 4px", color: "#555" }}>source</td>
                <td style={{ padding: "6px 4px" }}>{pack.source || "—"}</td>
              </tr>
              <tr>
                <td style={{ padding: "6px 4px", color: "#555" }}>description</td>
                <td style={{ padding: "6px 4px" }}>{pack.description || "—"}</td>
              </tr>
              <tr>
                <td style={{ padding: "6px 4px", color: "#555" }}>usage_scene</td>
                <td style={{ padding: "6px 4px", whiteSpace: "pre-wrap" }}>{pack.usage_scene || "—"}</td>
              </tr>
              <tr>
                <td style={{ padding: "6px 4px", color: "#555" }}>requirements</td>
                <td style={{ padding: "6px 4px", whiteSpace: "pre-wrap" }}>{pack.requirements || "—"}</td>
              </tr>
              <tr>
                <td style={{ padding: "6px 4px", color: "#555" }}>limitations</td>
                <td style={{ padding: "6px 4px", whiteSpace: "pre-wrap" }}>{pack.limitations || "—"}</td>
              </tr>
              <tr>
                <td style={{ padding: "6px 4px", color: "#555" }}>tags</td>
                <td style={{ padding: "6px 4px" }}>
                  <pre style={{ margin: 0, whiteSpace: "pre-wrap" }}>{JSON.stringify(pack.tags, null, 2)}</pre>
                </td>
              </tr>
              <tr>
                <td style={{ padding: "6px 4px", color: "#555" }}>base_pack_id</td>
                <td style={{ padding: "6px 4px" }}>{pack.base_pack_id || "—"}</td>
              </tr>
              <tr>
                <td style={{ padding: "6px 4px", color: "#555" }}>checksum</td>
                <td style={{ padding: "6px 4px", fontFamily: "monospace" }}>{pack.checksum || "—"}</td>
              </tr>
              <tr>
                <td style={{ padding: "6px 4px", color: "#555" }}>created_at</td>
                <td style={{ padding: "6px 4px" }}>{formatDateTime(pack.created_at)}</td>
              </tr>
              <tr>
                <td style={{ padding: "6px 4px", color: "#555" }}>updated_at</td>
                <td style={{ padding: "6px 4px" }}>{formatDateTime(pack.updated_at)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      )}

      <div style={{ marginTop: 16, color: "#777" }}>后端地址：{API_BASE}</div>
    </div>
  );
}
