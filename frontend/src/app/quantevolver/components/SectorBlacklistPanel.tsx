"use client";
import { useEffect, useRef, useState } from "react";

// 与后端 sw2-tree 返回字段一致
interface Sw2Item {
  l1_code: string;
  l1_name: string;
  l2_code: string;
  l2_name: string;
}

interface Sw1Group {
  l1_code: string;
  l1_name: string;
  children: { l2_code: string; l2_name: string }[];
}

interface BlacklistEntry {
  sw2_code: string;
  sw2_name: string;
  sw1_code: string;
  sw1_name: string;
  effective_from: string;
  effective_to: string;
  reason: string;
}

interface Props {
  enabled: boolean;
  onEnabledChange: (v: boolean) => void;
  onPoolPathChange: (wslPath: string | null) => void;
  onBlacklistSnapshotChange?: (snapshot: any | null) => void;
}

export default function SectorBlacklistPanel({ enabled, onEnabledChange, onPoolPathChange, onBlacklistSnapshotChange }: Props) {
  const [tree, setTree] = useState<Sw1Group[]>([]);
  const [blacklist, setBlacklist] = useState<BlacklistEntry[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [generating, setGenerating] = useState(false);
  const [blacklistLoaded, setBlacklistLoaded] = useState(false);
  const [genMsg, setGenMsg] = useState("");
  const [error, setError] = useState("");
  const generationInFlightRef = useRef(false);
  const pendingGenerateRef = useRef(false);
  const generationSeqRef = useRef(0);

  const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

  useEffect(() => {
    // 加载行业树
    fetch(`${API}/quantevolver/stock-pool/sw2-tree`)
      .then(r => {
        if (!r.ok) throw new Error(`sw2-tree HTTP ${r.status}`);
        return r.json();
      })
      .then(d => {
        if (!d.ok) throw new Error(`sw2-tree 返回错误: ${JSON.stringify(d)}`);
        setTree(d.tree || []);
      })
      .catch(e => setError(`行业树加载失败: ${e.message}`));

    // 加载已有黑名单
    fetch(`${API}/quantevolver/stock-pool/blacklist`)
      .then(r => {
        if (!r.ok) throw new Error(`blacklist HTTP ${r.status}`);
        return r.json();
      })
      .then(d => {
        if (!d.ok) throw new Error(`blacklist 返回错误: ${JSON.stringify(d)}`);
        setBlacklist(d.items || []);
      })
      .catch(e => setError(`黑名单加载失败: ${e.message}`))
      .finally(() => setBlacklistLoaded(true));
  }, []);

  // Wait for the initial blacklist load and serialize pool generation requests.
  useEffect(() => {
    if (!enabled) {
      generationSeqRef.current += 1;
      pendingGenerateRef.current = false;
      onPoolPathChange(null);
      onBlacklistSnapshotChange?.(null);
      return;
    }
    if (!blacklistLoaded) return;
    generatePool();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled, blacklistLoaded]);

  async function generatePool() {
    if (!enabled) {
      pendingGenerateRef.current = false;
      setGenerating(false);
      setGenMsg("");
      onPoolPathChange(null);
      onBlacklistSnapshotChange?.(null);
      return;
    }
    if (generationInFlightRef.current) {
      generationSeqRef.current += 1;
      pendingGenerateRef.current = true;
      return;
    }
    generationInFlightRef.current = true;
    const seq = ++generationSeqRef.current;
    setGenerating(true);
    setGenMsg("生成中...");
    setError("");
    try {
      const res = await fetch(`${API}/quantevolver/stock-pool/generate`, { method: "POST" });
      const d = await res.json();
      if (!res.ok) {
        if (seq !== generationSeqRef.current) return;
        const msg = d.detail || JSON.stringify(d);
        setError(`生成失败 (HTTP ${res.status}): ${msg}`);
        onPoolPathChange(null);
        onBlacklistSnapshotChange?.(null);
        setGenMsg("");
        return;
      }
      if (seq !== generationSeqRef.current) return;
      if (d.wsl_path) {
        onPoolPathChange(d.wsl_path);
        onBlacklistSnapshotChange?.(d.blacklist_snapshot || null);
        setGenMsg(`已生成: ${d.wsl_path} (${d.stock_count} 只)`);
      } else {
        setError(`生成失败: 后端未返回 wsl_path。响应: ${JSON.stringify(d)}`);
        onPoolPathChange(null);
        onBlacklistSnapshotChange?.(null);
        setGenMsg("");
      }
    } catch (e: any) {
      if (seq !== generationSeqRef.current) return;
      setError(`生成请求异常: ${e.message}`);
      onPoolPathChange(null);
      onBlacklistSnapshotChange?.(null);
      setGenMsg("");
    } finally {
      generationInFlightRef.current = false;
      if (pendingGenerateRef.current) {
        pendingGenerateRef.current = false;
        void generatePool();
      } else {
        setGenerating(false);
      }
    }
  }

  async function addToBlacklist(item: Sw2Item) {
    if (blacklist.find(b => b.sw2_code === item.l2_code)) return;
    const entry: BlacklistEntry = {
      sw2_code: item.l2_code,
      sw2_name: item.l2_name,
      sw1_code: item.l1_code,
      sw1_name: item.l1_name,
      effective_from: "",
      effective_to: "",
      reason: "",
    };
    const payload = { ...entry, status: "blocked", effective_from: null, effective_to: null };
    try {
      const res = await fetch(`${API}/quantevolver/stock-pool/blacklist`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const d = await res.json();
      if (!res.ok || !d.ok) throw new Error(d.detail || JSON.stringify(d));
      setBlacklist(prev => [...prev, entry]);
      void generatePool();
    } catch (e: any) {
      setError(`添加黑名单失败: ${e.message}`);
    }
  }

  async function removeFromBlacklist(sw2_code: string) {
    try {
      const res = await fetch(`${API}/quantevolver/stock-pool/blacklist/${encodeURIComponent(sw2_code)}`, { method: "DELETE" });
      const d = await res.json();
      if (!res.ok || !d.ok) throw new Error(d.detail || JSON.stringify(d));
      setBlacklist(prev => prev.filter(b => b.sw2_code !== sw2_code));
      void generatePool();
    } catch (e: any) {
      setError(`删除黑名单失败: ${e.message}`);
    }
  }

  async function updateEntry(sw2_code: string, field: string, value: string) {
    const updated = blacklist.map(b => b.sw2_code === sw2_code ? { ...b, [field]: value } : b);
    setBlacklist(updated);
    const entry = updated.find(b => b.sw2_code === sw2_code);
    if (!entry) return;
    try {
      const res = await fetch(`${API}/quantevolver/stock-pool/blacklist`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...entry, status: "blocked", effective_from: entry.effective_from || null, effective_to: entry.effective_to || null }),
      });
      const d = await res.json();
      if (!res.ok || !d.ok) throw new Error(d.detail || JSON.stringify(d));
      void generatePool();
    } catch (e: any) {
      setError(`更新失败: ${e.message}`);
    }
  }

  const inBlacklist = new Set(blacklist.map(b => b.sw2_code));

  return (
    <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden", marginBottom: "24px" }}>
      {/* 标题栏 */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 16px", backgroundColor: "#f8fafc", borderBottom: enabled ? "1px solid #e2e8f0" : "none" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
          <label style={{ display: "flex", alignItems: "center", gap: "8px", cursor: "pointer", fontSize: "14px", fontWeight: 600, color: "#1e293b" }}>
            <input type="checkbox" checked={enabled} onChange={e => onEnabledChange(e.target.checked)}
              style={{ width: "16px", height: "16px", accentColor: "#dc2626" }} />
            启用行业黑名单过滤
          </label>
          <span style={{ fontSize: "12px", color: "#64748b" }}>排除特定申万二级行业的股票（硬排除）</span>
        </div>
        {enabled && blacklist.length > 0 && (
          <span style={{ fontSize: "12px", color: "#dc2626", fontWeight: 600 }}>{blacklist.length} 个行业已屏蔽</span>
        )}
      </div>

      {/* 错误提示 */}
      {error && (
        <div style={{ padding: "8px 12px", backgroundColor: "#fef2f2", color: "#dc2626", fontSize: "12px", borderBottom: "1px solid #fecaca", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span>⚠ {error}</span>
          <button onClick={() => setError("")} style={{ background: "none", border: "none", color: "#dc2626", cursor: "pointer", fontWeight: 700 }}>✕</button>
        </div>
      )}

      {enabled && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 0 }}>
          {/* 左侧：行业树 */}
          <div style={{ borderRight: "1px solid #e2e8f0", maxHeight: "320px", overflowY: "auto" }}>
            <div style={{ padding: "8px 12px", fontSize: "12px", fontWeight: 600, color: "#64748b", borderBottom: "1px solid #f1f5f9", backgroundColor: "#f8fafc" }}>可选行业（点击 + 加入黑名单）</div>
            {tree.length === 0 && !error && (
              <div style={{ padding: "24px", textAlign: "center", color: "#94a3b8", fontSize: "13px" }}>加载中...</div>
            )}
            {tree.map(sw1 => (
              <div key={sw1.l1_code}>
                <div onClick={() => setExpanded(prev => { const s = new Set(prev); s.has(sw1.l1_code) ? s.delete(sw1.l1_code) : s.add(sw1.l1_code); return s; })}
                  style={{ padding: "6px 12px", fontSize: "13px", fontWeight: 600, color: "#334155", cursor: "pointer", backgroundColor: "#f8fafc", display: "flex", alignItems: "center", gap: "6px", userSelect: "none" }}>
                  <span style={{ fontSize: "10px" }}>{expanded.has(sw1.l1_code) ? "▼" : "▶"}</span>
                  {sw1.l1_name}
                </div>
                {expanded.has(sw1.l1_code) && sw1.children.map(sw2 => (
                  <div key={sw2.l2_code}
                    style={{ padding: "5px 12px 5px 28px", fontSize: "12px", color: inBlacklist.has(sw2.l2_code) ? "#94a3b8" : "#475569", display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid #f8fafc" }}>
                    <span>{sw2.l2_name}</span>
                    {!inBlacklist.has(sw2.l2_code) && (
                      <button onClick={() => addToBlacklist({ ...sw2, l1_code: sw1.l1_code, l1_name: sw1.l1_name })}
                        style={{ fontSize: "11px", padding: "2px 8px", backgroundColor: "#fee2e2", color: "#dc2626", border: "none", borderRadius: "4px", cursor: "pointer" }}>+ 屏蔽</button>
                    )}
                    {inBlacklist.has(sw2.l2_code) && (
                      <span style={{ fontSize: "11px", color: "#94a3b8" }}>已屏蔽</span>
                    )}
                  </div>
                ))}
              </div>
            ))}
          </div>

          {/* 右侧：黑名单列表 */}
          <div style={{ maxHeight: "320px", overflowY: "auto" }}>
            <div style={{ padding: "8px 12px", fontSize: "12px", fontWeight: 600, color: "#64748b", borderBottom: "1px solid #f1f5f9", backgroundColor: "#f8fafc" }}>黑名单（可设置日期范围，为空=永久生效）</div>
            {blacklist.length === 0 && (
              <div style={{ padding: "24px", textAlign: "center", color: "#94a3b8", fontSize: "13px" }}>暂无屏蔽行业，从左侧添加</div>
            )}
            {blacklist.map(b => (
              <div key={b.sw2_code} style={{ padding: "8px 12px", borderBottom: "1px solid #f1f5f9", fontSize: "12px" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "4px" }}>
                  <span style={{ fontWeight: 600, color: "#dc2626" }}>{b.sw2_name}</span>
                  <button onClick={() => removeFromBlacklist(b.sw2_code)}
                    style={{ fontSize: "11px", padding: "2px 8px", backgroundColor: "#f1f5f9", color: "#64748b", border: "none", borderRadius: "4px", cursor: "pointer" }}>移除</button>
                </div>
                <div style={{ color: "#94a3b8", marginBottom: "4px" }}>{b.sw1_name}</div>
                <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                  <input type="date" value={b.effective_from || ""} onChange={e => updateEntry(b.sw2_code, "effective_from", e.target.value)}
                    style={{ fontSize: "11px", padding: "2px 4px", border: "1px solid #e2e8f0", borderRadius: "4px", color: "#475569" }} />
                  <span style={{ color: "#94a3b8" }}>至</span>
                  <input type="date" value={b.effective_to || ""} onChange={e => updateEntry(b.sw2_code, "effective_to", e.target.value)}
                    style={{ fontSize: "11px", padding: "2px 4px", border: "1px solid #e2e8f0", borderRadius: "4px", color: "#475569" }} />
                </div>
                <input type="text" placeholder="备注原因（可选）" value={b.reason || ""} onChange={e => updateEntry(b.sw2_code, "reason", e.target.value)}
                  style={{ marginTop: "4px", width: "100%", fontSize: "11px", padding: "2px 6px", border: "1px solid #e2e8f0", borderRadius: "4px", color: "#475569", boxSizing: "border-box" }} />
              </div>
            ))}
          </div>
        </div>
      )}

      {enabled && (
        <div style={{ padding: "8px 12px", borderTop: "1px solid #f1f5f9", backgroundColor: "#fefce8", fontSize: "12px", color: "#92400e", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span>{generating ? "⏳ " : "✓ "}{genMsg || "黑名单已启用，提交时自动生成股票池"}</span>
          <button onClick={generatePool} disabled={generating}
            style={{ fontSize: "11px", padding: "3px 10px", backgroundColor: "#f59e0b", color: "#fff", border: "none", borderRadius: "4px", cursor: generating ? "not-allowed" : "pointer" }}>手动刷新</button>
        </div>
      )}
    </div>
  );
}
