"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Stats = {
  factors: number;
  models: number;
  strategies: number;
  classifications: number;
  experiments: number;
};

export default function QuantEvolverDashboard() {
  const [stats, setStats] = useState<Stats>({
    factors: 0, models: 0, strategies: 0, classifications: 0, experiments: 0,
  });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;

    async function fetchTotal(path: string): Promise<number> {
      try {
        const r = await fetch(`${API}${path}`);
        const d = await r.json();
        return d.total || 0;
      } catch {
        return 0;
      }
    }

    (async () => {
      try {
        const [factors, models, strategies, classifications, experiments] =
          await Promise.all([
            fetchTotal("/quantevolver/factors?limit=1"),
            fetchTotal("/quantevolver/models?limit=1"),
            fetchTotal("/quantevolver/strategies?limit=1"),
            fetchTotal("/quantevolver/factor-analyst/classifications?limit=1"),
            fetchTotal("/quantevolver/experiments?limit=1"),
          ]);
        if (!cancelled) {
          setStats({ factors, models, strategies, classifications, experiments });
        }
      } catch { /* ignore */ }
      if (!cancelled) setLoading(false);
    })();

    return () => { cancelled = true; };
  }, []);

  const cards = [
    { label: "因子总数", value: stats.factors, href: "/quantevolver/factors", color: "#8b5cf6", icon: "📊" },
    { label: "模型总数", value: stats.models, href: "/quantevolver/models", color: "#06b6d4", icon: "🧠" },
    { label: "策略总数", value: stats.strategies, href: "/quantevolver/strategies", color: "#10b981", icon: "🎯" },
    { label: "已分类因子", value: stats.classifications, href: "/quantevolver/factors", color: "#f59e0b", icon: "🏷️" },
    { label: "实验总数", value: stats.experiments, href: "/quantevolver/experiments", color: "#ef4444", icon: "🧪" },
  ];

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 50%, #06b6d4 100%)",
          borderRadius: 16, padding: 24, color: "#fff", marginBottom: 24,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 28, fontWeight: 700 }}>QuantEvolver</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 14 }}>
          多Agent组合演进系统 — 因子分析 · 模型评估 · 策略组合 · 配置生成
        </p>
      </section>

      {/* 统计卡片 */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 16, marginBottom: 24 }}>
        {cards.map(c => (
          <Link key={c.label} href={c.href} style={{ textDecoration: "none" }}>
            <div
              style={{
                background: "#fff", borderRadius: 12, padding: 20,
                boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
                borderLeft: `4px solid ${c.color}`,
                cursor: "pointer", transition: "transform 0.15s",
              }}
              onMouseEnter={e => (e.currentTarget.style.transform = "translateY(-2px)")}
              onMouseLeave={e => (e.currentTarget.style.transform = "none")}
            >
              <div style={{ fontSize: 24, marginBottom: 8 }}>{c.icon}</div>
              <div style={{ fontSize: 28, fontWeight: 700, color: c.color }}>
                {loading ? "..." : c.value}
              </div>
              <div style={{ fontSize: 13, color: "#6b7280", marginTop: 4 }}>{c.label}</div>
            </div>
          </Link>
        ))}
      </div>

      {/* 快捷操作 */}
      <section
        style={{
          background: "#fff", borderRadius: 12, padding: 20,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)", marginBottom: 24,
        }}
      >
        <h2 style={{ margin: "0 0 16px", fontSize: 18, fontWeight: 600 }}>快捷操作</h2>
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
          <Link href="/quantevolver/compose">
            <button style={btnStyle("#7c3aed")}>🔧 创建新组合</button>
          </Link>
          <button style={btnStyle("#2563eb")} onClick={() => batchAnalyze()}>
            📊 批量分析因子
          </button>
          <button style={btnStyle("#06b6d4")} onClick={() => syncAlpha()}>
            🔄 同步Alpha因子
          </button>
          <Link href="/quantevolver/experiments">
            <button style={btnStyle("#10b981")}>🧪 查看实验</button>
          </Link>
          <Link href="/quantevolver/factor-transformation">
            <button style={btnStyle("#f59e0b")}>⚡ 因子代码改造</button>
          </Link>
        </div>
      </section>

      {/* 工作流程 */}
      <section
        style={{
          background: "#fff", borderRadius: 12, padding: 20,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        <h2 style={{ margin: "0 0 16px", fontSize: 18, fontWeight: 600 }}>工作流程</h2>
        <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", fontSize: 14 }}>
          {[
            { step: "1", text: "浏览因子库", href: "/quantevolver/factors" },
            { step: "2", text: "选择模型", href: "/quantevolver/models" },
            { step: "3", text: "选择策略", href: "/quantevolver/strategies" },
            { step: "4", text: "组合配置", href: "/quantevolver/compose" },
            { step: "5", text: "生成QLib配置" },
            { step: "6", text: "WSL执行回测" },
            { step: "7", text: "查看结果", href: "/quantevolver/experiments" },
          ].map((s, i) => (
            <div key={s.step} style={{ display: "flex", alignItems: "center", gap: 8 }}>
              {i > 0 && <span style={{ color: "#d1d5db", fontSize: 18 }}>→</span>}
              {s.href ? (
                <Link href={s.href} style={{ textDecoration: "none" }}>
                  <span
                    style={{
                      background: "#f3f4f6", borderRadius: 8, padding: "6px 12px",
                      cursor: "pointer", color: "#374151",
                    }}
                  >
                    <strong>{s.step}</strong> {s.text}
                  </span>
                </Link>
              ) : (
                <span style={{ background: "#f3f4f6", borderRadius: 8, padding: "6px 12px", color: "#9ca3af" }}>
                  <strong>{s.step}</strong> {s.text}
                </span>
              )}
            </div>
          ))}
        </div>
      </section>
    </main>
  );
}

function btnStyle(bg: string): React.CSSProperties {
  return {
    padding: "8px 16px", fontSize: 13, fontWeight: 600,
    background: bg, color: "#fff", border: "none", borderRadius: 8,
    cursor: "pointer",
  };
}

async function batchAnalyze() {
  if (!confirm("确认批量分析所有因子？（使用规则分类，不调用LLM）")) return;
  try {
    const res = await fetch(`${API}/quantevolver/factor-analyst/batch-analyze`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ use_llm: false }),
    });
    const data = await res.json();
    alert(`分析完成: 总计${data.total}个, 已分析${data.analyzed}个, 错误${data.errors?.length || 0}个`);
    window.location.reload();
  } catch (e: any) {
    alert("批量分析失败: " + (e?.message || "未知错误"));
  }
}

async function syncAlpha() {
  if (!confirm("确认同步Alpha158/360因子？")) return;
  try {
    const res = await fetch(`${API}/quantevolver/sync/alpha-factors`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ alpha_types: ["alpha158", "alpha360"] }),
    });
    const data = await res.json();
    alert(data.ok ? "同步成功" : "同步失败: " + JSON.stringify(data));
    window.location.reload();
  } catch (e: any) {
    alert("同步失败: " + (e?.message || "未知错误"));
  }
}
