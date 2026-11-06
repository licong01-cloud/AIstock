"use client";

import { useCallback, useEffect, useState } from "react";
import { usePathname, useRouter } from "next/navigation";

interface AlertItem {
  alert_id: string;
  created_at: string;
  severity: "info" | "warning" | "error" | "critical";
  dataset: string;
  alert_type: string;
  title: string;
  message: string;
  details: unknown;
  acknowledged: boolean;
}

const API_BASE = (
  process.env.NEXT_PUBLIC_TDX_BACKEND_BASE ||
  process.env.NEXT_PUBLIC_API_BASE ||
  "http://127.0.0.1:8001"
).replace(/\/api\/v1\/?$/, "");

/** Fixed toast for data health alerts. Polls every 30s. */
export default function DataAlertsToast() {
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  const [dismissed, setDismissed] = useState<Set<string>>(new Set());
  const router = useRouter();
  const pathname = usePathname();

  const fetchAlerts = useCallback(async () => {
    try {
      const res = await fetch(
        `${API_BASE}/api/ingestion/alerts/active?limit=10`
      );
      if (!res.ok) return;
      const data = await res.json();
      setAlerts((data.alerts as AlertItem[]) || []);
    } catch {
      // silently ignore — avoid noise on every poll
    }
  }, []);

  useEffect(() => {
    fetchAlerts();
    const timer = setInterval(fetchAlerts, 30_000);
    return () => clearInterval(timer);
  }, [fetchAlerts]);

  // Auto-dismiss warning-level toasts after 5 seconds
  useEffect(() => {
    const warnings = alerts.filter(
      (a) => a.severity === "warning" && !dismissed.has(a.alert_id)
    );
    if (warnings.length === 0) return;
    const timers = warnings.map((a) =>
      setTimeout(
        () =>
          setDismissed((prev) => {
            const next = new Set(prev);
            next.add(a.alert_id);
            return next;
          }),
        5_000
      )
    );
    return () => timers.forEach(clearTimeout);
  }, [alerts, dismissed]);

  const visible = alerts.filter((a) => !dismissed.has(a.alert_id));
  if (visible.length === 0) return null;

  if (pathname === "/simulation/localsim" || pathname?.startsWith("/simulation/localsim/")) {
    const hasError = visible.some((a) => a.severity === "error" || a.severity === "critical");
    return (
      <details
        aria-label="数据准备提醒"
        style={{ position: "fixed", bottom: 12, right: 16, zIndex: 9999,
          maxWidth: "min(320px, calc(100vw - 32px))", border: "1px solid #ead4a5",
          borderRadius: 8, background: "#fffaf0", color: "#79551a", fontSize: 13,
          boxShadow: "0 2px 8px #17243318" }}
      >
        <summary style={{ padding: "8px 12px", cursor: "pointer" }}>
          {hasError ? "数据准备异常" : "数据准备提醒"} · {visible.length} 项
        </summary>
        <div style={{ padding: "0 12px 12px" }}>
          <p>数据准备仍有待处理提醒。请到数据准备页查看原因与处理进度。</p>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
            <button type="button" onClick={() => router.push("/local-data?tab=jobs")}>
              查看数据准备
            </button>
            <button type="button" onClick={() => setDismissed((prev) => {
              const next = new Set(prev);
              visible.forEach((a) => next.add(a.alert_id));
              return next;
            })}>关闭本页提醒</button>
          </div>
        </div>
      </details>
    );
  }

  const severityBg: Record<string, string> = {
    critical: "#dc2626",
    error: "#f97316",
    warning: "#eab308",
    info: "#3b82f6",
  };

  return (
    <div
      style={{
        position: "fixed",
        top: 16,
        right: 16,
        zIndex: 9999,
        display: "flex",
        flexDirection: "column",
        gap: 8,
        maxWidth: 400,
      }}
    >
      {visible.map((a) => {
        const bg = severityBg[a.severity] || severityBg.info;
        const isSticky = a.severity === "error" || a.severity === "critical";
        return (
          <div
            key={a.alert_id}
            role="alert"
            onClick={async () => {
              try {
                await fetch(
                  `${API_BASE}/api/ingestion/alerts/${a.alert_id}/acknowledge`,
                  { method: "POST" }
                );
              } catch {
                // best effort
              }
              setDismissed((prev) => {
                const next = new Set(prev);
                next.add(a.alert_id);
                return next;
              });
              router.push("/local-data?tab=jobs");
            }}
            style={{
              background: bg,
              color: "#fff",
              padding: "12px 40px 12px 16px",
              borderRadius: 8,
              boxShadow: "0 4px 12px rgba(0,0,0,0.25)",
              cursor: "pointer",
              fontSize: 14,
              fontWeight: 500,
              position: "relative",
              transition: "opacity 0.3s",
            }}
          >
            <button
              type="button"
              aria-label={`关闭 ${a.dataset} 提醒`}
              title="关闭提醒并留在当前页面"
              onClick={(event) => {
                event.stopPropagation();
                setDismissed((prev) => {
                  const next = new Set(prev);
                  next.add(a.alert_id);
                  return next;
                });
              }}
              style={{
                position: "absolute",
                top: 6,
                right: 8,
                width: 28,
                height: 28,
                border: 0,
                borderRadius: 6,
                background: "rgba(255,255,255,0.16)",
                color: "#fff",
                cursor: "pointer",
                fontSize: 20,
                lineHeight: "28px",
                padding: 0,
              }}
            >
              ×
            </button>
            <div style={{ fontWeight: 700, marginBottom: 4 }}>
              [{a.severity.toUpperCase()}] {a.title}
            </div>
            <div style={{ fontSize: 12, opacity: 0.9 }}>{a.message}</div>
            <div style={{ fontSize: 11, opacity: 0.7, marginTop: 4 }}>
              {a.dataset} &middot; {a.alert_type}
              {isSticky ? " — 点击确认并跳转" : " — 5秒后自动关闭"}
            </div>
          </div>
        );
      })}
    </div>
  );
}
