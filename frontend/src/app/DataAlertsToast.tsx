"use client";

import { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";

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
              padding: "12px 16px",
              borderRadius: 8,
              boxShadow: "0 4px 12px rgba(0,0,0,0.25)",
              cursor: "pointer",
              fontSize: 14,
              fontWeight: 500,
              transition: "opacity 0.3s",
            }}
          >
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
