"use client";

import Link from "next/link";
import { NAV_GROUPS } from "@/lib/navigation/nav-groups";
import { useCallback, useEffect, useState } from "react";

const API_BASE = (
  process.env.NEXT_PUBLIC_TDX_BACKEND_BASE ||
  process.env.NEXT_PUBLIC_API_BASE ||
  "http://127.0.0.1:8001"
).replace(/\/api\/v1\/?$/, "");

export default function Sidebar() {
  // 管理每个一级目录的展开状态，默认全部折叠
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());
  const [alertCount, setAlertCount] = useState(0);

  const fetchAlertCount = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/ingestion/alerts/unack-count`);
      if (!res.ok) return;
      const data = await res.json();
      setAlertCount((data.count as number) || 0);
    } catch {
      // silently ignore
    }
  }, []);

  useEffect(() => {
    fetchAlertCount();
    const timer = setInterval(fetchAlertCount, 60_000);
    return () => clearInterval(timer);
  }, [fetchAlertCount]);

  const toggleGroup = (title: string) => {
    setExpandedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(title)) {
        next.delete(title);
      } else {
        next.add(title);
      }
      return next;
    });
  };

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <h1 className="sidebar-title">📈 多AI智能体股票分析</h1>
        <p className="sidebar-subtitle">基于 DeepSeek 的专业量化投资系统</p>
      </div>

      <nav className="sidebar-nav">
        {NAV_GROUPS.map((group) => {
          const isExpanded = expandedGroups.has(group.title);
          return (
            <div key={group.title} className="sidebar-group">
              <div
                className="sidebar-group-title sidebar-group-toggle"
                onClick={() => toggleGroup(group.title)}
              >
                <span className="toggle-icon">{isExpanded ? "▼" : "▶"}</span>
                {group.title}
              </div>
              {isExpanded && (
                <div className="sidebar-group-items">
                  {group.items.map((item) => (
                    <Link key={item.href} href={item.href} className="sidebar-link">
                      {item.label}
                      {item.href === "/local-data" && alertCount > 0 && (
                        <span
                          style={{
                            marginLeft: 8,
                            background: "#ef4444",
                            color: "#fff",
                            borderRadius: 10,
                            padding: "1px 7px",
                            fontSize: 11,
                            fontWeight: 700,
                            lineHeight: "18px",
                          }}
                        >
                          {alertCount}
                        </span>
                      )}
                    </Link>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </nav>
    </aside>
  );
}
