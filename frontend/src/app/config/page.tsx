"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

type FieldType = "text" | "password" | "boolean" | "select";

interface ConfigFieldMeta {
  value: string;
  description: string;
  required: boolean;
  type: FieldType;
  options?: string[];
}

type ConfigInfo = Record<string, ConfigFieldMeta>;

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export default function ConfigPage() {
  const [config, setConfig] = useState<ConfigInfo | null>(null);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(
          `${API_BASE.replace(/\/$/, "")}/config/env`,
        );
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const data: ConfigInfo = await res.json();
        setConfig(data);
      } catch (e: any) {
        setError(e?.message || "加载配置失败");
      }
    })();
  }, []);

  const handleChange = (key: string, value: string | boolean) => {
    setConfig((prev) => {
      if (!prev) return prev;
      return {
        ...prev,
        [key]: {
          ...prev[key],
          value:
            typeof value === "boolean" ? (value ? "true" : "false") : value,
        },
      };
    });
  };

  const proxyKeys: Set<string> = new Set([
    "USE_PROXY",
    "PROXYPOOL_ENABLED",
    "PROXYPOOL_BASE_URL",
    "PROXYPOOL_AUTH_TYPE",
    "PROXYPOOL_TOKEN",
    "PROXYPOOL_USERNAME",
    "PROXYPOOL_PASSWORD",
    "PROXY_REFRESH_INTERVAL_MIN",
  ]);

  const sectionDef: { title: string; keys: string[] }[] = [
    {
      title: "DeepSeek / AI 接口",
      keys: ["DEEPSEEK_API_KEY", "DEEPSEEK_BASE_URL"],
    },
    {
      title: "Tushare 数据源",
      keys: ["TUSHARE_TOKEN"],
    },
    {
      title: "TDX 本地数据与调度后端",
      keys: ["TDX_API_BASE"],
    },
    {
      title: "Qlib / WSL / 路径配置",
      keys: [
        "QLIB_WSL_DISTRO",
        "QLIB_WSL_CONDA_SH",
        "QLIB_WSL_CONDA_ENV",
        "QLIB_RDAGENT_ROOT_WIN",
        "QLIB_RDAGENT_ROOT_WSL",
        "QLIB_SCRIPTS_SUBDIR",
        "QLIB_CSV_ROOT_WIN",
        "QLIB_BIN_ROOT_WIN",
      ],
    },
    {
      title: "TimescaleDB / PostgreSQL",
      keys: [
        "TDX_DB_HOST",
        "TDX_DB_PORT",
        "TDX_DB_NAME",
        "TDX_DB_USER",
        "TDX_DB_PASSWORD",
      ],
    },
    {
      title: "MiniQMT 量化交易",
      keys: [
        "MINIQMT_ENABLED",
        "MINIQMT_ACCOUNT_ID",
        "MINIQMT_HOST",
        "MINIQMT_PORT",
      ],
    },
    {
      title: "邮件通知",
      keys: [
        "EMAIL_ENABLED",
        "SMTP_SERVER",
        "SMTP_PORT",
        "EMAIL_FROM",
        "EMAIL_PASSWORD",
        "EMAIL_TO",
      ],
    },
    {
      title: "Webhook 通知",
      keys: [
        "WEBHOOK_ENABLED",
        "WEBHOOK_TYPE",
        "WEBHOOK_URL",
        "WEBHOOK_KEYWORD",
      ],
    },
    {
      title: "其他",
      keys: ["NEWS_INGEST_VERBOSE_LOG"],
    },
  ];

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!config) return;
    setSaving(true);
    setMessage(null);
    setError(null);
    try {
      const payload: Record<string, string> = {};
      Object.entries(config).forEach(([k, v]) => {
        payload[k] = v.value ?? "";
      });
      const res = await fetch(
        `${API_BASE.replace(/\/$/, "")}/config/env`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(data?.detail || data?.message || "保存失败");
      }
      setMessage(data?.message || "配置保存成功");
    } catch (e: any) {
      setError(e?.message || "保存失败");
    } finally {
      setSaving(false);
    }
  };

  const renderField = (key: string, meta: ConfigFieldMeta) => {
    const label = `${key}`;
    const commonStyle: React.CSSProperties = {
      marginTop: 4,
      width: "100%",
      padding: "6px 8px",
      borderRadius: 8,
      border: "1px solid #d4d4d4",
      fontSize: 13,
    };

    if (meta.type === "boolean") {
      const checked = (meta.value || "").toLowerCase() === "true";
      return (
        <label
          key={key}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 6,
            fontSize: 13,
          }}
        >
          <input
            type="checkbox"
            checked={checked}
            onChange={(e) => handleChange(key, e.target.checked)}
          />
          <span>
            <strong>{label}</strong>
            {meta.required && <span style={{ color: "#dc2626" }}> *</span>}
            <span style={{ display: "block", color: "#6b7280", fontSize: 12 }}>
              {meta.description}
            </span>
          </span>
        </label>
      );
    }

    if (meta.type === "select" && meta.options && meta.options.length > 0) {
      return (
        <div key={key} style={{ marginBottom: 10 }}>
          <label style={{ fontSize: 13 }}>
            <strong>{label}</strong>
            {meta.required && <span style={{ color: "#dc2626" }}> *</span>}
          </label>
          <select
            value={meta.value ?? ""}
            onChange={(e) => handleChange(key, e.target.value)}
            style={commonStyle}
          >
            {meta.options.map((opt) => (
              <option key={opt} value={opt}>
                {opt}
              </option>
            ))}
          </select>
          <p style={{ margin: 0, marginTop: 2, fontSize: 12, color: "#6b7280" }}>
            {meta.description}
          </p>
        </div>
      );
    }

    return (
      <div key={key} style={{ marginBottom: 10 }}>
        <label style={{ fontSize: 13 }}>
          <strong>{label}</strong>
          {meta.required && <span style={{ color: "#dc2626" }}> *</span>}
        </label>
        <input
          type={meta.type === "password" ? "password" : "text"}
          value={meta.value ?? ""}
          onChange={(e) => handleChange(key, e.target.value)}
          style={commonStyle}
        />
        <p style={{ margin: 0, marginTop: 2, fontSize: 12, color: "#6b7280" }}>
          {meta.description}
        </p>
      </div>
    );
  };

  return (
    <main style={{ padding: 24 }}>
      <section style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 22 }}>⚙️ 环境配置管理</h1>
        <p style={{ marginTop: 4, fontSize: 13, color: "#666" }}>
          该页面与旧版 Streamlit 环境配置功能保持一致，可视化管理根目录 <code>.env</code>
          配置。
        </p>
      </section>

      <section
        style={{
          padding: 14,
          borderRadius: 12,
          background: "#fff",
          boxShadow: "0 2px 8px rgba(0,0,0,0.04)",
          fontSize: 13,
        }}
      >
        <form onSubmit={handleSubmit}>
          {error && (
            <div
              style={{
                marginBottom: 10,
                padding: 8,
                borderRadius: 6,
                background: "#fee2e2",
                color: "#b91c1c",
                fontSize: 12,
              }}
            >
              {error}
            </div>
          )}
          {message && (
            <div
              style={{
                marginBottom: 10,
                padding: 8,
                borderRadius: 6,
                background: "#dcfce7",
                color: "#166534",
                fontSize: 12,
              }}
            >
              {message}
            </div>
          )}

          {!config && !error && (
            <p style={{ fontSize: 13 }}>正在加载配置...</p>
          )}

          {config && (
            <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
              {sectionDef.map((section) => {
                const visibleFields = section.keys
                  .filter((k) => !proxyKeys.has(k))
                  .filter((k) => config[k]);
                if (visibleFields.length === 0) return null;
                return (
                  <div
                    key={section.title}
                    style={{
                      border: "1px solid #e5e7eb",
                      borderRadius: 12,
                      padding: 12,
                      background: "#f9fafb",
                    }}
                  >
                    <h2
                      style={{
                        margin: 0,
                        marginBottom: 8,
                        fontSize: 15,
                        color: "#111827",
                      }}
                    >
                      {section.title}
                    </h2>
                    <div
                      style={{
                        display: "grid",
                        gridTemplateColumns:
                          "repeat(auto-fit, minmax(260px, 1fr))",
                        gap: 10,
                      }}
                    >
                      {visibleFields.map((key) => (
                        <div
                          key={key}
                          style={{
                            border: "1px solid #e5e7eb",
                            borderRadius: 10,
                            padding: 8,
                            background: "#ffffff",
                          }}
                        >
                          {renderField(key, config[key])}
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          <div
            style={{
              marginTop: 16,
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
            }}
          >
            <button
              type="submit"
              disabled={saving || !config}
              style={{
                padding: "6px 14px",
                borderRadius: 8,
                border: "none",
                background: saving ? "#9ca3af" : "#0f766e",
                color: "#fff",
                cursor: saving ? "default" : "pointer",
                fontSize: 13,
              }}
            >
              {saving ? "保存中..." : "保存配置"}
            </button>

            <Link href="/" style={{ fontSize: 13, color: "#0ea5e9" }}>
              ← 返回首页
            </Link>
          </div>
        </form>
      </section>
    </main>
  );
}
