"use client";

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <html lang="zh-CN">
      <body>
        <div style={{ padding: 40, textAlign: "center" }}>
          <h2 style={{ color: "#dc2626", marginBottom: 16 }}>系统错误</h2>
          <p style={{ color: "#6b7280", marginBottom: 16 }}>{error.message || "发生了未知错误"}</p>
          <button
            onClick={() => reset()}
            style={{
              padding: "8px 20px",
              background: "#2563eb",
              color: "#fff",
              border: "none",
              borderRadius: 6,
              cursor: "pointer",
              fontSize: 14,
            }}
          >
            重试
          </button>
        </div>
      </body>
    </html>
  );
}
