"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export default function MarketNewsPage() {
  const [fastNews, setFastNews] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState<number>(20);
  const [sourceFilter, setSourceFilter] = useState<string>("");

  async function loadFastNews(targetPage: number = page, nextSource?: string) {
    const safePage = targetPage < 1 ? 1 : targetPage;
    const offset = (safePage - 1) * pageSize;
    const src = nextSource !== undefined ? nextSource : sourceFilter;
    setLoading(true);
    setError(null);
    try {
      const qs = new URLSearchParams();
      qs.set("limit", String(pageSize));
      qs.set("offset", String(offset));
      if (src) {
        qs.set("source", src);
      }
      const res = await fetch(`${API_BASE}/news/fast?${qs.toString()}`);
      if (!res.ok) throw new Error(`市场快讯请求失败: ${res.status}`);
      const data = await res.json();
      setFastNews(Array.isArray(data?.items) ? data.items : []);
      setPage(safePage);
    } catch (e: any) {
      setError(e?.message || "获取市场快讯失败");
      setFastNews([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadFastNews();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "#fff",
          borderRadius: 16,
          padding: 16,
          boxShadow: "0 2px 8px rgba(0,0,0,0.06)",
          marginBottom: 16,
          fontSize: 13,
        }}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 6,
          }}
        >
          <div>
            <h1 style={{ margin: 0, fontSize: 20 }}>📰 市场资讯 · 市场快讯</h1>
            <p style={{ margin: 0, color: "#6b7280", fontSize: 12 }}>
              实时展示本地数据库中的市场快讯，来源于多渠道新闻数据入库，可用于选股前的盘面环境研判。
            </p>
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <select
              value={sourceFilter}
              onChange={(e) => {
                const value = e.target.value;
                setSourceFilter(value);
                // 切换来源时从第 1 页开始
                loadFastNews(1, value);
              }}
              style={{
                padding: "4px 8px",
                borderRadius: 999,
                border: "1px solid #d4d4d4",
                fontSize: 12,
                background: "#ffffff",
              }}
            >
              <option value="">全部来源</option>
              <option value="cls_telegraph">财联社</option>
              <option value="sina_finance">新浪财经</option>
              <option value="tradingview">TradingView 外媒</option>
            </select>
            <button
              type="button"
              onClick={() => loadFastNews(page)}
              disabled={loading}
              style={{
                padding: "6px 12px",
                borderRadius: 999,
                border: "1px solid #d4d4d4",
                background: "#f9fafb",
                fontSize: 13,
                cursor: "pointer",
              }}
            >
              {loading ? "刷新中..." : "刷新市场快讯"}
            </button>
            <select
              value={pageSize}
              onChange={(e) => {
                const value = Number(e.target.value) || 20;
                setPageSize(value);
                // 修改每页条数后，从第 1 页重新加载
                setPage(1);
                loadFastNews(1);
              }}
              style={{
                padding: "4px 8px",
                borderRadius: 999,
                border: "1px solid #d4d4d4",
                fontSize: 12,
                background: "#ffffff",
              }}
            >
              <option value={10}>每页 10 条</option>
              <option value={20}>每页 20 条</option>
              <option value={50}>每页 50 条</option>
              <option value={100}>每页 100 条</option>
            </select>
            <span style={{ fontSize: 12, color: "#6b7280" }}>第 {page} 页</span>
          </div>
        </div>

        {error && (
          <p style={{ fontSize: 12, color: "#b91c1c", marginTop: 4 }}>{error}</p>
        )}

        <div
          style={{
            borderRadius: 10,
            border: "1px solid #e5e7eb",
            padding: 10,
            background: "#f9fafb",
            marginTop: 8,
          }}
        >
          {fastNews.length === 0 && !loading ? (
            <p style={{ fontSize: 12, color: "#6b7280", margin: 0 }}>
              当前暂无本地新闻记录，请确认新闻入库任务已启动。
            </p>
          ) : (
            fastNews.map((it, idx) => {
              const ts = it.publish_time
                ? new Date(it.publish_time).toLocaleString("zh-CN", {
                    timeZone: "Asia/Shanghai",
                    month: "2-digit",
                    day: "2-digit",
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit",
                    hour12: false,
                  })
                : "";
              const rawSource = (it.source || "") as string;
              let source = rawSource;
              if (rawSource === "cls_telegraph") source = "财联社";
              else if (rawSource === "sina_finance") source = "新浪财经";
              else if (rawSource === "tradingview") source = "TradingView 外媒";
              const content = (it.title || it.content || "").trim();
              const important = !!it.is_important;
              return (
                <div
                  key={it.id ?? idx}
                  style={{
                    padding: 8,
                    borderRadius: 8,
                    marginBottom: 6,
                    background: important ? "#fef2f2" : "#fff",
                    border: important
                      ? "1px solid #fecaca"
                      : "1px solid #e5e7eb",
                  }}
                >
                  <div
                    style={{
                      fontSize: 11,
                      color: "#4b5563",
                      marginBottom: 2,
                      display: "flex",
                      justifyContent: "space-between",
                      gap: 8,
                    }}
                  >
                    <span>
                      [{ts}] [{source}]
                      {important && (
                        <span style={{ marginLeft: 4, color: "#dc2626", fontWeight: 500 }}>
                          重要
                        </span>
                      )}
                    </span>
                    {it.url && (
                      <a
                        href={it.url}
                        target="_blank"
                        rel="noreferrer"
                        style={{ fontSize: 11, color: "#2563eb" }}
                      >
                        原文
                      </a>
                    )}
                  </div>
                  <div
                    style={{
                      fontSize: 12,
                      color: "#111827",
                      lineHeight: 1.4,
                    }}
                  >
                    {content || "(无内容)"}
                  </div>
                </div>
              );
            })
          )}
        </div>
        <div
          style={{
            marginTop: 8,
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            fontSize: 12,
            color: "#4b5563",
          }}
        >
          <div>
            <button
              type="button"
              onClick={() => loadFastNews(page - 1)}
              disabled={page <= 1 || loading}
              style={{
                padding: "4px 10px",
                borderRadius: 8,
                border: "1px solid #d4d4d4",
                background: page <= 1 ? "#f3f4f6" : "#f9fafb",
                cursor: page <= 1 || loading ? "default" : "pointer",
                marginRight: 8,
              }}
            >
              上一页
            </button>
            <button
              type="button"
              onClick={() => loadFastNews(page + 1)}
              disabled={fastNews.length < pageSize || loading}
              style={{
                padding: "4px 10px",
                borderRadius: 8,
                border: "1px solid #d4d4d4",
                background:
                  fastNews.length < pageSize || loading ? "#f3f4f6" : "#f9fafb",
                cursor:
                  fastNews.length < pageSize || loading ? "default" : "pointer",
              }}
            >
              下一页
            </button>
          </div>
          <div>提示：当“下一页”灰色不可点时，说明已经没有更多历史快讯。</div>
        </div>
      </section>
    </main>
  );
}
