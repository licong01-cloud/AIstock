"use client";

import React, { useEffect, useState, useCallback, useRef } from "react";
import ComputePanel, { ComputeStatus, ComputeScope } from "./components/ComputePanel";
import CorrelationHeatmap from "./components/CorrelationHeatmap";
import HighCorrTable from "./components/HighCorrTable";
import PairDetail, { PairData, RelatedFactor } from "./components/PairDetail";
import FactorDedupPanel from "./components/FactorDedupPanel";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
const BASE = `${API}/quantevolver/evolution`;

type Tab = "heatmap" | "warnings" | "pair" | "dedup";

interface MatrixData {
  factor_names: string[];
  matrix: number[][];
  high_corr_pairs: { factor_a: string; factor_b: string; correlation: number }[];
  factor_count: number;
  effective_window: number;
  metadata: Record<string, any>;
  disabled_factors?: string[];
}

export default function FactorCorrelationPage() {
  const [activeTab, setActiveTab] = useState<Tab>("heatmap");
  const [computeStatus, setComputeStatus] = useState<ComputeStatus | null>(null);
  const [matrixData, setMatrixData] = useState<MatrixData | null>(null);
  const [loading, setLoading] = useState(true);
  const [computing, setComputing] = useState(false);
  const [computeScope, setComputeScope] = useState<ComputeScope>("smart_incremental");
  const [asOfDate, setAsOfDate] = useState("");
  const [includeDisabled, setIncludeDisabled] = useState(false);
  const [selectedPair, setSelectedPair] = useState<{ fa: string; fb: string }>({
    fa: "",
    fb: "",
  });
  const [pairData, setPairData] = useState<PairData | null>(null);
  const [pairLoading, setPairLoading] = useState(false);
  const [relatedFactors, setRelatedFactors] = useState<RelatedFactor[]>([]);
  const [toastMsg, setToastMsg] = useState<{
    text: string;
    type: "success" | "error";
  } | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [dedupBaseFactor, setDedupBaseFactor] = useState("");
  const [smartPreview, setSmartPreview] = useState<{
    factors: string[];
    count: number;
  } | null>(null);

  const showToast = (text: string, type: "success" | "error" = "success") => {
    setToastMsg({ text, type });
    setTimeout(() => setToastMsg(null), 4000);
  };

  // ── 加载计算状态 ──
  const loadStatus = useCallback(async () => {
    try {
      const res = await fetch(`${BASE}/correlations/status`);
      if (res.ok) {
        const data = await res.json();
        setComputeStatus(data);
        // 如果进度显示正在计算，同步设置 computing 状态
        if (data.progress?.status === "computing") {
          setComputing(true);
        }
        return data;
      }
    } catch (e) {
      console.error("加载状态失败", e);
    }
    return null;
  }, []);

  // ── 加载矩阵数据 ──
  const loadMatrix = useCallback(async () => {
    try {
      const res = await fetch(`${BASE}/correlations/matrix?threshold=0&include_disabled=${includeDisabled}`);
      if (res.ok) {
        const data = await res.json();
        setMatrixData(data);
        return data;
      }
      // 404 = 无数据，不是错误
      if (res.status === 404) {
        setMatrixData(null);
        return null;
      }
    } catch (e) {
      console.error("加载矩阵失败", e);
    }
    return null;
  }, [includeDisabled]);

  // ── 加载因子对详情 ──
  const loadPairDetail = useCallback(async (fa: string, fb: string) => {
    if (!fa || !fb || fa === fb) return;
    setPairLoading(true);
    setPairData(null);
    setRelatedFactors([]);
    try {
      const [pairRes, relatedRes] = await Promise.all([
        fetch(
          `${BASE}/correlations/pair?fa=${encodeURIComponent(fa)}&fb=${encodeURIComponent(fb)}&include_daily=true`
        ),
        fetch(
          `${BASE}/correlations/factors/${encodeURIComponent(fa)}/related?threshold=0.3&limit=10&include_disabled=${includeDisabled}`
        ),
      ]);

      if (pairRes.ok) {
        const pd = await pairRes.json();
        setPairData(pd);
      }
      if (relatedRes.ok) {
        const rd = await relatedRes.json();
        setRelatedFactors(rd.related_factors || []);
      }
    } catch (e) {
      console.error("加载因子对详情失败", e);
    } finally {
      setPairLoading(false);
    }
  }, [includeDisabled]);

  // ── 触发计算 ──
  const handleCompute = useCallback(async () => {
    setComputing(true);
    try {
      const body: Record<string, any> = { force_recompute: true };

      if (asOfDate) {
        body.as_of_date = asOfDate;
      }

      if (computeScope === "full") {
        body.mode = "full";
        body.include_disabled = includeDisabled;
      } else if (computeScope === "cache") {
        body.mode = "cache_only";
        body.include_disabled = includeDisabled;
      } else if (computeScope === "smart_incremental") {
        // 智能增量: 先 dry_run 预览，再确认计算
        const previewUrl = asOfDate
          ? `${BASE}/correlations/compute-smart-incremental?dry_run=true&as_of_date=${asOfDate}&include_disabled=${includeDisabled}`
          : `${BASE}/correlations/compute-smart-incremental?dry_run=true&include_disabled=${includeDisabled}`;
        const res = await fetch(previewUrl, { method: "POST" });
        if (!res.ok) {
          showToast("查询新因子失败", "error");
          setComputing(false);
          return;
        }
        const result = await res.json();
        if (result.status === "no_new_factors") {
          showToast("所有因子均已计算过相关性，无需增量计算");
          setComputing(false);
          return;
        }
        // 显示预览弹窗，等待用户确认
        setSmartPreview({ factors: result.new_factors, count: result.new_factor_count });
        setComputing(false);
        return;
      } else if (computeScope === "incremental") {
        // 增量模式: 使用不同的 API 端点
        const res = await fetch(`${BASE}/correlations/compute-incremental`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            factor_names: [],  // TODO: 支持因子选择器
            as_of_date: asOfDate || undefined,
          }),
        });
        if (!res.ok) {
          showToast("触发增量计算失败", "error");
          setComputing(false);
          return;
        }
        showToast("增量计算已启动...");
        pollRef.current = setInterval(async () => {
          const st = await loadStatus();
          if (st && st.progress?.status !== "computing") {
            clearInterval(pollRef.current!);
            pollRef.current = null;
            setComputing(false);
            if (st.progress?.status === "failed") {
              showToast(`增量计算失败: ${st.progress.error || "未知错误"}`, "error");
            } else {
              showToast("增量计算完成!");
            }
            await loadMatrix();
          }
        }, 2000);
        return;
      }

      const res = await fetch(`${BASE}/correlations/compute`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        showToast("触发计算失败", "error");
        setComputing(false);
        return;
      }
      const result = await res.json();
      showToast(`相关性计算已启动 (${result.factor_count ?? "?"} 因子, ${result.mode})...`);
      // 轮询状态 (2秒间隔)
      pollRef.current = setInterval(async () => {
        const st = await loadStatus();
        if (st && st.progress?.status !== "computing") {
          clearInterval(pollRef.current!);
          pollRef.current = null;
          setComputing(false);
          if (st.progress?.status === "failed") {
            showToast(`计算失败: ${st.progress.error || "未知错误"}`, "error");
          } else {
            showToast("计算完成!");
          }
          await loadMatrix();
        }
      }, 2000);
    } catch (e) {
      showToast("触发计算异常", "error");
      setComputing(false);
    }
  }, [loadStatus, loadMatrix, computeScope, asOfDate, includeDisabled]);

  // ── 智能增量: 用户确认后开始计算 ──
  const handleSmartIncrementalConfirm = useCallback(async () => {
    setSmartPreview(null);
    setComputing(true);
    try {
      const url = asOfDate
        ? `${BASE}/correlations/compute-smart-incremental?as_of_date=${asOfDate}&include_disabled=${includeDisabled}`
        : `${BASE}/correlations/compute-smart-incremental?include_disabled=${includeDisabled}`;
      const res = await fetch(url, { method: "POST" });
      if (!res.ok) {
        showToast("触发智能增量计算失败", "error");
        setComputing(false);
        return;
      }
      const result = await res.json();
      showToast(`${result.new_factor_count ?? "?"} 个新因子，智能增量计算已启动...`);
      pollRef.current = setInterval(async () => {
        const st = await loadStatus();
        if (st && st.progress?.status !== "computing") {
          clearInterval(pollRef.current!);
          pollRef.current = null;
          setComputing(false);
          if (st.progress?.status === "failed") {
            showToast(`智能增量计算失败: ${st.progress.error || "未知错误"}`, "error");
          } else {
            showToast("智能增量计算完成!");
          }
          await loadMatrix();
        }
      }, 2000);
    } catch (e) {
      showToast("触发智能增量计算异常", "error");
      setComputing(false);
    }
  }, [loadStatus, loadMatrix, asOfDate, includeDisabled]);

  // ── 热力图点击 / 预警表点击 → 跳转因子对分析 ──
  const handleSelectPair = useCallback(
    (fa: string, fb: string) => {
      setSelectedPair({ fa, fb });
      setActiveTab("pair");
      loadPairDetail(fa, fb);
    },
    [loadPairDetail]
  );

  // ── 因子对分析页手动切换因子 ──
  const handleChangePair = useCallback(
    (fa: string, fb: string) => {
      setSelectedPair({ fa, fb });
      if (fa && fb && fa !== fb) {
        loadPairDetail(fa, fb);
      }
    },
    [loadPairDetail]
  );

  // ── 因子操作回调 ──
  const handleDeleteFactor = useCallback(async (factorName: string, source: string) => {
    const params = new URLSearchParams({ factor_name: factorName, source });
    const res = await fetch(`${API}/quantevolver/factors?${params}`, { method: "DELETE" });
    if (!res.ok) throw new Error("删除失败");
    showToast(`因子「${factorName}」已彻底删除`);
    await loadMatrix();
    if (selectedPair.fa && selectedPair.fb) await loadPairDetail(selectedPair.fa, selectedPair.fb);
  }, [loadMatrix, loadPairDetail, selectedPair]);

  const handleSetUnavailable = useCallback(async (factorName: string, source: string) => {
    const res = await fetch(`${API}/quantevolver/factors/${encodeURIComponent(factorName)}/availability`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source, is_available: false }),
    });
    if (!res.ok) throw new Error("设置失败");
    showToast(`因子「${factorName}」已标记为不可用`);
    if (selectedPair.fa && selectedPair.fb) await loadPairDetail(selectedPair.fa, selectedPair.fb);
  }, [loadPairDetail, selectedPair]);

  // ── 跳转到因子去重 tab ──
  const handleGoToDedup = useCallback((factorName: string) => {
    setDedupBaseFactor(factorName);
    setActiveTab("dedup");
  }, []);

  // ── 切换"含禁用因子"时重新加载矩阵 ──
  const prevIncludeDisabled = useRef(includeDisabled);
  useEffect(() => {
    if (prevIncludeDisabled.current !== includeDisabled) {
      prevIncludeDisabled.current = includeDisabled;
      loadMatrix();
    }
  }, [includeDisabled, loadMatrix]);

  // ── 初始加载（仅 mount 时执行一次）──
  const initRef = useRef(false);
  useEffect(() => {
    if (initRef.current) return;
    initRef.current = true;
    (async () => {
      setLoading(true);
      const [st] = await Promise.all([loadStatus(), loadMatrix()]);
      setLoading(false);
      // 如果后端已在计算中，启动轮询
      if (st && st.progress?.status === "computing") {
        setComputing(true);
        pollRef.current = setInterval(async () => {
          const s = await loadStatus();
          if (s && s.progress?.status !== "computing") {
            clearInterval(pollRef.current!);
            pollRef.current = null;
            setComputing(false);
            await loadMatrix();
          }
        }, 2000);
      }
    })();
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const tabs: { key: Tab; label: string }[] = [
    { key: "heatmap", label: "相关性热力图" },
    { key: "warnings", label: "高相关预警" },
    { key: "pair", label: "因子对分析" },
    { key: "dedup", label: "因子去重" },
  ];

  return (
    <div style={{ maxWidth: 1400, margin: "0 auto" }}>
      {/* Toast */}
      {toastMsg && (
        <div
          style={{
            position: "fixed",
            top: 24,
            right: 24,
            zIndex: 9999,
            padding: "10px 20px",
            borderRadius: 8,
            fontSize: 13,
            fontWeight: 500,
            color: "#fff",
            background: toastMsg.type === "success" ? "#10b981" : "#ef4444",
            boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
          }}
        >
          {toastMsg.text}
        </div>
      )}

      {/* 智能增量预览弹窗 */}
      {smartPreview && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            zIndex: 10000,
            background: "rgba(0,0,0,0.5)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
          }}
          onClick={() => setSmartPreview(null)}
        >
          <div
            style={{
              background: "#fff",
              borderRadius: 16,
              padding: "24px 28px",
              maxWidth: 520,
              width: "90%",
              maxHeight: "70vh",
              display: "flex",
              flexDirection: "column",
              boxShadow: "0 8px 32px rgba(0,0,0,0.2)",
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3 style={{ margin: "0 0 4px", fontSize: 16, fontWeight: 700, color: "#111827" }}>
              智能增量计算预览
            </h3>
            <p style={{ margin: "0 0 16px", fontSize: 13, color: "#6b7280" }}>
              检测到 <strong style={{ color: "#7c3aed" }}>{smartPreview.count}</strong> 个新因子需要计算相关性。
              将执行因子代码生成缓存，并与所有已有因子计算相关系数。
            </p>
            <div
              style={{
                flex: 1,
                overflowY: "auto",
                border: "1px solid #e5e7eb",
                borderRadius: 8,
                marginBottom: 16,
              }}
            >
              <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ background: "#f9fafb", position: "sticky", top: 0 }}>
                    <th style={{ padding: "8px 12px", textAlign: "left", fontWeight: 600, color: "#374151" }}>#</th>
                    <th style={{ padding: "8px 12px", textAlign: "left", fontWeight: 600, color: "#374151" }}>因子名称</th>
                  </tr>
                </thead>
                <tbody>
                  {smartPreview.factors.map((f, i) => (
                    <tr key={f} style={{ borderTop: "1px solid #f3f4f6" }}>
                      <td style={{ padding: "6px 12px", color: "#9ca3af" }}>{i + 1}</td>
                      <td style={{ padding: "6px 12px", fontFamily: "monospace", color: "#111827" }}>{f}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div style={{ display: "flex", justifyContent: "flex-end", gap: 10 }}>
              <button
                onClick={() => setSmartPreview(null)}
                style={{
                  padding: "8px 20px",
                  fontSize: 13,
                  fontWeight: 500,
                  border: "1px solid #d1d5db",
                  borderRadius: 8,
                  background: "#fff",
                  color: "#374151",
                  cursor: "pointer",
                }}
              >
                取消
              </button>
              <button
                onClick={handleSmartIncrementalConfirm}
                style={{
                  padding: "8px 24px",
                  fontSize: 13,
                  fontWeight: 600,
                  border: "none",
                  borderRadius: 8,
                  background: "#7c3aed",
                  color: "#fff",
                  cursor: "pointer",
                }}
              >
                确认计算 ({smartPreview.count} 个因子)
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Banner + 计算状态 */}
      <ComputePanel
        status={computeStatus}
        computing={computing}
        scope={computeScope}
        asOfDate={asOfDate}
        includeDisabled={includeDisabled}
        onScopeChange={setComputeScope}
        onAsOfDateChange={setAsOfDate}
        onIncludeDisabledChange={setIncludeDisabled}
        onCompute={handleCompute}
      />

      {/* Tab 切换 */}
      <div style={{ display: "flex", gap: 8, marginBottom: 20 }}>
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => setActiveTab(t.key)}
            style={{
              padding: "8px 20px",
              borderRadius: 8,
              fontSize: 13,
              fontWeight: 600,
              border: "none",
              cursor: "pointer",
              background: activeTab === t.key ? "#7c3aed" : "#f3f4f6",
              color: activeTab === t.key ? "#fff" : "#6b7280",
            }}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* 内容区域 */}
      <div
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 20,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
          minHeight: 400,
        }}
      >
        {loading ? (
          <div
            style={{ color: "#9ca3af", textAlign: "center", padding: 64 }}
          >
            加载中...
          </div>
        ) : (
          <>
            {/* Tab 1: 热力图 */}
            {activeTab === "heatmap" && (
              matrixData && matrixData.factor_names.length > 0 ? (
                <CorrelationHeatmap
                  factorNames={matrixData.factor_names}
                  matrix={matrixData.matrix}
                  disabledFactors={matrixData.disabled_factors || []}
                  onCellClick={(fa, fb, corr) => handleSelectPair(fa, fb)}
                />
              ) : (
                <div
                  style={{
                    color: "#9ca3af",
                    textAlign: "center",
                    padding: 64,
                  }}
                >
                  暂无相关性矩阵数据，请先点击"全量计算"
                </div>
              )
            )}

            {/* Tab 2: 高相关预警 */}
            {activeTab === "warnings" && (
              matrixData && matrixData.high_corr_pairs.length > 0 ? (
                <HighCorrTable
                  pairs={matrixData.high_corr_pairs}
                  onSelectPair={(fa, fb) => handleSelectPair(fa, fb)}
                  onGoToDedup={handleGoToDedup}
                />
              ) : (
                <div
                  style={{
                    color: "#9ca3af",
                    textAlign: "center",
                    padding: 64,
                  }}
                >
                  暂无高相关因子对数据
                </div>
              )
            )}

            {/* Tab 3: 因子对分析 */}
            {activeTab === "pair" && (
              <PairDetail
                factorA={selectedPair.fa}
                factorB={selectedPair.fb}
                factorNames={matrixData?.factor_names ?? []}
                pairData={pairData}
                relatedFactors={relatedFactors}
                loading={pairLoading}
                onChangePair={handleChangePair}
                onDeleteFactor={handleDeleteFactor}
                onSetUnavailable={handleSetUnavailable}
                onGoToDedup={handleGoToDedup}
              />
            )}

            {/* Tab 4: 因子去重 */}
            {activeTab === "dedup" && (
              <FactorDedupPanel
                factorNames={matrixData?.factor_names ?? []}
                initialBaseFactor={dedupBaseFactor}
                showToast={showToast}
                onRefreshMatrix={loadMatrix}
                includeDisabled={includeDisabled}
              />
            )}
          </>
        )}
      </div>
    </div>
  );
}
