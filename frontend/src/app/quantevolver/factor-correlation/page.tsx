"use client";

import React, { useEffect, useState, useCallback, useRef } from "react";
import ComputePanel, { ComputeStatus, ComputeScope, FactorStats, OfficialCacheWindow } from "./components/ComputePanel";
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
  const [computeScope, setComputeScope] = useState<ComputeScope>("cache");
  const [includeDisabled, setIncludeDisabled] = useState(false);
  const [overviewData, setOverviewData] = useState<{
    factor_stats: FactorStats;
    single_cache: { cached_count: number; total_size_mb: number; date_range: string | null; as_of_date: string | null; cache_root?: string | null; cache_source?: string | null; data_source_mode?: string | null; window_train_start?: string | null; window_backtest_end?: string | null };
    official_cache_window?: OfficialCacheWindow | null;
    correlation_meta: Record<string, any> | null;
  } | null>(null);
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

  const showToast = (text: string, type: "success" | "error" = "success") => {
    setToastMsg({ text, type });
    setTimeout(() => setToastMsg(null), 4000);
  };

  // ── 加载计算状态 ──
  // include_disabled 透传, 让 uncorrelated_factor_count / db_correlation_count
  // 按用户当前勾选的口径返回 (禁用因子切换时立即反映).
  const loadStatus = useCallback(async () => {
    try {
      const res = await fetch(`${BASE}/correlations/status?include_disabled=${includeDisabled}`);
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
  }, [includeDisabled]);

  // ── 加载总览数据（官方缓存窗口 + 因子统计） ──
  const loadOverview = useCallback(async () => {
    try {
      const res = await fetch(`${BASE}/correlations/overview`);
      if (res.ok) {
        const data = await res.json();
        setOverviewData(data);
        return data;
      }
      // 非 ok 响应：清空数据，避免旧官方缓存状态残留
      setOverviewData(null);
    } catch (e) {
      console.error("加载总览数据失败", e);
      setOverviewData(null);
    }
    return null;
  }, []);

  // ── 加载矩阵数据 ──
  // 相关性矩阵在 compute 时冻结，无法按 include_disabled 增量更新。
  // 子组件（HighCorrTable/FactorDedupPanel）自行按 includeDisabled 做客户端过滤/标识。
  const loadMatrix = useCallback(async () => {
    try {
      const res = await fetch(`${BASE}/correlations/matrix?threshold=0&include_disabled=true`);
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
      // 其他非 ok 响应：清空旧数据
      setMatrixData(null);
    } catch (e) {
      console.error("加载矩阵失败", e);
      setMatrixData(null);
    }
    return null;
  }, []);

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
      const body: Record<string, any> = { force_recompute: true, include_disabled: includeDisabled };

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
      showToast(`相关性计算已启动 (${result.factor_count ?? "?"} 因子)...`);
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
          await loadOverview();
        }
      }, 2000);
    } catch (e) {
      showToast("触发计算异常", "error");
      setComputing(false);
    }
  }, [loadStatus, loadMatrix, loadOverview, computeScope, includeDisabled]);

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

  const initRef = useRef(false);
  useEffect(() => {
    if (initRef.current) return;
    initRef.current = true;
    (async () => {
      setLoading(true);
      const [st] = await Promise.all([loadStatus(), loadMatrix(), loadOverview()]);
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
            await loadOverview();
          }
        }, 2000);
      }
    })();
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // includeDisabled 切换时刷新 status (后端按口径分桶返回 db_count/uncorrelated_count)
  useEffect(() => {
    if (!initRef.current) return;
    loadStatus();
  }, [includeDisabled, loadStatus]);

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

      {/* Banner + 计算状态 */}
      <ComputePanel
        status={computeStatus}
        computing={computing}
        scope={computeScope}
        includeDisabled={includeDisabled}
        factorStats={overviewData?.factor_stats || null}
        singleCache={overviewData?.single_cache || null}
        officialCacheWindow={overviewData?.official_cache_window || null}
        onScopeChange={setComputeScope}
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
                  暂无相关性矩阵数据，请先点击&quot;全量计算&quot;
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
                  includeDisabled={includeDisabled}
                  onIncludeDisabledChange={setIncludeDisabled}
                  disabledFactors={matrixData.disabled_factors || []}
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
                disabledFactors={matrixData?.disabled_factors || []}
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
