"use client";
import React, { useState, useCallback } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface Props {
  open: boolean;
  onClose: () => void;
  onCreated?: () => void;
  dataDate?: string;
}

const DATASETS_REF = `可用数据集（MultiIndex: datetime, instrument，日频 A 股全市场）:

daily_pv.h5 (7列): open, close, high, low, volume, factor, amount
daily_basic.h5 (16列): db_close, db_turnover_rate, db_turnover_rate_f, db_volume_ratio, db_pe, db_pe_ttm, db_pb, db_ps, db_ps_ttm, db_dv_ratio, db_dv_ttm, db_total_share, db_float_share, db_free_share, db_total_mv, db_circ_mv
moneyflow.h5 (18列): mf_sm_buy_vol/amt, mf_sm_sell_vol/amt, mf_md_buy_vol/amt, mf_md_sell_vol/amt, mf_lg_buy_vol/amt, mf_lg_sell_vol/amt, mf_elg_buy_vol/amt, mf_elg_sell_vol/amt, mf_net_vol, mf_net_amt
bak_basic.h5 (15列): bb_pe_dyn, bb_total_assets, bb_liquid_assets, bb_fixed_assets, bb_reserved, bb_reserved_pershare, bb_eps, bb_bvps, bb_undp, bb_per_undp, bb_rev_yoy, bb_profit_yoy, bb_gpr, bb_npr, bb_holder_num
cyq_perf.h5 (9列): cp_his_low, cp_his_high, cp_cost_5pct, cp_cost_15pct, cp_cost_50pct, cp_cost_85pct, cp_cost_95pct, cp_weight_avg, cp_winner_rate
sector_data.h5 (22列): sw2_open/high/low/close, sw2_pct_change, sw2_vol, sw2_amount, sw2_pe, sw2_pb, sw2_total_mv, sw2_mf_*_amt (9列资金流), sw2_mf_*_vol (3列)`;

const DEFAULT_CODE = `import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "REPLACE_ME"

def compute_factor():
    df = pd.read_hdf(DATA_DIR / "daily_pv.h5")

    # ===== FACTOR COMPUTATION AREA =====
    factor = df["close"].pct_change(5)  # 示例：5日动量
    # ===== END FACTOR COMPUTATION =====

    result = factor.to_frame(FACTOR_NAME)
    result.index.names = ["datetime", "instrument"]
    result = result.dropna()
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")

if __name__ == "__main__":
    compute_factor()
`;

export default function ManualFactorDialog({ open, onClose, onCreated, dataDate }: Props) {
  const [factorName, setFactorName] = useState("m_");
  const [codeText, setCodeText] = useState(DEFAULT_CODE);
  const [description, setDescription] = useState("");
  const [showDatasets, setShowDatasets] = useState(false);

  const [loading, setLoading] = useState(false);
  const [stage, setStage] = useState("");
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState("");

  const handleValidate = useCallback(async () => {
    setLoading(true); setStage("验证中..."); setError(""); setResult(null);
    try {
      const code = codeText.replace(/REPLACE_ME/g, factorName);
      const resp = await fetch(`${API}/quantevolver/factors/manual/validate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ factor_name: factorName, code_text: code }),
      });
      if (!resp.ok) { setError(`HTTP ${resp.status}: ${await resp.text()}`); setLoading(false); setStage(""); return; }
      const data = await resp.json();
      setResult({ type: "validate", ...data });
      if (!data.success) setError(data.message || "验证失败");
    } catch (e: any) { setError(e?.message || "请求失败"); }
    setLoading(false); setStage("");
  }, [factorName, codeText]);

  const handleSave = useCallback(async () => {
    setLoading(true); setStage("入库中（含 LLM 分类）..."); setError(""); setResult(null);
    try {
      const code = codeText.replace(/REPLACE_ME/g, factorName);
      const resp = await fetch(`${API}/quantevolver/factors/manual`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ factor_name: factorName, code_text: code, description: description || undefined }),
      });
      if (!resp.ok) { setError(`HTTP ${resp.status}: ${await resp.text()}`); setLoading(false); setStage(""); return; }
      const data = await resp.json();
      setResult({ type: "save", ...data });
      if (data.catalog_id) onCreated?.();
    } catch (e: any) { setError(e?.message || "请求失败"); }
    setLoading(false); setStage("");
  }, [factorName, codeText, description, onCreated]);

  const handleFullPipeline = useCallback(async () => {
    setLoading(true); setStage("完整流水线（验证→入库→指标→分类）..."); setError(""); setResult(null);
    try {
      if (!dataDate) {
        setError("请先在因子库选择数据快照；手工因子全流程必须写入官方独立指标快照。");
        setLoading(false); setStage("");
        return;
      }
      const code = codeText.replace(/REPLACE_ME/g, factorName);
      const resp = await fetch(`${API}/quantevolver/factors/manual/full-pipeline`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ factor_name: factorName, code_text: code, description: description || undefined, data_date: dataDate }),
      });
      if (!resp.ok) { setError(`HTTP ${resp.status}: ${await resp.text()}`); setLoading(false); setStage(""); return; }
      const data = await resp.json();
      setResult({ type: "pipeline", ...data });
      if (data.success) onCreated?.();
      else setError(data.error || "流水线失败");
    } catch (e: any) { setError(e?.message || "请求失败"); }
    setLoading(false); setStage("");
  }, [factorName, codeText, description, dataDate, onCreated]);

  if (!open) return null;

  return (
    <div style={{
      position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)", zIndex: 1000,
      display: "flex", alignItems: "center", justifyContent: "center",
    }}>
      <div style={{
        background: "#fff", borderRadius: 12, width: 800, maxHeight: "90vh", overflow: "auto",
        boxShadow: "0 20px 60px rgba(0,0,0,0.3)", padding: 24,
      }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <h2 style={{ margin: 0, fontSize: 18, fontWeight: 700 }}>手工创建因子</h2>
          <button onClick={onClose} disabled={loading} style={{
            border: "none", background: "transparent", fontSize: 20, cursor: "pointer", color: "#9ca3af",
          }}>&times;</button>
        </div>

        {/* 因子名 + 描述 */}
        <div style={{ display: "flex", gap: 12, marginBottom: 12 }}>
          <div style={{ flex: 1 }}>
            <label style={{ fontSize: 12, fontWeight: 600, color: "#374151" }}>因子名称</label>
            <input
              value={factorName}
              onChange={e => setFactorName(e.target.value.toLowerCase().replace(/[^a-z0-9_]/g, ""))}
              placeholder="m_example_factor"
              style={{ width: "100%", padding: "8px 12px", border: "1px solid #d1d5db", borderRadius: 6, fontSize: 13, fontFamily: "monospace", marginTop: 4 }}
            />
          </div>
          <div style={{ flex: 2 }}>
            <label style={{ fontSize: 12, fontWeight: 600, color: "#374151" }}>描述（可选）</label>
            <input
              value={description}
              onChange={e => setDescription(e.target.value)}
              placeholder="例：5日收盘价动量"
              style={{ width: "100%", padding: "8px 12px", border: "1px solid #d1d5db", borderRadius: 6, fontSize: 13, marginTop: 4 }}
            />
          </div>
        </div>

        {/* 代码编辑器 */}
        <div style={{ marginBottom: 12 }}>
          <label style={{ fontSize: 12, fontWeight: 600, color: "#374151" }}>因子代码</label>
          <textarea
            value={codeText}
            onChange={e => setCodeText(e.target.value)}
            style={{
              width: "100%", height: 300, padding: 12, border: "1px solid #d1d5db", borderRadius: 6,
              fontFamily: "'Fira Code', 'Consolas', monospace", fontSize: 12, lineHeight: 1.5,
              resize: "vertical", marginTop: 4, background: "#f9fafb",
            }}
          />
        </div>

        {/* 数据集参考 */}
        <div style={{ marginBottom: 12 }}>
          <button
            onClick={() => setShowDatasets(!showDatasets)}
            style={{
              fontSize: 12, color: "#2563eb", cursor: "pointer", background: "transparent",
              border: "none", fontWeight: 600, padding: 0,
            }}
          >
            {showDatasets ? "▼" : "▶"} 数据集字段参考
          </button>
          {showDatasets && (
            <pre style={{
              marginTop: 8, padding: 12, background: "#f3f4f6", borderRadius: 6,
              fontSize: 11, lineHeight: 1.5, overflow: "auto", maxHeight: 200,
              whiteSpace: "pre-wrap", color: "#374151",
            }}>
              {DATASETS_REF}
            </pre>
          )}
        </div>

        {/* 操作按钮 */}
        <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
          <button
            onClick={handleValidate}
            disabled={loading || !factorName || factorName.length < 3}
            style={{
              padding: "8px 16px", fontSize: 13, borderRadius: 6, border: "none",
              background: "#059669", color: "#fff", fontWeight: 600, cursor: loading ? "not-allowed" : "pointer",
              opacity: loading ? 0.5 : 1,
            }}
          >验证代码</button>
          <button
            onClick={handleSave}
            disabled={loading || !factorName || factorName.length < 3}
            style={{
              padding: "8px 16px", fontSize: 13, borderRadius: 6, border: "none",
              background: "#2563eb", color: "#fff", fontWeight: 600, cursor: loading ? "not-allowed" : "pointer",
              opacity: loading ? 0.5 : 1,
            }}
          >保存入库</button>
          <button
            onClick={handleFullPipeline}
            disabled={loading || !factorName || factorName.length < 3 || !dataDate}
            title={dataDate ? `使用快照 ${dataDate} 写入官方指标和评级` : "请先选择因子库数据快照"}
            style={{
              padding: "8px 16px", fontSize: 13, borderRadius: 6, border: "none",
              background: "#7c3aed", color: "#fff", fontWeight: 600, cursor: loading ? "not-allowed" : "pointer",
              opacity: (loading || !dataDate) ? 0.5 : 1,
            }}
          >完整入库（含指标）</button>
        </div>

        {/* 状态 */}
        {loading && (
          <div style={{ padding: 10, background: "#dbeafe", borderRadius: 6, fontSize: 12, color: "#1e40af", marginBottom: 12 }}>
            {stage}
          </div>
        )}
        {error && (
          <div style={{ padding: 10, background: "#fef2f2", borderRadius: 6, fontSize: 12, color: "#991b1b", marginBottom: 12 }}>
            {error}
          </div>
        )}

        {/* 结果展示 */}
        {result && (
          <div style={{ padding: 12, background: "#f0fdf4", borderRadius: 6, fontSize: 12, color: "#065f46" }}>
            {result.type === "validate" && result.success && (
              <div>
                <strong>验证通过</strong> | shape: [{result.shape?.join(", ")}] | 耗时: {result.duration_sec}s
                {result.sample_data && (
                  <pre style={{ marginTop: 8, fontSize: 11, overflow: "auto", maxHeight: 100 }}>
                    {JSON.stringify(result.sample_data, null, 2)}
                  </pre>
                )}
              </div>
            )}
            {result.type === "save" && (
              <div>
                <strong>入库成功</strong>: {result.factor_name}
                {result.classification && (
                  <span> | 语义分类: {result.classification.category}</span>
                )}
              </div>
            )}
            {result.type === "pipeline" && result.success && (
              <div>
                <strong>完整流水线完成</strong>: {result.factor_name}
                {result.save?.classification && (
                  <div>语义分类: {result.save.classification.category}</div>
                )}
                {result.metrics?.success && (
                  <div style={{ marginTop: 4 }}>
                    官方独立指标已写入: inserted={result.metrics.db_result?.inserted ?? "-"} |
                    snapshot={result.metrics.snapshot_date ?? dataDate ?? "-"}
                  </div>
                )}
                {result.rating?.ok && (
                  <div style={{ marginTop: 4 }}>
                    官方评级已完成: success={result.rating.success_count ?? "-"} / total={result.rating.total_factors ?? "-"}
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
