"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ConfirmAction from "@/components/paper-v2/ConfirmAction";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { hmmTrainingApi, strategyPackageApi } from "@/lib/paper-v2/api";
import { hmmSnapshotLabel, shortHash, todayIso } from "@/lib/paper-v2/format";
import type { HmmConfig, HmmDailyCoefficientJob, HmmJob, HmmSnapshot, JsonObject, StrategyPackage } from "@/lib/paper-v2/types";

function uniqueStrings(values: Array<string | null | undefined>): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const value of values) {
    const text = String(value || "").trim();
    if (!text || seen.has(text)) continue;
    seen.add(text);
    out.push(text);
  }
  return out;
}

function presetKeysFromConfig(config: HmmConfig | undefined): string[] {
  const presets = config?.config_json?.signal_presets;
  if (!presets || typeof presets !== "object" || Array.isArray(presets)) return [];
  return Object.keys(presets as Record<string, unknown>);
}

function completedSnapshot(snapshot: HmmSnapshot): boolean {
  return ["COMPLETED", "SUCCESS", "SUCCEEDED", "READY"].includes(String(snapshot.status || "").toUpperCase());
}

function dailyPreviewSummary(value: JsonObject | null): JsonObject | null {
  if (!value) return null;
  const existing = value.existing_artifact_status;
  return {
    snapshot_id: value.snapshot_id,
    snapshot_display_name: value.snapshot_display_name,
    config_id: value.config_id,
    config_display_name: value.config_display_name,
    signal_preset: value.signal_preset,
    as_of_trade_date: value.as_of_trade_date,
    effective_trade_date: value.effective_trade_date,
    generation_mode: value.generation_mode,
    input_data_max_dates: value.input_data_max_dates,
    existing_artifact: value.existing_artifact,
    existing_artifact_status: existing && typeof existing === "object" && !Array.isArray(existing)
      ? {
          status: (existing as JsonObject).status,
          artifact_sha256: (existing as JsonObject).artifact_sha256,
          date_count: (existing as JsonObject).date_count,
        }
      : existing,
    requires_wsl: value.requires_wsl,
    confirm_boolean_required: value.confirm_boolean_required,
  };
}

function dailyJobSummary(value: JsonObject | null): JsonObject | null {
  if (!value) return null;
  return {
    job_id: value.job_id,
    snapshot_id: value.snapshot_id,
    config_id: value.config_id,
    signal_preset: value.signal_preset,
    as_of_trade_date: value.as_of_trade_date,
    effective_trade_date: value.effective_trade_date,
    generation_mode: value.generation_mode,
    status: value.status,
    result_status: value.result_status,
    requested_at: value.requested_at,
    started_at: value.started_at,
    completed_at: value.completed_at,
    input_data_max_dates: value.input_data_max_dates,
    artifact_sha256: value.artifact_sha256,
    error_message: compactError(value.error_message),
  };
}

function compactError(value: unknown): string {
  const text = String(value || "").trim();
  if (!text) return "-";
  const head = text.split("; stderr_tail=")[0].split("\n")[0].trim();
  if (!head) return "任务失败，详细诊断已记录在后台日志";
  return head.length > 180 ? `${head.slice(0, 180)}...` : head;
}

function PreviewSummary({ value }: { value: JsonObject | null }) {
  if (!value) return null;
  return (
    <div className="pv2-readable-panel">
      <div className="pv2-readable-table">
        <div className="pv2-readable-row"><div className="pv2-readable-key">状态</div><div className="pv2-readable-value">{String(value.status || value.result_status || "已生成")}</div></div>
        <div className="pv2-readable-row"><div className="pv2-readable-key">任务</div><div className="pv2-readable-value pv2-mono">{shortHash(value.job_id || value.task_id || value.request_id)}</div></div>
        <div className="pv2-readable-row"><div className="pv2-readable-key">日期</div><div className="pv2-readable-value">{String(value.as_of_date || value.as_of_trade_date || "-")}{" -> "}{String(value.effective_trade_date || value.end_date || "-")}</div></div>
        <div className="pv2-readable-row"><div className="pv2-readable-key">说明</div><div className="pv2-readable-value">{String(value.message || value.reason || value.error_message || "已返回，可复制诊断给 Codex 分析")}</div></div>
      </div>
    </div>
  );
}

export default function PaperV2ModelHmmPage() {
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [packageId, setPackageId] = useState("");
  const [modelState, setModelState] = useState<JsonObject | null>(null);
  const [modelPreview, setModelPreview] = useState<JsonObject | null>(null);
  const [modelJobs, setModelJobs] = useState<JsonObject[]>([]);
  const [asOfDate, setAsOfDate] = useState(todayIso());
  const [lookbackDays, setLookbackDays] = useState(756);

  const [configs, setConfigs] = useState<HmmConfig[]>([]);
  const [configId, setConfigId] = useState("");
  const [hmmAsOfDate, setHmmAsOfDate] = useState(todayIso());
  const [trainWindowYears, setTrainWindowYears] = useState(3);
  const [validationMonths, setValidationMonths] = useState(3);
  const [hmmPreview, setHmmPreview] = useState<JsonObject | null>(null);
  const [hmmJobs, setHmmJobs] = useState<HmmJob[]>([]);
  const [snapshots, setSnapshots] = useState<HmmSnapshot[]>([]);

  const [dailySnapshotId, setDailySnapshotId] = useState("");
  const [dailyPreset, setDailyPreset] = useState("preset_A");
  const [dailyAsOfDate, setDailyAsOfDate] = useState("");
  const [dailyEffectiveDate, setDailyEffectiveDate] = useState("");
  const [dailyPreview, setDailyPreview] = useState<JsonObject | null>(null);
  const [dailyResult, setDailyResult] = useState<JsonObject | null>(null);
  const [dailyJob, setDailyJob] = useState<HmmDailyCoefficientJob | null>(null);
  const [dailyJobs, setDailyJobs] = useState<HmmDailyCoefficientJob[]>([]);

  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const selectedPackage = useMemo(() => packages.find((item) => item.package_id === packageId), [packages, packageId]);
  const selectedConfig = useMemo(() => configs.find((item) => item.config_id === configId), [configs, configId]);
  const selectedDailySnapshot = useMemo(() => snapshots.find((item) => item.snapshot_id === dailySnapshotId), [snapshots, dailySnapshotId]);
  const snapshotLabelById = useMemo(() => new Map(snapshots.map((item) => [item.snapshot_id, hmmSnapshotLabel(item)])), [snapshots]);
  const readySnapshots = snapshots.filter(completedSnapshot);
  const dailyPresetOptions = useMemo(() => uniqueStrings([
    ...presetKeysFromConfig(selectedConfig),
    ...(selectedDailySnapshot?.coefficient_artifacts || []).map((item) => item.preset),
    "preset_A",
    "preset_B",
  ]), [selectedConfig, selectedDailySnapshot]);

  const loadPackages = useCallback(async () => {
    setError(null);
    try {
      const rows = await strategyPackageApi.list(undefined, 300);
      setPackages(rows);
      if (!packageId) setPackageId(rows[0]?.package_id || "");
    } catch (exc) {
      setError(exc);
    }
  }, [packageId]);

  const loadModelDetail = useCallback(async () => {
    if (!packageId) return;
    setError(null);
    try {
      const [state, jobs] = await Promise.all([
        strategyPackageApi.modelState(packageId),
        strategyPackageApi.modelRetrainJobs(packageId),
      ]);
      setModelState(state);
      setModelJobs(jobs);
    } catch (exc) {
      setError(exc);
    }
  }, [packageId]);

  const loadHmm = useCallback(async () => {
    setError(null);
    try {
      const rows = await hmmTrainingApi.configs();
      setConfigs(rows);
      if (!configId) setConfigId(rows[0]?.config_id || "");
    } catch (exc) {
      setError(exc);
    }
  }, [configId]);

  const loadHmmDetail = useCallback(async () => {
    if (!configId) return;
    setError(null);
    try {
      const [jobs, snapshotRows] = await Promise.all([
        hmmTrainingApi.jobs(configId),
        hmmTrainingApi.snapshots(configId),
      ]);
      setHmmJobs(jobs);
      setSnapshots(snapshotRows);
    } catch (exc) {
      setError(exc);
    }
  }, [configId]);

  const loadDailyJobs = useCallback(async () => {
    if (!dailySnapshotId) {
      setDailyJobs([]);
      return;
    }
    setError(null);
    try {
      const rows = await hmmTrainingApi.dailyCoefficientJobs(dailySnapshotId);
      setDailyJobs(rows);
    } catch (exc) {
      setError(exc);
    }
  }, [dailySnapshotId]);

  useEffect(() => { loadPackages(); loadHmm(); }, [loadPackages, loadHmm]);
  useEffect(() => { loadModelDetail(); }, [loadModelDetail]);
  useEffect(() => { loadHmmDetail(); }, [loadHmmDetail]);
  useEffect(() => { loadDailyJobs(); }, [loadDailyJobs]);
  useEffect(() => {
    if (dailySnapshotId && snapshots.some((item) => item.snapshot_id === dailySnapshotId)) return;
    setDailySnapshotId(readySnapshots[0]?.snapshot_id || "");
  }, [dailySnapshotId, readySnapshots, snapshots]);
  useEffect(() => {
    if (!dailyPresetOptions.includes(dailyPreset)) setDailyPreset(dailyPresetOptions[0] || "preset_A");
  }, [dailyPreset, dailyPresetOptions]);
  useEffect(() => {
    if (!dailyJob?.job_id) return;
    if (["COMPLETED", "FAILED"].includes(String(dailyJob.status || "").toUpperCase())) return;
    let active = true;
    const timer = window.setInterval(async () => {
      try {
        const next = await hmmTrainingApi.dailyCoefficientJob(dailyJob.job_id);
        if (!active) return;
        setDailyJob(next);
        setDailyResult(next as unknown as JsonObject);
        if (["COMPLETED", "FAILED"].includes(String(next.status || "").toUpperCase())) {
          await loadHmmDetail();
          await loadDailyJobs();
        }
      } catch (exc) {
        if (active) setError(exc);
      }
    }, 2000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [dailyJob?.job_id, dailyJob?.status, loadDailyJobs, loadHmmDetail]);

  async function previewModelRetrain() {
    setBusy(true);
    setError(null);
    try {
      setModelPreview(await strategyPackageApi.modelRetrainPreview(packageId, { as_of_date: asOfDate, lookback_days: lookbackDays }));
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function startModelRetrain() {
    setBusy(true);
    setError(null);
    try {
      const job = await strategyPackageApi.modelRetrainStart(packageId, {
        as_of_date: asOfDate,
        lookback_days: lookbackDays,
        job_type: "rolling_retrain",
        confirm_retrain: true,
      });
      setModelPreview(job);
      await loadModelDetail();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function previewHmmRolling() {
    setBusy(true);
    setError(null);
    try {
      setHmmPreview(await hmmTrainingApi.previewRolling(configId, {
        as_of_date: hmmAsOfDate,
        train_window_years: trainWindowYears,
        validation_window_months: validationMonths,
      }));
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function triggerHmmRolling() {
    setBusy(true);
    setError(null);
    try {
      const job = await hmmTrainingApi.triggerRolling(configId, {
        as_of_date: hmmAsOfDate,
        train_window_years: trainWindowYears,
        validation_window_months: validationMonths,
        confirm_retrain: true,
      });
      setHmmPreview(job as unknown as JsonObject);
      await loadHmmDetail();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  function dailyPayload(): JsonObject {
    return {
      signal_preset: dailyPreset,
      ...(dailyAsOfDate ? { as_of_date: dailyAsOfDate } : {}),
      ...(dailyEffectiveDate ? { effective_trade_date: dailyEffectiveDate } : {}),
    };
  }

  async function previewDailyCoefficients() {
    setBusy(true);
    setError(null);
    try {
      setDailyPreview(await hmmTrainingApi.previewDailyCoefficients(dailySnapshotId, dailyPayload()));
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function generateDailyCoefficients() {
    setBusy(true);
    setError(null);
    try {
      const job = await hmmTrainingApi.startDailyCoefficientJob(dailySnapshotId, {
        ...dailyPayload(),
        confirm_generate: true,
      });
      setDailyJob(job);
      setDailyResult(job as unknown as JsonObject);
      await loadDailyJobs();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  const modelStatus = String(modelState?.staleness_status || "unknown");

  return (
    <main>
      <ErrorPanel error={error} title="模型与 HMM 操作失败" />
      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="策略包" value={packages.length} />
        <MetricCard label="模型状态" value={modelStatus} tone={modelStatus.includes("STALE") ? "warning" : "success"} />
        <MetricCard label="HMM 配置" value={configs.length} />
        <MetricCard label="HMM 模型缓存" value={readySnapshots.length} hint="运行时按模型配置自动选择最新可用模型并计算每日系数" tone={readySnapshots.length ? "success" : "warning"} />
      </div>

      <div style={{ display: "grid", gap: 16 }}>
        <SectionCard title="StrategyPackage 模型新鲜度" eyebrow="人工重训">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>策略包</label><select className="pv2-select" data-testid="model-package" value={packageId} onChange={(event) => setPackageId(event.target.value)}><option value="">选择策略包</option>{packages.map((item) => <option key={item.package_id} value={item.package_id}>{item.package_name} / {item.package_status}</option>)}</select></div>
            <div className="pv2-field"><label>截至日期</label><input className="pv2-input" data-testid="model-as-of-date" type="date" value={asOfDate} onChange={(event) => setAsOfDate(event.target.value)} /></div>
            <div className="pv2-field"><label>回看天数</label><input className="pv2-input" data-testid="model-lookback-days" type="number" min={30} value={lookbackDays} onChange={(event) => setLookbackDays(Number(event.target.value))} /></div>
          </div>
          <div className="pv2-chip-row" style={{ marginTop: 12 }}>
            <span className="pv2-chip">策略包：{selectedPackage?.package_name || "-"}</span>
            <span className="pv2-chip">manifest：{shortHash(selectedPackage?.manifest_sha256)}</span>
          </div>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" data-testid="model-retrain-preview" disabled={busy || !packageId} onClick={previewModelRetrain} type="button">预览重训方案</button>
            <ConfirmAction label="提交重训任务" disabled={busy || !packageId} danger confirmText={packageId || "-"} onConfirm={startModelRetrain} mode="dialog" />
          </div>
          <h3>当前模型状态</h3>
          {modelState ? (
            <div className="pv2-readable-panel">
              <div className="pv2-readable-table">
                <div className="pv2-readable-row"><div className="pv2-readable-key">策略包</div><div className="pv2-readable-value">{String(selectedPackage?.package_name || modelState.package_name || "-")}</div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">策略包ID</div><div className="pv2-readable-value pv2-mono">{String(selectedPackage?.package_id || modelState.package_id || "-")}</div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">模型状态</div><div className="pv2-readable-value"><StatusBadge status={modelStatus} /></div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">训练区间</div><div className="pv2-readable-value">{String(modelState.train_start_date || "-")}{" -> "}{String(modelState.train_end_date || "-")}</div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">最近重训</div><div className="pv2-readable-value">{String(modelState.last_retrained_at || modelState.trained_at || "-")}</div></div>
              </div>
            </div>
          ) : <div className="pv2-muted">选择策略包后加载模型状态。</div>}
          {modelPreview ? <><h3>预览 / 任务响应</h3><PreviewSummary value={modelPreview} /></> : null}
        </SectionCard>

        <SectionCard title="HMM 滚动训练" eyebrow="WSL 执行维护">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>HMM 配置</label><select className="pv2-select" data-testid="hmm-config" value={configId} onChange={(event) => setConfigId(event.target.value)}><option value="">选择配置</option>{configs.map((item) => <option key={item.config_id} value={item.config_id}>{item.display_name} / {item.model_type}</option>)}</select></div>
            <div className="pv2-field"><label>截至日期</label><input className="pv2-input" data-testid="hmm-as-of-date" type="date" value={hmmAsOfDate} onChange={(event) => setHmmAsOfDate(event.target.value)} /></div>
            <div className="pv2-field"><label>验证月数</label><input className="pv2-input" data-testid="hmm-validation-months" type="number" min={1} max={3} value={validationMonths} onChange={(event) => setValidationMonths(Number(event.target.value))} /></div>
            <div className="pv2-field"><label>训练年数</label><input className="pv2-input" data-testid="hmm-train-years" type="number" min={1} step="0.5" value={trainWindowYears} onChange={(event) => setTrainWindowYears(Number(event.target.value))} /></div>
          </div>
          <div className="pv2-chip-row" style={{ marginTop: 12 }}>
            <span className="pv2-chip">配置：{selectedConfig?.display_name || "-"}</span>
            <span className="pv2-chip">推荐验证期：3 个月</span>
          </div>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" data-testid="hmm-rolling-preview" disabled={busy || !configId} onClick={previewHmmRolling} type="button">预览滚动切分</button>
            <ConfirmAction label="触发 HMM 滚动训练" disabled={busy || !configId} danger confirmText={configId || "-"} onConfirm={triggerHmmRolling} mode="dialog" />
          </div>
          {hmmPreview ? <PreviewSummary value={hmmPreview} /> : null}
        </SectionCard>
      </div>

      <NoticePanel title="HMM 每日系数由平台自动计算" tone="info">
        选股、AIstock 模拟盘和 MiniQMT 模拟盘只选择 HMM 模型配置与 preset。每日系数按交易日首次运行时自动计算并写入缓存，同一天后续运行直接复用；本页只展示模型缓存状态和历史任务审计。
      </NoticePanel>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="模型重训任务" eyebrow="StrategyPackage">
          <PaperTable
            rows={modelJobs}
            empty="暂无模型重训任务。"
            columns={[
              { key: "job", header: "任务", render: (row) => <span className="pv2-mono">{shortHash(row.job_id)}</span> },
              { key: "type", header: "类型", render: (row) => String(row.job_type || "-") },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={String(row.status || "unknown")} /> },
              { key: "start", header: "训练开始", render: (row) => String(row.requested_train_start_date || "-") },
              { key: "end", header: "训练结束", render: (row) => String(row.requested_train_end_date || "-") },
            ]}
          />
        </SectionCard>

        <SectionCard title="HMM 训练任务" eyebrow="训练中心">
          <PaperTable
            rows={hmmJobs}
            empty="暂无 HMM 任务。"
            columns={[
              { key: "job", header: "任务", render: (row) => <span className="pv2-mono">{shortHash(row.job_id)}</span> },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
              { key: "snapshot", header: "模型缓存", render: (row) => row.snapshot_id ? <span title={row.snapshot_id}>{snapshotLabelById.get(row.snapshot_id) || shortHash(row.snapshot_id)}</span> : "-" },
              { key: "started", header: "开始时间", render: (row) => row.started_at || "-" },
              { key: "error", header: "错误", render: (row) => <span title={row.error_message || ""}>{compactError(row.error_message)}</span> },
            ]}
          />
        </SectionCard>
      </div>

      <SectionCard title="HMM 每日系数任务" eyebrow="异步生成审计">
        <PaperTable
          rows={dailyJobs}
          empty="暂无每日系数生成任务。"
          columns={[
            { key: "job", header: "任务", render: (row) => <span className="pv2-mono">{shortHash(row.job_id)}</span> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "preset", header: "预设", render: (row) => row.signal_preset },
            { key: "dates", header: "日期", render: (row) => `${row.as_of_trade_date} -> ${row.effective_trade_date}` },
            { key: "result", header: "产物状态", render: (row) => row.result_status || "-" },
            { key: "sha", header: "产物哈希", render: (row) => shortHash(row.artifact_sha256) },
            { key: "error", header: "错误", render: (row) => <span title={row.error_message || ""}>{compactError(row.error_message)}</span> },
          ]}
        />
      </SectionCard>

      <SectionCard title="HMM 模型缓存状态" eyebrow="最新可用模型与已缓存日期">
        <PaperTable
          rows={snapshots}
          empty="暂无 HMM 模型缓存。启用 HMM 的运行会在缺少可用模型时明确失败。"
          columns={[
            { key: "snapshot", header: "模型", render: (row) => <span title={row.snapshot_id}>{hmmSnapshotLabel(row)}</span> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "trained", header: "训练时间", render: (row) => row.trained_at },
            { key: "sectors", header: "行业数", render: (row) => row.sector_count },
            { key: "artifacts", header: "最新缓存日期", render: (row) => (row.coefficient_artifacts || []).map((item) => `${item.preset}:${item.end_date || item.start_date}`).join("；") || "运行时自动生成" },
            { key: "asset", header: "模型产物", render: (row) => row.model_path ? <StatusBadge status="READY" /> : <StatusBadge status="NO_DATA" /> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
