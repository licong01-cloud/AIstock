"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ConfirmAction from "@/components/paper-v2/ConfirmAction";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { hmmTrainingApi, strategyPackageApi } from "@/lib/paper-v2/api";
import { hmmSnapshotLabel, shortHash, todayIso } from "@/lib/paper-v2/format";
import type { HmmConfig, HmmJob, HmmSnapshot, JsonObject, StrategyPackage } from "@/lib/paper-v2/types";

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

  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const selectedPackage = useMemo(() => packages.find((item) => item.package_id === packageId), [packages, packageId]);
  const selectedConfig = useMemo(() => configs.find((item) => item.config_id === configId), [configs, configId]);
  const snapshotLabelById = useMemo(() => new Map(snapshots.map((item) => [item.snapshot_id, hmmSnapshotLabel(item)])), [snapshots]);

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

  useEffect(() => { loadPackages(); loadHmm(); }, [loadPackages, loadHmm]);
  useEffect(() => { loadModelDetail(); }, [loadModelDetail]);
  useEffect(() => { loadHmmDetail(); }, [loadHmmDetail]);

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
        confirm_text: packageId,
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
        confirm_text: configId,
      });
      setHmmPreview(job as unknown as JsonObject);
      await loadHmmDetail();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  const modelStatus = String(modelState?.staleness_status || "unknown");
  const readySnapshots = snapshots.filter((snapshot) => String(snapshot.status).toUpperCase() === "COMPLETED").length;

  return (
    <main>
      <ErrorPanel error={error} title="模型与 HMM 操作失败" />
      <NoticePanel title="人工维护边界" tone="info">
        模拟盘 v2 和选股中心不会在交易运行中训练模型，只消费 StrategyPackage 模型产物和已完成的 HMM 快照。重训练/滚动训练按钮只会在确认后排队或触发明确的维护流程。
      </NoticePanel>

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="策略包" value={packages.length} />
        <MetricCard label="模型状态" value={modelStatus} tone={modelStatus.includes("STALE") ? "warning" : "success"} />
        <MetricCard label="HMM 配置" value={configs.length} />
        <MetricCard label="可用快照" value={readySnapshots} tone={readySnapshots ? "success" : "warning"} />
      </div>

      <div className="pv2-grid pv2-grid-main">
        <SectionCard title="StrategyPackage 模型新鲜度" eyebrow="人工重训练">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>策略包</label><select className="pv2-select" data-testid="model-package" value={packageId} onChange={(event) => setPackageId(event.target.value)}><option value="">选择策略包</option>{packages.map((item) => <option key={item.package_id} value={item.package_id}>{item.package_name} / {item.package_status}</option>)}</select></div>
            <div className="pv2-field"><label>截至日期</label><input className="pv2-input" data-testid="model-as-of-date" type="date" value={asOfDate} onChange={(event) => setAsOfDate(event.target.value)} /></div>
            <div className="pv2-field"><label>回看天数</label><input className="pv2-input" data-testid="model-lookback-days" type="number" min={30} value={lookbackDays} onChange={(event) => setLookbackDays(Number(event.target.value))} /></div>
          </div>
          <div className="pv2-chip-row" style={{ marginTop: 12 }}>
            <span className="pv2-chip">package: {selectedPackage?.package_name || "-"}</span>
            <span className="pv2-chip">manifest: {shortHash(selectedPackage?.manifest_sha256)}</span>
          </div>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" data-testid="model-retrain-preview" disabled={busy || !packageId} onClick={previewModelRetrain} type="button">预览重训练</button>
            <ConfirmAction label="提交重训练任务" disabled={busy || !packageId} danger confirmText={packageId || "-"} onConfirm={startModelRetrain} />
          </div>
          <h3>当前模型状态</h3>
          {modelState ? <JsonPanel value={modelState} /> : <div className="pv2-muted">选择策略包后加载模型状态。</div>}
          {modelPreview ? <><h3>预览 / 任务响应</h3><JsonPanel value={modelPreview} /></> : null}
        </SectionCard>

        <SectionCard title="HMM 滚动训练" eyebrow="WSL 执行维护">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>HMM 配置</label><select className="pv2-select" data-testid="hmm-config" value={configId} onChange={(event) => setConfigId(event.target.value)}><option value="">选择配置</option>{configs.map((item) => <option key={item.config_id} value={item.config_id}>{item.display_name} / {item.model_type}</option>)}</select></div>
            <div className="pv2-field"><label>截至日期</label><input className="pv2-input" data-testid="hmm-as-of-date" type="date" value={hmmAsOfDate} onChange={(event) => setHmmAsOfDate(event.target.value)} /></div>
            <div className="pv2-field"><label>验证月数</label><input className="pv2-input" data-testid="hmm-validation-months" type="number" min={1} max={3} value={validationMonths} onChange={(event) => setValidationMonths(Number(event.target.value))} /></div>
            <div className="pv2-field"><label>训练年数</label><input className="pv2-input" data-testid="hmm-train-years" type="number" min={1} step="0.5" value={trainWindowYears} onChange={(event) => setTrainWindowYears(Number(event.target.value))} /></div>
          </div>
          <div className="pv2-chip-row" style={{ marginTop: 12 }}>
            <span className="pv2-chip">config: {selectedConfig?.display_name || "-"}</span>
            <span className="pv2-chip">建议验证期：3 个月</span>
          </div>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" data-testid="hmm-rolling-preview" disabled={busy || !configId} onClick={previewHmmRolling} type="button">预览滚动切分</button>
            <ConfirmAction label="触发 HMM 滚动训练" disabled={busy || !configId} danger confirmText={configId || "-"} onConfirm={triggerHmmRolling} />
          </div>
          {hmmPreview ? <JsonPanel value={hmmPreview} /> : null}
        </SectionCard>
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="模型重训练任务" eyebrow="StrategyPackage">
          <PaperTable
            rows={modelJobs}
            empty="暂无模型重训练任务。"
            columns={[
              { key: "job", header: "任务", render: (row) => <span className="pv2-mono">{shortHash(row.job_id)}</span> },
              { key: "type", header: "类型", render: (row) => String(row.job_type || "-") },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={String(row.status || "unknown")} /> },
              { key: "start", header: "训练开始", render: (row) => String(row.requested_train_start_date || "-") },
              { key: "end", header: "训练结束", render: (row) => String(row.requested_train_end_date || "-") },
            ]}
          />
        </SectionCard>

        <SectionCard title="HMM 任务" eyebrow="训练中心">
          <PaperTable
            rows={hmmJobs}
            empty="暂无 HMM 任务。"
            columns={[
              { key: "job", header: "任务", render: (row) => <span className="pv2-mono">{shortHash(row.job_id)}</span> },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
              { key: "snapshot", header: "快照", render: (row) => row.snapshot_id ? <span title={row.snapshot_id}>{snapshotLabelById.get(row.snapshot_id) || shortHash(row.snapshot_id)}</span> : "-" },
              { key: "started", header: "开始时间", render: (row) => row.started_at || "-" },
              { key: "error", header: "错误", render: (row) => row.error_message || "-" },
            ]}
          />
        </SectionCard>
      </div>

      <SectionCard title="HMM 快照" eyebrow="运行时产物">
        <PaperTable
          rows={snapshots}
          empty="暂无 HMM 快照。启用 HMM 的选股运行会在缺少已完成快照和系数产物时 fail-fast。"
          columns={[
            { key: "snapshot", header: "快照版本", render: (row) => <span title={row.snapshot_id}>{hmmSnapshotLabel(row)}</span> },
            { key: "id", header: "ID", render: (row) => <span className="pv2-mono">{shortHash(row.snapshot_id)}</span> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "trained", header: "训练时间", render: (row) => row.trained_at },
            { key: "sectors", header: "行业数", render: (row) => row.sector_count },
            { key: "path", header: "路径", render: (row) => <span className="pv2-mono">{row.model_path}</span> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
