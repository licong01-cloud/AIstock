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

  useEffect(() => { loadPackages(); loadHmm(); }, [loadPackages, loadHmm]);
  useEffect(() => { loadModelDetail(); }, [loadModelDetail]);
  useEffect(() => { loadHmmDetail(); }, [loadHmmDetail]);
  useEffect(() => {
    if (dailySnapshotId && snapshots.some((item) => item.snapshot_id === dailySnapshotId)) return;
    setDailySnapshotId(readySnapshots[0]?.snapshot_id || "");
  }, [dailySnapshotId, readySnapshots, snapshots]);
  useEffect(() => {
    if (!dailyPresetOptions.includes(dailyPreset)) setDailyPreset(dailyPresetOptions[0] || "preset_A");
  }, [dailyPreset, dailyPresetOptions]);

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
      const result = await hmmTrainingApi.generateDailyCoefficients(dailySnapshotId, {
        ...dailyPayload(),
        confirm_text: dailySnapshotId,
      });
      setDailyResult(result);
      await loadHmmDetail();
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
      <NoticePanel title="人工维护边界" tone="info">
        Paper Trading v2 与选股中心只消费已经冻结的 StrategyPackage 模型产物和已完成的 HMM 快照。模型重训、HMM 滚动训练、每日系数生成都必须由人工明确触发，并且缺少数据或产物时直接失败。
      </NoticePanel>

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="策略包" value={packages.length} />
        <MetricCard label="模型状态" value={modelStatus} tone={modelStatus.includes("STALE") ? "warning" : "success"} />
        <MetricCard label="HMM 配置" value={configs.length} />
        <MetricCard label="可用快照" value={readySnapshots.length} tone={readySnapshots.length ? "success" : "warning"} />
      </div>

      <div className="pv2-grid pv2-grid-main">
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
            <ConfirmAction label="提交重训任务" disabled={busy || !packageId} danger confirmText={packageId || "-"} onConfirm={startModelRetrain} />
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
            <span className="pv2-chip">配置：{selectedConfig?.display_name || "-"}</span>
            <span className="pv2-chip">推荐验证期：3 个月</span>
          </div>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" data-testid="hmm-rolling-preview" disabled={busy || !configId} onClick={previewHmmRolling} type="button">预览滚动切分</button>
            <ConfirmAction label="触发 HMM 滚动训练" disabled={busy || !configId} danger confirmText={configId || "-"} onConfirm={triggerHmmRolling} />
          </div>
          {hmmPreview ? <JsonPanel value={hmmPreview} /> : null}
        </SectionCard>
      </div>

      <SectionCard title="HMM 每日系数生成" eyebrow="实盘/模拟盘预测">
        <NoticePanel title="PIT 规则" tone="warning">
          对交易日 D 使用的 HMM 系数必须由 D 之前最近一个已完成交易日的数据生成。系统只创建新的系数文件，不修改已有 HMM 模型权重，也不会在缺系数时回退到中性系数。
        </NoticePanel>
        <div className="pv2-form-grid" style={{ marginTop: 12 }}>
          <div className="pv2-field"><label>快照版本</label><select className="pv2-select" data-testid="hmm-daily-snapshot" value={dailySnapshotId} onChange={(event) => setDailySnapshotId(event.target.value)}><option value="">选择已完成快照</option>{readySnapshots.map((item) => <option key={item.snapshot_id} value={item.snapshot_id}>{hmmSnapshotLabel(item)}</option>)}</select></div>
          <div className="pv2-field"><label>信号预设</label><select className="pv2-select" data-testid="hmm-daily-preset" value={dailyPreset} onChange={(event) => setDailyPreset(event.target.value)}>{dailyPresetOptions.map((item) => <option key={item} value={item}>{item}</option>)}</select></div>
          <div className="pv2-field"><label>数据截至日</label><input className="pv2-input" data-testid="hmm-daily-as-of-date" type="date" value={dailyAsOfDate} onChange={(event) => setDailyAsOfDate(event.target.value)} /><small>留空表示后端自动使用最新已完成公共数据日。</small></div>
          <div className="pv2-field"><label>生效交易日</label><input className="pv2-input" data-testid="hmm-daily-effective-date" type="date" value={dailyEffectiveDate} onChange={(event) => setDailyEffectiveDate(event.target.value)} /><small>留空表示后端自动选择数据截至日后的下一个交易日。</small></div>
        </div>
        <div className="pv2-chip-row" style={{ marginTop: 12 }}>
          <span className="pv2-chip">快照：{selectedDailySnapshot ? hmmSnapshotLabel(selectedDailySnapshot) : "-"}</span>
          <span className="pv2-chip">产物数：{selectedDailySnapshot?.coefficient_artifacts?.length || 0}</span>
        </div>
        <div className="pv2-row-actions" style={{ marginTop: 12 }}>
          <button className="pv2-button" data-testid="hmm-daily-preview" disabled={busy || !dailySnapshotId || !dailyPreset} onClick={previewDailyCoefficients} type="button">预览每日系数</button>
          <ConfirmAction testId="hmm-daily-generate" label="生成每日系数" disabled={busy || !dailySnapshotId || !dailyPreset} danger confirmText={dailySnapshotId || "-"} onConfirm={generateDailyCoefficients} />
        </div>
        {dailyPreview ? <><h3>每日系数生成预览</h3><JsonPanel value={dailyPreview} /></> : null}
        {dailyResult ? <><h3>每日系数生成结果</h3><JsonPanel value={dailyResult} /></> : null}
      </SectionCard>

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
              { key: "snapshot", header: "快照", render: (row) => row.snapshot_id ? <span title={row.snapshot_id}>{snapshotLabelById.get(row.snapshot_id) || shortHash(row.snapshot_id)}</span> : "-" },
              { key: "started", header: "开始时间", render: (row) => row.started_at || "-" },
              { key: "error", header: "错误", render: (row) => row.error_message || "-" },
            ]}
          />
        </SectionCard>
      </div>

      <SectionCard title="HMM 快照与系数产物" eyebrow="运行时产物">
        <PaperTable
          rows={snapshots}
          empty="暂无 HMM 快照。启用 HMM 的选股运行会在缺少快照或系数产物时 fail-fast。"
          columns={[
            { key: "snapshot", header: "快照版本", render: (row) => <span title={row.snapshot_id}>{hmmSnapshotLabel(row)}</span> },
            { key: "id", header: "ID", render: (row) => <span className="pv2-mono">{shortHash(row.snapshot_id)}</span> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "trained", header: "训练时间", render: (row) => row.trained_at },
            { key: "sectors", header: "行业数", render: (row) => row.sector_count },
            { key: "artifacts", header: "系数覆盖", render: (row) => (row.coefficient_artifacts || []).map((item) => `${item.preset}:${item.start_date}~${item.end_date}`).join("；") || "无" },
            { key: "asset", header: "模型产物", render: (row) => row.model_path ? <StatusBadge status="READY" /> : <StatusBadge status="NO_DATA" /> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
