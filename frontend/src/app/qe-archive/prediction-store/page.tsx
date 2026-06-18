"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { formatCompact, formatNumber, shortHash } from "@/lib/paper-v2/format";
import {
  API_BASE,
  type ArchivedRunListItem,
  type ArchiveSummary,
  qeArchiveApi,
} from "@/lib/qe-archive/api";
import {
  type JsonObject,
  type PredictionPreview,
  type PredictionStoreArtifact,
  type PredictionStoreHealth,
  type PredictionStoreManifest,
  type PredictionStorePointer,
  predictionStoreApi,
} from "@/lib/prediction-store/api";

type PointerRow = {
  run: ArchivedRunListItem;
  pointer?: PredictionStorePointer;
  pointerError?: string;
};

type FailureMarkerRow = {
  run_id: string;
  status: string;
  message: string;
  source: string;
  written_at?: string | null;
};

const SAMPLE_LIMIT_OPTIONS = [50, 100, 200, 500];

function n(value: unknown): number {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function formatDateTime(value: unknown): string {
  const text = String(value || "");
  return text ? text.replace("T", " ").slice(0, 19) : "-";
}

function formatBytes(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  const bytes = Number(value);
  if (!Number.isFinite(bytes)) return "-";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = bytes;
  let unitIndex = 0;
  while (Math.abs(size) >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(unitIndex === 0 ? 0 : 2)} ${units[unitIndex]}`;
}

function formatPercentValue(value: number): string {
  if (!Number.isFinite(value)) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

function errorText(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error || "unknown error");
}

function runLabel(run: ArchivedRunListItem): string {
  const source = run.loop_id || run.experiment_id || run.task_id || run.logical_experiment_id || run.run_id;
  const loop = run.loop_index ? ` / Loop ${run.loop_index}` : "";
  return `${shortHash(run.run_id)} / ${run.run_type || "-"} / ${source || "-"}${loop}`;
}

function manifestOf(row: PointerRow): PredictionStoreManifest | null {
  return row.pointer?.prediction_store_manifest || null;
}

function artifactsOf(row: PointerRow): PredictionStoreArtifact[] {
  const manifest = manifestOf(row);
  if (Array.isArray(manifest?.artifacts)) return manifest.artifacts;
  if (Array.isArray(row.pointer?.artifacts)) return row.pointer.artifacts;
  return [];
}

function pointerAvailable(row: PointerRow): boolean {
  if (row.pointerError) return false;
  const status = String(row.pointer?.pointer_status || "").toLowerCase();
  return Boolean(
    status === "available"
      || status === "store_only"
      || (row.pointer?.mlflow_artifact_uri && !row.pointer?.manifest_error),
  );
}

function artifactTypeLabel(value: unknown): string {
  const raw = String(value || "");
  if (raw === "prediction") return "pred.pkl";
  if (raw === "model_params") return "params.pkl";
  if (raw === "label") return "label.pkl";
  return raw || "-";
}

function artifactUri(item: PredictionStoreArtifact): string {
  return String(item.uri || item.artifact_uri || "-");
}

function uploadMetadata(row: PointerRow): JsonObject | null {
  const manifestMetadata = manifestOf(row)?.metadata;
  if (isObject(manifestMetadata)) return manifestMetadata;
  const sourceMetadata = row.pointer?.source?.metadata;
  if (isObject(sourceMetadata) && isObject(sourceMetadata.prediction_store)) return sourceMetadata.prediction_store;
  return null;
}

function uploadStatus(row: PointerRow): string {
  const metadata = uploadMetadata(row);
  if (typeof metadata?.status === "string") return metadata.status;
  if (pointerAvailable(row)) return "success";
  if (row.pointerError) return "pointer_error";
  return String(row.pointer?.pointer_status || "missing");
}

function uploadSwitchState(rows: PointerRow[]): { label: string; hint: string; tone: "success" | "warning" | "neutral" } {
  const states = rows
    .map((row) => uploadMetadata(row)?.upload_enabled)
    .filter((value) => value !== undefined && value !== null);
  if (states.some((value) => value === true || value === "true")) {
    return { label: "enabled", hint: "采样 run 的 marker 显示 upload_enabled=true", tone: "success" };
  }
  if (states.some((value) => value === false || value === "false")) {
    return { label: "disabled", hint: "采样 run 的 marker 显示 upload_enabled=false", tone: "neutral" };
  }
  return {
    label: "unknown",
    hint: "现有只读接口不直接回传节点 env；等待新 run marker 或后端 health 扩展后可精确显示",
    tone: "warning",
  };
}

function missingArtifactHint(row: PointerRow): string {
  const metadata = uploadMetadata(row);
  const missing = metadata?.missing_artifacts;
  if (Array.isArray(missing) && missing.length) return missing.map(String).join(", ");
  return "-";
}

function artifactByType(row: PointerRow, type: string): PredictionStoreArtifact | undefined {
  return artifactsOf(row).find((item) => String(item.artifact_type || "") === type);
}

function artifactDownloadHref(row: PointerRow, artifact: PredictionStoreArtifact): string | null {
  const artifactType = String(artifact.artifact_type || "");
  if (artifactType === "prediction") return predictionStoreApi.downloadUrl(row.run.run_id, "prediction");
  if (artifactType === "model_params") return predictionStoreApi.downloadUrl(row.run.run_id, "model_params");
  if (artifactType === "label") return predictionStoreApi.downloadUrl(row.run.run_id, "label");
  return null;
}

function latestFailureMarkers(rows: PointerRow[]): FailureMarkerRow[] {
  const markers: FailureMarkerRow[] = [];
  for (const row of rows) {
    const metadata = uploadMetadata(row);
    if (metadata && String(metadata.status || "").toLowerCase() === "failed") {
      markers.push({
        run_id: row.run.run_id,
        status: "failed",
        message: String(metadata.error || "prediction-store upload marker reported failed"),
        source: "qe_prediction_store_upload.json",
        written_at: typeof metadata.written_at === "string" ? metadata.written_at : null,
      });
    }
    if (row.pointer?.manifest_error) {
      markers.push({
        run_id: row.run.run_id,
        status: "manifest_error",
        message: row.pointer.manifest_error,
        source: "prediction-store pointer",
      });
    }
    if (row.pointerError) {
      markers.push({
        run_id: row.run.run_id,
        status: "pointer_error",
        message: row.pointerError,
        source: "prediction-store pointer",
      });
    }
    for (const artifact of artifactsOf(row)) {
      const parserError = artifact.parser_error || (isObject(artifact.metadata) ? artifact.metadata.parser_error : null);
      if (parserError) {
        markers.push({
          run_id: row.run.run_id,
          status: String(artifact.parser_status || "parser_error"),
          message: String(parserError),
          source: `manifest.${artifact.artifact_type || artifact.artifact_name || "artifact"}`,
        });
      }
    }
  }
  return markers.slice(0, 12);
}

function PreviewPanel({ preview }: { preview: PredictionPreview | null }) {
  if (!preview) {
    return <div className="pv2-help">选择一个带 pred.pkl 指针的 run 后，可通过只读 preview 接口查看 head，不会触发上传或写入。</div>;
  }
  const rows = preview.head || [];
  const columns = preview.columns || [];
  return (
    <div className="pv2-readable-list">
      <div className="pv2-grid pv2-grid-3">
        <MetricCard label="Pred 行数" value={formatCompact(preview.row_count || 0, 0)} tone="info" />
        <MetricCard label="Preview 行数" value={formatCompact(rows.length, 0)} />
        <MetricCard label="文件大小" value={formatBytes(preview.size_bytes)} />
      </div>
      <div className="pv2-table-wrap">
        <table className="pv2-table">
          <thead>
            <tr>{columns.map((column) => <th key={column}>{column}</th>)}</tr>
          </thead>
          <tbody>
            {rows.length ? rows.map((row, index) => (
              <tr key={index}>
                {columns.map((column) => (
                  <td key={column}>{String(row[column] ?? "-")}</td>
                ))}
              </tr>
            )) : (
              <tr><td className="pv2-empty-cell" colSpan={Math.max(columns.length, 1)}>preview 为空</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function HealthPanel({ health }: { health: PredictionStoreHealth | null }) {
  if (!health) return <div className="pv2-help">正在等待 model_store_health 只读接口返回。</div>;
  const store = health.artifact_store || {};
  const disk = store.disk || {};
  const policy = store.policy || {};
  return (
    <div className="pv2-readable-list">
      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="Store Root" value={store.store_root ? shortHash(store.store_root, 18) : "-"} hint={store.store_root || "-"} tone={store.exists ? "success" : "warning"} />
        <MetricCard label="占用" value={formatBytes(disk.used_bytes)} hint={`free ${formatBytes(disk.free_bytes)} / total ${formatBytes(disk.total_bytes)}`} tone={disk.error ? "danger" : "info"} />
        <MetricCard label="MLflow PG" value={health.mlflow_pg_enabled ? "enabled" : "deferred"} hint={String(health.tracking_backend || "deferred_to_m4")} />
        <MetricCard label="存储策略" value={store.scheme || "-"} hint={`policy ${JSON.stringify(policy)}`} tone="info" />
      </div>
      <div className="pv2-readable-panel">
        <div className="pv2-readable-table">
          <div className="pv2-readable-row"><div className="pv2-readable-key">合规判断</div><div className="pv2-readable-value">以后端 health 返回为准；若 root 违反后端策略，health 会返回错误而不是前端硬编码盘符。</div></div>
          <div className="pv2-readable-row"><div className="pv2-readable-key">E: 拒绝策略</div><div className="pv2-readable-value pv2-mono">{JSON.stringify(policy)}</div></div>
          {disk.error ? <div className="pv2-readable-row"><div className="pv2-readable-key">Disk Error</div><div className="pv2-readable-value">{disk.error}</div></div> : null}
        </div>
      </div>
    </div>
  );
}

export default function PredictionStorePage() {
  const [summary, setSummary] = useState<ArchiveSummary | null>(null);
  const [health, setHealth] = useState<PredictionStoreHealth | null>(null);
  const [rows, setRows] = useState<PointerRow[]>([]);
  const [sampleLimit, setSampleLimit] = useState(100);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [selectedRunId, setSelectedRunId] = useState("");
  const [preview, setPreview] = useState<PredictionPreview | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextSummary, nextHealth, nextRuns] = await Promise.all([
        qeArchiveApi.health(),
        predictionStoreApi.health(),
        qeArchiveApi.runs({ limit: sampleLimit, status: "all" }),
      ]);
      const pointerRows = await Promise.all(nextRuns.map(async (run) => {
        try {
          const pointer = await predictionStoreApi.pointer(run.run_id, run.experiment_id);
          return { run, pointer };
        } catch (err) {
          return { run, pointerError: errorText(err) };
        }
      }));
      setSummary(nextSummary);
      setHealth(nextHealth);
      setRows(pointerRows);
      const firstAvailable = pointerRows.find(pointerAvailable);
      setSelectedRunId((previous) => previous || firstAvailable?.run.run_id || "");
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [sampleLimit]);

  useEffect(() => {
    void load();
  }, [load]);

  const availableRows = useMemo(() => rows.filter(pointerAvailable), [rows]);
  const missingRows = useMemo(() => rows.filter((row) => !pointerAvailable(row) && !row.pointerError), [rows]);
  const pointerErrorRows = useMemo(() => rows.filter((row) => row.pointerError), [rows]);
  const failureMarkers = useMemo(() => latestFailureMarkers(rows), [rows]);
  const artifactRows = useMemo(
    () => availableRows.flatMap((row) => artifactsOf(row).map((artifact) => ({ row, artifact }))),
    [availableRows],
  );
  const uploadSwitch = useMemo(() => uploadSwitchState(rows), [rows]);
  const coverage = rows.length ? availableRows.length / rows.length : 0;
  const manifestCount = availableRows.filter((row) => manifestOf(row)).length;
  const selectedRow = rows.find((row) => row.run.run_id === selectedRunId);
  const selectedHasPred = selectedRow ? Boolean(artifactByType(selectedRow, "prediction")) : false;

  async function loadPreview(runId = selectedRunId) {
    const target = runId.trim();
    if (!target) return;
    setPreviewLoading(true);
    setPreviewError(null);
    try {
      setPreview(await predictionStoreApi.previewPred(target, 8));
      setSelectedRunId(target);
    } catch (err) {
      setPreview(null);
      setPreviewError(err);
    } finally {
      setPreviewLoading(false);
    }
  }

  return (
    <main className="pv2-shell">
      <section className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">Prediction Store / Read-only Observability</div>
            <h1>QE pred.pkl 持久化观测</h1>
            <p>
              只读查看 prediction-store 指针、manifest、下载入口、store 健康和灰度上传 marker。
              页面只调用 <span className="pv2-mono">{API_BASE}/prediction-store</span> 与 <span className="pv2-mono">{API_BASE}/qe-archive</span> 只读接口，不新增上传或写路径。
            </p>
          </div>
          <div className="pv2-row-actions">
            <select className="pv2-select" value={sampleLimit} onChange={(event) => setSampleLimit(Number(event.target.value))} aria-label="prediction-store coverage sample limit">
              {SAMPLE_LIMIT_OPTIONS.map((value) => <option key={value} value={value}>最近 {value} run</option>)}
            </select>
            <button className="pv2-button-primary" type="button" onClick={() => void load()} disabled={loading}>
              {loading ? "加载中" : "刷新观测"}
            </button>
          </div>
        </div>
      </section>

      <ErrorPanel error={error} title="Prediction Store 加载失败" />

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="指针覆盖率" value={formatPercentValue(coverage)} hint={`采样 ${rows.length} / 仓库 ${formatCompact(summary?.run_count || 0, 0)} run`} tone={coverage > 0 ? "success" : "warning"} />
        <MetricCard label="有指针 Run" value={formatCompact(availableRows.length, 0)} hint={`manifest ${formatCompact(manifestCount, 0)}`} tone="info" />
        <MetricCard label="无指针 Run" value={formatCompact(missingRows.length, 0)} hint="前向 only：历史 run 允许为空" />
        <MetricCard label="指针错误" value={formatCompact(pointerErrorRows.length, 0)} hint="只读查询失败或 manifest 不可读" tone={pointerErrorRows.length ? "danger" : "success"} />
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="Store 健康与占用" eyebrow="model_store_health">
          <HealthPanel health={health} />
        </SectionCard>

        <SectionCard title="灰度开关与失败 Marker" eyebrow="upload marker visibility">
          <div className="pv2-readable-list">
            <div className="pv2-grid pv2-grid-3">
              <MetricCard label="上传开关" value={uploadSwitch.label} hint={uploadSwitch.hint} tone={uploadSwitch.tone} />
              <MetricCard label="上传成功可见" value={formatCompact(availableRows.length, 0)} hint="manifest / pointer present" tone={availableRows.length ? "success" : "warning"} />
              <MetricCard label="失败 Marker" value={formatCompact(failureMarkers.length, 0)} hint="qe_prediction_store_upload.json / manifest errors" tone={failureMarkers.length ? "danger" : "success"} />
            </div>
            <div className="pv2-readable-panel">
              <div className="pv2-readable-table">
                <div className="pv2-readable-row"><div className="pv2-readable-key">上传开关</div><div className="pv2-readable-value">Runner 侧由 <span className="pv2-mono">AISTOCK_PREDICTION_STORE_UPLOAD_URL</span> 或后端 base env 启用；现有只读接口不回传节点 env，前端只展示已落库 manifest/marker 的实际观测结果。</div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">缺失处理</div><div className="pv2-readable-value">Top-level 指针缺失显示为 missing，不按 0 或成功处理；历史 run 前向 only，允许无指针。</div></div>
              </div>
            </div>
            <PaperTable
              rows={failureMarkers}
              empty="当前采样未发现可见失败 marker；若后端尚未重启或历史 run 无 marker，这里会保持空态。"
              columns={[
                { key: "run", header: "Run", render: (row) => <span className="pv2-mono">{shortHash(row.run_id)}</span> },
                { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
                { key: "source", header: "来源", render: (row) => row.source },
                { key: "message", header: "错误", render: (row) => <span style={{ color: "#b91c1c" }}>{row.message}</span> },
                { key: "time", header: "写入时间", render: (row) => formatDateTime(row.written_at) },
              ]}
            />
          </div>
        </SectionCard>
      </div>

      <SectionCard title="Run / pred 浏览与下载" eyebrow="prediction_store_get_pointer / prediction_store_pull_pred">
        <div className="pv2-row-actions" style={{ marginBottom: 12 }}>
          <label className="pv2-field" style={{ minWidth: 360 }}>
            <span>选择 Run</span>
            <select className="pv2-select" value={selectedRunId} onChange={(event) => setSelectedRunId(event.target.value)} aria-label="select prediction store run">
              <option value="">选择带指针的 run</option>
              {availableRows.map((row) => (
                <option key={row.run.run_id} value={row.run.run_id}>{runLabel(row.run)}</option>
              ))}
            </select>
          </label>
          <button className="pv2-button" type="button" onClick={() => void loadPreview()} disabled={!selectedRunId || !selectedHasPred || previewLoading}>
            {previewLoading ? "读取中" : "预览 pred head"}
          </button>
          {selectedRunId && selectedHasPred ? (
            <a className="pv2-button-ghost" href={predictionStoreApi.downloadUrl(selectedRunId, "prediction")}>下载 pred.pkl</a>
          ) : null}
          {selectedRunId && selectedRow && artifactByType(selectedRow, "model_params") ? (
            <a className="pv2-button-ghost" href={predictionStoreApi.downloadUrl(selectedRunId, "model_params")}>下载 params.pkl</a>
          ) : null}
          {selectedRunId && selectedRow && artifactByType(selectedRow, "label") ? (
            <a className="pv2-button-ghost" href={predictionStoreApi.downloadUrl(selectedRunId, "label")}>下载 label.pkl</a>
          ) : null}
        </div>
        <ErrorPanel error={previewError} title="Pred preview 读取失败" />
        <PreviewPanel preview={preview} />
        <div className="pv2-help" style={{ marginTop: 10 }}>
          下载路由支持 pred.pkl、params.pkl 与 label.pkl；label 缺失时后端返回明确 4xx，不伪造或回退数据。
        </div>
      </SectionCard>

      <SectionCard title="Manifest Artifact 清单" eyebrow="artifact type / sha256 / bytes / store URI">
        <PaperTable
          rows={artifactRows}
          empty={loading ? "正在加载 prediction-store 指针..." : "当前采样没有 prediction-store artifact；历史 run 前向 only 可能为空。"}
          columns={[
            { key: "run", header: "Run", render: ({ row }) => <><div className="pv2-mono">{shortHash(row.run.run_id)}</div><div className="pv2-muted">{row.run.run_type || "-"}</div></> },
            { key: "artifact", header: "Artifact", render: ({ artifact }) => <><div>{artifactTypeLabel(artifact.artifact_type)}</div><div className="pv2-muted">{artifact.artifact_name || "-"}</div></> },
            { key: "sha", header: "SHA256", render: ({ artifact }) => <span className="pv2-mono">{shortHash(artifact.sha256, 10)}</span> },
            { key: "size", header: "大小/行数", render: ({ artifact }) => <><div>{formatBytes(artifact.size_bytes)}</div><div className="pv2-muted">rows {formatCompact(artifact.row_count ?? "-", 0)} / symbols {formatCompact(artifact.symbol_count ?? "-", 0)}</div></> },
            { key: "dates", header: "日期范围", render: ({ artifact }) => <>{artifact.date_start || "-"}<div className="pv2-muted">to {artifact.date_end || "-"}</div></> },
            { key: "status", header: "解析", render: ({ artifact }) => <><StatusBadge status={artifact.parser_status || artifact.collection_status || artifact.collected_status || "available"} />{artifact.parser_error ? <div className="pv2-muted" style={{ color: "#b91c1c" }}>{artifact.parser_error}</div> : null}</> },
            { key: "uri", header: "Store URI", render: ({ artifact }) => <span className="pv2-mono">{shortHash(artifactUri(artifact), 18)}</span> },
            {
              key: "download",
              header: "下载",
              render: ({ row, artifact }) => {
                const href = artifactDownloadHref(row, artifact);
                if (href) return <a className="pv2-button-ghost" href={href}>下载</a>;
                return <span className="pv2-muted">-</span>;
              },
            },
          ]}
        />
      </SectionCard>

      <SectionCard title="指针覆盖明细" eyebrow="available / missing / unreadable">
        <PaperTable
          rows={rows}
          empty={loading ? "正在扫描 qe_archive run..." : "没有 qe_archive run 可展示。"}
          columns={[
            { key: "run", header: "Run", render: (row) => <><div className="pv2-mono">{shortHash(row.run.run_id)}</div><div className="pv2-muted">{row.run.task_id || row.run.experiment_id || "-"}</div></> },
            { key: "status", header: "Pointer", render: (row) => <><StatusBadge status={row.pointerError ? "pointer_error" : row.pointer?.pointer_status || "missing"} /><div className="pv2-muted">{row.pointer?.reason || row.pointer?.manifest_error || row.pointerError || "-"}</div></> },
            { key: "upload", header: "上传观测", render: (row) => <><div>{uploadStatus(row)}</div><div className="pv2-muted">missing: {missingArtifactHint(row)}</div></> },
            { key: "uri", header: "Run Store URI", render: (row) => <span className="pv2-mono">{shortHash(row.pointer?.mlflow_artifact_uri || manifestOf(row)?.uri || "-", 16)}</span> },
            { key: "artifacts", header: "Artifacts", render: (row) => formatNumber(artifactsOf(row).length, 0) },
            { key: "time", header: "完成时间", render: (row) => formatDateTime(row.run.completed_at || row.run.archived_at) },
          ]}
        />
      </SectionCard>
    </main>
  );
}
