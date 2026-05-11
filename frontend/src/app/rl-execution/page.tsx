"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import {
  API_BASE,
  type RLDevLineage,
  type RLModelVersion,
  rlExecutionApi,
} from "@/lib/rl-execution/api";

function shortDate(value?: string | null): string {
  if (!value) return "-";
  return value.slice(0, 10);
}

function formatBps(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return `${value.toFixed(2)} bps`;
}

export default function RLExecutionPage() {
  const [models, setModels] = useState<RLModelVersion[]>([]);
  const [devLineage, setDevLineage] = useState<RLDevLineage[]>([]);
  const [statusFilter, setStatusFilter] = useState<string>("");
  const [devFilter, setDevFilter] = useState<string>("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [rlModels, lineage] = await Promise.all([
        rlExecutionApi.models({
          status: statusFilter || undefined,
          dev_version: devFilter || undefined,
        }),
        rlExecutionApi.devVersions(),
      ]);
      setModels(rlModels);
      setDevLineage(lineage);
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [statusFilter, devFilter]);

  useEffect(() => {
    void load();
  }, [load]);

  const activeCount = useMemo(
    () => models.filter((m) => (m.status || "").toLowerCase() === "active").length,
    [models],
  );

  const latestActive = useMemo(() => {
    const active = models.filter((m) => (m.status || "").toLowerCase() === "active");
    if (!active.length) return null;
    return active.reduce((acc, m) => {
      if (!acc) return m;
      const accAt = acc.activated_at ? Date.parse(acc.activated_at) : 0;
      const mAt = m.activated_at ? Date.parse(m.activated_at) : 0;
      return mAt > accAt ? m : acc;
    });
  }, [models]);

  const bestPa = useMemo(() => {
    let best: RLModelVersion | null = null;
    for (const model of models) {
      if (model.eval_pa_bps == null) continue;
      if (!best || (model.eval_pa_bps ?? -Infinity) > (best.eval_pa_bps ?? -Infinity)) {
        best = model;
      }
    }
    return best;
  }, [models]);

  return (
    <main className="pv2-shell">
      <section className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">RL Execution Models</div>
            <h1>RL 执行模型</h1>
            <p>
              展示 <span className="pv2-mono">backend.routers.rl_execution</span> 注册的执行 RL 模型版本，
              用于跟踪 v13~v24 dev_version 的滚动训练与激活状态。
              <span className="pv2-mono"> {API_BASE}/rl-execution </span> API。
            </p>
          </div>
          <div className="pv2-row-actions">
            <button
              className="pv2-button-primary"
              type="button"
              onClick={() => void load()}
              disabled={loading}
              data-testid="refresh-rl"
            >
              {loading ? "刷新中" : "刷新"}
            </button>
          </div>
        </div>
      </section>

      <ErrorPanel error={error} title="RL Execution API 调用失败" />

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="模型总数" value={String(models.length)} hint="dev DB 当前条数" />
        <MetricCard label="active" value={String(activeCount)} hint="status=active 的版本" tone="success" />
        <MetricCard
          label="最新激活"
          value={latestActive ? latestActive.version_tag : "-"}
          hint={latestActive ? `at ${shortDate(latestActive.activated_at)}` : "无 active 版本"}
        />
        <MetricCard
          label="最佳 PA"
          value={bestPa ? formatBps(bestPa.eval_pa_bps) : "-"}
          hint={bestPa ? bestPa.version_tag : "无评估数据"}
          tone={bestPa ? "info" : "neutral"}
        />
      </div>

      <SectionCard title="过滤" eyebrow="dev_version / status">
        <div className="pv2-form-grid">
          <label className="pv2-field">
            <span>dev_version</span>
            <select
              className="pv2-select"
              value={devFilter}
              onChange={(event) => setDevFilter(event.target.value)}
              data-testid="dev-filter"
            >
              <option value="">全部</option>
              {devLineage.map((dev) => (
                <option key={dev.dev_version} value={dev.dev_version}>
                  {dev.dev_version} ({dev.roll_count} rolls)
                </option>
              ))}
            </select>
          </label>
          <label className="pv2-field">
            <span>status</span>
            <select
              className="pv2-select"
              value={statusFilter}
              onChange={(event) => setStatusFilter(event.target.value)}
              data-testid="status-filter"
            >
              <option value="">全部</option>
              <option value="active">active</option>
              <option value="candidate">candidate</option>
              <option value="archived">archived</option>
              <option value="failed">failed</option>
            </select>
          </label>
        </div>
      </SectionCard>

      <SectionCard title="开发版本谱系" eyebrow="dev_version lineage with roll counts">
        <PaperTable
          rows={devLineage}
          empty="暂无 dev_version 数据"
          columns={[
            {
              key: "dev",
              header: "dev_version",
              render: (row) => (
                <span className="pv2-mono" data-testid={`dev-${row.dev_version}`}>
                  {row.dev_version}
                </span>
              ),
            },
            { key: "desc", header: "描述", render: (row) => row.dev_description || "-" },
            { key: "parent", header: "父版本", render: (row) => row.parent_dev || "-" },
            {
              key: "rolls",
              header: "Roll 数 / 标签",
              render: (row) => (
                <>
                  <div>{row.roll_count}</div>
                  <div className="pv2-muted pv2-mono">{row.roll_tags.slice(0, 4).join(",")}</div>
                </>
              ),
            },
            {
              key: "latest",
              header: "最近 train_end",
              render: (row) => shortDate(row.latest_train_end),
            },
          ]}
        />
      </SectionCard>

      <SectionCard title="模型版本列表" eyebrow="version_tag = dev-roll">
        <PaperTable
          rows={models}
          empty="暂无符合条件的模型版本"
          columns={[
            {
              key: "version",
              header: "version_tag",
              render: (row) => (
                <span className="pv2-mono" data-testid={`model-${row.version_tag}`}>
                  {row.version_tag}
                </span>
              ),
            },
            {
              key: "status",
              header: "status",
              render: (row) => <StatusBadge status={row.status} />,
            },
            {
              key: "train",
              header: "训练区间",
              render: (row) => (
                <>
                  <div>{shortDate(row.train_start)} → {shortDate(row.train_end)}</div>
                  <div className="pv2-muted">epochs {row.train_epochs ?? "-"}</div>
                </>
              ),
            },
            {
              key: "eval",
              header: "评估指标",
              render: (row) => (
                <>
                  <div data-testid={`pa-${row.version_tag}`}>PA {formatBps(row.eval_pa_bps)}</div>
                  <div className="pv2-muted">FFR {row.eval_ffr == null ? "-" : row.eval_ffr.toFixed(3)} | OracleGap {formatBps(row.eval_oracle_gap_bps)}</div>
                </>
              ),
            },
            {
              key: "activated",
              header: "激活时间",
              render: (row) => shortDate(row.activated_at),
            },
          ]}
        />
      </SectionCard>
    </main>
  );
}
