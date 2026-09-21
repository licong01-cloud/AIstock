"use client";

import { useMemo, useState } from "react";

type RequestFn = <T = unknown>(
  method: string,
  path: string,
  body?: unknown,
  headers?: Record<string, string>,
) => Promise<T>;

interface MonthlyReleasePanelProps {
  request: RequestFn;
}

interface MonthlyResponse {
  schema_version?: string;
  data?: {
    operation_id?: string;
    status?: string;
    [key: string]: unknown;
  };
  items?: unknown[];
  [key: string]: unknown;
}

const OPERATION_ID = /^dmr_[0-9a-f]{32}$/;

export default function MonthlyReleasePanel({ request }: MonthlyReleasePanelProps) {
  const [cutoff, setCutoff] = useState("");
  const [idempotencyKey, setIdempotencyKey] = useState("");
  const [token, setToken] = useState("");
  const [operationId, setOperationId] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<MonthlyResponse | null>(null);
  const operatorHeaders = useMemo(
    () => ({ "X-Dataset-Release-Operator-Token": token }),
    [token],
  );
  const requestBody = {
    schema_version: "aistock_monthly_release_request_v1",
    target_cutoff: cutoff,
    product_profile: "qe_hmm_full_v2",
    activation_mode: "prepare_only",
    activation_authorization_ref: null,
    repair_authorization_refs: [],
  };
  const resultSummary = result
    ? [
        ["schema", result.schema_version || "unknown"],
        ["operation", result.data?.operation_id || operationId || "not-created"],
        ["status", result.data?.status || "request-succeeded"],
        ["receipt count", Array.isArray(result.items) ? String(result.items.length) : "not-applicable"],
      ]
    : [];

  async function execute(action: "plan" | "submit" | "status" | "receipts") {
    setBusy(true);
    setError("");
    try {
      let response: MonthlyResponse;
      if (action === "plan" || action === "submit") {
        if (!/^\d{4}-\d{2}-\d{2}$/.test(cutoff)) {
          throw new Error("cutoff 必须为 YYYY-MM-DD");
        }
        if (!idempotencyKey.trim() || idempotencyKey.length > 256) {
          throw new Error("请输入稳定且不超过 256 字符的 idempotency key");
        }
        response = await request<MonthlyResponse>(
          "POST",
          `/api/v1/qlib/monthly-releases${action === "plan" ? "/plan" : ""}`,
          requestBody,
          { ...operatorHeaders, "Idempotency-Key": idempotencyKey.trim() },
        );
      } else {
        if (!OPERATION_ID.test(operationId)) {
          throw new Error("operation_id 格式无效");
        }
        response = await request<MonthlyResponse>(
          "GET",
          `/api/v1/qlib/monthly-releases/${operationId}${action === "receipts" ? "/receipts" : ""}`,
          undefined,
          operatorHeaders,
        );
      }
      const returnedOperation = response.data?.operation_id;
      if (typeof returnedOperation === "string" && OPERATION_ID.test(returnedOperation)) {
        setOperationId(returnedOperation);
      }
      setResult(response);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "月更请求失败");
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="mb-6 rounded-xl border border-blue-200 bg-blue-50 p-4">
      <h2 className="text-lg font-semibold">统一月度数据集发布</h2>
      <p className="mb-3 text-sm text-gray-600">
        QE、HMM、因子与荐股共用一个 successor。此入口只提交和查询 durable operation；默认不激活。
      </p>
      <div className="grid gap-3 md:grid-cols-2">
        <label className="text-sm">
          <span className="mb-1 block font-medium">目标 cutoff</span>
          <input
            aria-label="月更目标 cutoff"
            className="w-full rounded border p-2"
            placeholder="YYYY-MM-DD"
            value={cutoff}
            onChange={(event) => setCutoff(event.target.value)}
          />
        </label>
        <label className="text-sm">
          <span className="mb-1 block font-medium">Idempotency key</span>
          <input
            aria-label="月更幂等键"
            className="w-full rounded border p-2"
            value={idempotencyKey}
            onChange={(event) => setIdempotencyKey(event.target.value)}
          />
        </label>
        <label className="text-sm">
          <span className="mb-1 block font-medium">Operator token（仅当前页面内存）</span>
          <input
            aria-label="月更操作令牌"
            autoComplete="off"
            className="w-full rounded border p-2"
            type="password"
            value={token}
            onChange={(event) => setToken(event.target.value)}
          />
        </label>
        <label className="text-sm">
          <span className="mb-1 block font-medium">Operation ID</span>
          <input
            aria-label="月更操作 ID"
            className="w-full rounded border p-2"
            placeholder="dmr_..."
            value={operationId}
            onChange={(event) => setOperationId(event.target.value)}
          />
        </label>
      </div>
      <div className="mt-3 flex flex-wrap gap-2">
        <button className="rounded bg-slate-700 px-3 py-2 text-sm text-white" disabled={busy} onClick={() => execute("plan")} type="button">
          预检计划
        </button>
        <button className="rounded bg-blue-700 px-3 py-2 text-sm text-white" disabled={busy} onClick={() => execute("submit")} type="button">
          提交 prepare-only
        </button>
        <button className="rounded border bg-white px-3 py-2 text-sm" disabled={busy} onClick={() => execute("status")} type="button">
          刷新状态
        </button>
        <button className="rounded border bg-white px-3 py-2 text-sm" disabled={busy} onClick={() => execute("receipts")} type="button">
          查看回执
        </button>
      </div>
      {error && <p className="mt-3 text-sm text-red-700">{error}</p>}
      {result && (
        <dl className="mt-3 grid gap-2 rounded bg-slate-950 p-3 text-xs text-slate-100 md:grid-cols-2">
          {resultSummary.map(([label, value]) => (
            <div key={label}>
              <dt className="text-slate-400">{label}</dt>
              <dd className="break-all">{value}</dd>
            </div>
          ))}
        </dl>
      )}
    </section>
  );
}
