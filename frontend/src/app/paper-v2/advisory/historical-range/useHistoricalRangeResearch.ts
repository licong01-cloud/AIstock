"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import {
  AdvisoryApiError,
  historicalRangeApi,
  type HistoricalRangeCreatePayload,
  type HistoricalRangeMutationData,
  type HistoricalRangeOptions,
  type HistoricalRangePage,
  type HistoricalRangeRecord,
} from "@/lib/api/advisory";

const STABLE_BATCH = new Set(["PARTIAL", "WAITING_INPUT", "COMPLETED", "FAILED", "CANCELLED", "DEDUPLICATED"]);
const STABLE_OPERATION = new Set(["WAITING_INPUT", "COMPLETED", "RETRYABLE_FAILED", "FAILED"]);
const EMPTY_PAGE: HistoricalRangePage = { limit: 50, next_cursor: null, has_more: false };

function asApiError(cause: unknown): AdvisoryApiError {
  return cause instanceof AdvisoryApiError
    ? cause
    : new AdvisoryApiError({ error_code: "ADVISORY_API_NETWORK_ERROR", message: String(cause) });
}

function idempotencyKey(scope: string, payload: unknown): { storageKey: string; value: string } {
  const storageKey = `advisory-hr:${scope}:${JSON.stringify(payload)}`;
  const existing = sessionStorage.getItem(storageKey);
  if (existing) return { storageKey, value: existing };
  const value = crypto.randomUUID();
  sessionStorage.setItem(storageKey, value);
  return { storageKey, value };
}

export function useHistoricalRangeResearch() {
  const [options, setOptions] = useState<HistoricalRangeOptions | null>(null);
  const [batches, setBatches] = useState<HistoricalRangeRecord[]>([]);
  const [batchPage, setBatchPage] = useState<HistoricalRangePage>(EMPTY_PAGE);
  const [selectedBatch, setSelectedBatch] = useState<HistoricalRangeRecord | null>(null);
  const [runs, setRuns] = useState<HistoricalRangeRecord[]>([]);
  const [operations, setOperations] = useState<HistoricalRangeRecord[]>([]);
  const [selectedRun, setSelectedRun] = useState<HistoricalRangeRecord | null>(null);
  const [days, setDays] = useState<HistoricalRangeRecord[]>([]);
  const [dayPage, setDayPage] = useState<HistoricalRangePage>(EMPTY_PAGE);
  const [selectedDay, setSelectedDay] = useState<HistoricalRangeRecord | null>(null);
  const [candidates, setCandidates] = useState<HistoricalRangeRecord[]>([]);
  const [candidatePage, setCandidatePage] = useState<HistoricalRangePage>(EMPTY_PAGE);
  const [listVersion, setListVersion] = useState<HistoricalRangeRecord | null>(null);
  const [listItems, setListItems] = useState<HistoricalRangeRecord[]>([]);
  const [listItemPage, setListItemPage] = useState<HistoricalRangePage>(EMPTY_PAGE);
  const [outcomes, setOutcomes] = useState<HistoricalRangeRecord[]>([]);
  const [outcomePage, setOutcomePage] = useState<HistoricalRangePage>(EMPTY_PAGE);
  const [summaries, setSummaries] = useState<HistoricalRangeRecord[]>([]);
  const [activeOperation, setActiveOperation] = useState<HistoricalRangeRecord | null>(null);
  const [selectedHorizons, setSelectedHorizons] = useState<number[]>([]);
  const [loading, setLoading] = useState(true);
  const [mutating, setMutating] = useState(false);
  const [error, setError] = useState<AdvisoryApiError | null>(null);
  const selectedBatchId = selectedBatch ? String(selectedBatch.batch_id) : "";
  const selectedRunId = selectedRun ? String(selectedRun.range_run_id) : "";
  const abortRef = useRef<AbortController | null>(null);

  const refreshBatches = useCallback(async (signal?: AbortSignal) => {
    const result = await historicalRangeApi.batches(null, signal);
    setBatches(result.rows);
    setBatchPage(result.page);
  }, []);

  const refreshSelectedBatch = useCallback(async (batchId: string, signal?: AbortSignal) => {
    const [batch, nextRuns, nextOperations] = await Promise.all([
      historicalRangeApi.batch(batchId, signal),
      historicalRangeApi.runs(batchId, signal),
      historicalRangeApi.operations(batchId, signal),
    ]);
    setSelectedBatch(batch);
    setRuns(nextRuns);
    setOperations(nextOperations);
    setActiveOperation((current) => {
      if (!current) return current;
      return nextOperations.find((item) => item.operation_id === current.operation_id) || current;
    });
    return nextRuns;
  }, []);

  const selectBatch = useCallback(async (batchId: string) => {
    setError(null);
    try {
      const nextRuns = await refreshSelectedBatch(batchId);
      if (nextRuns[0]) setSelectedRun(nextRuns[0]);
    } catch (cause) {
      setError(asApiError(cause));
    }
  }, [refreshSelectedBatch]);

  const selectRun = useCallback(async (run: HistoricalRangeRecord) => {
    const runId = String(run.range_run_id);
    setSelectedRun(run);
    setError(null);
    try {
      const [nextDays, nextOutcomes, nextSummaries] = await Promise.all([
        historicalRangeApi.days(runId),
        historicalRangeApi.outcomes(runId),
        historicalRangeApi.summaries(runId),
      ]);
      setDays(nextDays.rows);
      setDayPage(nextDays.page);
      setSelectedDay(null);
      setCandidates([]);
      setCandidatePage(EMPTY_PAGE);
      setListVersion(null);
      setListItems([]);
      setListItemPage(EMPTY_PAGE);
      setOutcomes(nextOutcomes.rows);
      setOutcomePage(nextOutcomes.page);
      setSummaries(nextSummaries);
    } catch (cause) {
      setError(asApiError(cause));
    }
  }, []);

  const selectDay = useCallback(async (day: HistoricalRangeRecord) => {
    if (!selectedRunId) return;
    const tradeDate = String(day.decision_trade_date);
    setError(null);
    try {
      const [dayDetail, listDetail] = await Promise.all([
        historicalRangeApi.day(selectedRunId, tradeDate),
        historicalRangeApi.list(selectedRunId, tradeDate),
      ]);
      setSelectedDay(dayDetail.day);
      setCandidates(dayDetail.rows);
      setCandidatePage(dayDetail.page);
      setListVersion(listDetail.list);
      setListItems(listDetail.rows);
      setListItemPage(listDetail.page);
    } catch (cause) {
      setError(asApiError(cause));
    }
  }, [selectedRunId]);

  const selectOperation = useCallback(async (operationId: string) => {
    setError(null);
    try {
      setActiveOperation(await historicalRangeApi.operation(operationId));
    } catch (cause) {
      setError(asApiError(cause));
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    abortRef.current = controller;
    setLoading(true);
    Promise.all([historicalRangeApi.options(controller.signal), historicalRangeApi.batches(null, controller.signal)])
      .then(([nextOptions, nextBatches]) => {
        setOptions(nextOptions);
        setSelectedHorizons((current) => current.length ? current : nextOptions.outcome_catalog.default_horizons);
        setBatches(nextBatches.rows);
        setBatchPage(nextBatches.page);
      })
      .catch((cause) => {
        if (!controller.signal.aborted) setError(asApiError(cause));
      })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    const hasActive = batches.some((item) => !STABLE_BATCH.has(String(item.status)));
    if (!hasActive) return;
    let controller = new AbortController();
    const handleVisibility = () => {
      controller.abort();
      controller = new AbortController();
    };
    const timer = window.setInterval(() => {
      if (document.visibilityState !== "visible") return;
      void refreshBatches(controller.signal).catch((cause) => {
        if (!controller.signal.aborted) setError(asApiError(cause));
      });
    }, 5000);
    document.addEventListener("visibilitychange", handleVisibility);
    return () => {
      controller.abort();
      document.removeEventListener("visibilitychange", handleVisibility);
      window.clearInterval(timer);
    };
  }, [batches, refreshBatches]);

  useEffect(() => {
    if (!selectedBatchId) return;
    const operationActive = activeOperation && !STABLE_OPERATION.has(String(activeOperation.status));
    const batchActive = selectedBatch && !STABLE_BATCH.has(String(selectedBatch.status));
    if (!operationActive && !batchActive) return;
    let controller = new AbortController();
    const handleVisibility = () => {
      controller.abort();
      controller = new AbortController();
    };
    const timer = window.setInterval(() => {
      if (document.visibilityState !== "visible") return;
      void refreshSelectedBatch(selectedBatchId, controller.signal).catch((cause) => {
        if (!controller.signal.aborted) setError(asApiError(cause));
      });
      if (activeOperation?.operation_id) {
        void historicalRangeApi.operation(String(activeOperation.operation_id), controller.signal)
          .then(setActiveOperation)
          .catch((cause) => {
            if (!controller.signal.aborted) setError(asApiError(cause));
          });
      }
    }, 3000);
    document.addEventListener("visibilitychange", handleVisibility);
    return () => {
      controller.abort();
      document.removeEventListener("visibilitychange", handleVisibility);
      window.clearInterval(timer);
    };
  }, [activeOperation, refreshSelectedBatch, selectedBatch, selectedBatchId]);

  const create = useCallback(async (payload: HistoricalRangeCreatePayload) => {
    const key = idempotencyKey("create", payload);
    setMutating(true);
    setError(null);
    try {
      const result = await historicalRangeApi.create(payload, key.value);
      sessionStorage.removeItem(key.storageKey);
      setSelectedBatch(result.batch);
      setActiveOperation(result.operation);
      await refreshBatches();
      return result;
    } catch (cause) {
      setError(asApiError(cause));
      return null;
    } finally {
      setMutating(false);
    }
  }, [refreshBatches]);

  const mutate = useCallback(async (
    action: "resume" | "cancel" | "refresh" | "bridge",
    labelAsOfTradeDate?: string,
  ) => {
    if (!selectedBatch) return null;
    const batchId = String(selectedBatch.batch_id);
    const rowVersion = Number(selectedBatch.row_version);
    const basePayload = { expected_row_version: rowVersion };
    setMutating(true);
    setError(null);
    try {
      let result: HistoricalRangeMutationData;
      if (action === "resume" || action === "cancel") {
        const key = idempotencyKey(`${batchId}:${action}`, basePayload);
        result = await historicalRangeApi.command(batchId, action, { ...basePayload, operation_idempotency_key: key.value });
        sessionStorage.removeItem(key.storageKey);
      } else if (action === "refresh") {
        if (!labelAsOfTradeDate || !selectedHorizons.length) {
          throw new AdvisoryApiError({
            error_code: "ADVISORY_HR_REFRESH_INPUT_REQUIRED",
            message: "刷新 Outcome 前必须显式选择 label-as-of 交易日和至少一个 horizon。",
          });
        }
        const semanticPayload = {
          ...basePayload,
          label_as_of_trade_date: labelAsOfTradeDate,
          range_run_ids: [] as string[],
          horizons: selectedHorizons,
        };
        const key = idempotencyKey(`${batchId}:${action}`, semanticPayload);
        result = await historicalRangeApi.refreshOutcomes(batchId, {
          ...semanticPayload,
          operation_idempotency_key: key.value,
        });
        sessionStorage.removeItem(key.storageKey);
      } else {
        if (!selectedHorizons.length) {
          throw new AdvisoryApiError({
            error_code: "ADVISORY_HR_BRIDGE_INPUT_REQUIRED",
            message: "构建 Dataset bridge 前必须选择至少一个 horizon。",
          });
        }
        const semanticPayload = {
          ...basePayload,
          range_run_ids: [] as string[],
          requested_horizons: selectedHorizons,
          requested_maturity_statuses: ["COMPLETE", "TERMINAL"] as ("COMPLETE" | "CENSORED" | "TERMINAL")[],
        };
        const key = idempotencyKey(`${batchId}:${action}`, semanticPayload);
        result = await historicalRangeApi.buildBridge(batchId, {
          ...semanticPayload,
          operation_idempotency_key: key.value,
        });
        sessionStorage.removeItem(key.storageKey);
      }
      setActiveOperation(result.operation);
      setSelectedBatch(result.batch);
      return result;
    } catch (cause) {
      const typed = asApiError(cause);
      if (typed.http_status === 409) {
        try {
          await refreshSelectedBatch(batchId);
        } catch (refreshCause) {
          const refreshError = asApiError(refreshCause);
          setError(new AdvisoryApiError({
            error_code: typed.error_code,
            reason_code: typed.reason_code,
            message: `${typed.message}；刷新最新 row version 也失败：${refreshError.message}`,
            http_status: typed.http_status,
            retryable: typed.retryable,
            context: { ...typed.context, refresh_error_code: refreshError.error_code },
            correlation_id: typed.correlation_id,
          }));
          return null;
        }
      }
      setError(typed);
      return null;
    } finally {
      setMutating(false);
    }
  }, [refreshSelectedBatch, selectedBatch, selectedHorizons]);

  const loadMoreBatches = useCallback(async () => {
    if (!batchPage.next_cursor) return;
    try {
      const result = await historicalRangeApi.batches(batchPage.next_cursor);
      setBatches((current) => [...current, ...result.rows]);
      setBatchPage(result.page);
    } catch (cause) {
      setError(asApiError(cause));
    }
  }, [batchPage]);

  const loadMoreDays = useCallback(async () => {
    if (!selectedRunId || !dayPage.next_cursor) return;
    try {
      const result = await historicalRangeApi.days(selectedRunId, dayPage.next_cursor);
      setDays((current) => [...current, ...result.rows]);
      setDayPage(result.page);
    } catch (cause) {
      setError(asApiError(cause));
    }
  }, [dayPage, selectedRunId]);

  const loadMoreOutcomes = useCallback(async () => {
    if (!selectedRunId || !outcomePage.next_cursor) return;
    try {
      const result = await historicalRangeApi.outcomes(selectedRunId, outcomePage.next_cursor);
      setOutcomes((current) => [...current, ...result.rows]);
      setOutcomePage(result.page);
    } catch (cause) {
      setError(asApiError(cause));
    }
  }, [outcomePage, selectedRunId]);

  const loadMoreCandidates = useCallback(async () => {
    if (!selectedRunId || !selectedDay || !candidatePage.next_cursor) return;
    try {
      const result = await historicalRangeApi.day(selectedRunId, String(selectedDay.decision_trade_date), candidatePage.next_cursor);
      setCandidates((current) => [...current, ...result.rows]);
      setCandidatePage(result.page);
    } catch (cause) {
      setError(asApiError(cause));
    }
  }, [candidatePage, selectedDay, selectedRunId]);

  const loadMoreListItems = useCallback(async () => {
    if (!selectedRunId || !selectedDay || !listItemPage.next_cursor) return;
    try {
      const result = await historicalRangeApi.list(selectedRunId, String(selectedDay.decision_trade_date), listItemPage.next_cursor);
      setListItems((current) => [...current, ...result.rows]);
      setListItemPage(result.page);
    } catch (cause) {
      setError(asApiError(cause));
    }
  }, [listItemPage, selectedDay, selectedRunId]);

  return {
    options, batches, batchPage, selectedBatch, runs, operations, selectedRun, days, dayPage,
    selectedDay, candidates, candidatePage, listVersion, listItems, listItemPage,
    outcomes, outcomePage, summaries, activeOperation, selectedHorizons, setSelectedHorizons,
    loading, mutating, error,
    create, mutate, selectBatch, selectRun, selectDay, selectOperation,
    loadMoreBatches, loadMoreDays, loadMoreCandidates, loadMoreListItems, loadMoreOutcomes,
  };
}
