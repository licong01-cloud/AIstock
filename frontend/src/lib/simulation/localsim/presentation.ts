import type { LocalSimCutoverReadiness, LocalSimReplay, LocalSimReplayStatus } from "./types";
import { human } from "./workbench";

export function chinaTime(value: string | number | null | undefined): string {
  if (value == null) return "尚未读取";
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return "时间无效";
  return `${new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23",
  }).format(date)}（北京时间）`;
}

export function onlyInFlightBlocker(readiness: LocalSimCutoverReadiness): boolean {
  return !readiness.ready && readiness.in_flight_economic_run_count > 0
    && readiness.blockers.length > 0
    && readiness.blockers.every((reason) => reason === `in_flight_economic_run_count:${readiness.in_flight_economic_run_count}`);
}

const blockerLabels: Record<string, string> = {
  required_relation_missing: "必要的账户或账本结构缺失",
  ledger_scope_runtime_fk_count: "账本关联约束不完整",
  orphan_ledger_scope_count: "存在未关联的账本记录",
  invalid_ledger_scope_count: "账本身份校验未通过",
  retained_lineage_missing: "保留账户缺少迁移关联记录",
  legacy_active_session_count: "旧版交易会话尚未结束",
  legacy_auto_run_count: "旧版自动运行尚未关闭",
  legacy_sentinel_count: "旧版校验账户尚未结束",
  in_flight_economic_run_count: "仍有交易日正在执行",
};

export function cutoverBlockerText(reason: string): string {
  const [code, ...values] = reason.split(":");
  const label = blockerLabels[code];
  const count = values.length === 1 && /^\d+$/.test(values[0]) ? `：${values[0]} 项` : "";
  return label ? `${label}${count}` : "存在尚未分类的前置条件，请按核验时间联系模拟盘维护人员";
}

const replayLabels: Record<LocalSimReplayStatus, string> = {
  CREATED: "等待开始",
  RUNNING_HISTORICAL: "历史回放进行中",
  CAUGHT_UP: "已追赶至回放截止日",
  READY_FOR_LIVE: "等待接续实时模拟盘",
  ACTIVATION_PENDING_SAFE_BOUNDARY: "等待安全切换时点",
  LIVE_ACTIVE: "实时模拟盘已接续",
  FAILED_RETRYABLE: "执行受阻，等待自动重试",
  FAILED_TERMINAL: "回放已终止，需诊断",
  CANCELLED: "回放已取消",
};

export function replayPresentation(replay: LocalSimReplay) {
  const inferencePending = replay.status === "FAILED_RETRYABLE"
    && replay.failure_context?.reason_code === "SIMULATION_SELECTION_INFERENCE_IN_PROGRESS";
  const label = inferencePending ? "等待选股推理（自动接续）" : replayLabels[replay.status] || "未识别的回放状态";
  const blocked = !inferencePending && (replay.status === "FAILED_RETRYABLE" || replay.status === "FAILED_TERMINAL");
  const rawMessage = replay.failure_context?.message;
  const message = inferencePending
    ? "选股正在后台计算，完成后由调度器继续。"
    : rawMessage === "LocalSim could not load minute market data"
      ? "历史分钟行情读取失败，需要核对数据及交易资格；尚未完成该交易日。"
      : replay.failure_context?.reason_code === "REFERENCE_CLOSE_MISSING" ? "真实参考价缺失，需要核对合法估值依据。"
        : blocked ? "原任务尚未完成当前交易日；请核对数据、交易资格与正式恢复条件，资产以最后合法估值为准。" : null;
  return { label, blocked, message };
}

export const readableName = human;
