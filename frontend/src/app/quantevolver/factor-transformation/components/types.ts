export type TransformStatus =
  | "PENDING"
  | "RULE_TRANSFORMING"
  | "COMPILE_TESTING"
  | "EXECUTION_TESTING"
  | "LLM_REPAIRING"
  | "ANALYSIS_REVIEWING"
  | "SUCCESS"
  | "FAILED";

export type FactorItem = {
  factor_name: string;
  source: string;
  transformation_status: TransformStatus | null;
  last_transformation_at: string | null;
  /** 是否有非官方改造代码（基于 qe_code_path 文件路径判断） */
  has_realtime_code: boolean;
  /** 是否有原始代码（基于 asset_path 文件路径判断） */
  has_original_code: boolean;
  is_sota_factor: boolean | null;
  ic: number | null;
  sharpe: number | null;
  /** 非官方改造代码在文件系统中的相对路径 */
  qe_code_path: string | null;
  /** 原始因子代码在文件系统中的相对路径（权威数据源） */
  asset_path: string | null;
};

export type TransformJob = {
  job_id: string;
  factor_name: string;
  factor_source: string;
  status: TransformStatus;
  error_message: string | null;
  llm_retry_count: number;
  max_llm_retries: number;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
};

export type Stats = {
  total: number;
  success: number;
  failed: number;
  pending: number;
  in_progress: number;
  has_original_code: number;
  has_realtime_code: number;
};

export type CodeDetail = {
  factor_name: string;
  source: string;
  transformation_status: string | null;
  /** 非官方改造代码内容（从文件系统 qe_code_path 读取，仅用于展示） */
  realtime_code_text: string | null;
  /** 原始代码内容（从文件系统 asset_path 读取，仅用于展示） */
  code_text: string | null;
  last_transformation_at: string | null;
  /** 非官方改造代码在文件系统中的相对路径 */
  qe_code_path: string | null;
  /** 原始因子代码在文件系统中的相对路径（权威数据源） */
  asset_path: string | null;
  /** 代码来源标记（filesystem / none） */
  _transformed_code_source?: string;
  _original_code_source?: string;
  _transformed_code_error?: string;
  _original_code_error?: string;
};

export type TransformConfig = {
  max_llm_retries: number;
  test_start_date: string;
  test_end_date: string;
  llm_model_id: string;
};

export const STATUS_CONFIG: Record<string, { label: string; color: string; bg: string }> = {
  PENDING:            { label: "待处理",    color: "text-gray-500",   bg: "bg-gray-100" },
  RULE_TRANSFORMING:  { label: "规则转换中", color: "text-blue-600",   bg: "bg-blue-50" },
  COMPILE_TESTING:    { label: "编译测试中", color: "text-blue-600",   bg: "bg-blue-50" },
  EXECUTION_TESTING:  { label: "执行测试中", color: "text-blue-600",   bg: "bg-blue-50" },
  LLM_REPAIRING:      { label: "LLM修复中", color: "text-yellow-600", bg: "bg-yellow-50" },
  ANALYSIS_REVIEWING: { label: "AI审核中",  color: "text-purple-600", bg: "bg-purple-50" },
  SUCCESS:            { label: "改造成功",  color: "text-green-600",  bg: "bg-green-50" },
  FAILED:             { label: "改造失败",  color: "text-red-600",    bg: "bg-red-50" },
};

export function fmtTime(ts: string | null): string {
  if (!ts) return "-";
  return new Date(ts).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" });
}
