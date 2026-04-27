"use client";

import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import Editor, { DiffEditor } from "@monaco-editor/react";
import yaml from "js-yaml";
import styles from "./scheduler.module.css";

const SCHEDULER_BASE =
  process.env.NEXT_PUBLIC_SCHEDULER_BASE || "http://127.0.0.1:9000/scheduler";
const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
const ENV_KEY_DISPLAY: Record<string, string> = {
  LITELLM_API_KEY: "LiteLLM API Key",
  OPENAI_API_KEY: "OpenAI API Key",
  QLIB_DATA_DIR: "Qlib 数据目录",
  COSTEER_KB_PATH: "CoSTEER KB Path",
};

type Task = {
  name?: string;
  id?: string | number;
  status?: string;
  loop_n?: number;
  all_duration?: string;
  evolving_mode?: string;
};

type Dataset = {
  name?: string;
  provider_uri?: string;
};

type ResultItem = {
  returncode?: number;
  workdir?: string;
  cmd?: string[];
  log_path?: string;
  result_files?: string[];
  summary?: Record<string, unknown>;
};

type TemplateFile = {
  path: string;
  content: string;
};

type TemplateHistoryItem = {
  file_name?: string;
  backup_path?: string;
  task_id?: string;
  created_at?: string;
  extra?: Record<string, unknown>;
};

type TemplateSummary = {
  scenario: string;
  version: string;
  created_at?: string;
  description?: string;
  base_version?: string;
  changed_files?: string[];
  files_count?: number;
  scenario_desc?: string;
  manifest_hash?: string;
  is_editable?: boolean;
  is_active?: boolean;
};

type TemplateFileItem = {
  path: string;
  sha256?: string;
  size?: number;
};

type TemplateFileDoc = {
  title: string;
  role: string;
  affects: string[];
  rules: string[];
  tips?: string[];
  fields?: { key: string; desc: string }[];
  numbers?: { key: string; value: number | string; desc: string }[];
};

type ApplyLogItem = {
  path: string;
  size: number;
  sha256: string;
  verified: boolean;
  old_size?: number;
  size_changed?: boolean;
  mtime_changed?: boolean;
};

type ApplyResult = {
  ok: boolean;
  scenario: string;
  version: string;
  backup_id?: string;
  applied_files: ApplyLogItem[];
  verification: {
    verified: boolean;
    total_files: number;
    verified_files: number;
    failed_files?: Array<{ path: string; expected: string; actual: string }>;
  };
};

function formatBeijingTime(value?: string | null) {
  if (!value) return "-";
  try {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" });
  } catch {
    return value;
  }
}

async function fetchBaseJSON<T>(base: string, path: string, options?: RequestInit): Promise<T> {
  const resp = await fetch(`${base.replace(/\/$/, "")}${path}`, {
    headers: { "Content-Type": "application/json" },
    cache: "no-store",
    ...options,
  });
  if (!resp.ok) throw new Error(await resp.text());
  return resp.json();
}

async function fetchJSON<T>(path: string, options?: RequestInit, base = SCHEDULER_BASE): Promise<T> {
  return fetchBaseJSON<T>(base, path, options);
}

async function fetchApiJSON<T>(path: string, options?: RequestInit, base = API_BASE): Promise<T> {
  return fetchBaseJSON<T>(base, path, options);
}

export default function SchedulerPage() {
  const [nodeId, setNodeId] = useState("");
  const [nodeRouteReady, setNodeRouteReady] = useState(false);
  const schedulerBase = useMemo(
    () =>
      nodeId
        ? `${API_BASE}/dispatch/nodes/${encodeURIComponent(nodeId)}/proxy/scheduler`
        : SCHEDULER_BASE,
    [nodeId],
  );
  const [tasks, setTasks] = useState<Task[]>([]);
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [taskId, setTaskId] = useState("");
  const [log, setLog] = useState("");
  const [results, setResults] = useState<ResultItem[]>([]);
  const [creating, setCreating] = useState(false);
  const [envText, setEnvText] = useState("");
  const [envSaving, setEnvSaving] = useState(false);
  const [tplScenario, setTplScenario] = useState("all");
  const [tplVersion, setTplVersion] = useState("v1");
  const [tplTaskId, setTplTaskId] = useState("");
  const [tplDescription, setTplDescription] = useState("");
  const [tplBaseScenario, setTplBaseScenario] = useState("all");
  const [tplBaseVersion, setTplBaseVersion] = useState("v0");
  const [tplChangedFiles, setTplChangedFiles] = useState("");
  const [tplDraftFiles, setTplDraftFiles] = useState<TemplateFile[]>([]);
  const [tplDraftPath, setTplDraftPath] = useState("");
  const [tplDraftContent, setTplDraftContent] = useState("");
  const [tplDraftOriginal, setTplDraftOriginal] = useState("");
  const [tplDraftLoading, setTplDraftLoading] = useState(false);
  const [tplDraftSaving, setTplDraftSaving] = useState(false);
  const [tplDraftValidating, setTplDraftValidating] = useState(false);
  const [tplDraftValidateMsg, setTplDraftValidateMsg] = useState<string | null>(null);
  const [tplPromptAuditing, setTplPromptAuditing] = useState(false);
  const [tplPromptAuditMsg, setTplPromptAuditMsg] = useState<string | null>(null);
  const [tplPublishing, setTplPublishing] = useState(false);
  const [tplHistoryLoading, setTplHistoryLoading] = useState(false);
  const [tplHistoryItems, setTplHistoryItems] = useState<TemplateHistoryItem[]>([]);
  const [tplHistoryScenario, setTplHistoryScenario] = useState("qlib");
  const [tplHistoryVersion, setTplHistoryVersion] = useState("");
  const [tplRollbackScenario, setTplRollbackScenario] = useState("qlib");
  const [tplRollbackVersion, setTplRollbackVersion] = useState("");
  const [tplRollbackBackup, setTplRollbackBackup] = useState("");
  const [tplRollbackLoading, setTplRollbackLoading] = useState(false);
  const [tplList, setTplList] = useState<TemplateSummary[]>([]);
  const [tplListLoading, setTplListLoading] = useState(false);
  const [tplListSearch, setTplListSearch] = useState("");
  const [selectedTemplate, setSelectedTemplate] = useState<TemplateSummary | null>(null);
  const [tplActivating, setTplActivating] = useState(false);
  const [tplDiffOpen, setTplDiffOpen] = useState(false);
  const [tplReadOnly, setTplReadOnly] = useState(false);
  const [tplDiffSideBySide, setTplDiffSideBySide] = useState(true);
  const [tplDiffHideUnchanged, setTplDiffHideUnchanged] = useState(true);
  const [tplDragIndex, setTplDragIndex] = useState<number | null>(null);
  const [tplError, setTplError] = useState<string | null>(null);
  const [tplDocOpen, setTplDocOpen] = useState(false);
  const [tplDocPath, setTplDocPath] = useState<string | null>(null);
  const [tplApplyLogs, setTplApplyLogs] = useState<ApplyLogItem[]>([]);
  const [tplApplyResult, setTplApplyResult] = useState<ApplyResult | null>(null);
  const [tplShowApplyLog, setTplShowApplyLog] = useState(false);
  const [tplRefreshingSha256, setTplRefreshingSha256] = useState<string | null>(null);
  const monacoConfiguredRef = useRef(false);
  const editorRef = useRef<any>(null);

  const baseUrlLabel = useMemo(() => schedulerBase.replace(/\/$/, ""), [schedulerBase]);

  useEffect(() => {
    const readNodeId = () => {
      setNodeId((new URLSearchParams(window.location.search).get("node_id") || "").trim());
      setNodeRouteReady(true);
    };
    readNodeId();
    window.addEventListener("popstate", readNodeId);
    return () => window.removeEventListener("popstate", readNodeId);
  }, []);

  async function fetchSchedulerJSON<T>(path: string, options?: RequestInit): Promise<T> {
    return fetchJSON<T>(path, options, schedulerBase);
  }

  async function fetchTemplateJSON<T>(path: string, options?: RequestInit): Promise<T> {
    if (!nodeId) {
      return fetchApiJSON<T>(path, options);
    }
    const schedulerPath = path.replace(/^\/rdagent\/templates/, "/templates");
    return fetchJSON<T>(schedulerPath, options, schedulerBase);
  }

  const loadTasks = async () => {
    try {
      const data = await fetchSchedulerJSON<{ items: Task[] }>("/tasks");
      setTasks(data.items || []);
    } catch (e) {
      console.error(e);
    }
  };

  const extractYamlKeys = (content: string) => {
    const lines = content.split("\n");
    const keys = new Set<string>();
    lines.forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) return;
      const normalized = trimmed.replace(/^[-\s]+/, "");
      if (!normalized) return;
      const match = normalized.match(/^([A-Za-z0-9_\-.]+)\s*:/);
      if (match && match[1]) {
        keys.add(match[1]);
      }
    });
    return Array.from(keys);
  };

  const extractNumbersFromText = (content: string) => {
    const lines = content.split("\n");
    const numbers: { key: string; value: number | string }[] = [];
    lines.forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) return;
      const normalized = trimmed.replace(/^[-\s]+/, "");
      const match = normalized.match(/^([A-Za-z0-9_\-.]+)\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)/);
      if (match) {
        numbers.push({ key: match[1], value: match[2] });
      }
    });
    return numbers;
  };

  const describeFieldKey = (key: string) => {
    const lower = key.toLowerCase();
    const explicitMap: Record<string, string> = {
      task_id: "任务唯一标识，用于追踪调度与产出文件",
      scenario: "场景名称，决定加载的提示词与模板目录",
      version: "模板版本号，对应 app_tpl/<scenario>/<version>",
      app_tpl: "直接指定模板路径，优先级高于 scenario/version",
      mode: "运行模式，影响 RD-Agent 执行链路（factor/model/quant 等）",
      loop_n: "迭代轮数，每轮会生成新的方案与实验",
      all_duration: "总时长预算，限制整体执行时间",
      template_base: "是否复制默认模板后再应用覆盖",
      template_files: "追加模板文件列表，用于注入或覆盖",
      "template_files.path": "模板文件的相对路径（相对仓库根目录）",
      "template_files.source": "文件来源：default 复制模板，inline 使用内联内容",
      "template_files.content": "内联文件内容，仅在 source=inline 时生效",
      template_description: "模板版本说明，用于记录变更目的",
      base_version: "派生模板的基础版本（如 v0）",
      changed_files: "本版本重点变更文件清单",
      prompt_patch: "提示词补丁，用于追加/替换提示词片段",
      "prompt_patch.replace": "替换指定提示词内容",
      "prompt_patch.prepend": "在提示词前追加内容",
      "prompt_patch.append": "在提示词后追加内容",
      model_allowlist: "模型白名单，仅允许这些模型进入实验",
      factor_allowlist: "因子白名单，仅允许这些因子进入实验",
      sota_factor_task_path: "SOTA 因子任务目录，用于读取已有因子",
      runtime_env: "运行时环境变量注入",
      qlib_init: "Qlib 初始化参数块，指定数据源与市场区域",
      provider_uri: "Qlib 数据源 URI，指向 qlib_bin 数据目录",
      region: "市场区域标识，用于选择交易日历与市场规则",
      market: "股票池或市场范围（如 all/csi300）",
      benchmark: "基准指数或标的，用于回测收益对比",
      data_handler_config: "数据处理器配置（特征、标签、处理器）",
      data_loader: "特征加载器配置（组合静态/动态因子数据）",
      alpha158_config: "Alpha158 特征/标签配置块",
      feature: "特征表达式清单，用于生成特征矩阵",
      dataset_cls: "数据集类模板变量（默认 DatasetH）",
      static_path: "静态因子 parquet 路径，用于合并 SOTA 因子",
      dynamic_path: "动态因子 parquet 路径，用于合并新生成因子",
      join: "合并方式（left/outer），决定对齐范围",
      min_dynamic_non_nan_ratio: "动态因子列的最小有效值比例阈值",
      min_instrument_overlap_ratio: "股票池交集比例阈值，过低则剔除",
      enforce_instrument_format: "是否强制检查股票代码格式",
      start_time: "特征/标签生成的起始日期",
      end_time: "特征/标签生成的结束日期",
      fit_start_time: "归一化拟合起始日期（训练前置区间）",
      fit_end_time: "归一化拟合结束日期",
      instruments: "训练/回测使用的股票池",
      infer_processors: "推理阶段处理器列表（过滤、标准化、填充）",
      learn_processors: "训练阶段处理器列表（清洗、排序归一化）",
      fields_group: "处理器作用的字段分组（feature/label）",
      col_list: "保留的特征列清单",
      clip_outlier: "是否裁剪异常值，避免极端值影响",
      label: "标签定义公式，用于监督学习目标",
      port_analysis_config: "组合分析配置（收益/风险/换手等）",
      strategy: "回测策略配置块（选股、调仓逻辑）",
      class: "组件类名（策略/模型/处理器/记录器）",
      module_path: "组件类所在的 Python 模块路径",
      kwargs: "组件初始化参数字典",
      signal: "策略信号字段（通常为模型预测输出）",
      topk: "持仓股票数量上限",
      n_drop: "每期剔除的持仓数量",
      min_score: "最低信号阈值，低于则清仓/不入选",
      max_position_ratio: "最大仓位比例上限",
      stop_loss: "止损阈值（负值表示亏损比例）",
      backtest: "回测配置入口（区间、费用、成交规则）",
      account: "初始资金规模",
      exchange_kwargs: "交易所撮合与成交规则参数",
      limit_threshold: "涨跌停阈值（影响成交判断）",
      deal_price: "成交价选择规则（close/open/vwap）",
      open_cost: "开仓手续费率",
      close_cost: "平仓手续费率",
      min_cost: "最小手续费",
      task: "实验任务配置入口",
      model: "模型配置入口（类别/参数）",
      loss: "模型损失函数或优化目标",
      device_type: "训练设备类型（cpu/gpu）",
      gpu_use_dp: "是否启用 GPU 双精度计算",
      max_bin: "GBDT 分桶数量",
      colsample_bytree: "特征采样比例",
      learning_rate: "学习率，影响收敛速度",
      subsample: "样本采样比例",
      lambda_l1: "L1 正则项权重",
      lambda_l2: "L2 正则项权重",
      max_depth: "树模型最大深度",
      num_leaves: "树模型叶子数量",
      num_threads: "训练线程数",
      n_epochs: "训练轮次",
      lr: "学习率（深度模型/通用训练参数）",
      early_stop: "是否启用提前停止",
      batch_size: "训练 batch 大小",
      weight_decay: "权重衰减正则项",
      metric: "评估指标名称",
      n_jobs: "训练并行进程数",
      GPU: "GPU 编号或设备索引",
      pt_model_uri: "PyTorch 模型类路径",
      pt_model_kwargs: "PyTorch 模型初始化参数",
      num_features: "特征维度数量",
      num_timesteps: "序列时间步长度",
      dataset: "数据集配置入口",
      handler: "数据处理器配置入口",
      segments: "数据分段设置（train/valid/test）",
      train: "训练集区间",
      valid: "验证集区间",
      test: "测试集区间",
      step_len: "滑动窗口步长（序列建模用）",
      record: "结果记录与分析配置入口",
      ana_long_short: "多空分析开关",
      ann_scaler: "年化系数（如 252 个交易日）",
      config: "组合分析子配置块",
      qlib_quant_background: "量化研究背景，定义研究员角色与总体目标",
      qlib_factor_background: "因子背景说明，定义因子含义与输出要求",
      qlib_factor_interface: "因子接口约束，规定包含的子提示词",
      qlib_factor_interface_core_constraints: "因子脚本核心约束，规定输出格式/索引/模板结构",
      qlib_factor_interface_data_loading: "数据加载规范，限定可用字段与静态因子 join 规则",
      qlib_factor_interface_error_prevention: "常见错误预防清单，强调字段缺失/索引错误处理",
      qlib_factor_interface_dataset_info: "数据集信息说明，强调股票池一致性与预计算因子",
      qlib_factor_interface_language_spec: "语言规范与代码风格要求",
      qlib_factor_strategy: "因子编写策略，约束索引/处理规则",
      qlib_factor_output_format: "因子输出格式示例与规范",
      qlib_factor_simulator: "因子模拟流程说明",
      qlib_factor_rich_style_description: "因子演进演示说明",
      qlib_factor_from_report_rich_style_description: "从报告提取因子的演示说明",
      qlib_factor_experiment_setting: "因子实验数据集/切分说明",
      qlib_model_background: "模型背景说明，定义模型目标与结构要求",
      qlib_model_interface: "模型接口规范与实现要求",
      qlib_model_output_format: "模型输出格式与保存要求",
      qlib_model_simulator: "模型训练与评测流程说明",
      qlib_model_rich_style_description: "模型演示说明",
      qlib_model_experiment_setting: "模型实验数据集/切分说明",
      factor_hypothesis_specification: "因子假设生成规则，约束因子数量/多样性/数据来源",
      factor_experiment_output_format: "因子实验输出 JSON 格式说明（字段、变量、公式）",
      model_experiment_output_format: "模型实验输出 JSON 格式说明（结构、超参与训练参数）",
      hypothesis_and_feedback: "历史实验假设与反馈汇总模板",
      last_hypothesis_and_feedback: "上一轮假设与反馈摘要模板",
      sota_hypothesis_and_feedback: "当前 SOTA 假设与反馈摘要模板",
      hypothesis_output_format: "假设输出 JSON 结构定义",
      factor_hypothesis_output_format: "因子假设输出 JSON 结构定义",
      hypothesis_output_format_with_action: "带 action 的假设输出 JSON 结构定义",
      model_hypothesis_specification: "模型假设生成规则与设计约束",
      factor_feedback_generation: "因子结果反馈生成提示词模板",
      model_feedback_generation: "模型结果反馈生成提示词模板",
      action_gen: "决定下一轮因子/模型方向的 action 生成模板",
    };
    if (explicitMap[lower]) return explicitMap[lower];
    if (lower.includes("system")) return "系统提示词，定义角色与总体目标";
    if (lower.includes("user")) return "用户提示词，描述输入与任务需求";
    if (lower.includes("assistant")) return "助手回复模板或示例";
    if (lower.includes("constraint") || lower.includes("rule")) return "约束/规则，限定输出范围";
    if (lower.includes("output") || lower.includes("format")) return "输出格式定义";
    if (lower.includes("step") || lower.includes("flow")) return "执行步骤/流程说明";
    if (lower.includes("tool")) return "工具或能力定义";
    if (lower.includes("goal") || lower.includes("objective")) return "目标/目的描述，决定任务方向";
    if (lower.includes("context") || lower.includes("background")) return "背景信息，提供上下文约束";
    if (lower.includes("input")) return "输入字段说明，影响解析方式";
    if (lower.includes("output")) return "输出格式/结构约束";
    if (lower.includes("example") || lower.includes("few")) return "示例/少样本参考";
    if (lower.includes("prompt")) return "提示词内容";
    return "配置字段";
  };

  const describeNumberKey = (key: string) => {
    const lower = key.toLowerCase();
    if (lower === "loop_n" || lower.includes("loop")) return "循环轮数，决定迭代次数";
    if (lower === "topk") return "持仓数量上限";
    if (lower === "n_drop") return "每期剔除的持仓数量";
    if (lower === "min_score") return "信号最低阈值";
    if (lower === "max_position_ratio") return "最大仓位比例";
    if (lower === "stop_loss") return "止损阈值";
    if (lower === "limit_threshold") return "涨跌停阈值";
    if (lower === "open_cost") return "开仓手续费率";
    if (lower === "close_cost") return "平仓手续费率";
    if (lower === "min_cost") return "最小手续费";
    if (lower === "account") return "初始资金规模";
    if (lower === "min_dynamic_non_nan_ratio") return "动态因子有效值比例阈值";
    if (lower === "min_instrument_overlap_ratio") return "股票池交集比例阈值";
    if (lower === "max_bin") return "GBDT 分桶数量";
    if (lower === "colsample_bytree") return "特征采样比例";
    if (lower === "subsample") return "样本采样比例";
    if (lower === "lambda_l1") return "L1 正则权重";
    if (lower === "lambda_l2") return "L2 正则权重";
    if (lower === "max_depth") return "树模型最大深度";
    if (lower === "num_leaves") return "叶子节点数量";
    if (lower === "num_threads") return "训练线程数";
    if (lower === "n_epochs") return "训练轮次";
    if (lower === "batch_size") return "训练 batch 大小";
    if (lower === "weight_decay") return "权重衰减系数";
    if (lower === "n_jobs") return "训练并行进程数";
    if (lower === "gpu") return "GPU 设备编号";
    if (lower === "num_features") return "特征维度数量";
    if (lower === "num_timesteps") return "序列时间步长度";
    if (lower === "step_len") return "滑动窗口步长";
    if (lower === "ann_scaler") return "年化系数（交易日数量）";
    if (lower.includes("temperature")) return "采样温度，越高越发散";
    if (lower.includes("top_p")) return "核采样阈值，控制随机性";
    if (lower.includes("max") && lower.includes("token")) return "最大输出 token 限制";
    if (lower.includes("seed")) return "随机种子";
    if (lower.includes("timeout")) return "超时时间";
    if (lower.includes("epoch")) return "训练轮数或迭代次数";
    if (lower.includes("batch")) return "批大小，影响训练效率与显存";
    if (lower.includes("lr") || lower.includes("learning")) return "学习率，影响收敛速度";
    if (lower.includes("price")) return "价格阈值或限制";
    if (lower.includes("amount")) return "金额或数量上限";
    return "数值型参数";
  };

  const detectEditorLanguage = (path: string) => {
    const lower = path.toLowerCase();
    if (lower.endsWith(".yaml") || lower.endsWith(".yml")) return "yaml";
    if (lower.endsWith(".json")) return "json";
    return "plaintext";
  };

  const formatDraftContent = () => {
    if (!tplDraftPath) return;
    if (tplReadOnly) return;
    const language = detectEditorLanguage(tplDraftPath);
    try {
      if (language === "json") {
        const parsed = JSON.parse(tplDraftContent || "{}") as unknown;
        setTplDraftContent(JSON.stringify(parsed, null, 2));
      } else if (language === "yaml") {
        const parsed = yaml.load(tplDraftContent || "") as unknown;
        setTplDraftContent(yaml.dump(parsed, { lineWidth: 120, noRefs: true }));
      } else {
        editorRef.current?.getAction("editor.action.formatDocument").run();
      }
      setTplDraftValidateMsg("格式化完成");
    } catch (err) {
      const message = err instanceof Error ? err.message : "格式化失败";
      setTplDraftValidateMsg(message);
    }
  };

  const getDefaultBaseTemplate = (items: TemplateSummary[]) => {
    if (!items.length) return null;
    return (
      items.find((item) => item.scenario === "all" && item.version === "v0") ||
      items.find((item) => item.version === "v0") ||
      items[0]
    );
  };

  const sortTemplates = (items: TemplateSummary[]) =>
    [...items].sort((a, b) => {
      const aIsV0 = a.version === "v0";
      const bIsV0 = b.version === "v0";
      if (aIsV0 && !bIsV0) return -1;
      if (!aIsV0 && bIsV0) return 1;
      const aTime = String(a.created_at || "");
      const bTime = String(b.created_at || "");
      return bTime.localeCompare(aTime);
    });

  const loadTemplateList = async () => {
    setTplListLoading(true);
    setTplError(null);
    try {
      const data = await fetchTemplateJSON<{ items: TemplateSummary[] }>("/rdagent/templates");
      const items = sortTemplates(data.items || []);
      setTplList(items);
      const defaultBase = getDefaultBaseTemplate(items);
      if (defaultBase) {
        if (tplBaseScenario !== defaultBase.scenario) setTplBaseScenario(defaultBase.scenario);
        if (tplBaseVersion !== defaultBase.version) setTplBaseVersion(defaultBase.version);
        if (!tplDraftFiles.length) {
          await loadDraftFromTemplate(defaultBase.scenario, defaultBase.version);
        }
      }
    } catch (e) {
      console.error(e);
      setTplError(e instanceof Error ? e.message : "加载模板列表失败");
      setTplList([]);
    } finally {
      setTplListLoading(false);
    }
  };

  const loadDraftFromTemplate = async (scenario: string, version: string) => {
    setTplDraftLoading(true);
    setTplError(null);
    try {
      const listData = await fetchTemplateJSON<{ items: TemplateFileItem[] }>(
        `/rdagent/templates/${encodeURIComponent(scenario)}/${encodeURIComponent(version)}/files`,
      );
      const items = (listData.items || []).slice().sort((a, b) => a.path.localeCompare(b.path));
      const files = await Promise.all(
        items.map(async (file) => {
          const data = await fetchTemplateJSON<{ content: string; path: string }>(
            `/rdagent/templates/${encodeURIComponent(scenario)}/${encodeURIComponent(
              version,
            )}/file?path=${encodeURIComponent(file.path)}`,
          );
          return { path: file.path, content: data.content || "" };
        }),
      );
      setTplDraftFiles(files);
      if (files.length) {
        setTplDraftPath(files[0].path);
        setTplDraftContent(files[0].content);
        setTplDraftOriginal(files[0].content);
      } else {
        setTplDraftPath("");
        setTplDraftContent("");
        setTplDraftOriginal("");
      }
    } catch (e) {
      console.error(e);
      setTplError(e instanceof Error ? e.message : "加载模板文件失败");
      setTplDraftFiles([]);
    } finally {
      setTplDraftLoading(false);
    }
  };

  const confirmDiscardDraft = () => {
    if (tplDraftContent === tplDraftOriginal) return true;
    return window.confirm("当前文件尚未保存，是否继续切换并丢弃更改？");
  };

  const selectDraftFile = (path: string) => {
    if (!confirmDiscardDraft()) return;
    const target = tplDraftFiles.find((file) => file.path === path);
    if (!target) return;
    setTplDraftPath(path);
    setTplDraftContent(target.content || "");
    setTplDraftOriginal(target.content || "");
    setTplDraftValidateMsg(null);
  };

  const reorderDraftFiles = (fromIndex: number, toIndex: number) => {
    if (fromIndex === toIndex) return;
    setTplDraftFiles((prev) => {
      const next = [...prev];
      const [moved] = next.splice(fromIndex, 1);
      next.splice(toIndex, 0, moved);
      return next;
    });
  };

  const saveDraftFile = () => {
    if (!tplDraftPath) return;
    setTplDraftSaving(true);
    setTplDraftFiles((prev) =>
      prev.map((item) => (item.path === tplDraftPath ? { ...item, content: tplDraftContent } : item)),
    );
    setTplDraftOriginal(tplDraftContent);
    setTplDraftSaving(false);
    setTplDraftValidateMsg(null);
  };

  const validateDraftContent = async () => {
    if (!tplDraftPath) {
      window.alert("请先选择需要检查的文件");
      return;
    }
    setTplDraftValidating(true);
    setTplDraftValidateMsg(null);
    try {
      await fetchApiJSON("/rdagent/templates/validate", {
        method: "POST",
        body: JSON.stringify({ path: tplDraftPath, content: tplDraftContent }),
      });
      setTplDraftValidateMsg("语法检查通过");
    } catch (e) {
      const message = e instanceof Error ? e.message : "语法检查失败";
      setTplDraftValidateMsg(message);
    } finally {
      setTplDraftValidating(false);
    }
  };

  const buildDocWithContent = (baseDoc: TemplateFileDoc, path: string, content: string | null) => {
    const fileName = path.split("/").pop() || path;
    if (!content) return { ...baseDoc, title: `${baseDoc.title}（${fileName}）` };
    const trimmed = content.trim();
    let fields: string[] = [];
    let numbers: { key: string; value: number | string }[] = [];
    if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
      try {
        const data = JSON.parse(trimmed);
        const walk = (node: any, prefix = "") => {
          if (!node || typeof node !== "object") return;
          Object.entries(node).forEach(([key, value]) => {
            const fullKey = prefix ? `${prefix}.${key}` : key;
            fields.push(fullKey);
            if (typeof value === "number") {
              numbers.push({ key: fullKey, value });
            } else if (typeof value === "object") {
              walk(value, fullKey);
            }
          });
        };
        walk(data);
      } catch {
        fields = extractYamlKeys(trimmed);
        numbers = extractNumbersFromText(trimmed);
      }
    } else {
      fields = extractYamlKeys(trimmed);
      numbers = extractNumbersFromText(trimmed);
    }
    const uniqueFields = Array.from(new Set(fields));
    const tips = [...(baseDoc.tips || [])];
    if (/{%\s*include/.test(trimmed)) {
      tips.push(
        "prompts.yaml 支持 {% include \"path/to/file\" %} 语法引入片段，保持路径相对模板目录，避免循环引用。",
      );
    }
    return {
      ...baseDoc,
      title: `${baseDoc.title}（${fileName}）`,
      fields: uniqueFields.map((key) => ({ key, desc: describeFieldKey(key) })),
      numbers: numbers.map((item) => ({ ...item, desc: describeNumberKey(item.key) })),
      tips: tips.length ? tips : undefined,
    };
  };

  const getTemplateFileDoc = (path: string, content: string | null): TemplateFileDoc => {
    let baseDoc: TemplateFileDoc = {
      title: "模板文件说明",
      role: "该文件用于驱动 RD-Agent/QLib 的模板配置或提示词。",
      affects: ["相关场景的行为输出", "生成流程的稳定性"],
      rules: ["保持 YAML/JSON 语法合法", "修改时保留必要字段"],
    };
    if (path === "configs/task_config.example.json") {
      baseDoc = {
        title: "任务配置示例",
        role: "RD-Agent 调度任务的示例配置，展示可填写的字段与默认结构。",
        affects: ["任务创建时的默认参数展示", "任务运行的基础配置覆盖"],
        rules: ["保持 JSON 语法完整", "字段名需与 schema 中定义一致", "时间/数值字段保持原类型"],
        tips: ["建议复制后再做定制版本", "不要删除必填字段"],
      };
    } else if (path === "configs/task_config.schema.json") {
      baseDoc = {
        title: "任务配置校验规则",
        role: "定义任务配置的 JSON Schema，用于约束前端/后端参数校验。",
        affects: ["任务创建表单校验", "运行时参数合法性"],
        rules: ["仅调整字段定义时修改", "保持 schema 语法合法", "修改后同步更新示例配置"],
      };
    } else if (path.includes("rdagent/scenarios/general_model/prompts")) {
      baseDoc = {
        title: "通用模型提示词",
        role: "为 general_model 场景提供通用模型选择和评估提示词。",
        affects: ["模型选择结果", "评估输出格式"],
        rules: ["保留输出格式模板", "修改时保持评估维度完整"],
      };
    } else if (path.includes("rdagent/scenarios/kaggle/experiment/prompts")) {
      baseDoc = {
        title: "Kaggle 实验提示词",
        role: "驱动 Kaggle 实验设计、数据处理和模型迭代的提示词。",
        affects: ["Kaggle 实验流程", "评测指标选择"],
        rules: ["保持实验步骤顺序", "保留评测指标字段"],
      };
    } else if (path.includes("rdagent/scenarios/kaggle/knowledge_management/prompts")) {
      baseDoc = {
        title: "Kaggle 知识管理提示词",
        role: "整理 Kaggle 方案的知识库与复盘总结。",
        affects: ["知识条目归档", "复盘总结结构"],
        rules: ["保持知识条目结构", "不要删除复盘字段"],
      };
    } else if (path.includes("rdagent/scenarios/kaggle/prompts")) {
      baseDoc = {
        title: "Kaggle 场景总提示词",
        role: "定义 Kaggle 场景的总体目标、约束和输出要求。",
        affects: ["Kaggle 场景的总流程", "通用输出格式"],
        rules: ["保留核心目标描述", "保持角色定义完整"],
      };
    } else if (path.includes("rdagent/scenarios/qlib/experiment/factor_template")) {
      baseDoc = {
        title: "Qlib 因子模板配置",
        role: "定义因子实验的模型、特征和回测配置，生成因子实验任务。",
        affects: ["因子组合方式", "回测与评估指标"],
        rules: ["保持 handler/model/dataset 结构", "参数类型需符合 Qlib 规范"],
        tips: ["修改后建议重新跑一轮基线实验"],
      };
    } else if (path.includes("rdagent/scenarios/qlib/experiment/model_template")) {
      baseDoc = {
        title: "Qlib 模型模板配置",
        role: "用于模型训练与评估的 Qlib 配置模板，影响模型选择与训练参数。",
        affects: ["模型训练参数", "数据集与评估配置"],
        rules: ["确保 model/dataset/handler 字段齐全", "调参时保持字段类型一致"],
      };
    } else if (path.includes("rdagent/scenarios/qlib/experiment/prompts_core_constraints")) {
      baseDoc = {
        title: "Qlib 核心约束提示词",
        role: "定义 Qlib 实验的核心约束、禁止项与输出边界。",
        affects: ["实验规划边界", "错误预防"],
        rules: ["避免删除约束条款", "新增约束需明确格式"],
      };
    } else if (path.includes("rdagent/scenarios/qlib/experiment/prompts_data_loading")) {
      baseDoc = {
        title: "Qlib 数据加载提示词",
        role: "描述数据加载流程与数据源约束，指导数据准备。",
        affects: ["数据加载步骤", "数据源选择"],
        rules: ["保持数据加载步骤顺序", "确保数据字段说明完整"],
      };
    } else if (path.includes("rdagent/scenarios/qlib/experiment/prompts_dataset_info")) {
      baseDoc = {
        title: "Qlib 数据集信息提示词",
        role: "用于生成数据集描述、字段说明与数据质量检查。",
        affects: ["数据集说明输出", "字段解释"],
        rules: ["保持字段说明结构", "不要删除质量检查项"],
      };
    } else if (path.includes("rdagent/scenarios/qlib/experiment/prompts_error_prevention")) {
      baseDoc = {
        title: "Qlib 错误预防提示词",
        role: "归纳常见错误并引导模型规避，提升实验稳定性。",
        affects: ["错误预防策略", "异常提示内容"],
        rules: ["保留错误类型列表", "新增错误需说明触发条件"],
      };
    } else if (path.includes("rdagent/scenarios/qlib/experiment/prompts_language_spec")) {
      baseDoc = {
        title: "Qlib 输出规范提示词",
        role: "限制输出语言和格式，保证报告一致性。",
        affects: ["输出语言", "报告格式"],
        rules: ["保留格式约束", "修改时保持模板变量不变"],
      };
    } else if (path.includes("rdagent/scenarios/qlib/experiment/prompts")) {
      baseDoc = {
        title: "Qlib 实验提示词",
        role: "驱动 Qlib 实验设计与迭代的主提示词。",
        affects: ["实验流程", "模型/因子选择"],
        rules: ["保持实验步骤顺序", "保留指标输出格式"],
      };
    } else if (path.includes("rdagent/scenarios/qlib/factor_experiment_loader/prompts")) {
      baseDoc = {
        title: "Qlib 因子实验加载提示词",
        role: "负责加载已有因子实验结果并总结使用方式。",
        affects: ["因子复用方式", "结果摘要"],
        rules: ["保持加载流程描述", "不要删除输出字段"],
      };
    } else if (path.includes("rdagent/scenarios/qlib/prompts")) {
      baseDoc = {
        title: "Qlib 场景总提示词",
        role: "定义 Qlib 场景的总体目标、能力边界与输出结构。",
        affects: ["Qlib 场景总流程", "通用输出格式"],
        rules: ["保留总目标", "保持角色/流程定义完整"],
      };
    }
    return buildDocWithContent(baseDoc, path, content);
  };

  const isPromptFile = (path: string) => /prompts?/.test(path);

  const handleEditorMount = (editor: any, monaco: any) => {
    editorRef.current = editor;
    if (!monacoConfiguredRef.current && typeof window !== "undefined") {
      import("monaco-yaml")
        .then(({ setDiagnosticsOptions }) => {
          setDiagnosticsOptions({
            enableSchemaRequest: false,
            validate: true,
            hover: true,
            completion: true,
            format: true,
          });
          monacoConfiguredRef.current = true;
        })
        .catch(() => {
          // ignore yaml diagnostics init failure
        });
    }
  };

  const auditPromptContent = async () => {
    if (!tplDraftPath) {
      window.alert("请先选择需要检查的提示词文件");
      return;
    }
    if (!isPromptFile(tplDraftPath)) {
      window.alert("当前文件不是提示词文件");
      return;
    }
    setTplPromptAuditing(true);
    setTplPromptAuditMsg(null);
    try {
      const data = await fetchApiJSON<{ result: string }>("/rdagent/templates/prompt-audit", {
        method: "POST",
        body: JSON.stringify({ path: tplDraftPath, content: tplDraftContent }),
      });
      setTplPromptAuditMsg(data.result || "未返回分析建议");
    } catch (e) {
      const message = e instanceof Error ? e.message : "提示词检查失败";
      setTplPromptAuditMsg(message);
    } finally {
      setTplPromptAuditing(false);
    }
  };

  const applyDraftContent = (files: TemplateFile[]) => {
    if (!tplDraftPath) return files;
    return files.map((item) =>
      item.path === tplDraftPath ? { ...item, content: tplDraftContent } : item,
    );
  };

  const startEditTemplate = async (item: TemplateSummary) => {
    if (!confirmDiscardDraft()) return;
    setSelectedTemplate(item);
    setTplScenario(item.scenario);
    setTplBaseScenario(item.scenario);
    setTplBaseVersion(item.version);
    setTplVersion("");
    setTplDescription("");
    setTplTaskId("");
    setTplChangedFiles("");
    setTplDiffOpen(false);
    await loadDraftFromTemplate(item.scenario, item.version);
  };

  const activateTemplate = async (item: TemplateSummary) => {
    setTplActivating(true);
    setTplError(null);
    setTplApplyLogs([]);
    setTplApplyResult(null);
    setTplShowApplyLog(true);
    
    try {
      const result = await fetchTemplateJSON<ApplyResult>(
        `/rdagent/templates/${encodeURIComponent(item.scenario)}/${encodeURIComponent(
          item.version,
        )}/apply?force=true&backup=true`,
        { method: "POST" },
      );
      
      setTplApplyResult(result);
      setTplApplyLogs(result.applied_files || []);
      await loadTemplateList();
      
      const successMsg = `应用成功！\n场景/版本: ${result.scenario}/${result.version}\n应用文件: ${result.applied_files.length}\n验证通过: ${result.verification.verified_files}/${result.verification.total_files}\n备份ID: ${result.backup_id || 'N/A'}`;
      window.alert(successMsg);
    } catch (e) {
      console.error(e);
      const message = e instanceof Error ? e.message : "应用模板失败";
      setTplError(message);
      window.alert(message);
    } finally {
      setTplActivating(false);
    }
  };

  const refreshTemplateSha256 = async (item: TemplateSummary) => {
    const key = `${item.scenario}-${item.version}`;
    setTplRefreshingSha256(key);
    setTplError(null);
    
    try {
      const result = await fetchTemplateJSON<{
        ok: boolean;
        scenario: string;
        version: string;
        total_files: number;
        updated_files: number;
        updated_file_list: Array<{
          path: string;
          old_sha256: string;
          new_sha256: string;
          size: number;
        }>;
      }>(
        `/rdagent/templates/${encodeURIComponent(item.scenario)}/${encodeURIComponent(
          item.version,
        )}/refresh-sha256`,
        { method: "POST" },
      );
      
      await loadTemplateList();
      
      if (result.updated_files === 0) {
        window.alert(`SHA256验证值已是最新\n场景/版本: ${result.scenario}/${result.version}\n总文件数: ${result.total_files}\n无需更新`);
      } else {
        const updatedList = result.updated_file_list.map(f => `  - ${f.path}`).join('\n');
        window.alert(`SHA256验证值更新成功！\n场景/版本: ${result.scenario}/${result.version}\n总文件数: ${result.total_files}\n更新文件数: ${result.updated_files}\n\n更新的文件:\n${updatedList}`);
      }
    } catch (e) {
      console.error(e);
      const message = e instanceof Error ? e.message : "更新SHA256失败";
      setTplError(message);
      window.alert(message);
    } finally {
      setTplRefreshingSha256(null);
    }
  };

  const changeBaseTemplate = async (scenario: string, version: string) => {
    if (!confirmDiscardDraft()) return;
    setTplBaseScenario(scenario);
    setTplBaseVersion(version);
    await loadDraftFromTemplate(scenario, version);
  };

  const publishTemplates = async () => {
    setTplPublishing(true);
    setTplError(null);
    try {
      const changedFiles = tplChangedFiles
        .split(/\n|,/)
        .map((item) => item.trim())
        .filter(Boolean);
      const files = applyDraftContent(tplDraftFiles).filter((f) => f.path && f.content !== undefined);
      if (!files.length) {
        window.alert("请先加载模板文件");
        return;
      }
      const payload = {
        scenario: tplScenario,
        version: tplVersion,
        task_id: tplTaskId || undefined,
        description: tplDescription || undefined,
        base_version: tplBaseVersion || undefined,
        changed_files: changedFiles,
        files,
      };
      await fetchTemplateJSON("/rdagent/templates/publish", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      window.alert("发布成功");
      setTplVersion("");
      setTplDescription("");
      setTplTaskId("");
      setTplChangedFiles("");
      await loadTemplateList();
    } catch (e) {
      console.error(e);
      const message = e instanceof Error ? e.message : "发布失败";
      setTplError(message);
      window.alert(message);
    } finally {
      setTplPublishing(false);
    }
  };

  const loadTemplateHistory = async () => {
    setTplHistoryLoading(true);
    setTplError(null);
    try {
      const payload = {
        scenario: tplHistoryScenario || undefined,
        version: tplHistoryVersion || undefined,
      };
      const data = await fetchSchedulerJSON<{ items: TemplateHistoryItem[] }>("/templates/history", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      setTplHistoryItems(data.items || []);
    } catch (e) {
      console.error(e);
      setTplError(e instanceof Error ? e.message : "加载历史失败");
      setTplHistoryItems([]);
    } finally {
      setTplHistoryLoading(false);
    }
  };

  const rollbackTemplate = async () => {
    setTplRollbackLoading(true);
    setTplError(null);
    try {
      const payload = tplRollbackBackup
        ? { backup_path: tplRollbackBackup }
        : { scenario: tplRollbackScenario, version: tplRollbackVersion };
      await fetchSchedulerJSON("/templates/rollback", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      window.alert("回滚成功");
    } catch (e) {
      console.error(e);
      const message = e instanceof Error ? e.message : "回滚失败";
      setTplError(message);
      window.alert(message);
    } finally {
      setTplRollbackLoading(false);
    }
  };

  const loadEnv = async () => {
    try {
      const data = await fetchSchedulerJSON<{ content: string }>("/config/env");
      setEnvText(data.content || "");
    } catch (e) {
      console.error(e);
    }
  };

  const saveEnv = async () => {
    setEnvSaving(true);
    try {
      await fetchSchedulerJSON("/config/env", { method: "POST", body: JSON.stringify({ content: envText }) });
    } catch (e) {
      console.error(e);
    } finally {
      setEnvSaving(false);
    }
  };

  const parseEnvKV = useMemo(() => {
    const lines = envText.split("\n");
    const kv: Record<string, string> = {};
    lines.forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#") || !trimmed.includes("=")) return;
      const [k, ...rest] = trimmed.split("=");
      kv[k] = rest.join("=");
    });
    return kv;
  }, [envText]);

  useEffect(() => {
    if (!nodeRouteReady) return;
    loadEnv();
  }, [schedulerBase, nodeRouteReady]);

  const loadDatasets = async () => {
    try {
      const data = await fetchSchedulerJSON<{ items: Dataset[] }>("/datasets");
      setDatasets(data.items || []);
    } catch (e) {
      console.error(e);
    }
  };

  const loadLog = async () => {
    if (!taskId) return;
    try {
      const data = await fetchSchedulerJSON<{ log: string }>(`/tasks/${taskId}/logs`);
      setLog(data.log || "");
    } catch (e) {
      console.error(e);
    }
  };

  const loadResults = async () => {
    if (!taskId) return;
    try {
      const data = await fetchSchedulerJSON<{ items: ResultItem[] }>(`/tasks/${taskId}/results`);
      setResults(data.items || []);
    } catch (e) {
      console.error(e);
    }
  };

  useEffect(() => {
    if (!nodeRouteReady) return;
    loadTasks();
    loadDatasets();
    loadTemplateList();
  }, [schedulerBase, nodeRouteReady]);

  const handleCreateTask = async (formData: FormData) => {
    setCreating(true);
    try {
      const payload = {
        name: formData.get("name") || "",
        loop_n: Number(formData.get("loop_n") || 1),
        all_duration: formData.get("all_duration") || "1:00:00",
        evolving_mode: formData.get("evolving_mode") || "llm",
      };
      await fetchSchedulerJSON("/tasks", { method: "POST", body: JSON.stringify(payload) });
      await loadTasks();
    } catch (e) {
      console.error(e);
    } finally {
      setCreating(false);
    }
  };

  const handleCreateDataset = async (formData: FormData) => {
    try {
      const payload = {
        name: formData.get("ds_name") || "",
        provider_uri: formData.get("provider_uri") || "",
      };
      await fetchSchedulerJSON("/datasets", { method: "POST", body: JSON.stringify(payload) });
      await loadDatasets();
    } catch (e) {
      console.error(e);
    }
  };

  return (
    <main className={styles.page}>
      <section className={styles.sectionBlock}>
        <h1 className={styles.sectionHeading}>🗓️ RD-Agent 调度</h1>
        <p className={styles.sectionSubtext}>
          集中管理 RD-Agent 任务调度、模板管理与数据集管理。
        </p>
      </section>

      <section className={`${styles.heroCard} ${styles.sectionBlock}`}>
        <div className={styles.rowBetweenWrap}>
          <div>
            <div className={styles.textMain}>
              当前调度后端地址：
              <code className={styles.codeChip}>{baseUrlLabel}</code>
            </div>
            {nodeId && (
              <div className={styles.textSmall}>
                Node: <code className={styles.codeChip}>{nodeId}</code>
              </div>
            )}
          </div>
          <div className={styles.rowWrap}>
            <button
              type="button"
              onClick={() => {
                loadTasks();
                loadDatasets();
                if (taskId) {
                  loadLog();
                  loadResults();
                }
                loadTemplateList();
              }}
              className={styles.btnPrimary}
            >
              刷新
            </button>
          </div>
        </div>
      </section>

      {/* 任务管理 */}
      <section className={styles.sectionBlock}>
        <h2 className={styles.headingSmall}>任务管理</h2>
        <div className={styles.gridTwo}>
          {/* 新建任务 */}
          <div className={styles.card}>
            <h3 className={styles.headingSmall}>新建任务</h3>
            <form
              onSubmit={(e) => {
                e.preventDefault();
                const fd = new FormData(e.currentTarget);
                handleCreateTask(fd);
              }}
            >
              <div className={styles.formGroup}>
                <label className={styles.label} htmlFor="task-name">任务名</label>
                <input
                  id="task-name"
                  name="name"
                  className={styles.input}
                  placeholder="task-1"
                  defaultValue="task-1"
                />
              </div>
              <div className={styles.gridTwo}>
                <div className={styles.formGroup}>
                  <label className={styles.label} htmlFor="loop_n">loop_n</label>
                  <input
                    id="loop_n"
                    name="loop_n"
                    type="number"
                    min={1}
                    className={styles.input}
                    defaultValue={1}
                  />
                </div>
                <div className={styles.formGroup}>
                  <label className={styles.label} htmlFor="all_duration">all_duration</label>
                  <input
                    id="all_duration"
                    name="all_duration"
                    className={styles.input}
                    defaultValue="1:00:00"
                  />
                </div>
              </div>
              <div className={styles.formGroup}>
                <label className={styles.label} htmlFor="evolving-mode">evolving_mode</label>
                <select id="evolving-mode" name="evolving_mode" className={styles.select}>
                  <option value="llm">llm</option>
                  <option value="fixed">fixed</option>
                </select>
              </div>
              <button
                type="submit"
                className={styles.btnSuccess}
                disabled={creating}
              >
                {creating ? "创建中..." : "创建并运行"}
              </button>
            </form>
          </div>

          {/* 任务列表 */}
          <div className={styles.card}>
            <div className={styles.rowBetween}>
              <h3 className={styles.headingSmall}>任务列表</h3>
              <button className={styles.btnSecondary} onClick={loadTasks}>
                刷新
              </button>
            </div>
            <div className={styles.tableWrapper}>
              <table className={styles.statsTable}>
                <thead>
                  <tr>
                    <th className={styles.statsHeaderCell}>ID/Name</th>
                    <th className={styles.statsHeaderCell}>状态</th>
                    <th className={styles.statsHeaderCell}>loop_n</th>
                    <th className={styles.statsHeaderCell}>时长</th>
                  </tr>
                </thead>
                <tbody>
                  {tasks.map((t) => (
                    <tr key={String(t.id || t.name)}>
                      <td className={styles.statsCell}>{t.name || t.id}</td>
                      <td className={styles.statsCell}>
                        <span className={`${styles.badge} ${styles.badgeInfo}`}>
                          {t.status}
                        </span>
                      </td>
                      <td className={styles.statsCell}>{t.loop_n}</td>
                      <td className={styles.statsCell}>{t.all_duration}</td>
                    </tr>
                  ))}
                  {!tasks.length && (
                    <tr>
                      <td className={styles.statsCell} colSpan={4}>
                        暂无任务
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </section>

      {/* 模板管理 */}
      <section className={styles.sectionBlock}>
        <h2 className={styles.headingSmall}>模板管理</h2>
        {tplError && (
          <p className={styles.textDangerSmall}>{tplError}</p>
        )}

        {/* 模板列表 */}
        <div className={styles.card}>
          <h3 className={styles.headingSmall}>模板列表（全量模板）</h3>
          {tplListLoading && <p className={styles.textSmall}>模板列表加载中...</p>}
          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="tpl-list-search">搜索（版本/描述）</label>
            <input
              id="tpl-list-search"
              className={styles.input}
              placeholder="v1 或描述关键字"
              value={tplListSearch}
              onChange={(e) => setTplListSearch(e.target.value)}
            />
          </div>
          <div className={styles.tableWrapper}>
            <table className={styles.statsTable}>
              <thead>
                <tr>
                  <th className={styles.statsHeaderCell}>场景/版本</th>
                  <th className={styles.statsHeaderCell}>状态</th>
                  <th className={styles.statsHeaderCell}>创建时间</th>
                  <th className={styles.statsHeaderCell}>描述</th>
                  <th className={styles.statsHeaderCell}>操作</th>
                </tr>
              </thead>
              <tbody>
                {tplList
                  .filter((item) => {
                    if (!tplListSearch) return true;
                    const needle = tplListSearch.toLowerCase();
                    return (
                      item.version.toLowerCase().includes(needle) ||
                      String(item.description || "").toLowerCase().includes(needle)
                    );
                  })
                  .map((item) => (
                    <tr
                      key={`${item.scenario}-${item.version}`}
                      className={item.version === "v0" ? styles.templateRowPinned : undefined}
                    >
                      <td className={styles.statsCell}>
                        <div>{item.scenario}/{item.version}</div>
                        <div className={styles.textSmall}>{item.scenario_desc || "-"}</div>
                      </td>
                      <td className={styles.statsCell}>
                        <span className={`${styles.badge} ${item.is_active ? styles.badgeSuccess : styles.badgeInfo}`}>
                          {item.is_active ? "已应用" : "未应用"}
                        </span>
                      </td>
                      <td className={styles.statsCell}>{formatBeijingTime(item.created_at)}</td>
                      <td className={styles.statsCell}>{item.description || "-"}</td>
                      <td className={styles.statsCell}>
                        <div className={styles.rowWrap}>
                          <button
                            className={styles.btnInline}
                            onClick={() => startEditTemplate(item)}
                            disabled={!item.is_editable}
                          >
                            编辑
                          </button>
                          <button
                            className={styles.btnInline}
                            onClick={() => activateTemplate(item)}
                            disabled={tplActivating || item.is_active}
                          >
                            {item.is_active ? "已应用" : "应用"}
                          </button>
                          <button
                            className={styles.btnInline}
                            onClick={() => refreshTemplateSha256(item)}
                            disabled={tplRefreshingSha256 === `${item.scenario}-${item.version}`}
                            title="重新计算并更新manifest.json中的SHA256验证值"
                          >
                            {tplRefreshingSha256 === `${item.scenario}-${item.version}` ? "更新中..." : "更新SHA256"}
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                {!tplList.length && (
                  <tr>
                    <td className={styles.statsCell} colSpan={5}>
                      暂无模板
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* 应用日志 */}
        {tplShowApplyLog && (
          <div className={styles.card}>
            <div className={styles.rowBetweenWrap}>
              <h3 className={styles.headingSmall}>模板应用日志</h3>
              <button
                className={styles.btnInline}
                onClick={() => setTplShowApplyLog(false)}
              >
                关闭
              </button>
            </div>
            
            {tplApplyResult && (
              <div className={styles.note}>
                <div className={styles.gridTwo}>
                  <div>
                    <div className={styles.textSmall}>场景/版本</div>
                    <div>{tplApplyResult.scenario}/{tplApplyResult.version}</div>
                  </div>
                  <div>
                    <div className={styles.textSmall}>备份ID</div>
                    <div>{tplApplyResult.backup_id || 'N/A'}</div>
                  </div>
                  <div>
                    <div className={styles.textSmall}>应用文件数</div>
                    <div>{tplApplyResult.applied_files.length}</div>
                  </div>
                  <div>
                    <div className={styles.textSmall}>验证结果</div>
                    <div>
                      <span className={tplApplyResult.verification.verified ? styles.textSuccess : styles.textDanger}>
                        {tplApplyResult.verification.verified_files}/{tplApplyResult.verification.total_files} 通过
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            )}

            <div className={styles.tableWrapper} style={{ maxHeight: '400px', overflow: 'auto' }}>
              <table className={styles.statsTable}>
                <thead>
                  <tr>
                    <th className={styles.statsHeaderCell}>序号</th>
                    <th className={styles.statsHeaderCell}>文件路径</th>
                    <th className={styles.statsHeaderCell}>大小变化</th>
                    <th className={styles.statsHeaderCell}>SHA256</th>
                    <th className={styles.statsHeaderCell}>验证</th>
                    <th className={styles.statsHeaderCell}>状态</th>
                  </tr>
                </thead>
                <tbody>
                  {tplApplyLogs.map((log, idx) => (
                    <tr key={`${log.path}-${idx}`}>
                      <td className={styles.statsCell}>{idx + 1}</td>
                      <td className={styles.statsCell}>
                        <div className={styles.textSmall}>{log.path}</div>
                      </td>
                      <td className={styles.statsCell}>
                        {log.old_size !== undefined ? (
                          <div className={styles.textSmall}>
                            {log.old_size} → {log.size} 字节
                            {log.size_changed && (
                              <span className={styles.textSuccess}> (已变更)</span>
                            )}
                          </div>
                        ) : (
                          <div className={styles.textSmall}>{log.size} 字节</div>
                        )}
                      </td>
                      <td className={styles.statsCell}>
                        <div className={styles.textSmall}>{log.sha256.substring(0, 16)}...</div>
                      </td>
                      <td className={styles.statsCell}>
                        <span className={log.verified ? styles.textSuccess : styles.textDanger}>
                          {log.verified ? '✓' : '✗'}
                        </span>
                      </td>
                      <td className={styles.statsCell}>
                        {log.verified && log.mtime_changed ? (
                          <span className={styles.badgeSuccess}>已替换</span>
                        ) : log.verified && !log.mtime_changed ? (
                          <span className={styles.badgeInfo}>内容相同</span>
                        ) : (
                          <span className={styles.badgeWarn}>验证失败</span>
                        )}
                      </td>
                    </tr>
                  ))}
                  {!tplApplyLogs.length && (
                    <tr>
                      <td className={styles.statsCell} colSpan={6}>
                        {tplActivating ? '正在应用模板...' : '暂无日志'}
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            {tplApplyResult?.verification.failed_files && tplApplyResult.verification.failed_files.length > 0 && (
              <div className={styles.note} style={{ marginTop: '1rem', backgroundColor: '#fff3cd' }}>
                <div className={styles.textSmall} style={{ fontWeight: 'bold', marginBottom: '0.5rem' }}>
                  验证失败的文件:
                </div>
                <ul className={styles.listLogs}>
                  {tplApplyResult.verification.failed_files.map((file, idx) => (
                    <li key={idx} className={styles.textSmall}>
                      <strong>{file.path}</strong>
                      <div>期望: {file.expected.substring(0, 16)}...</div>
                      <div>实际: {file.actual.substring(0, 16)}...</div>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}

        {/* 模板编辑与发布 */}
        <div className={styles.card}>
          <h3 className={styles.headingSmall}>模板编辑与发布</h3>
          <div className={styles.note}>
            <div className={styles.textSmall}>当前基线模板</div>
            <div>{tplBaseScenario}/{tplBaseVersion}</div>
          </div>
          {tplDraftLoading && <p className={styles.textSmall}>模板文件加载中...</p>}
          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="tpl-base-template">选择源模板</label>
            <select
              id="tpl-base-template"
              className={styles.select}
              value={`${tplBaseScenario}::${tplBaseVersion}`}
              onChange={(e) => {
                const [scenario, version] = e.target.value.split("::");
                if (scenario && version) {
                  changeBaseTemplate(scenario, version);
                }
              }}
            >
              {tplList.map((item) => (
                <option key={`${item.scenario}-${item.version}`} value={`${item.scenario}::${item.version}`}>
                  {item.scenario}/{item.version}
                </option>
              ))}
            </select>
          </div>
          <div className={styles.gridTwo}>
            <div className={styles.cardInfo}>
              <div className={styles.textSmall}>文件列表（可拖拽排序）</div>
              <ul className={styles.listLogs}>
                {tplDraftFiles.map((file, idx) => (
                  <Fragment key={file.path}>
                    <li
                      draggable
                      onDragStart={() => setTplDragIndex(idx)}
                      onDragOver={(e) => e.preventDefault()}
                      onDrop={() => {
                        if (tplDragIndex === null) return;
                        reorderDraftFiles(tplDragIndex, idx);
                        setTplDragIndex(null);
                      }}
                      className={tplDraftPath === file.path ? styles.draggableItemActive : styles.draggableItem}
                    >
                      <button
                        className={styles.btnInline}
                        onClick={() => selectDraftFile(file.path)}
                      >
                        {file.path}
                      </button>
                      <button
                        className={styles.btnInline}
                        onClick={() => {
                          if (tplDocPath === file.path) {
                            setTplDocOpen((prev) => !prev);
                          } else {
                            setTplDocPath(file.path);
                            setTplDocOpen(true);
                          }
                        }}
                      >
                        说明
                      </button>
                    </li>
                    {tplDocPath === file.path && (
                      <li className={styles.docListItem}>
                        <div className={styles.docPanel}>
                          <div className={styles.docHeader}>
                            <div>
                              <div className={styles.textSmall}>模板文件说明</div>
                              <div className={styles.headingSmall}>{tplDocPath}</div>
                            </div>
                            <button
                              className={styles.btnInline}
                              onClick={() => setTplDocOpen((prev) => !prev)}
                            >
                              {tplDocOpen ? "收起" : "展开"}
                            </button>
                          </div>
                          {tplDocOpen && (() => {
                            const content = tplDraftFiles.find((item) => item.path === tplDocPath)?.content || null;
                            const doc = getTemplateFileDoc(tplDocPath, content);
                            return (
                              <div className={styles.docBody}>
                                <h4 className={styles.headingSmall}>{doc.title}</h4>
                                <p className={styles.textSmall}>{doc.role}</p>
                                <div className={styles.modalSection}>
                                  <div className={styles.textSmall}>影响范围</div>
                                  <ul className={styles.listLogs}>
                                    {doc.affects.map((item) => (
                                      <li key={item} className={styles.textSmall}>{item}</li>
                                    ))}
                                  </ul>
                                </div>
                                <div className={styles.modalSection}>
                                  <div className={styles.textSmall}>修改规则</div>
                                  <ul className={styles.listLogs}>
                                    {doc.rules.map((item) => (
                                      <li key={item} className={styles.textSmall}>{item}</li>
                                    ))}
                                  </ul>
                                </div>
                                {doc.tips && doc.tips.length > 0 && (
                                  <div className={styles.modalSection}>
                                    <div className={styles.textSmall}>操作建议</div>
                                    <ul className={styles.listLogs}>
                                      {doc.tips.map((item) => (
                                        <li key={item} className={styles.textSmall}>{item}</li>
                                      ))}
                                    </ul>
                                  </div>
                                )}
                                {doc.fields && doc.fields.length > 0 && (
                                  <div className={styles.modalSection}>
                                    <div className={styles.textSmall}>字段说明</div>
                                    <ul className={styles.listLogs}>
                                      {doc.fields.map((item) => (
                                        <li key={item.key} className={styles.textSmall}>
                                          <strong>{item.key}</strong>：{item.desc}
                                        </li>
                                      ))}
                                    </ul>
                                  </div>
                                )}
                                {doc.numbers && doc.numbers.length > 0 && (
                                  <div className={styles.modalSection}>
                                    <div className={styles.textSmall}>数值参数</div>
                                    <ul className={styles.listLogs}>
                                      {doc.numbers.map((item, idx) => (
                                        <li key={`${item.key}-${idx}`} className={styles.textSmall}>
                                          <strong>{item.key}</strong> = {item.value}（{item.desc}）
                                        </li>
                                      ))}
                                    </ul>
                                  </div>
                                )}
                              </div>
                            );
                          })()}
                        </div>
                      </li>
                    )}
                  </Fragment>
                ))}
                {!tplDraftFiles.length && (
                  <li className={styles.textSmall}>暂无文件</li>
                )}
              </ul>
            </div>
            <div className={styles.cardInfo}>
              <div className={`${styles.rowBetweenWrap} ${styles.editorToolbar}`}>
                <div className={styles.rowWrap}>
                  <div className={styles.textSmall}>{tplDraftPath || "请选择文件"}</div>
                  {tplDraftPath && (
                    <span className={styles.statusBadge}>
                      {detectEditorLanguage(tplDraftPath).toUpperCase()}
                    </span>
                  )}
                  {tplReadOnly && <span className={styles.statusBadgeWarn}>只读</span>}
                </div>
                <div className={styles.rowWrap}>
                  {tplDraftContent !== tplDraftOriginal && (
                    <span className={styles.textDangerSmall}>未保存</span>
                  )}
                  <button
                    className={styles.btnInline}
                    onClick={() => setTplDiffOpen((v) => !v)}
                  >
                    {tplDiffOpen ? "隐藏 Diff" : "查看 Diff"}
                  </button>
                  {tplDiffOpen && (
                    <>
                      <button
                        className={styles.btnInline}
                        onClick={() => setTplDiffSideBySide((v) => !v)}
                      >
                        {tplDiffSideBySide ? "上下对比" : "左右对比"}
                      </button>
                      <button
                        className={styles.btnInline}
                        onClick={() => setTplDiffHideUnchanged((v) => !v)}
                      >
                        {tplDiffHideUnchanged ? "显示全部" : "只看变更"}
                      </button>
                    </>
                  )}
                  <button
                    className={styles.btnInline}
                    onClick={() => setTplReadOnly((v) => !v)}
                    disabled={!tplDraftPath}
                  >
                    {tplReadOnly ? "切回可编辑" : "只读模式"}
                  </button>
                  <button
                    className={styles.btnInline}
                    onClick={saveDraftFile}
                    disabled={tplDraftSaving || !tplDraftPath || tplDraftContent === tplDraftOriginal}
                  >
                    {tplDraftSaving ? "保存中..." : "保存"}
                  </button>
                  <button
                    className={styles.btnInline}
                    onClick={formatDraftContent}
                    disabled={!tplDraftPath || tplReadOnly}
                  >
                    格式化
                  </button>
                  <button
                    className={styles.btnInline}
                    onClick={validateDraftContent}
                    disabled={tplDraftValidating || !tplDraftPath}
                  >
                    {tplDraftValidating ? "检查中..." : "语法检查"}
                  </button>
                  <button
                    className={styles.btnInline}
                    onClick={auditPromptContent}
                    disabled={tplPromptAuditing || !tplDraftPath || !isPromptFile(tplDraftPath)}
                  >
                    {tplPromptAuditing ? "分析中..." : "提示词一致性检查"}
                  </button>
                </div>
              </div>
              <label className={styles.label}>文件内容</label>
              <div className={styles.editorPanel}>
                <Editor
                  language={detectEditorLanguage(tplDraftPath)}
                  value={tplDraftContent}
                  onChange={(value: string | undefined) => setTplDraftContent(value ?? "")}
                  onMount={handleEditorMount}
                  height="640px"
                  options={{
                    readOnly: tplReadOnly || !tplDraftPath,
                    minimap: { enabled: false },
                    fontSize: 13,
                    wordWrap: "on",
                    scrollBeyondLastLine: false,
                    automaticLayout: true,
                  }}
                />
              </div>
              {tplDraftValidateMsg && (
                <p className={styles.textSmall}>{tplDraftValidateMsg}</p>
              )}
              {tplPromptAuditMsg && (
                <pre className={styles.auditBox}>{tplPromptAuditMsg}</pre>
              )}
              {tplDiffOpen && (
                <div className={styles.editorDiffPanel}>
                  <DiffEditor
                    language={detectEditorLanguage(tplDraftPath)}
                    original={tplDraftOriginal}
                    modified={tplDraftContent}
                    height="520px"
                    options={{
                      readOnly: true,
                      renderSideBySide: tplDiffSideBySide,
                      hideUnchangedRegions: { enabled: tplDiffHideUnchanged },
                      minimap: { enabled: false },
                      fontSize: 13,
                      wordWrap: "on",
                      automaticLayout: true,
                    }}
                  />
                </div>
              )}
            </div>
          </div>
          <div className={styles.gridTwo}>
            <div className={styles.formGroup}>
              <label className={styles.label} htmlFor="tpl-scenario">scenario</label>
              <input
                id="tpl-scenario"
                className={styles.input}
                value={tplScenario}
                onChange={(e) => setTplScenario(e.target.value)}
              />
            </div>
            <div className={styles.formGroup}>
              <label className={styles.label} htmlFor="tpl-version">version</label>
              <input
                id="tpl-version"
                className={styles.input}
                value={tplVersion}
                onChange={(e) => setTplVersion(e.target.value)}
              />
            </div>
            <div className={styles.formGroup}>
              <label className={styles.label} htmlFor="tpl-task-id">task_id</label>
              <input
                id="tpl-task-id"
                className={styles.input}
                value={tplTaskId}
                onChange={(e) => setTplTaskId(e.target.value)}
              />
            </div>
            <div className={styles.formGroup}>
              <label className={styles.label} htmlFor="tpl-base-version">base_version</label>
              <input
                id="tpl-base-version"
                className={styles.input}
                value={tplBaseVersion}
                onChange={(e) => setTplBaseVersion(e.target.value)}
              />
            </div>
            <div className={styles.formGroup}>
              <label className={styles.label} htmlFor="tpl-description">description</label>
              <input
                id="tpl-description"
                className={styles.input}
                value={tplDescription}
                onChange={(e) => setTplDescription(e.target.value)}
              />
            </div>
            <div className={styles.formGroup}>
              <label className={styles.label} htmlFor="tpl-changed-files">changed_files</label>
              <textarea
                id="tpl-changed-files"
                className={styles.textarea}
                rows={2}
                value={tplChangedFiles}
                onChange={(e) => setTplChangedFiles(e.target.value)}
              />
            </div>
          </div>
          <button
            className={styles.btnSuccess}
            onClick={publishTemplates}
            disabled={tplPublishing}
          >
            {tplPublishing ? "发布中..." : "发布模板"}
          </button>
        </div>
      </section>


      {/* 发布历史 */}
      <section className={styles.sectionBlock}>
        <h2 className={styles.headingSmall}>发布历史</h2>
        <div className={styles.gridTwo}>
          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="tpl-history-scenario">scenario</label>
            <input
              id="tpl-history-scenario"
              className={styles.input}
              value={tplHistoryScenario}
              onChange={(e) => setTplHistoryScenario(e.target.value)}
            />
          </div>
          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="tpl-history-version">version</label>
            <input
              id="tpl-history-version"
              className={styles.input}
              value={tplHistoryVersion}
              onChange={(e) => setTplHistoryVersion(e.target.value)}
            />
          </div>
        </div>
        <button className={styles.btnSecondary} onClick={loadTemplateHistory}>
          {tplHistoryLoading ? "加载中..." : "查询历史"}
        </button>
        <div className={styles.tableWrapper}>
          <table className={styles.statsTable}>
            <thead>
              <tr>
                <th className={styles.statsHeaderCell}>时间</th>
                <th className={styles.statsHeaderCell}>动作</th>
                <th className={styles.statsHeaderCell}>场景/版本</th>
                <th className={styles.statsHeaderCell}>备份路径</th>
              </tr>
            </thead>
            <tbody>
              {tplHistoryItems.map((item, idx) => (
                <tr key={`${item.backup_path || idx}`}>
                  <td className={styles.statsCell}>{formatBeijingTime(item.created_at)}</td>
                  <td className={styles.statsCell}>
                    <span className={`${styles.badge} ${styles.badgeInfo}`}>
                      {String(item.extra?.action || "-")}
                    </span>
                  </td>
                  <td className={styles.statsCell}>
                    {String(item.extra?.scenario || "-")}/{String(item.extra?.version || "-")}
                  </td>
                  <td className={styles.statsCell}>{item.backup_path || "-"}</td>
                </tr>
              ))}
              {!tplHistoryItems.length && (
                <tr>
                  <td className={styles.statsCell} colSpan={4}>
                    暂无历史
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      {/* 回滚模板 */}
      <section className={styles.sectionBlock}>
        <h2 className={styles.headingSmall}>回滚模板</h2>
        <div className={styles.card}>
          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="tpl-rollback-backup">backup_path（优先）</label>
            <input
              id="tpl-rollback-backup"
              className={styles.input}
              value={tplRollbackBackup}
              onChange={(e) => setTplRollbackBackup(e.target.value)}
            />
          </div>
          <div className={styles.gridTwo}>
            <div className={styles.formGroup}>
              <label className={styles.label} htmlFor="tpl-rollback-scenario">scenario</label>
              <input
                id="tpl-rollback-scenario"
                className={styles.input}
                value={tplRollbackScenario}
                onChange={(e) => setTplRollbackScenario(e.target.value)}
              />
            </div>
            <div className={styles.formGroup}>
              <label className={styles.label} htmlFor="tpl-rollback-version">version</label>
              <input
                id="tpl-rollback-version"
                className={styles.input}
                value={tplRollbackVersion}
                onChange={(e) => setTplRollbackVersion(e.target.value)}
              />
            </div>
          </div>
          <button
            className={styles.btnDanger}
            onClick={rollbackTemplate}
            disabled={tplRollbackLoading}
          >
            {tplRollbackLoading ? "回滚中..." : "执行回滚"}
          </button>
        </div>
      </section>

      {/* 日志和结果 */}
      <section className={styles.sectionBlock}>
        <h2 className={styles.headingSmall}>日志与结果</h2>
        <div className={styles.row}>
          <label className={styles.label} htmlFor="task-id-input">任务 ID</label>
          <input
            id="task-id-input"
            className={styles.input}
            placeholder="输入任务 ID"
            value={taskId}
            onChange={(e) => setTaskId(e.target.value)}
          />
          <button className={styles.btnPrimary} onClick={loadLog}>
            查看日志
          </button>
          <button className={styles.btnSecondary} onClick={loadResults}>
            查看结果
          </button>
        </div>
        <div className={styles.gridTwo}>
          <div>
            <h3 className={styles.headingSmall}>日志</h3>
            <pre className={styles.logTerminal}>{log || "无日志"}</pre>
          </div>
          <div>
            <h3 className={styles.headingSmall}>结果</h3>
            <pre className={styles.resultTerminal}>
              {results.length ? JSON.stringify(results, null, 2) : "无结果"}
            </pre>
          </div>
        </div>
      </section>

      {/* 数据集管理 */}
      <section className={styles.sectionBlock}>
        <h2 className={styles.headingSmall}>数据集管理</h2>
        <form
          className={styles.gridThree}
          onSubmit={(e) => {
            e.preventDefault();
            const fd = new FormData(e.currentTarget);
            handleCreateDataset(fd);
            e.currentTarget.reset();
          }}
        >
          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="ds-name">数据集名称</label>
            <input id="ds-name" name="ds_name" className={styles.input} placeholder="数据集名称" required />
          </div>
          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="provider-uri">provider_uri</label>
            <input id="provider-uri" name="provider_uri" className={styles.input} placeholder="provider_uri" required />
          </div>
          <button className={styles.btnSuccess} type="submit">
            新建数据集
          </button>
        </form>
        <div className={styles.tableWrapper}>
          <table className={styles.statsTable}>
            <thead>
              <tr>
                <th className={styles.statsHeaderCell}>名称</th>
                <th className={styles.statsHeaderCell}>provider_uri</th>
              </tr>
            </thead>
            <tbody>
              {datasets.map((d) => (
                <tr key={String(d.name)}>
                  <td className={styles.statsCell}>{d.name}</td>
                  <td className={styles.statsCell}>{d.provider_uri}</td>
                </tr>
              ))}
              {!datasets.length && (
                <tr>
                  <td className={styles.statsCell} colSpan={2}>
                    暂无数据集
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
