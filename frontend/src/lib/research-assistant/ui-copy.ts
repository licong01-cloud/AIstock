const researchAssistantUiCopy = {
  "chat": {
    "initialSteps": [
      {
        "label": "接收问题",
        "status": "idle"
      },
      {
        "label": "理解意图",
        "status": "idle"
      },
      {
        "label": "回答",
        "status": "idle"
      },
      {
        "label": "等待任务指令",
        "status": "idle"
      },
      {
        "label": "MCP 预检查",
        "status": "locked"
      },
      {
        "label": "执行",
        "status": "locked"
      },
      {
        "label": "写入记忆",
        "status": "locked"
      }
    ],
    "thinkingSteps": [
      {
        "label": "接收问题",
        "status": "current"
      },
      {
        "label": "理解意图",
        "status": "idle"
      },
      {
        "label": "回答",
        "status": "idle"
      },
      {
        "label": "等待任务指令",
        "status": "idle"
      },
      {
        "label": "MCP 预检查",
        "status": "locked"
      },
      {
        "label": "执行",
        "status": "locked"
      },
      {
        "label": "写入记忆",
        "status": "locked"
      }
    ],
    "welcomeMessages": [
      "你好，我是 AIstock 研究助理。你可以直接问我能力、状态或原理，也可以描述明确任务，例如设计 QE 实验草案、诊断报错或整理 Issue 证据。"
    ],
    "emptyInputReply": "请直接提问，或描述你要完成的研究任务。",
    "fallbackReply": "我可以直接回答你的问题；如果你要我执行任务，请描述目标和边界。",
    "catalogSetupReply": {
      "notReady": "助理目录尚未初始化完整，所以我还不能安全地理解和执行这次对话。",
      "missingPrefix": "缺少的目录：",
      "missingFallback": "缺少 Prompt Tree、MCP、Skill 或模型路由目录。",
      "nextStep": "请点击右侧初始化按钮；初始化完成后，再重新发送你的研究或实验目标。"
    },
    "statusText": {
      "done": "已完成",
      "current": "进行中",
      "locked": "未解锁",
      "failed": "失败",
      "default": "等待"
    },
    "proposalStatusText": {
      "waiting_confirmation": "等待确认",
      "draft_only": "仅生成草稿",
      "ready": "可继续讨论",
      "default": "待处理"
    },
    "avatar": {
      "user": "我",
      "assistant": "AI"
    },
    "rail": {
      "ariaLabel": "任务状态轨道",
      "eyebrow": "实时状态",
      "title": "助理正在做什么",
      "body": "这里只展示简要进度；计划、确认、Trace 和 payload 留在可折叠详情或审计页面。",
      "capabilityEyebrow": "能力选择",
      "defaultMcp": "将按需选择 Research Assistant、QE、Validation 等 MCP。",
      "defaultSkill": "将按需选择本地 Skill Catalog，不要求用户记住工具名。",
      "defaultModel": "主模型负责理解、确认和调度。",
      "contextEyebrow": "上下文健康",
      "contextStatusPrefix": "状态：",
      "contextWindowUsage": "窗口使用率：",
      "contextSummaryPrefix": "摘要",
      "contextSummaryUnit": "条",
      "contextKeyFactPrefix": "关键事实",
      "contextKeyFactSuffix": "条，可在审计页回溯原文。",
      "adminLink": "打开后台管理 / 审计"
    },
    "planSummary": {
      "welcomeEyebrow": "下一步",
      "welcomeTitle": "直接提问或描述任务",
      "welcomeExample": "示例：你能生成 QE 实验、诊断 bug、提交 Issue 吗？",
      "detailEyebrow": "操作细节",
      "detailTitle": "对话细节",
      "clarificationTitle": "需要你确认",
      "proposalSeparator": " · "
    },
    "catalogCard": {
      "readyEyebrow": "初始化完成",
      "notReadyEyebrow": "需要初始化",
      "readyTitle": "助理目录已准备好",
      "notReadyTitle": "助理目录尚未准备好",
      "readyBody": "Prompt Tree、MCP、Skill 和模型路由目录已经可用。请重新发送你的研究或实验目标。",
      "notReadyBody": "Prompt Tree、MCP、Skill 和模型路由目录必须先写入数据库。完成后，助理才能按设计方案选择提示词分支、模型和工具。",
      "checkCurrent": "当前",
      "checkExpected": "至少",
      "initializing": "正在初始化目录...",
      "initialize": "初始化助理目录",
      "initDone": "目录初始化完成。请重新发送你的研究或实验目标。",
      "initIncomplete": "目录仍未完整，请查看缺少项后再次初始化。",
      "initFailed": "目录初始化失败，请查看后台日志。"
    },
    "composer": {
      "placeholder": "直接提问或描述任务，例如：你能诊断 QE 实验失败原因吗？",
      "send": "发送"
    },
    "hero": {
      "eyebrow": "AIstock Research Assistant",
      "title": "像研究搭档一样对话，由 MCP 安全执行",
      "body": "先回答你真正问的问题；只有明确任务才进入计划、预检查和执行边界。",
      "newConversation": "新建对话"
    }
  },
  "workbench": {
    "defaultQeDraftPayload": {
      "template_kind": "custom_evo",
      "title": "QE experiment draft",
      "config_json": {
        "loops": [
          {
            "factor_keys": [
              "alpha001"
            ],
            "model_id": "lightgbm"
          }
        ],
        "stock_pool": "fixed_pit_pool",
        "backtest_window": {
          "start": "2023-01-01",
          "end": "2024-12-31"
        }
      }
    },
    "legacyDryRunPayload": {
      "title": "候选 Issue",
      "problem_statement": "用于验证 dry-run 边界"
    },
    "defaultProposalTitle": "生成 QE template 草案",
    "defaultProposalSummary": "只生成草案，不触发 materialize/run；确认后进入 Action Proposal、preflight 和审批流程",
    "disabledReasons": {
      "busy": "操作正在执行",
      "selectCapability": "请选择 capability",
      "selectTask": "请选择任务账本",
      "payloadObject": "payload 必须是 JSON object",
      "selectProposal": "请选择 Action Proposal",
      "notConfirmablePrefix": "当前",
      "notConfirmableSuffix": "不可确认",
      "enterConfirmationPrefix": "请输入确认文本",
      "confirmFirst": "请先确认 Action Proposal",
      "approvalOnly": "仅 approval_required 状态需要审批",
      "enterApprovalConfirmationPrefix": "请输入审批确认文本",
      "preflightFirst": "请先通过 preflight；如需要请完成 approval"
    },
    "result": {
      "emptyTitle": "等待 Action Proposal",
      "emptyHint": "执行结果会以卡片展示；raw JSON 仅在调试抽屉中查看",
      "nextStepPrefix": "下一步：",
      "auditLinkPrefix": "审计链接：",
      "eyebrow": "执行结果",
      "emptySummary": "暂无摘要"
    },
    "sections": {
      "actionErrorTitle": "Action Proposal 操作失败",
      "consoleTitle": "Action Proposal 执行控制台",
      "consoleEyebrow": "proposal / confirm / preflight / approval / execute",
      "capabilityLabel": "选择 capability",
      "taskLedger": "任务账本",
      "selectTaskOption": "请选择任务",
      "proposalTitle": "Proposal 标题",
      "proposalSummary": "Proposal 摘要",
      "inputJson": "输入 JSON",
      "invalidProposalJson": "JSON 无效，无法创建 Proposal",
      "creating": "创建中...",
      "createProposal": "创建 Proposal",
      "loading": "加载中...",
      "refresh": "刷新",
      "postCreateHelp": "创建 Proposal 后仍不会直接调用 MCP。",
      "capabilityDrawer": "capability schema / gates",
      "noCapability": "无 capability",
      "executionStatusTitle": "执行状态",
      "executionStatusEyebrow": "human-readable state",
      "selectActionProposal": "选择 Action Proposal",
      "selectProposalOption": "请选择 Proposal",
      "status": "状态",
      "updatedAt": "更新时间",
      "noProposalSelected": "尚未选择 Proposal",
      "confirmationText": "确认文本",
      "approvalConfirmationText": "审批确认文本",
      "canRunNext": "可以执行下一步",
      "preflightResultTitle": "Preflight / Result",
      "preflightResultEyebrow": "cards first, json debug drawer second",
      "approval": "Approval",
      "approvalRequired": "需要审批",
      "approvalNotRequired": "无需审批",
      "waitingPreflightTitle": "等待 Action preflight",
      "waitingPreflightHint": "preflight 结果会先以卡片展示",
      "debugPreflightPayload": "调试 preflight payload",
      "debugExecutePayload": "调试 execute payload",
      "auditEventPayload": "审计事件 payload",
      "legacyTitle": "旧版 dry-run 兼容",
      "legacyEyebrow": "no real execution",
      "legacyErrorTitle": "旧版 dry-run 失败",
      "selectMcpTool": "选择 MCP 工具",
      "preflightRunning": "preflight 中...",
      "executePreflight": "执行 preflight",
      "dryRunRunning": "dry-run 中...",
      "executeDryRun": "执行 dry-run",
      "invalidExecutionJson": "JSON 无效，无法执行",
      "waitingDryRunTitle": "等待 dry-run",
      "waitingDryRunHint": "仅用于验证旧 execute gateway 兼容边界",
      "catalogTitle": "Capability 与 Proposal 目录",
      "catalogEyebrow": "real catalog",
      "emptyCapabilities": "暂无 capability；请先执行 catalog seed 或 capability sync。",
      "capabilityColumn": "能力",
      "riskColumn": "风险",
      "effectColumn": "副作用",
      "statusColumn": "状态"
    },
    "gateLabels": {
      "propose": "创建",
      "confirm": "确认",
      "preflight": "Preflight",
      "approve": "审批",
      "execute": "执行"
    }
  }
} as const;

export default researchAssistantUiCopy;
