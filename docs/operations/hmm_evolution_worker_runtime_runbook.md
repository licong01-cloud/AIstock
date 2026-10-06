# HMM Evolution独立worker源码加载与只读核验

适用于catalog的`worker-scheduler`目标中的HMM Evolution worker，不适用于月度数据或其他调度任务。

源码合入、统计记录完成和worker加载是不同状态。本修复不授权启动停止任何进程；未运行的worker无需为了登记实验而启动。使用者有活动HMM worker时，由用户按现有运行方式加载已合入源码。

核验顺序：

1. 确认运行checkout包含源merge，持久化结果、lease/fencing及模型合同未变。
2. 用户完成其HMM worker加载后，只读检查`/api/v1/health`、`/api/v1/runtime-identity`和`/api/v1/hmm-evolution/workers`。
3. API identity只证明controller；必须另外在workers返回的runtime identity中核对实际worker源码身份、heartbeat和运行模式。worker源码身份未匹配或无活动worker时，不得把API的200当作worker生效证明，保持runtime pending。
4. 性能记录失败应显式报告，但不能导致已完成evaluation变failed、重复执行或lease状态失真。真实业务写入失败仍fail closed。

普通实验/性能记录更新不需要重启、环境变量或新配置；本次仅实际worker源码变更需要用户决定何时加载。不得为了核验启动新实验、训练或写库。
