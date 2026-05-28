上下文预算必须由 active runtime config 和 active model profile 决定。不得依赖代码中的固定 token 窗口、固定历史条数、固定 fresh tail 或固定压缩阈值。预算不足时只能按配置裁剪低优先级派生上下文，不能静默丢弃用户确认、审批状态、风险边界和 open tasks。
