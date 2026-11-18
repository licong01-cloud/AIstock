# DeepSeek Flash 策略交付和验收

目标为 `backend-main`。BUG-2251 将共享默认值、请求覆盖与当前模型绑定中的
DeepSeek Pro 改为 `deepseek-v4-flash`；历史模型身份与费用记录不改写。

先在现有 `aistock-pg-dev / aistock_dev` 中事务验证
`backend/db/migrations/replace_deepseek_pro_with_flash_20261118.sql`，回滚测试数据。
源码合入后，仅在用户授权的 `timescaledb / aistock` 中执行同一合入版本的迁移，
使用显式事务、ON_ERROR_STOP 和目标身份核对；失败立即回滚，不能更换数据库继续。
迁移可以重复执行；新 Flash 模型的验证状态和价格未知，不继承 Pro 的验证结论或价格。

后端重启由用户执行，在 `F:\Dev\AIstock` 运行 `start_backend.bat` 的既有重启入口。
关闭标签页或同步源码不能证明旧客户端已经卸载。

重启后的只读验收：

1. `GET http://127.0.0.1:8001/api/v1/runtime-identity`：进程冻结身份必须匹配合入 SHA。
2. `GET http://127.0.0.1:8001/api/v1/llm/deepseek-policy`：默认值与两种 Pro 覆盖均解析为
   Flash，`remaining_pro_configurations=0`、`status=ready`、`live_model_called=false`。
3. `python scripts/aistock_issue_workflow.py post-restart-verify --bug-id BUG-2251 --target backend-main --expected-identity <本次合入SHA>`。

该 smoke 检查加载策略和当前配置，不发起付费模型调用，不证明余额、模型可用性或外部
RD-Agent 进程已加载新配置。存量进程、其他检出目录或独立远端服务必须另行同步并由其
操作者重启/验收；完成之前保持运行态 pending，不关闭 BUG。
