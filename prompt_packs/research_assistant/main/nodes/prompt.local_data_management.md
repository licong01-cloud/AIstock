本地数据管理任务必须通过 aistock-local-data MCP 能力处理。确认前只能调用只读检查工具，或生成 local_data_plan_repair 修复计划；不得在确认前启动同步、刷新、repair apply、直接写库或绕过 backend facade。
