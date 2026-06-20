# Research Assistant Real-Model Smoke Runbook (2026-06-20)

关联：BUG-436 / BUG-431 / BUG-423。

## 目的

把 BUG-431 合并后遗留的真实模型验收缺口固化成可重复 smoke：

- A1：当 RA 现有 MCP 确实没有对应数据源时，最终回答必须显式说明“没有对应数据源 / 无法获取该数据”，并指出缺少的数据类别。
- A2：股票研究类问题必须实际向 agent 提供 capability-backed 的 `external_research` + `stock_analysis` 只读工具，且 `external_research_search_web` 可执行，不出现 `capability_not_found` / `KeyError` / `chat_turn_unexpected_error`。

## 运行命令

前置条件：调用方在当前 shell 提供 DeepSeek key。脚本只读取进程环境变量，不从 DB 查 key；缺 key 时以退出码 `77` loud-skip，不伪造通过。

```powershell
$env:DEEPSEEK_API_KEY = "<operator-provided-key>"
rtk proxy python scripts/research_assistant_real_model_smoke.py --output tmp/validation/research_assistant/real_model_smoke.json
```

缺 key 的预期输出：

```text
research-assistant real-model smoke: SKIPPED reason_code=deepseek_api_key_missing; missing=DEEPSEEK_API_KEY; db_lookup=false; fake_pass=false
```

## 安全边界

- 不启动、不重启 backend/frontend/TDX 服务；不触碰生产 `8001` / `3000` / `19080`。
- 不连接生产 DB，不执行 DDL，不对生产 seed。
- 使用 in-memory RA repository；只调用外部 LLM 和 RA 只读 MCP loopback/smoke provider。
- `external_research_save_evidence` 等写入/草稿能力仍按既有审批/证据门禁处理，本 smoke 不调用写入工具。

## BUG-431 / BUG-423 合并后生产解封 smoke

用户完成生产侧动作后再运行：

1. 拉取最新 `main`。
2. 从 `configs/research_assistant/runtime_context.yaml` 重新 seed/import 并激活 RA runtime config。
3. 用户自行重启 backend，使合并代码和新 runtime config 生效。
4. 运行本 smoke 命令，确认 A1/A2 真实模型路径通过；若 key 缺失，先配置 `DEEPSEEK_API_KEY` 后重跑。
5. 追加人工 live smoke：用真实模型问“国城矿业的基本情况、近期走势、未来趋势怎样”，确认不再返回 `chat_turn_unexpected_error`；如仍有历史脏配置，应返回具体 `runtime_config_invalid_*`。

合并代码不等于生产已激活；runtime config seed/import 与 backend restart 仍由用户执行。
