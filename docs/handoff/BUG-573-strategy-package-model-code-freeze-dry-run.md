# BUG-573 StrategyPackage model_code freeze / dry-run 影响预览

更新时间：2026-07-02

## 1. 边界

- 本轮为 develop-only 修复：代码 + 测试 + dry-run 预览；未执行 apply，未写生产 DB，未启停服务，未跑 operator，未发/撤券商订单。
- RCA 证据已拷贝至 `docs/handoff/miniqmt_prerun_multifailure_wsl_qe_readonly_rootcause_2026-07-02.md`。
- production gates：`production_ddl_gate=noop`，`production_backend_dependency_gate=noop`，`production_frontend_dependency_gate=noop`。

## 2. 根因复述

- 2026-07-01 asset backfill 冻结了 `model_weight(params.pkl)`，但未冻结 pickle 本地类所需的 `model.py` / helper 代码资产。
- 受影响 MiniQMT L2/L16 runtime workspace 只有 `model/params.pkl`，无 `model/model.py`；`params.pkl` 引用 `model.LSTM_10D_hs64_d02`，WSL 子进程在 `pickle.load()` 才抛 `AttributeError`。
- 期望行为：freeze/backfill 阶段若发现本地 pickle module 引用，必须把 `model_code_assets` 一起冻结；runtime prepare/preflight 若缺代码必须 fail-fast 为 `strategy_package_model_code_missing`。

## 3. 本 PR 修复点

- Part 1：`live_inference.py` 复用 pickle/torch payload scanner，识别 protocol 4 / `STACK_GLOBAL` 形态的 `model.LSTM_10D_hs64_d02`，缺 `model.py` 时在 preflight/prepare 阶段 loud fail。
- Part 2：`package_asset_freeze.py` 在冻结 `model_weight` 时扫描 params payload；即使 `conf.yaml` 没有 `pt_model_uri`，只要 pickle 引用本地 `model` module，就从 QE workspace 冻结 `model.py` 及本地 helper closure。
- Part 2：`package_asset_backfill.py` 不再把“manifest hash 相同且 ledger 覆盖”作为唯一 skip 条件；若现有 frozen weight 引用本地 model 但 manifest 缺 code assets，dry-run 会计划 re-freeze。
- Part 3：`scripts/strategy_package_asset_backfill.py` 增加 `--model-code-repair-preview` 与只读筛选 `--model-weight-backfilled-date YYYY-MM-DD`；默认仍是 dry-run，apply gate 保持不变。

## 4. Dry-run 命令与结果

命令（只读 / dry-run，连接生产 DB 设置为 readonly；未传 `--apply`）：

```powershell
$env:AISTOCK_PACKAGE_ASSET_STORE_ROOT="F:\Dev\AIstock\rdagent_assets\package_assets"
rtk python scripts/strategy_package_asset_backfill.py `
  --env-file F:\Dev\AIstock\.env `
  --model-weight-backfilled-date 2026-07-01 `
  --limit 100 `
  --model-code-repair-preview `
  --output tmp/issue_workflow/BUG-573/model_code_repair_preview_20260701_13.json
```

- `total_scanned=13`；counts=`{'planned_freeze': 9, 'skipped_already_frozen': 3, 'unrecoverable': 1}`；`model_code_repair_preview.impacted_package_count=9`。
- 退出码为 `2`，原因是 dry-run 发现 1 个既有 ledger 异常 package；这是 loud dry-run 报告，不是 apply 失败，生产 DB 未写入。
- 13 个 2026-07-01 model_weight backfill 包中：9 个计划补 `model.py`；3 个已可跳过（scanner 未发现本地 `model.*` pickle 引用）；1 个因既有 `protected_asset_ledger_evidence` 非法 asset_type 无法在本入口恢复。

| package_id | dry-run status | old sha | new sha | model_code to add | source |
|---|---|---|---|---:|---|
| `pkg_006a42323f7c4e81a468fdaad2cb16a3` | `skipped_already_frozen` | `ee8d8fa694c5225d4537860cce5d5faa6a994711bdb9ddf4277bf02531a7cbc2` | `ee8d8fa694c5225d4537860cce5d5faa6a994711bdb9ddf4277bf02531a7cbc2` | 0 | `` |
| `pkg_09750b4944ca434db03efd399ccf2144` | `planned_freeze` | `74346d7cb68a9d2b70af309e432b42aa9d7bbb49d8d265f0bad7afddfddd9969` | `147591cab048d386c80cebb38d69d20e3bfc1f3baa01862c46448d8079046859` | 1 | `qe-workspace://node/wsl2-5080/tasks/qe_20260607_093306_1f70/loops/Loop2/model.py` |
| `pkg_1de32357724a4c5b874f2abd90f22da5` | `planned_freeze` | `0e35983c9fc03ec62ca682f9fcde8f52e194819daad1dc9c73e65f719d31e304` | `1af6f284ae49d3b2df632e417d628b884e49e573b0cc3a8fb5d84814c32cbd89` | 1 | `qe-workspace://node/rdagent-node1/tasks/qe_20260502_231229_0565/loops/Loop1/model.py` |
| `pkg_2563063e544f4d1fa601e740d019f8c7` | `planned_freeze` | `04afd65b11020723dfb5956d01d3ffab98e3094f6666e0c73f4bbfed31fa6149` | `157712743027972874baf54e570d280377e4f3e2dbebcf897ab35117d49c0682` | 1 | `qe-workspace://node/wsl2-5080/tasks/qe_20260513_151128_12ea/loops/Loop1/model.py` |
| `pkg_2a9fccb83da840c9a27a2d7a4118af9a` | `planned_freeze` | `27f7e6b41f92bf2d11b58060cea9b8a553462b1d8f57201fb81eafee22296bed` | `7fe96762e0652e6205a61e0515d28f52233f642f3d3e9dc9c9881b739585ccc3` | 1 | `qe-workspace://node/wsl2-5080/tasks/qe_20260513_151128_12ea/loops/Loop1/model.py` |
| `pkg_378eb9c91e104c64935404e257e932ee` | `planned_freeze` | `2aae3560563bd669e5f1951c40ae939744f82a67be5b7479f239b9f910270300` | `80d2e782e43aae5cb2687df2197ef4d443e0b434376350310a7a82ceaf736318` | 1 | `qe-workspace://node/wsl2-5080/tasks/qe_20260520_215627_abbc/loops/Loop16/model.py` |
| `pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27` | `unrecoverable` | `06af542422702473d8ecea82a9bcc3c92aa51d3d6dd4131b8e94a8c4989fe545` | `None` | 0 | `strategy_package_asset_backfill_unexpected_error` |
| `pkg_99142cb1440c40a7824e83902f4e7da9` | `skipped_already_frozen` | `cc20f64d31d4e2593de8bb1fd35a425b6cf791596923e05b95074452462bed71` | `cc20f64d31d4e2593de8bb1fd35a425b6cf791596923e05b95074452462bed71` | 0 | `` |
| `pkg_a2f53f3f2f3e4095a910b939464c35e6` | `planned_freeze` | `77402e38e2cb215b213c7bd9e243bd2a74cdc855acb180cdbc5196b6916ef207` | `4d5716db1a7aec8297dae9f0b32edd9a16c8b17eacef1f4d7fda57d36785569b` | 1 | `qe-workspace://node/wsl2-5080/tasks/qe_20260601_172505_fe17/loops/Loop2/model.py` |
| `pkg_b2faccade8d549af9621c51d285bdc06` | `planned_freeze` | `3816bb5ce3dbb2b4d83569572270fcd1a58e8039b80c826f8d22de0b4e48e002` | `02cc846e693996018e9562767a07db42279e6e039f5bbbcd52345d0eeb51aa17` | 1 | `qe-workspace://node/wsl2-5080/tasks/qe_20260512_113610_b19c/loops/Loop2/model.py` |
| `pkg_b668f8a633c44b72a5d557a2cb8970e3` | `skipped_already_frozen` | `a4a21c0175b9e926e130202bca8950ef14f9a75f9cfcec3be9ec41f9c36e27fa` | `a4a21c0175b9e926e130202bca8950ef14f9a75f9cfcec3be9ec41f9c36e27fa` | 0 | `` |
| `pkg_c4703dfc2fdf4e548cf8dd3027ef228b` | `planned_freeze` | `39d921334a1544c698a88f3133bc38ef163756f5d0ded9f80fa14e6ee797f530` | `d9e4c8c3ff696f3506dbfe6f1d205547812f92c4a0395c09e5a6697c22d8bd6e` | 1 | `qe-workspace://node/wsl2-5080/tasks/qe_20260614_022643_edaf/loops/Loop13/model.py` |
| `pkg_cfa3c5b4068d4db1ad06db352bfece93` | `planned_freeze` | `2e570c05c6b2136d84a8bef37bb1f5b2ed8fdc86bf863ab4fc10d8b0c5161494` | `918a2d7841a230e7165f59ec3fea46b77a8510ddb350f4bd70d509606e12cd24` | 1 | `qe-workspace://node/wsl2-5080/tasks/qe_20260512_113610_b19c/loops/Loop1/model.py` |

## 5. L2/L16 重点证据

- `pkg_a2f53f3f2f3e4095a910b939464c35e6`: old `77402e38e2cb215b213c7bd9e243bd2a74cdc855acb180cdbc5196b6916ef207` -> dry-run new `4d5716db1a7aec8297dae9f0b32edd9a16c8b17eacef1f4d7fda57d36785569b`；补 `model.py`，source `qe-workspace://node/wsl2-5080/tasks/qe_20260601_172505_fe17/loops/Loop2/model.py`，sha `fab0d14c023dd1101925983a1b6ab413b6feb59ca356833036b17916ac4c2b99`。
- `pkg_378eb9c91e104c64935404e257e932ee`: old `2aae3560563bd669e5f1951c40ae939744f82a67be5b7479f239b9f910270300` -> dry-run new `80d2e782e43aae5cb2687df2197ef4d443e0b434376350310a7a82ceaf736318`；补 `model.py`，source `qe-workspace://node/wsl2-5080/tasks/qe_20260520_215627_abbc/loops/Loop16/model.py`，sha `fab0d14c023dd1101925983a1b6ab413b6feb59ca356833036b17916ac4c2b99`。

## 6. 下一步授权边界

- 本 PR 只交 Tier2 审核，不合并。
- 即使本 PR 合并，也还需要战略 session 二次授权才能对生产 13 包执行 re-freeze / re-backfill apply；本轮未执行。
- 后续 apply 前应先处理或明确排除 `pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27` 的既有 ledger 异常，否则 gate 会继续 loud 返回 unrecoverable。
