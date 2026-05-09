# Paper v2 数据采集完整性审计 (2026-05-10)

> 状态：审计文档，仅诊断，不改代码 / schema / 业务逻辑。
> 范围：`paper_v2.*` 全量表 × 字段，源自 `backend/db/init_trading_core_v2_schema.py` + D1 migration `add_paper_v2_portfolio_broker_backend_20260509.sql`，写入路径取自 `backend/services/paper_trading_v2/repository.py`。

## §1 范围与方法

- **审计目标**：`paper_v2` schema 下全部表的全部字段；以 schema DDL 为列基准，以 repository.py 中 raw SQL `INSERT INTO paper_v2.*` / `UPDATE paper_v2.*` 为写入证据。
- **数据源**：
  - DDL：`backend/db/init_trading_core_v2_schema.py`（行号引用 = `init_trading_core_v2_schema.py:LINE`，下同）
  - D1 delta：`backend/db/add_paper_v2_portfolio_broker_backend_20260509.sql`
  - 写入路径：仅 `backend/services/paper_trading_v2/repository.py`（已用 Grep 验证：在 `backend/` 下其他目录均无 `INSERT INTO paper_v2` / `UPDATE paper_v2` 命中；`broker/`、`daemon/`、`session.py` 等不直接写 PG）
- **不审计**：QE / qe_archive / strategy_package（除 live_inference.py，由独立审计 `live_inference_capture_audit_20260510.md` 覆盖）。
- **方法**：
  1. 读 schema DDL，枚举每张表与每个列。
  2. 应用 D1 migration delta 到 `portfolio`：新增 `broker_backend`，新增 `portfolio_broker_market_source_check`，扩展 `portfolio_data_source_check` 允许 `MINIQMT_REALTIME`。
  3. 对每个 INSERT/UPDATE 列出现状况（表中所有列 vs 写入语句中提供的列），分类为 always / sometimes / never。
  4. 给出 DW 价值评级。

## §2 表清单

发现 **21 张** `paper_v2.*` 表（全部来自 `init_trading_core_v2_schema.py`，无外部 .sql 文件）。

| # | 表 | DDL 行号 | 用途 |
|---|----|----------|------|
| 1 | `portfolio` | 265 | Paper 组合主表（含 D1 broker_backend 绑定） |
| 2 | `execution_policy_activation` | 289 | 每日执行策略激活记录（policy 快照 + 状态机） |
| 3 | `runtime_profile` | 306 | 运行时配置 profile 主表 |
| 4 | `runtime_profile_version` | 319 | profile 版本（含 config_json + sha256 + 校验状态） |
| 5 | `runtime_config_activation` | 336 | 每日 runtime config 激活（绑 profile_version） |
| 6 | `config_change_audit` | 350 | 配置变更审计流水（before/after JSON） |
| 7 | `run` | 369 | 每日 paper run（一组合 × 一交易日） |
| 8 | `trade_session` | 383 | 跨日 trading session（REPLAY/LIVE/CATCHUP） |
| 9 | `session_day` | 404 | session 内的交易日明细（含 bar 进度） |
| 10 | `order_execution_state` | 422 | 单笔 order 的算法状态（algo_state_json + plan） |
| 11 | `intraday_snapshots` | 443 | 盘中权益快照（cash/mv/nav/positions JSON） |
| 12 | `session_events` | 460 | session 级事件流水 |
| 13 | `orders` | 471 | 订单簿（intent → order） |
| 14 | `order_events` | 491 | order 生命周期事件（含 fill_json） |
| 15 | `fills` | 503 | 成交记录 |
| 16 | `cash_ledger` | 518 | 现金流水（fill / fee / cash_after） |
| 17 | `positions` | 534 | 当日持仓快照（每 run 快照式重写） |
| 18 | `daily_snapshots` | 550 | 收盘日级权益快照 |
| 19 | `run_events` | 565 | run 级事件流水 |
| 20 | `errors` | 575 | 错误日志（typed error_code + context） |
| 21 | `reset_audit` | 586 | reset 操作审计（删了哪些 run/cnt） |

注：`init_trading_core_v2_schema.py` 同时定义 `strategy_pkg.*` (8 表)、`selection.*` (5 表)、`market.dataset_date_refresh_audit` (1 表)，不在本审计范围。

## §3 总表（每行一字段）

> 列：表 / 字段 / 类型 / NOT NULL / 写入位置 / 写入频率 / DW 价值 / 备注。
> 写入位置 cite 格式 `repository.py:LINE`；NOT NULL 同时记 DEFAULT。
> 写入频率：A=always-written / S=sometimes / N=never / DDL-DEFAULT=未在 INSERT 列出，依赖 DB DEFAULT。

### 3.1 portfolio (含 D1 delta)

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| portfolio_id | TEXT PK | Y | `repository.py:104` (INSERT) | A | HIGH | 主键 |
| portfolio_name | TEXT | Y | `repository.py:105` | A | HIGH | |
| package_id | TEXT | Y | `repository.py:106` | A | HIGH | |
| manifest_sha256 | TEXT | Y | `repository.py:107` | A | HIGH | |
| frozen_manifest_json | JSONB | Y | `repository.py:108` | A | MEDIUM | 巨型 JSON，归档用 |
| initial_cash | NUMERIC(20,6) | Y CHECK>0 | `repository.py:109` | A | HIGH | |
| start_date | DATE | Y | `repository.py:110` | A | HIGH | |
| data_source | TEXT CHECK | Y | `repository.py:111` | A | HIGH | D1 后允许 `MINIQMT_REALTIME` |
| **broker_backend** | VARCHAR(32) DEF `local_sim` CHECK | Y | `repository.py:112` | **A** | **HIGH** | D1 新增（详见 §7） |
| fee_policy | JSONB DEF `{}` | Y | `repository.py:113` | A | MEDIUM | |
| risk_policy | JSONB DEF `{}` | Y | `repository.py:114` | A | MEDIUM | |
| execution_policy | JSONB DEF `{}` | Y | `repository.py:115` | A | MEDIUM | |
| status | TEXT | Y | `repository.py:116` (INSERT), `repository.py:486` (UPDATE) | A | HIGH | UPDATE 路径仅改 status + updated_at |
| created_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:117` | A | HIGH | |
| updated_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:118` (INSERT) / `NOW()` in `repository.py:486` | A | HIGH | |

### 3.2 execution_policy_activation

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| activation_id | TEXT PK | Y | `repository.py:974` | A | HIGH | |
| portfolio_id | TEXT FK | Y | `repository.py:975` | A | HIGH | |
| trade_date | DATE | Y | `repository.py:976` | A | HIGH | |
| policy_id | TEXT | Y | `repository.py:977` | A | HIGH | |
| policy_sha256 | TEXT | Y | `repository.py:978` | A | HIGH | |
| policy_name | TEXT | N | `repository.py:979` | A (可空) | MEDIUM | |
| policy_json | JSONB | Y | `repository.py:980` | A | MEDIUM | |
| status | TEXT | Y | `repository.py:981` (INSERT), `repository.py:1028` (UPDATE → SUPERSEDED) | A | HIGH | |
| activated_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:982` | A | HIGH | |
| activated_by | TEXT | N | `repository.py:983` | A (可空) | MEDIUM | |
| reason | TEXT | N | `repository.py:984` | A (可空) | MEDIUM | |
| context | JSONB DEF `{}` | Y | `repository.py:985` | A | MEDIUM | |
| superseded_at | TIMESTAMPTZ | N | `repository.py:986` (INSERT NULL), `NOW()` in UPDATE | S | HIGH | INSERT 时通常 NULL；supersede 时 UPDATE 填充 |

### 3.3 runtime_profile

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| profile_id | TEXT PK | Y | `repository.py:1066` | A | HIGH | |
| portfolio_id | TEXT FK | Y | `repository.py:1067` | A | HIGH | |
| package_id | TEXT | Y | `repository.py:1068` | A | HIGH | |
| profile_name | TEXT | Y | `repository.py:1069` | A | HIGH | |
| status | TEXT CHECK | Y | `repository.py:1070` | A | HIGH | |
| current_version_id | TEXT | N | `repository.py:1071` (INSERT), `repository.py:1090` (UPDATE) | S | HIGH | INSERT 可能为 NULL，绑定后 UPDATE |
| created_by | TEXT | N | `repository.py:1072` | A (可空) | MEDIUM | |
| created_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:1073` | A | HIGH | |
| updated_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:1074` (INSERT), `NOW()` in `repository.py:1091` UPDATE | A | HIGH | |

### 3.4 runtime_profile_version

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| profile_version_id | TEXT PK | Y | `repository.py:1135` | A | HIGH | |
| profile_id | TEXT FK | Y | `repository.py:1136` | A | HIGH | |
| version_no | INT CHECK>=1 | Y | `repository.py:1137` | A | HIGH | |
| config_json | JSONB | Y | `repository.py:1138` | A | MEDIUM | |
| config_sha256 | TEXT | Y | `repository.py:1139` | A | HIGH | |
| validation_status | TEXT CHECK | Y | `repository.py:1140` | A | HIGH | |
| validation_errors | JSONB DEF `[]` | Y | `repository.py:1141` | A | MEDIUM | |
| created_by | TEXT | N | `repository.py:1142` | A (可空) | MEDIUM | |
| reason | TEXT | N | `repository.py:1143` | A (可空) | MEDIUM | |
| created_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:1144` | A | HIGH | |
| supersedes_version_id | TEXT | N | `repository.py:1145` | A (可空) | MEDIUM | 多数为 NULL |

### 3.5 runtime_config_activation

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| activation_id | TEXT PK | Y | `repository.py:1198` | A | HIGH | |
| portfolio_id | TEXT FK | Y | `repository.py:1199` | A | HIGH | |
| trade_date | DATE | Y | `repository.py:1200` | A | HIGH | |
| profile_version_id | TEXT FK | Y | `repository.py:1201` | A | HIGH | |
| status | TEXT CHECK | Y | `repository.py:1202` (INSERT), `repository.py:1246` (UPDATE) | A | HIGH | |
| activated_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:1203` | A | HIGH | |
| activated_by | TEXT | N | `repository.py:1204` | A (可空) | MEDIUM | |
| reason | TEXT | N | `repository.py:1205` | A (可空) | MEDIUM | |
| context | JSONB DEF `{}` | Y | `repository.py:1206` | A | MEDIUM | |
| superseded_at | TIMESTAMPTZ | N | `repository.py:1207` / UPDATE NOW() | S | HIGH | |

### 3.6 config_change_audit

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| audit_id | BIGSERIAL PK | Y | DDL DEFAULT | A | HIGH | |
| portfolio_id | TEXT | N | `repository.py:1285` | A (可空) | HIGH | |
| package_id | TEXT | N | `repository.py:1286` | A (可空) | MEDIUM | |
| object_type | TEXT | Y | `repository.py:1287` | A | HIGH | |
| object_id | TEXT | Y | `repository.py:1288` | A | HIGH | |
| change_type | TEXT | Y | `repository.py:1289` | A | HIGH | |
| before_json | JSONB | N | `repository.py:1290` | S | MEDIUM | NULL on first create |
| after_json | JSONB | N | `repository.py:1291` | S | MEDIUM | NULL on delete |
| before_sha256 | TEXT | N | `repository.py:1292` | S | MEDIUM | |
| after_sha256 | TEXT | N | `repository.py:1293` | S | MEDIUM | |
| reason | TEXT | N | `repository.py:1294` | A (可空) | MEDIUM | |
| created_by | TEXT | N | `repository.py:1295` | A (可空) | MEDIUM | |
| request_id | TEXT | N | `repository.py:1296` | A (可空) | MEDIUM | |
| code_version | TEXT | N | `repository.py:1297` | A (可空) | MEDIUM | |
| created_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:1298` | A | HIGH | |

### 3.7 run

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| run_id | TEXT PK | Y | `repository.py:504` | A | HIGH | |
| portfolio_id | TEXT FK | Y | `repository.py:505` | A | HIGH | |
| trade_date | DATE | Y | `repository.py:506` | A | HIGH | |
| status | TEXT | Y | `repository.py:507` (INSERT), `repository.py:578` (UPDATE) | A | HIGH | |
| data_source | TEXT | Y | `repository.py:508` | A | HIGH | run-level data_source（与 portfolio 解耦：可在 LIVE/REPLAY 切换） |
| runtime_config | JSONB DEF `{}` | Y | `repository.py:509` (INSERT), `repository.py:595` (UPDATE) | A | MEDIUM | |
| started_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:510` | A | HIGH | |
| completed_at | TIMESTAMPTZ | N | `repository.py:511` (INSERT) / `repository.py:578` (UPDATE) | S | HIGH | INSERT 通常 NULL，run 完成时 UPDATE |
| error_json | JSONB | N | `repository.py:512` (INSERT) / `repository.py:582` (UPDATE) | S | HIGH | 仅失败时 |

### 3.8 trade_session

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| session_id | TEXT PK | Y | `repository.py:616` | A | HIGH | |
| portfolio_id | TEXT FK | Y | `repository.py:617` | A | HIGH | |
| mode | TEXT CHECK | Y | `repository.py:618` | A | HIGH | REPLAY/LIVE/CATCHUP |
| status | TEXT | Y | `repository.py:619` (INSERT), `repository.py:719` (UPDATE) | A | HIGH | |
| phase | TEXT | Y | `repository.py:620` (INSERT), `repository.py:719` (UPDATE) | A | HIGH | |
| start_date | DATE | Y | `repository.py:621` | A | HIGH | |
| end_date | DATE | N | `repository.py:622` | A (可空) | HIGH | LIVE 模式可能为 NULL |
| historical_data_source | TEXT CHECK | N | `repository.py:623` | S | HIGH | LIVE_ONLY 时 NULL |
| live_data_source | TEXT CHECK | N | `repository.py:624` | S | HIGH | REPLAY_ONLY 时 NULL |
| runtime_config_json | JSONB DEF `{}` | Y | `repository.py:625` | A | MEDIUM | |
| validated_execution_policy_json | JSONB DEF `{}` | Y | `repository.py:626` | A | MEDIUM | |
| created_by | TEXT | N | `repository.py:627` | A (可空) | MEDIUM | |
| created_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:628` | A | HIGH | |
| updated_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:629` (INSERT), `repository.py:719` (UPDATE) | A | HIGH | |
| started_at | TIMESTAMPTZ | N | `repository.py:630` (INSERT NULL), `repository.py:719` (UPDATE) | S | HIGH | INSERT 时 NULL，运行开始 UPDATE |
| completed_at | TIMESTAMPTZ | N | `repository.py:631` / UPDATE | S | HIGH | |
| last_error_json | JSONB | N | `repository.py:632` (INSERT) / `repository.py:720` (UPDATE) | S | HIGH | 失败时填充 |

### 3.9 session_day

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| session_day_id | TEXT PK | Y | `repository.py:759` | A | HIGH | |
| session_id | TEXT FK | Y | `repository.py:760` | A | HIGH | |
| portfolio_id | TEXT FK | Y | `repository.py:761` | A | HIGH | |
| trade_date | DATE | Y | `repository.py:762` | A | HIGH | |
| run_id | TEXT FK | N | `repository.py:763` (UPSERT) / `repository.py:1643` (RESET → NULL) | S | HIGH | reset_portfolio_runs 会把 run_id 清空 |
| status | TEXT | Y | `repository.py:764` | A | HIGH | |
| phase | TEXT | Y | `repository.py:765` | A | HIGH | |
| data_source | TEXT CHECK | Y | `repository.py:766` | A | HIGH | TDX_REALTIME / DB_HISTORICAL 二选一（无 MINIQMT） |
| expected_bar_count | INT | N | `repository.py:767` | A (可空) | MEDIUM | |
| latest_available_bar_time | TIMESTAMPTZ | N | `repository.py:768` | A (可空) | HIGH | bar 进度 |
| last_processed_bar_time | TIMESTAMPTZ | N | `repository.py:769` | A (可空) | HIGH | bar 进度 |
| created_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:770` | A | HIGH | |
| updated_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:771` | A | HIGH | |

### 3.10 order_execution_state

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| execution_state_id | TEXT PK | Y | `repository.py:847` | A | HIGH | |
| session_id | TEXT FK | Y | `repository.py:848` | A | HIGH | |
| run_id | TEXT FK | Y | `repository.py:849` | A | HIGH | |
| order_id | TEXT UNIQUE | Y | `repository.py:850` | A | HIGH | |
| symbol | TEXT | Y | `repository.py:851` | A | HIGH | |
| trade_date | DATE | Y | `repository.py:852` | A | HIGH | |
| algo_code | TEXT | Y | `repository.py:853` | A | HIGH | |
| algo_state_json | JSONB DEF `{}` | Y | `repository.py:854` | A | MEDIUM | 算法内部状态机 |
| plan_json | JSONB | N | `repository.py:855` | S | MEDIUM | 算法可能不产 plan |
| plan_sha256 | TEXT | N | `repository.py:856` | S | MEDIUM | 与 plan_json 同步 |
| last_processed_bar_time | TIMESTAMPTZ | N | `repository.py:857` | A (可空) | HIGH | |
| filled_quantity | INT CHECK>=0 | Y | `repository.py:858` | A | HIGH | |
| remaining_quantity | INT CHECK>=0 | Y | `repository.py:859` | A | HIGH | |
| status | TEXT | Y | `repository.py:860` | A | HIGH | |
| created_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:861` | A | HIGH | |
| updated_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:862` | A | HIGH | |

### 3.11 intraday_snapshots

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| snapshot_id | TEXT PK | Y | `repository.py:901` | A | HIGH | |
| session_id | TEXT FK | Y | `repository.py:902` | A | HIGH | |
| run_id | TEXT FK | Y | `repository.py:903` | A | HIGH | |
| portfolio_id | TEXT FK | Y | `repository.py:904` | A | HIGH | |
| trade_date | DATE | Y | `repository.py:905` | A | HIGH | |
| snapshot_time | TIMESTAMPTZ | Y | `repository.py:906` | A | HIGH | |
| cash | DOUBLE | Y | `repository.py:907` | A | HIGH | |
| market_value | DOUBLE | Y | `repository.py:908` | A | HIGH | |
| nav | DOUBLE | Y | `repository.py:909` | A | HIGH | |
| positions_json | JSONB DEF `[]` | Y | `repository.py:910` | A | MEDIUM | 持仓快照 JSON |
| source | TEXT | Y | `repository.py:911` | A | HIGH | |
| created_at | TIMESTAMPTZ DEF NOW() | Y | `repository.py:912` | A | HIGH | |

### 3.12 session_events

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| event_id | BIGSERIAL PK | Y | DDL DEFAULT | A | HIGH | |
| session_id | TEXT FK | Y | `repository.py:805` | A | HIGH | |
| run_id | TEXT | N | `repository.py:806` | A (可空) | HIGH | session 级事件可能没有 run |
| event_type | TEXT | Y | `repository.py:807` | A | HIGH | |
| message | TEXT | Y | `repository.py:808` | A | HIGH | |
| context | JSONB DEF `{}` | Y | `repository.py:809` | A | MEDIUM | |
| created_at | TIMESTAMPTZ DEF NOW() | Y | DDL DEFAULT | A | HIGH | |

### 3.13 orders

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| order_id | TEXT PK | Y | `repository.py:1371` | A | HIGH | UPSERT |
| run_id | TEXT FK | Y | `repository.py:1372` | A | HIGH | |
| portfolio_id | TEXT | Y | `repository.py:1373` | A | HIGH | |
| package_id | TEXT | Y | `repository.py:1374` | A | HIGH | |
| intent_id | TEXT | Y | `repository.py:1375` | A | HIGH | parent intent |
| symbol | TEXT | Y | `repository.py:1376` | A | HIGH | |
| side | TEXT | Y | `repository.py:1377` | A | HIGH | |
| quantity | INT | Y | `repository.py:1378` | A | HIGH | |
| order_type | TEXT | Y | `repository.py:1379` | A | HIGH | |
| limit_price | DOUBLE | N | `repository.py:1380` | S | HIGH | MARKET 时 NULL |
| status | TEXT | Y | `repository.py:1381` | A | HIGH | UPSERT 也更新 |
| filled_quantity | INT | Y | `repository.py:1382` | A | HIGH | UPSERT 也更新 |
| avg_fill_price | DOUBLE | N | `repository.py:1383` | S | HIGH | 未成交时 NULL |
| metadata | JSONB DEF `{}` | Y | `repository.py:1384` | A | MEDIUM | |
| created_at | TIMESTAMPTZ | Y | `repository.py:1385` | A | HIGH | NOT NULL，无 DEFAULT，必须由 caller 提供 |
| updated_at | TIMESTAMPTZ | Y | `repository.py:1386` | A | HIGH | 同上 |

### 3.14 order_events

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| event_id | TEXT PK | Y | `repository.py:1484` | A | HIGH | |
| run_id | TEXT FK | Y | `repository.py:1485` | A | HIGH | |
| order_id | TEXT | Y | `repository.py:1486` | A | HIGH | |
| event_type | TEXT | Y | `repository.py:1487` | A | HIGH | |
| event_time | TIMESTAMPTZ | Y | `repository.py:1488` | A | HIGH | |
| reason | TEXT | N | `repository.py:1489` | A (可空) | MEDIUM | |
| metadata | JSONB DEF `{}` | Y | `repository.py:1490` | A | MEDIUM | |
| fill_json | JSONB | N | `repository.py:1491` | S | MEDIUM | 仅 FILL/PARTIAL 事件填充 |

### 3.15 fills

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| fill_id | TEXT PK | Y | `repository.py:1423` | A | HIGH | |
| run_id | TEXT FK | Y | `repository.py:1424` | A | HIGH | |
| order_id | TEXT | Y | `repository.py:1425` | A | HIGH | |
| symbol | TEXT | Y | `repository.py:1426` | A | HIGH | |
| side | TEXT | Y | `repository.py:1427` | A | HIGH | |
| quantity | INT | Y | `repository.py:1428` | A | HIGH | |
| price | DOUBLE | Y | `repository.py:1429` | A | HIGH | 实际成交价 |
| trade_time | TIMESTAMPTZ | Y | `repository.py:1430` | A | HIGH | |
| bar_time | TIMESTAMPTZ | N | `repository.py:1431` | A (可空) | HIGH | 部分算法可能无 bar_time |
| reason | TEXT | Y | `repository.py:1432` | A | HIGH | 成交原因 |
| metadata | JSONB DEF `{}` | Y | `repository.py:1433` | A | MEDIUM | 但 §A2 §3 提示：意向价、滑点、子算法等关键 DW 维度未持久化在此 |

### 3.16 cash_ledger

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| cash_id | BIGSERIAL PK | Y | DDL DEFAULT | A | HIGH | |
| run_id | TEXT FK | Y | `repository.py:1506` | A | HIGH | |
| portfolio_id | TEXT | Y | `repository.py:1507` | A | HIGH | |
| fill_id | TEXT | N | `repository.py:1508` | A (可空) | HIGH | 非 fill 触发的现金调整可能 NULL |
| trade_date | DATE | Y | `repository.py:1509` | A | HIGH | |
| symbol | TEXT | N | `repository.py:1510` | A (可空) | HIGH | |
| side | TEXT | N | `repository.py:1511` | A (可空) | HIGH | |
| notional | NUMERIC(20,6) | Y | `repository.py:1512` | A | HIGH | |
| fee | NUMERIC(20,6) | Y | `repository.py:1513` | A | HIGH | |
| cash_delta | NUMERIC(20,6) | Y | `repository.py:1514` | A | HIGH | |
| cash_after | NUMERIC(20,6) | Y | `repository.py:1515` | A | HIGH | |
| created_at | TIMESTAMPTZ DEF NOW() | Y | DDL DEFAULT | A | HIGH | |

### 3.17 positions

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| position_id | BIGSERIAL PK | Y | DDL DEFAULT | A | HIGH | |
| run_id | TEXT FK | Y | `repository.py:1533` | A | HIGH | save_positions 先 DELETE 后批量 INSERT |
| portfolio_id | TEXT | Y | `repository.py:1534` | A | HIGH | |
| trade_date | DATE | Y | `repository.py:1535` | A | HIGH | |
| symbol | TEXT | Y | `repository.py:1536` | A | HIGH | |
| quantity | INT | Y | `repository.py:1537` | A | HIGH | |
| available_quantity | INT | Y | `repository.py:1538` | A | HIGH | |
| avg_cost | DOUBLE | Y | `repository.py:1539` | A | HIGH | |
| market_price | DOUBLE | Y | `repository.py:1540` | A | HIGH | |
| market_value | DOUBLE | Y | `repository.py:1541` | A | HIGH | |
| metadata | JSONB DEF `{}` | Y | `repository.py:1542` | A | LOW | 仅含 `position_trade_date` |

### 3.18 daily_snapshots

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| snapshot_id | BIGSERIAL PK | Y | DDL DEFAULT | A | HIGH | |
| run_id | TEXT FK | Y | `repository.py:1566` | A | HIGH | UPSERT (portfolio_id, trade_date) |
| portfolio_id | TEXT | Y | `repository.py:1567` | A | HIGH | |
| trade_date | DATE | Y | `repository.py:1568` | A | HIGH | |
| cash | DOUBLE | Y | `repository.py:1569` | A | HIGH | |
| market_value | DOUBLE | Y | `repository.py:1570` | A | HIGH | |
| nav | DOUBLE | Y | `repository.py:1571` | A | HIGH | |
| position_count | INT | Y | `repository.py:1572` | A | HIGH | metadata.get fallback 0 |
| snapshot_time | TIMESTAMPTZ | Y | `repository.py:1573` | A | HIGH | |
| metadata | JSONB DEF `{}` | Y | `repository.py:1574` | A | MEDIUM | |

### 3.19 run_events

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| event_seq | BIGSERIAL PK | Y | DDL DEFAULT | A | HIGH | |
| run_id | TEXT FK | Y | `repository.py:1583` | A | HIGH | |
| event_type | TEXT | Y | `repository.py:1583` | A | HIGH | |
| message | TEXT | Y | `repository.py:1583` | A | HIGH | |
| context | JSONB DEF `{}` | Y | `repository.py:1583` | A | MEDIUM | |
| created_at | TIMESTAMPTZ DEF NOW() | Y | DDL DEFAULT | A | HIGH | |

### 3.20 errors

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| error_id | BIGSERIAL PK | Y | DDL DEFAULT | A | HIGH | |
| run_id | TEXT | N | `repository.py:1595` | A (可空) | HIGH | portfolio 级错误可能无 run |
| portfolio_id | TEXT | N | `repository.py:1596` | A (可空) | HIGH | |
| error_code | TEXT | Y | `repository.py:1597` | A | HIGH | dict.get 默认 `PAPER_V2_ERROR` |
| message | TEXT | Y | `repository.py:1598` | A | HIGH | dict.get 默认 `paper v2 error` |
| context | JSONB DEF `{}` | Y | `repository.py:1599` | A | MEDIUM | |
| created_at | TIMESTAMPTZ DEF NOW() | Y | DDL DEFAULT | A | HIGH | |

### 3.21 reset_audit

| 字段 | 类型 | NN | 写入位置 | 频率 | DW | 备注 |
|------|------|----|----------|------|----|------|
| audit_id | BIGSERIAL PK | Y | DDL DEFAULT | A | HIGH | |
| portfolio_id | TEXT FK | Y | `repository.py:1683` | A | HIGH | |
| rerun_policy | TEXT | Y | `repository.py:1684` | A | HIGH | |
| start_date | DATE | Y | `repository.py:1685` | A | HIGH | |
| end_date | DATE | Y | `repository.py:1686` | A | HIGH | |
| confirm_text | TEXT | Y | `repository.py:1687` | A | HIGH | |
| deleted_counts | JSONB DEF `{}` | Y | `repository.py:1688` | A | HIGH | |
| status | TEXT | Y | `repository.py:1689`（截断 Read 但 INSERT 列含 status） | A | HIGH | |
| context | JSONB DEF `{}` | Y | INSERT 列含 context | A | MEDIUM | |
| created_at | TIMESTAMPTZ DEF NOW() | Y | DDL DEFAULT | A | HIGH | |

## §4 子表 1：always-written（DW 可直接 ETL）

下列字段在 INSERT 路径都被显式提供（包括 always-written-but-nullable，区分见备注列）。这些字段对 DW 是稳定的列，可直接抽取。

- **portfolio**: 全部 15 列 always-written（含 D1 `broker_backend`，详 §7）。
- **execution_policy_activation**: 除 `superseded_at`（仅 supersede UPDATE）以外全 always-written。
- **runtime_profile**: 除 `current_version_id`（先 INSERT 可空，再 UPDATE 补绑定）外全 always-written。
- **runtime_profile_version**: 全部 always-written。
- **runtime_config_activation**: 除 `superseded_at` 外全 always-written。
- **config_change_audit**: PK + audit 框架字段（object_type/object_id/change_type/created_at）always-written；before/after JSON sometimes（见 §5）。
- **run**: 除 `completed_at`/`error_json`（生命周期 UPDATE）外全 always-written。
- **trade_session**: 除 `historical_data_source`/`live_data_source`/`started_at`/`completed_at`/`last_error_json` 之外全 always-written。
- **session_day**: 除 `run_id`（reset 路径会清 NULL）外全 always-written。
- **order_execution_state**: 除 `plan_json`/`plan_sha256`（部分算法不产 plan）外全 always-written。
- **intraday_snapshots**: 全部 always-written。
- **session_events**: 全部 always-written。
- **orders**: 除 `limit_price`/`avg_fill_price` 外全 always-written。
- **order_events**: 除 `fill_json` 外全 always-written。
- **fills**: 除 `bar_time` 外全 always-written。
- **cash_ledger**: 除 `fill_id`/`symbol`/`side`（非 fill 触发记录）外全 always-written。
- **positions**: 全部 always-written。
- **daily_snapshots**: 全部 always-written。
- **run_events**: 全部 always-written。
- **errors**: 除 `run_id`/`portfolio_id` 外全 always-written。
- **reset_audit**: 全部 always-written。

## §5 子表 2：sometimes / conditionally written

| 表 | 字段 | 何时为 NULL/缺省 | 触发条件 cite |
|----|------|------------------|---------------|
| portfolio | （无 sometimes 字段） | — | — |
| execution_policy_activation | `superseded_at` | INSERT 时永远 NULL；仅 supersede UPDATE 路径填 NOW() | `repository.py:986` (INSERT NULL) / `repository.py:1028` (UPDATE) |
| runtime_profile | `current_version_id` | profile 创建 → 先 NULL；version 创建 → UPDATE 绑定 | `repository.py:1071` / `repository.py:1090` |
| runtime_profile_version | `supersedes_version_id` | 首版本 NULL；后续版本可指向前版 | `repository.py:1145` |
| runtime_config_activation | `superseded_at` | 同 execution_policy_activation | `repository.py:1207` / `repository.py:1246` |
| config_change_audit | `before_json`/`before_sha256` | object 首次创建（CREATE 类型） | `repository.py:1290`/`1292` |
| config_change_audit | `after_json`/`after_sha256` | object 删除（DELETE 类型） | `repository.py:1291`/`1293` |
| run | `completed_at` | run 创建瞬间 NULL；`update_run_status` 在 SUCCEEDED/FAILED 时填 | `repository.py:573` / `repository.py:578` |
| run | `error_json` | 仅 FAILED 时填 | `repository.py:512` / `repository.py:582` |
| trade_session | `historical_data_source` | LIVE_ONLY 模式 NULL | `repository.py:623` |
| trade_session | `live_data_source` | REPLAY_ONLY 模式 NULL | `repository.py:624` |
| trade_session | `started_at` | session 创建到运行启动期间 NULL | `repository.py:630` / UPDATE `repository.py:719` |
| trade_session | `completed_at` | 运行未结束 NULL | 同上 |
| trade_session | `last_error_json` | 无错时 NULL | `repository.py:632`/`720` |
| session_day | `run_id` | reset_portfolio_runs 会把对应 session_day 的 run_id 清 NULL（`repository.py:1643`） | INSERT 时通常非 NULL，reset 后 NULL |
| order_execution_state | `plan_json`/`plan_sha256` | 算法无 plan 时 NULL | `repository.py:855`/`856` |
| orders | `limit_price` | order_type=MARKET 时 NULL | `repository.py:1380` |
| orders | `avg_fill_price` | 未成交/已撤前 NULL | `repository.py:1383` |
| order_events | `fill_json` | 仅 FILL/PARTIAL 类事件填；其它事件 NULL | `repository.py:1491` |
| fills | `bar_time` | 部分算法（如对齐到 daily-only）无 bar_time | `repository.py:1431` |
| cash_ledger | `fill_id`/`symbol`/`side` | 当条目不是由具体 fill 引发（例如手续费独立调整）时可 NULL | `repository.py:1508`/`1510`/`1511`（schema 允许 NULL） |
| errors | `run_id` / `portfolio_id` | portfolio 级错误（如 reset 校验）可只填一个 | `repository.py:1595`/`1596` |

## §6 子表 3：defined but never written（疑似死字段）

**结论：当前 schema 内未发现 zero-INSERT 死字段。**

复核方法：每张表的 DDL 列都能在 `repository.py` 找到对应 INSERT/UPDATE 写入路径，或属于 BIGSERIAL/`DEFAULT NOW()` 由 PG 自动填充的"系统列"（已在 §3 标注 `DDL DEFAULT`）。

需要关注的"形式上有写入但语义降级"字段（不算死字段，但 DW 价值 LOW）：

| 表 | 字段 | 现象 |
|----|------|------|
| positions | `metadata` | 永远只写 `{"position_trade_date": ...}`，无其它键。可视为冗余（trade_date 列已经存在） |
| daily_snapshots | `metadata.position_count` | INSERT 用 `int(metadata.get("position_count") or 0)`：如果 caller 不传，永远是 0 → 该列在缺失元数据时无信息 |
| errors | `error_code` / `message` | dict.get 默认值 `PAPER_V2_ERROR` / `paper v2 error`；caller 漏字段时退化成无信息常量（不违反 NOT NULL，但 DW 难分类） |

## §7 D1 broker_backend 专项核查

### 7.1 现有 portfolio INSERT/UPDATE 路径是否都填了 broker_backend？

**INSERT 路径**（仅一条）：`PaperV2Repository.create_portfolio` 在 `repository.py:91-121`，列清单 `repository.py:96-101` 显式包含 `broker_backend`，参数 `repository.py:112` 传 `portfolio.broker_backend`。

- `models.py:51` 给 `PaperPortfolio.broker_backend` 默认 `"local_sim"`，且 `models.py:68-77` 的 `_broker_backend_matches_data_source` 在模型构造期 fail-fast 校验。
- `service.py:117-162` 的 `create_portfolio` 在落库前显式调用 `assert_broker_market_source_match(broker_backend, data_source)` 与 `_validate_broker_compatibility(manifest, broker_backend)`，并将 `broker_backend` 透传至 `PaperPortfolio` 构造。
- 结论：唯一 INSERT 路径覆盖 `broker_backend`。✅

**UPDATE 路径**：仅 `update_portfolio_status`（`repository.py:482-491`），仅改 `status` + `updated_at`，**不写 `broker_backend`**。这是正确的：D1 把 `broker_backend` 设计为不可变（immutable column），更新 status 不应触碰它。无 portfolio-level 字段更新接口（即不存在改 fee_policy/risk_policy/execution_policy/broker_backend 的写路径，所有这些通过 `runtime_profile_version` + `config_change_audit` 间接审计）。✅

### 7.2 data_source × broker_backend 联合 CHECK 是否在所有路径上满足？

D1 migration 增加联合 CHECK（`add_paper_v2_portfolio_broker_backend_20260509.sql:30-36`）：

```
(broker_backend = 'local_sim' AND data_source IN ('TDX_REALTIME', 'DB_HISTORICAL'))
OR (broker_backend = 'minqmt_sim' AND data_source = 'MINIQMT_REALTIME')
```

- 应用层在 `models.py:77` 通过 `assert_broker_market_source_match` 重叠校验：`market_data.py:65-90` 的 `ALLOWED_MARKET_SOURCES` 把 `local_sim → {TDX_REALTIME, DB_HISTORICAL}`、`minqmt_sim → {MINIQMT_REALTIME}` 锁死。
- `service.py:142` 在创建前再校验一次。
- 触发 CHECK violation 的可能路径：
  - 直接 SQL 越层 INSERT：本审计未在 backend 代码内发现 `INSERT INTO paper_v2.portfolio` 的越层调用（仅 `repository.py:96`）。
  - 手工 DDL/数据迁移脚本：如把存量 portfolio 的 `data_source` 改为 `MINIQMT_REALTIME` 而未同时更新 `broker_backend`，会触发联合 CHECK。**当前未发现此类脚本**。
  - 单元/集成测试若直绕过 model 校验构造对象 → 模型 validator 会先抛 `ValidationError`（fail-fast）。

结论：所有应用路径满足联合 CHECK。✅

### 7.3 D1 字段当前采集状态评级

**covered**：

- INSERT 写入：`repository.py:112` 显式带 `broker_backend`。
- DEFAULT：DDL `init_trading_core_v2_schema.py:274` 与 migration 都有 `DEFAULT 'local_sim'`，存量行回填安全。
- 应用层校验：3 层（model validator / service.create_portfolio / market_data.assert_broker_market_source_match）。
- 唯一不可变性约束：D1 设计文档言明 immutable，且代码侧无任何 UPDATE broker_backend 路径。
- DW 抽取建议：可立刻把 `broker_backend` 暴露到 fact_paper_portfolio 维度，无回填风险。

## §8 审计结论

1. **整体健康**：21 张 paper_v2 表的 schema 列都有对应 INSERT 路径，无死字段。always-written 占绝大多数；sometimes-written 字段都是合规的状态机半结构（INSERT 时为 NULL，状态机推进时 UPDATE 填充）。
2. **D1 broker_backend 评级 covered**（§7.3），可作为 DW 维度立即暴露。
3. **DW 抽取最大风险点**：
   - `fills.metadata` / `order_events.metadata` 是 schemaless JSONB，关键 DW 维度（意向价 vs 成交价、滑点、子算法序列、市场状态标签等）若由 service/broker 写进 metadata，会 enum→DW 时丢失结构。详见 A2 §3。
   - `cash_ledger.fill_id` 设计为可空，但在 SELL 退手续费等"幽灵现金事件"上是否填充取决于 service 调用方；DW 做 fill ↔ cash 关联时需注意空值率。
   - `daily_snapshots.metadata.position_count` 当 caller 未传 metadata 时退化成 0，DW 不应直接信任该列；建议 ETL 用 `positions` 表 GROUP BY count 重新计算。
4. **采集侧路风险**：`backend/services/paper_trading_v2/daemon/event_log.py` 的 `daemon_event_log` 是 **worktree-local SQLite** 文件（不在 paper_v2 schema 内），承载 ORDER_REJECTED / INTENT_CREATED / ORDER_SUBMITTED / FILL_RECEIVED / POSITION_UPDATED / RUN_STARTED/COMPLETED/FAILED 共 9 类事件。常规 live/replay 跑 path 通过 `live_session.py` / `runner.py` / `day_runner.py` 用 `save_run_event` / `save_order_event` 把事件写进 PG（已 cite 25+ 处），但 demo/sim 的 daemon path 不会。这构成 DW 看不到的捕获缺口（详细补齐方案在 A2）。
5. **总结**：当前 schema 对 portfolio/run/order 主线的字段覆盖足够 DW 起步；缺口主要在"事件流向旁路 SQLite"和"成交细节 enum 化"两块，由 A2 详述。
