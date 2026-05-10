# Dev PG 容器搭建（T17）— 2026-05-10

> 状态：已完成。aistock-pg-dev 容器运行中，schema 已从 prod 拷贝。
> 不影响 prod aistock DB / 5432 端口。

## §1 目的

解锁阶段 2 测试：
- Codex StrategyPackage governance live smoke（drawer 8b88cd3c blocker）
- Claude Code DW handler integration tests
- 数据库迁移 / schema 演进的 dev/staging 验证

## §2 容器配置

| 项 | 值 |
|----|-----|
| 名称 | `aistock-pg-dev` |
| 镜像 digest pin | `timescale/timescaledb@sha256:37effc5989e727125f56f264b5f18bf3fad89c430bda282c4725ba5e04cb9201` |
| 镜像 created | 2025-10-29（与 prod 同一份本地缓存） |
| 版本 | PostgreSQL 16.10 + TimescaleDB 2.23.0 |
| 端口 | 5433（host）→ 5432（container） |
| DB | `aistock_dev` |
| User | `postgres` |
| Volume | `aistock-pg-dev-data`（独立卷） |
| 重启策略 | `unless-stopped` |

## §3 docker 命令历史（密码占位 `${DEV_PASSWORD}`）

```bash
# 1. 生成密码（24 字节随机 base64）
DEV_PASSWORD=$(openssl rand -base64 24 | tr -d '\n')

# 2. 启动容器（digest pin 保证与 prod 比特一致）
docker run -d \
  --name aistock-pg-dev \
  --restart unless-stopped \
  -p 5433:5432 \
  -e POSTGRES_PASSWORD="${DEV_PASSWORD}" \
  -e POSTGRES_DB=aistock_dev \
  -e POSTGRES_USER=postgres \
  -v aistock-pg-dev-data:/var/lib/postgresql/data \
  timescale/timescaledb@sha256:37effc5989e727125f56f264b5f18bf3fad89c430bda282c4725ba5e04cb9201

# 3. 等待就绪
docker exec aistock-pg-dev pg_isready -U postgres -d aistock_dev

# 4. 启用 TimescaleDB 扩展（image 已自带，CREATE EXTENSION IF NOT EXISTS 即可）
docker exec aistock-pg-dev psql -U postgres -d aistock_dev \
  -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
```

## §4 schema 来源 + apply

| 项 | 值 |
|----|-----|
| pg_dump 时间 | 2026-05-10 12:42 |
| 来源 | prod `timescaledb` 容器 → `aistock` DB → `--schema-only` |
| 选项 | `--schema-only --no-owner --no-privileges --no-tablespaces` |
| 输出文件 | `F:/Dev/AIstock/backend/db/dev/prod_schema_snapshot_20260510.sql` |
| 大小 | **58 MB** |
| 行数 | 1,242,006 |
| INSERT 数 | 0（无数据泄漏） |
| COPY 数 | 0（无数据泄漏） |
| CREATE TABLE 数 | 26,528（多数为 TimescaleDB 每 chunk 子表） |
| CREATE INDEX 数 | 60,535 |
| apply ERROR 数 | **0** |
| apply NOTICE 数 | 0 |
| apply 耗时 | ~10 分钟（per-chunk 索引创建） |

> **关于 58 MB 的说明**：dump 体积大于初步预估的 1–15 MB，原因是 prod
> TimescaleDB 拥有 18+ 个 hypertables，多年运行累积了大量 chunk 子表
> （每 chunk 是独立子表 + 多个索引）。`pg_dump --schema-only` 会为每
> chunk 生成独立 DDL，因此 26.5k 张表 + 60.5k 个索引的纯 DDL 体积合理。
> 已确认 INSERT/COPY 数均为 0，**无数据泄漏**。

> pg_dump 在 stderr 输出了 3 条关于 `hypertable` / `chunk` /
> `continuous_agg` 循环外键的 warning，这些是 TimescaleDB 内部 catalog
> 表的已知特性，不影响 schema-only 导入。apply 阶段 0 ERROR 0 NOTICE
> 也佐证了这一点。

## §5 .env DEV_* keys（仅 key 名，值不入文档）

写入位置：`F:/Dev/AIstock/.env`（**不在 worktree，已 gitignore**）

- `TDX_DB_DEV_HOST`（值：127.0.0.1）
- `TDX_DB_DEV_PORT`（值：5433）
- `TDX_DB_DEV_NAME`（值：aistock_dev）
- `TDX_DB_DEV_USER`（值：postgres）
- `TDX_DB_DEV_PASSWORD`（值仅在本地 `.env`，**不进 git，不进文档，不进任何聊天记录**）

## §6 schema × table 数对比（dev vs prod）

| schema | prod | dev | 一致 |
|--------|------|-----|------|
| `app` | 40 | 40 | ✅ |
| `archive` | 5 | 5 | ✅ |
| `infra` | 3 | 3 | ✅ |
| `market` | 71 | 71 | ✅ |
| `paper_trading` | 8 | 8 | ✅ |
| `paper_v2` | 21 | 21 | ✅ |
| `public` | 49 | 49 | ✅ |
| `qe_archive` | 27 | 27 | ✅ |
| `rdagent` | 6 | 6 | ✅ |
| `selection` | 5 | 5 | ✅ |
| `strategy_pkg` | 7 | 7 | ✅ |
| `trading` | 9 | 9 | ✅ |
| **合计** | **251** | **251** | ✅ |

排除内部 schema：`pg_catalog`、`information_schema`、`_timescaledb_*`、
`timescaledb_information`、`timescaledb_experimental`。

## §7 重建命令（销毁重建）

```bash
docker stop aistock-pg-dev
docker rm aistock-pg-dev
docker volume rm aistock-pg-dev-data
# 然后重跑本文档 §3 + §4
```

snapshot 文件 `backend/db/dev/prod_schema_snapshot_20260510.sql` 受
`.gitignore` 保护，重建时如果丢失，需重新 `pg_dump` from prod。

## §8 后续访问

```bash
# Shell（在加载 .env 后）
psql -h 127.0.0.1 -p 5433 -U postgres -d aistock_dev

# Python
import os, psycopg2
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(
    host=os.environ['TDX_DB_DEV_HOST'],
    port=int(os.environ['TDX_DB_DEV_PORT']),
    dbname=os.environ['TDX_DB_DEV_NAME'],
    user=os.environ['TDX_DB_DEV_USER'],
    password=os.environ['TDX_DB_DEV_PASSWORD'],
)
```

## §9 与 prod 隔离保证

- **端口**：5433（dev）vs 5432（prod）
- **库名**：`aistock_dev` vs `aistock`
- **volume**：`aistock-pg-dev-data`（独立于 prod volume）
- **进程**：独立 docker 容器
- **镜像**：同 digest（确保版本一致），但运行时实例独立
- **代码层**：通过独立 env keys (`TDX_DB_DEV_*`)，绝不会与 prod 的
  `TDX_DB_*` 共用连接字符串

## §10 创建记录

- 完成时间：2026-05-10 13:00（apply 完成）
- 创建者：Claude Code paper-v2-vnpy-mvp team T17（impl-paper-v2 teammate）
- 测试通过时间戳：见 §11
- 关联 cross-tool drawer：待战略 session 通知 Codex 后补

## §11 smoke test

执行时间：2026-05-10 13:15（Python + psycopg2 + dotenv）

```
connected: ('aistock_dev', 'postgres')
('paper_trading', 8)
('paper_v2', 21)
('public', 49)
('qe_archive', 27)
('strategy_pkg', 7)
OK
```

各 schema 表数与 §6 一致。✅
