# TDX Go 数据服务运行与隔离验证

目标：`tdx-go-backend`；生产端口 `19080`；隔离验证仅 `19081/19082`。

## 配置与构建

现有一键启动与 `tdx-api-main/web/start.bat` 使用 `scripts/start_tdx_go_backend.py`，
显式传递 `--database-target production --env-file <repo>/.env`；只读取数据库配置，
不打印凭据，不继承其他目标的 DSN，按完整 Git SHA 编译整个 Go web package。
手工 `go run .` 仍要求操作者先提供完整环境变量，不再依赖源码内生产默认密码。
`--check` 只核验配置与源码身份，不启动服务。DEV 使用同一入口与依赖，
改为 `--database-target dev`、`TDX_HTTP_PORT=19081/19082` 及独立 X 盘 `TDX_DATA_DIR`。

必须显式提供 `TDX_DB_DSN`，或完整的 `TDX_DB_HOST/PORT/NAME/USER/PASSWORD`。
不使用默认生产库、硬编码密码或从当前目录隐式加载 dotenv。凭据只从受控配置位置加载，不写入日志和回执。

隔离实例设 `TDX_RUNTIME_ENV=dev`、`TDX_HTTP_HOST=127.0.0.1`、
`TDX_HTTP_PORT=19081`、`TDX_DB_NAME=aistock_dev`，使用现有 DEV 凭据，不允许 DSN 覆盖。
设置 `TDX_DATA_DIR` 为 X 盘独立目录；进程工作目录和输出日志同样在 X 盘，不复用生产 sqlite/cache。

在干净已审核源码的 `tdx-api-main/web` 目录执行：

```powershell
$revision = git rev-parse HEAD
$env:GOPROXY='off'; $env:GOSUMDB='off'; $env:GOTOOLCHAIN='local'
go build -mod=readonly -ldflags "-X main.buildRevision=$revision" -o <X盘版本化绝对二进制路径> .
```

必须记录源码 commit、二进制 SHA256、端口、PID、数据库名称；不得记录凭据。
生产实例仍由用户启动或重启，本 BUG 的源码合入不代表生产已更新。

## 只读探针与九月 DEV 验证

- health：`GET /api/health`，UTC 实际时间及 `source_revision`。
- identity：`GET /api/runtime-identity`，源码版本、显式目标和监听地址。
- business smoke：`GET /api/kline-all/tdx?code=sh688526&type=minute1` 和 `sh688570`。
- 检查 2026-09-10/11:21、2026-09-17/11:30 的 `AmountWire/VolumeWire`、厘金额和股数；
  同时保留正常大额记录。不将 Sina 报价直接当作 TDX 原始金额，也不手填 13/15 来制造日分钟一致。
- DEV 写入使用 `start_time/end_time` 绑定已完成月份；重放相同事实不重复入库。
  相同主键已有不同原始事实时拒绝覆盖，需独立、有范围的修复授权。
- DEV readback：现有 `market.kline_minute_raw` 中逐键 OHLC、金额、整手与原始股数及 provenance；
  同样的事实再运行不新增行。不得查询或修复与本月无关的数据。

## CI 和边界

沿用现有 AIstock CI Go lane：离线协议测试、分页边界测试、根模块编译、独立 web 模块测试与编译。
不在 PR CI 中连接行情服务器、写数据库、安装依赖或控制服务。
部署验证是独立的 DEV 状态；生产数据修复和服务重启需要单独授权。
停牌、真实 provider absence、未能证实的历史报文和成交额差异必须显式报告，不补零、不前填、不猜测改时。
