# 📈 TDX通达信股票数据查询系统（数据api接口服务）

**感谢源作者injoyai，请支持原作者。源项目地址https://github.com/injoyai/tdx**

> 基于通达信协议的股票数据获取库 + Web可视化界面 + RESTful API

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://golang.org)
[![Docker](https://img.shields.io/badge/Docker-支持-2496ED?style=flat&logo=docker)](https://www.docker.com)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## ✨ 功能特性

### 📊 核心功能
- ✅ **实时行情** - 五档买卖盘口、最新价、涨跌幅
- ✅ **K线数据** - 支持10种周期（1分钟~年K）
- ✅ **分时数据** - 当日分时走势、逐笔成交
- ✅ **股票搜索** - 代码/名称模糊搜索
- ✅ **批量查询** - 同时获取多只股票数据

### 🌐 Web可视化界面
- 📱 **现代化UI** - 渐变色设计、响应式布局
- 📊 **图表展示** - ECharts专业K线图和分时图
- 🔍 **智能搜索** - 支持代码和名称快速搜索
- 📈 **实时刷新** - 数据自动更新

### 🔌 RESTful API
- 🚀 **26个接口** - 覆盖所有数据需求
- 📖 **完整文档** - 详细的API说明和示例
- 🐍 **多语言** - Python/JavaScript/cURL示例
- ⚡ **高性能** - 快速响应、支持批量

### 🐳 Docker部署
- 📦 **开箱即用** - 一键启动
- 🔧 **无需配置** - 自动解决依赖
- 🌍 **跨平台** - Windows/Linux/Mac统一

---

## 🚀 快速开始

### 方式一：Docker部署（推荐）⭐

```bash
# 1. 克隆项目
git clone https://github.com/injoyai/tdx.git
cd tdx

# 2. 启动服务（自动构建）
docker-compose up -d

# 3. 访问Web界面
浏览器打开: http://localhost:8080
```

**就这么简单！** 🎉

### 方式二：源码运行

#### 前置要求
- Go 1.22+
- 网络连接

#### 启动步骤

```bash
# 1. 下载依赖
go mod download

# 2. 进入web目录
cd web

# 3. 运行服务器
go run server.go

# 4. 访问
浏览器打开: http://localhost:8080
```

---

## 📖 使用指南

### 1. Go库使用

```go
package main

import (
	"fmt"
	"github.com/injoyai/tdx"
)

func main() {
	// 连接服务器（自动重连）
	c, err := tdx.DialDefault(tdx.WithDebug(false))
	if err != nil {
		panic(err)
	}
	
	// 获取五档行情
	quotes, err := c.GetQuote("000001", "600519")
	if err != nil {
		panic(err)
	}
	
	for _, v := range quotes {
		fmt.Printf("%s: %.2f元\n", v.Code, float64(v.K.Close)/1000)
	}
	
	// 获取日K线
	kline, err := c.GetKlineDayAll("000001")
	if err != nil {
		panic(err)
	}
	fmt.Printf("获取%d条K线数据\n", len(kline.List))
}
```

### 2. Web界面使用

访问 http://localhost:8080

**功能演示**：
1. 🔍 **搜索股票**：输入"000001"或"平安银行"
2. 📊 **查看行情**：五档买卖盘、实时价格
3. 📈 **K线分析**：切换不同周期（日K/周K/月K/分钟K）
4. 📉 **分时图**：当日走势、成交量分布
5. 📋 **成交明细**：逐笔交易记录

### 3. API接口使用

#### 获取实时行情
```bash
curl "http://localhost:8080/api/quote?code=000001"
```

#### 获取K线数据
```bash
curl "http://localhost:8080/api/kline?code=000001&type=day"
```

#### 搜索股票
```bash
curl "http://localhost:8080/api/search?keyword=平安"
```

**更多接口**: 查看 [API接口文档](API_接口文档.md)

---

## 📡 API接口列表

| 接口 | 方法 | 说明 |
|-----|------|------|
| `/api/quote` | GET | 五档行情 |
| `/api/kline` | GET | K线数据 |
| `/api/minute` | GET | 分时数据 |
| `/api/trade` | GET | 分时成交 |
| `/api/search` | GET | 搜索股票 |
| `/api/stock-info` | GET | 综合信息 |

**完整API文档**: [API_接口文档.md](API_接口文档.md) (674行详细说明)

---

## 🐳 Docker部署

### 快速启动

```bash
# 启动服务
docker-compose up -d

# 查看日志
docker-compose logs -f

# 停止服务
docker-compose stop

# 重启服务
docker-compose restart
```

### 一键启动脚本

**Windows**:
```bash
双击运行: docker-start.bat
```

**Linux/Mac**:
```bash
chmod +x docker-start.sh
./docker-start.sh
```

**详细部署文档**: [DOCKER_DEPLOY.md](DOCKER_DEPLOY.md)

---

## 📊 数据类型

### 支持的数据类型

| 数据类型 | 获取方法 | 说明 |
|---------|---------|------|
| **五档行情** | GetQuote | 实时买卖五档、最新价、成交量 |
| **1分钟K线** | GetKlineMinuteAll | 最多24000条 |
| **5分钟K线** | GetKline5MinuteAll | 短期分析 |
| **15分钟K线** | GetKline15MinuteAll | 日内趋势 |
| **30分钟K线** | GetKline30MinuteAll | 波段参考 |
| **60分钟K线** | GetKlineHourAll | 短期趋势 |
| **日K线** | GetKlineDayAll | 中长期分析 |
| **周K线** | GetKlineWeekAll | 长期趋势 |
| **月K线** | GetKlineMonthAll | 超长期趋势 |
| **分时数据** | GetMinute | 当日每分钟价格 |
| **分时成交** | GetTrade | 逐笔成交记录 |
| **股票列表** | GetCodeAll | 全市场代码 |

### 数据校对

* ✅ 日K线已校对
  ![](docs/check_kline.png)
  ![](docs/check_kline_right.png)

* ✅ 所有K线已校验

* ✅ 分时成交已校对
  ![](docs/check_trade.png)

---

## 📁 项目结构

```
tdx/
├── client.go              # TDX客户端核心实现
├── protocol/              # 通达信协议实现
│   ├── model_quote.go     # 五档行情协议
│   ├── model_kline.go     # K线数据协议
│   ├── model_trade.go     # 分时成交协议
│   └── model_minute.go    # 分时数据协议
├── web/                   # Web应用
│   ├── server.go          # Web服务器 + API接口
│   └── static/            # 静态文件
│       ├── index.html     # 主页面
│       ├── style.css      # 样式表
│       └── app.js         # 前端逻辑
├── docker-compose.yml     # Docker编排配置
├── Dockerfile             # Docker镜像构建
└── docs/                  # 文档和示例

📚 文档：
├── README.md              # 项目说明（本文件）
├── API_接口文档.md        # 完整API文档（674行）
├── DOCKER_DEPLOY.md       # Docker部署指南（637行）
└── API_使用示例.py        # Python使用示例
```

---

## 💡 应用场景

### 🤖 量化交易
```python
# 获取全市场数据，筛选交易信号
codes = api.get_all_codes()
quotes = api.batch_get_quote(codes)
signals = analyze_strategy(quotes)
execute_trades(signals)
```

### 📊 数据分析
```python
# 获取历史K线进行回测
klines = api.get_kline('000001', 'day')
df = pd.DataFrame(klines)
backtest_result = backtest_ma_strategy(df)
```

### 📱 实时监控
```javascript
// 自选股实时监控
setInterval(() => {
    updateWatchlist(['000001', '600519', '601318']);
}, 3000);
```

### 🔔 价格提醒
```python
# 监控价格变化，触发提醒
while True:
    quote = get_quote('000001')
    if quote['price'] > target_price:
        send_notification('价格突破！')
```

---

## 🎯 技术特点

### 高性能
- ⚡ Go语言实现，并发性能优秀
- 🚀 Docker容器化，启动快速
- 📦 多阶段构建，镜像仅20MB

### 易用性
- 📖 详细文档（1500+行）
- 🎨 现代化Web界面
- 🔌 RESTful API设计
- 💻 多语言示例代码

### 可靠性
- ✅ 数据已校验
- 🔄 自动重连机制
- 🐳 Docker健康检查
- 📝 完善的错误处理

---

## 📚 完整文档

| 文档 | 说明 | 行数 |
|-----|------|------|
| [README.md](README.md) | 项目说明 | 本文件 |
| [API_接口文档.md](API_接口文档.md) | 完整API说明 | 674行 |
| [DOCKER_DEPLOY.md](DOCKER_DEPLOY.md) | Docker部署指南 | 637行 |
| [API_集成指南.md](API_集成指南.md) | API集成步骤 | 543行 |
| [API_使用示例.py](API_使用示例.py) | Python示例 | 340行 |

**总文档量**: 2000+ 行详细说明

---

## 🌟 快速链接

- 🚀 [5分钟快速开始](DOCKER_DEPLOY.md#快速开始)
- 📖 [完整API文档](API_接口文档.md)
- 🐍 [Python使用示例](API_使用示例.py)
- 🐳 [Docker部署指南](DOCKER_DEPLOY.md)
- 💡 [常见问题解答](DOCKER_DEPLOY.md#常见问题)

---

## 🔗 相关资源

### 参考项目
- Golang库: [`gotdx`](https://github.com/bensema/gotdx)
- Python库: [`mootdx`](https://github.com/mootdx/mootdx)
- 数据入库: [`stock`](https://github.com/injoyai/stock) (开发中)

### 通达信服务器地址

系统自动连接最快的服务器，也可手动指定：

| IP | 所属地区 | 运营商 |
|----|---------|--------|
| 124.71.187.122 | 上海 | 华为 |
| 122.51.120.217 | 上海 | 腾讯 |
| 121.36.54.217 | 北京 | 华为 |
| 124.71.85.110 | 广州 | 华为 |
| 119.97.185.59 | 武汉 | 电信 |

更多服务器地址请查看[完整列表](docs/servers.md)

---

## ⚠️ 免责声明

1. 本项目仅供学习和研究使用
2. 数据来源于通达信公共服务器，可能存在延迟
3. 不构成任何投资建议
4. 请勿用于商业用途
5. 投资有风险，入市需谨慎

---

## 🤝 贡献

欢迎提交Issue和Pull Request！

### 贡献指南
1. Fork本项目
2. 创建新分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交Pull Request

---

## 📄 许可证

本项目采用 MIT 许可证。详见 [LICENSE](LICENSE) 文件。

---

## 📞 联系方式

- 💬 提交Issue: [GitHub Issues](https://github.com/oficcejo/tdx-api/issues)
- 📧 邮件: [联系我们](mailto:your-email@example.com)

---

## ⭐ Star History

如果这个项目对您有帮助，请点个Star⭐️支持一下！

---




