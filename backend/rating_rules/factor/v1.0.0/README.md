# AIstock Multi-Alpha Daily Production Grade v1

这是 AIstock 当前唯一正式因子评级标准，面向日频、多 Alpha、低换手生产场景。

## 设计目标
- 统一正式评级口径，避免规则与 LLM 混用
- 优先评价可执行、可稳定、可组合的日频生产因子
- 将低换手与选股稳定性正式纳入评分

## 评分结构
- Predictive Strength: 25
- Stability: 25
- Economic Quality: 15
- Selection Stability & Cost: 15
- Monotonicity & Reliability: 10
- Multi-Alpha Fitness: 10

## 关键 hard gates
### S级
- core_ic >= 0.05
- recent_6m / recent_3m 不能同时显著转负
- 单调性 > 0
- 超额年化 > 0
- coverage >= 0.70
- turnover <= 0.08

### A级
- core_ic >= 0.03
- 不存在严重近端失效
- 单调性 > -0.10
- coverage >= 0.60
- turnover <= 0.12

## 适用边界
- 该规则不是通用学术评级，而是服务于 AIstock 当前日频生产目标
- v1 使用 turnover 作为低换手与选股稳定性的正式 proxy
- 若未来需要更精确描述股票池是否每日剧烈变化，应在数据库中新增 top bucket overlap / avg holding days 等指标并升级到 v2

## LLM 职责
LLM 只能基于数据库中的同一批指标与正式评分结果生成补充说明、风险提示和人工复核意见，不能修改 official_grade。
