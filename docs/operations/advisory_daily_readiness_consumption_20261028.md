# Advisory 每日输入就绪消费交付

日期：2026-10-28。设计PR [#6799](https://github.com/licong01-cloud/AIstock/pull/6799) 已合入；源码PR [#6808](https://github.com/licong01-cloud/AIstock/pull/6808) 的合入/CI状态以GitHub回执为准。设计：[F2详细设计](../architecture/advisory_daily_readiness_consumption_f2_design_20261028.md)；[三轮审核](../analysis/advisory_daily_readiness_review_20261028.md)；[机器回执](evidence/advisory_daily_readiness_20261028/receipt.json)。

## 实现结果

读取正式冻结因子及预处理的非执行元数据，解析完整输入依赖与实际窗口。新原生名单执行前检查包就绪；稳定输入绑定进入分数工件键；实际数据库输入快照、发布前及提交短事务核对同一版本。旧已发布事实仍幂等，不重建人口或复写分数。

影子观察继续读取原发布Top50，额外验证20TD行情、复权和沪深300基准分区；计算后再次核验并检查目标开盘截止时间。等待有持久化原因及UI显示，浏览不触发预测。

## 真实数据读回

以下为新源码在独立进程只读查询正式数据库/资产的结果，不代表运行中的服务已加载。D固定为2026-10-27；日期覆盖使用正式交易日历。

| 包 | 序列长度 | 完整输入窗口 | 状态 | 残余 |
|---|---:|---|---|---|
| LGB `pkg_c8d3d51fb6d24be89cfb3a50c30c80e4` | 1 | 255TD，2025-10-09～2026-10-27 | WAITING | 1778个UNVERIFIED、2个INCOMPLETE |
| LSTM `pkg_cc7eccb6202e4816b9d7c7d6b748bd37` | 20 | 284TD，2025-08-21～2026-10-27 | WAITING | 1698个UNVERIFIED、2个INCOMPLETE |
| TCN `pkg_8dbd396d27184de6aa5fe288011220f6` | 20 | 284TD，2025-08-21～2026-10-27 | WAITING | 1981个UNVERIFIED、2个INCOMPLETE |
| 旧single `pkg_378eb9c91e104c64935404e257e932ee` | 原生无配对预处理合同1 | 265TD，2025-09-17～2026-10-27 | WAITING | 1848个UNVERIFIED、2个INCOMPLETE |
| 多alpha父包 `pkg_ma_8ec5e389fa2c5e484a1ac7e9` | 未能确认全部seed | 未放行 | UNKNOWN | 每seed冻结资产绑定不齐 |

所有四包依赖 `kline_daily_raw / adj_factor / daily_basic / cyq_perf / moneyflow_ts / sector_data`；除LSTM外另需`bak_basic`。四包均不依赖`margin_detail`。窗口包含既有5日缓冲；LGB实际250日rolling已同步进入原生读取。

UNVERIFIED主要在D以前；10月27日`adj_factor`与`daily_basic`为INCOMPLETE。不能把全窗口UNVERIFIED描述为全窗口数据丢失，也不能只更新当日就声称整个模型输入已就绪。

## 给数据准备窗口的接续提示

共享包依赖/模型窗口解析已由Advisory本轮处理，请勿重复修改。先读取本报告和机器回执，对现有正式数据库做版本化验证：

1. 以共享readiness的每包精确窗口/依赖为准，优先验证已存在数据，保留源修订、scope/rule、validation_id。历史跨度最大从2025-08-21至当前D；只将该范围用于实际模型输入证明，不扩展研究人口。
2. 核查10月27日adj_factor、daily_basic的INCOMPLETE原因，真实缺数据再按数据窗口既有授权修复；不要把UNVERIFIED批量改VALID，也不要重采已有完整历史。
3. LSTM影子额外需要D及之前20TD的kline_daily_raw、adj_factor、index_daily（000300.SH），沿用现有共享验证合同。不得补零、前填未知或修改原Top50。
4. 返回每包READY/WAITING/UNKNOWN、精确残余日期/字段、当前修订和验证身份。全局all_sync_complete与每包就绪分别报告；完成标识必须对应同版本数据。
5. 父包缺seed资产归QE资产owner，不用数据补采代替。不得启动QE/重新拟合或补作错过的前向预测。

## 验证与运行接续

本地最小直接合同集85通过，增量边界反例4通过，现有DEV修订锁/回滚测试1通过；Ruff、前端语法编译和L0通过。源码PR CI负责较宽模块和UI回归；本机缺LightGBM的旧训练辅助测试不冒称通过。

生产DDL=noop；生产DML=未执行；DEV=仅独立测试键及临时表，准确清理/连接关闭；模型拟合=0；QE=0；收益标签读取=0；生产预测启动=0；数据集切换=0；服务控制=0。用户重启后核对runtime身份、五包readiness、forward状态及影子状态；数据未就绪时保持等待，不能以固定21:00或源码合入替代数据证明。

首份合法前向捕获和5TD成熟结果仍等待自然发生。BUG-2111 close-sync继续保持未完成业务验收状态；旧MISSED不回填。本轮源码工作树保留，未另行执行清理授权。
