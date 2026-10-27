# P20 LSTM零拟合每日并行观察交付

2026-10-27，接续P19。按用户批准复用训练截止最晚的第3折，完成日常观察接入。当前是源码、配置和技术验证交付；每日生产进程加载待用户重启，未取得新前向成熟收益。

- 精确LSTM包：pkg_cc7eccb6202e4816b9d7c7d6b748bd37，program advp_ed918504a02f4b15892f0dba65da139b；其它包不接入。
- 原模型文件SHA：8c915844002eab8b2dde0f8bc91f176ee7768b16ba5089520f106167bdf48813；训练截止2025-07-31，原系数、编码不变。0fit，累计248实际fit＋1历史索引不变。
- release SHA：877bfb0f0826487504ffbd5fcc92947497b226e864e10297ed1718392eb0b74e；独立配置与原model.json位于既有模型根目录的lstm_spo_shadow_v1子目录。准备/安装/合入状态见任务delivery_receipt.json。
- 每日沿原生forward调度后置调用，T开盘前固定原始分数Top50及两套Top10，原生Top20持仓政策不改。当前40行原生候选与完整分数资产分开，不伪称已经有原生50行名单。
- 第一次新D=2026-10-27。未来H=T+4收盘后沿固定5TD费用与十槽口径评价，保留UNKNOWN及其后续版本；完成结果不可改写。漏采显式MISSED，不倒填，不自动替换荐股榜单。

真实输入预览使用D=10月26日、T=27日的原生资产ssa_28462125fe714cfca2974c82ed4fc7b6，4918行原分数，40行消费分数完全一致，八字段50行全部有值，重排换入7只。新进程读回及独立矩阵计算/Top10复算一致。预览时T已开盘，所以这50行不计入前向试运行，也没有读取它们的收益标签。

本地10项直接测试覆盖模型身份/零拟合、数值复算、原生40与分数Top50差异、缺失、重复执行/重启及漏日恢复、开盘前后、UNKNOWN版本、成熟前仅查日历、原5TD估值、调度失败隔离、暂停与GET无执行副作用。更广模块回归由PR CI按ownership执行，不在本地重复整套。多轮自审不是外部独立审核。

只读查看：`GET /api/v1/advisory/programs/advp_ed918504a02f4b15892f0dba65da139b/reranking-shadow`，可加target_trade_date。返回release、捕获/成熟完整天数、逐日结果、漏采、最后失败及双排名。已有`GET /api/v1/advisory/forward/status`的每轮结果新增lstm_spo_shadow。

重启后先验证新路由、release SHA和forward hook加载；再核对首个T开盘前的prediction.json及50行人口，最后在H后检查成熟双臂结果。尚未产生预测时WAITING_NATIVE_PUBLICATION是等待原生当天发布，不能报告模型已有效运行。源码合入/配置安装不等于当前进程加载；经济有效性仍未确认。

无DDL/DML、数据库写入、数据集切换、QE操作、训练、服务控制或清理。production_ddl_gate=noop，依赖/client=noop，backend_restart_required=true由用户执行。研报/现金流/价格/Exit仍暂停。

设计及逐项验收：[P20 F2](../architecture/advisory_lstm_spo_shadow_v1_f2_design_20261027.md)。精确scope、实际预览、审核和交付收据：X:/AIstock_temp/ADVISORY/p20-lstm-shadow-20261027。
