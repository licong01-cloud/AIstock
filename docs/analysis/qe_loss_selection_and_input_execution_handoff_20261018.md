# QE 损失选型落实与下一批输入对照交接

日期：2026-10-18。用户已批准落实MSE选择、完成收尾后推进后续任务。本交接不修改全局模型目标，不启动实验、不复制模型或数据、不控制服务。正式比较证据见[MA-E60R1报告](qe_ma_e60r1_three_objective_analysis_20261017.md)；后续研究顺序见[执行计划§3.2](qe_trend_capture_result_driven_plan_20261014.md#32-t7-input下一优先级共享b历史高点消融a独立取消裁剪c)。

## 1. 已确定的执行决定

- TCN/F24+quality_capex_efficiency/h20保留MSE。基准任务`qe_20261017_045950_e4f3` L1/4/7，seed42/2024/2026。后续该配方新训练显式提交`model_params.loss=mse`、`model_params.ltr_loss_mode=mse`，其余合同沿用来源；不是新训练任务。
- LOG保留低换手/较低回撤线索，PAIR本轮不继续扩展；两者实现和已有资产保留，不替换默认、不自动增加调参批次。旧浮点下溢实现不恢复，BUG-1941修复不回退。
- 其他模型继续原已批准训练目标，不以本批证据统一覆盖QE。LGBM B/A/C原目标为MSE，保持不变；本TCN决定不构成LGBM效果证明。
- 月度数据更新、股票池对照和多Alpha组合不自动触发三目标重比。只有明确不同研究目标/风险互补假设才另行设计；不新增审批、环境pin或研究门禁。
- 现有Composer已满足上述行为：`_finalize_general_ptnn_ltr_routing`在MSE路径生成原生`GeneralPTNN`、`qlib.contrib.model.pytorch_general_nn`、`loss=mse`，移除LTR专用参数；只有显式排序请求进入稳定LTR实现。本次无需修改生产代码、模型注册表或历史任务。

配置证据：[TCN配方决策JSON](artifacts/qe_tcn_mse_recipe_decision_20261018.json)。该文件是来源和精确覆盖项，**不是完整可提交请求**；不能把旧任务注册/数据绑定复制进新任务，纯prediction replay也不需要重设训练目标。

## 2. B/A/C配置及基准资产已整理

持久文件：

- [九Loop正式入口请求草案及复用分支](artifacts/qe_t7_input_request_preparation_20261018.json)
- [六个来源Loop的资产清单](artifacts/qe_t7_input_baseline_assets_20261018.json)
- [三目标逐Loop/分段分析JSON](artifacts/ma_e60r1_three_objectives_20261017.json)

草案状态为`PREPARED_NOT_SUBMITTED_DATA_HANDOFF_PENDING`，不是UI中已创建/运行的task。三个seed先共用B与A，再执行独立C；LGBM CPU训练node1并行2，不在Windows训练，不为占满WSL改变节点规则。

原训练`qe_20261014_105052_14f2`：B为L1/3/5，v15训练组件；行情修正回放`qe_20261015_100601_fd78`：B为L1/4/7，v18行情。后者不是v18重训。源任务分别6/6、9/9入仓，三个B原预测与回放SHA逐seed一致。

六个来源路径均在`/home/lc999/projects/RD-Agent-main/qe_workspace/<task>/Loop<N>`。目录catalog完整，原B模型`params.pkl`、处理器状态`artifacts/dataset`、pred/label、最终YAML及回测结果均在清单中；没有复制一套模型，也没有因Windows无目录误判资产丢失。状态字段存在不等于本次重新加载模型验收。

B/A/C计算11因子，共用原`Price_Deviation_Historical_High`参考面板；B/C输入11列，A仅移除该模型输入列变为10列。C独立把训练/推理`clip_outlier`改false。原11因子digest为`1e391c6e24e5cae66ee29992def94de9d4a9392312c52c0db70471d098dad4b2`；模型列序另存，不能用该digest冒充A的10列身份。不修改共享缓存或加入ID1544。

保持LGBM原超参、三个seed、全市场PIT训练/交易池、h20、Top50、TWAP+TAIL_BOOST(235/15)、成本及风险配置。HMM和行业黑名单关闭，ST-PIT与停牌过滤开启。原RobustZScoreNorm拟合2018-08-01..2024-06-28，包含验证期；为单轴比较保持原条件，但不宣称严格仅train拟合的独立验证。

train=2018-08-01..2022-12-30；valid=2023-01-03..2024-06-28；signal/test=2024-07-01..2026-08-31；portfolio=2024-07-01..2026-08-28。原8月底可得数据下h20成熟截至7月31日；若新增数据使8月标签成熟，三臂统一并分别报告原共同区间与新增区间，不能静默扩训练/回测窗口或补零。原B标签与回放标签行数不同，须按键和成熟日对齐，不能直接混用。

## 3. 数据交接后的唯一分支决定

当前只读active为`20261015-v18-unified-quote1`，截止8月31日；六池gap_count=0不证明行业因子历史输入已修复。本任务尚未收到训练输入修复successor的正式交接，不自行准备数据。

```text
条件                              新训练  新回测  基准B
原训练/处理/预测/交易条件均可比        6       6    直接复用
仅交易行情变，B模型及预测可比          6       9    重放原B三条
训练输入变或不能支持原B比较            9       9    共用新B/A/C
```

不机械运行九条，也不为证明旧B可复用增加全历史哈希。原11因子命中缓存不证明新数据下缓存仍可比；无法确认时用现有任务隔离计算共同面板，不覆盖其他任务缓存。原两项行业资金因子缓存始于2021-08-09，不能为消融裁去早期训练历史。新任务只通过正式active解析一次、配对同身份、重试保持原绑定，不将v15/v18路径写成永久默认。

等待数据交接只影响依赖它的对照，不把图模型PIT L2、9月发布、HMM或全历史归档扩大成所有实验前置。本轮不凭猜测把数据修复收益归于模型改动。

## 4. 重启与验证证据

- 用户已完成重启。2026-10-18 `/api/v1/runtime-identity`读取`a93a57db49f72c3d374c328f66707760bf36a177`，OpenAPI `CustomEvoLoopConfig.feature_processing`可见；PR #6263 merge为`27d9b795dba02663d27b5a88e1ab7776b541acf5`。
- 直接合同回归：现有AIstock环境运行`test_qe_prepare_factors_cache_contract.py -k feature_processing`，15 passed、24 deselected，无skip；涵盖真实小Parquet面板/输入身份、B/A/C/B隔离、Qlib训练/推理处理链、请求/重试字段与非法未消费配置。没有训练模型或投资回测。
- 生成YAML和小型处理链验证不能冒充远端九条实验已成功；API schema可见也不冒充生产真实因子计算完成。来源SHA只记录，不作为人工环境锁。
- 文档审核：第一轮逐seed结果/损失范围/现有默认/三臂单轴；第二轮资产来源/数据复用/标签成熟/运行态与未启动边界。只读校验7个真实纯路由分支（默认/显式MSE、两排序及旧别名、其他模型不变）、三组seed单轴、9条草案、6条资产、9条分析、10个相对链接通过，生产模块导入/训练/提交均0。诊断脚本首轮遗漏logger依赖，已补正常logger后完整重验；不是生产BUG。最终HEAD检查与CI/合入状态由实际交付回读，不预写成功。

## 5. DESIGN-COMPLIANCE-001与收尾边界

1. 完整交付：选型、现有消费逻辑、显式配置及来源资产均给出，不用“9/9结束”替代研究结论；B/A/C仍如实标未启动。
2. 无静默错误：保留bootstrap跨零、分段退化、风险取舍和基准可比未知项，不默认回退旧数据或补零标签。
3. 无业务语义迁移：只落实用户批准的本TCN配方MSE，其他模型、历史任务、数据、因子和交易合同不变；A/C维持独立单轴。
4. 无私增门禁：已有配置逻辑足够，不新增环境锁、审批、全量哈希或自动三目标批次。

本次生产代码变更0、数据库/数据集写入0、依赖/客户端安装0、进程控制0、实验启动0。持久分析与配置交接先提交，再由workflow按精确对象判定工作树清理；protected/unknown或活动引用停止清理，不手工强删，不删除来源实验资产。全部收尾后再推进§3的数据交接与B/A/C，不把文档合入当作新实验成功。
