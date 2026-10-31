# LSTM-CONFIRM-1：固定模型前向确认与预算（F1）

2026-11-01。用户要求接续已建议的正式经济确认设计和有限预算，规划后直接执行；本设计交付文件型研究能力，不改变P20观察合同或新增生产准入门。

## Background / Goal / Feature Card

唯一问题：冻结P19第3折在同一原Top50中，是否能在新日期对原分数Top10产生正的5TD净估值增量。精确包、manifest/run/source、program/binding、model/release沿[P20](advisory_lstm_spo_shadow_v1_f2_design_20261027.md)，原生产Top20不变。开发+57.277530bps不能直接外推，LSTM-VALUE-1已完成，不重跑。

## Scope / 非目标 / 输入

仅新增operator、直接测试、本设计和结果报告，更新蓝图当前队列及脚本精确ownership/CI route；七文件清单见X:/AIstock_temp/ADVISORY/lstm-confirmation-20261101/scope.json。0新fit、0新政策、0原始收益查询、0DB写入、0服务控制；研报/现金流/价格/Exit暂停。读取旧report.json仅已有aggregate statistics，不读旧收益面板。

绑定模型物理SHA 8c915844002eab8b2dde0f8bc91f176ee7768b16ba5089520f106167bdf48813、release payload 877bfb0f0826487504ffbd5fcc92947497b226e864e10297ed1718392eb0b74e、精确源/提交闭包。源码只规范化CRLF，数据按物理字节。保护旧模型、release、所有预测和评价；不能写入它们所在目录。输出在独立X根，阶段create-only。

## Contracts / 研究时钟和预算

未来D≥2026-11-02的首150个官方交易日，D不晚于2027-07-31，构成固定研究人口；不以发布成功/输入完整/收益有利筛日期。D=10月30日是先行技术观察，明确不进入本次确认；其原P20评价照常。不补MISS、不以第151个捕获替换漏日。原Top50→双Top10、十等权槽、T开盘至T+4收盘、买0.95/卖5.95bps及UNKNOWN/CASH保持。

日历仅通过既有30秒只读连接导出market.trading_calendar，保存连续自然日的原is_trading及读取时点，不用工作日推算交易日。可先冻结规则，未来日历不齐为WAITING_CALENDAR，不能猜日期。最后D后第5会话为H；H后再固定5会话是数据修复宽限。**唯一正式揭盲**在宽限末日15:00后，不提前看本研究收益，也不以结果决定何时停止。绝对最晚2027-08-31 15:00停止；日历/捕获/结果仍不全则NOT_EVALUABLE，不延长研究、不把数据失败解释成没有alpha。

全150D必须有合法开盘前预测和截止时点之前的首个COMPLETE评价；任一UNKNOWN/缺预测/MISS保留并使总体NOT_EVALUABLE，不删除后对剩余日期做确认。P20可继续保存UNKNOWN及修复版本，但正式确认只接纳截止前按observed_at首个完整版本。截止后补齐仅属原观察，不追改这次结论。预测逐项复核原固定模型和50行人口，评价复算原paired_result并核对真实6会话、成熟时钟、政策和父hash。新工具不调用finance或run_once。

主终点delta=每D specialist_net_bps−native_net_bps，先对两臂固定十槽估值，再对150D等权；不是150笔独立交易、净资产曲线或真实成交。原源仍NON_VINTAGE，前向预测时钟成立不追认旧PIT，结果保留当前DB估值/不可证真实成交限制。

## Implementation Plan / 统计和决策

单一候选、单一对照、零均值原假设μ≤0，单侧α=0.05、计划power=0.80。零是本次科学问题的方向界限，**不是已经确定的业务最低收益或自动上线门槛**。旧report的102D、SE=27.291809958209306仅用于条件规划：长期波动代理SE×sqrt(102)；对25/50/57.277530/100bps和1/1.5/2倍波动计算所需D及150D的近似MDE。57.277530是旧开发值的情景，不是预计真实效果；旧三折模型及共同完整组估计不能保证当前固定第3折噪声，150D只是可承受的固定预算。

规划依据[NIST样本量公式](https://itl.nist.gov/div898/handbook/prc/section2/prc222.htm)，将相关数据长期方差代理代入仅为条件近似。正式计算使用固定Bartlett/Newey-West HAC lag=10（两个5TD持有长度）、n/(n−1)有限修正与渐近正态单侧95%下界；附lag4/20敏感性，不择优切主结果。实现公式和等距序列前提见[statsmodels官方HAC文档](https://www.statsmodels.org/stable/generated/statsmodels.stats.sandwich_covariance.cov_hac.html)。需要弱平稳、有限方差和足够混合，不能保证非平稳市场的精确5%误报率；lag10不保证消除全部相关，次级结果如不稳须在业务解释中保留。

下界>0只输出PROSPECTIVE_STATISTICAL_SUPPORT；否则INCONCLUSIVE_OR_NOT_SUPPORTED，不能把未显著当等效于零。方差退化单列。统计支持不自动激活、不赋值runtime的independent_economic_confirmation，也不单凭显著性宣称值得上线。未来业务决定须同时审视估值可执行性、绝对风险、效应大小和容量；没有真实成交/容量证据时仍不能声称交易上线验收。

CLI：freeze绑定protocol/budget；calendar导出无收益日历，操作环境需通过既有非秘密配置位置F:/Dev/AIstock/.env装载数据库配置，不复制、不输出凭据；inspect只看预测/MISS文件是否存在，不把文件存在当捕获验收，绝不打开evaluations；finalize先做日历和时钟检查，合法固定揭盲后才打开已有评价文件，发布单次最终结论。没有未成熟标签入口。源码/模型/原report前后hash复核；结果manifest绑定实际读入文件及日历。完成后再次finalize复用原终态，不因后来数据改善而覆盖。

原P20继续按原合同每日评价和展示描述性结果，不宣称整个系统被盲化。唯一正式统计决定固定在上述时点；不得因看见P20中间结果改参数、日期、预算或替换候选。新工具的读取隔离可以测试证明，组织层面完全无人见过中间结果则未证明。

protocol同时绑定唯一study_root；finalize只能在该目录发布，移动协议或用另一输出目录另起终态均拒绝。固定producer源码合入后在canonical checkout仍可复用，只允许Git规范化换行差异。

## Verification Plan / 多轮审核

直接测试覆盖预算独立算术、HAC与权威实现/独立矩阵一致、强正/非正/退化、先检查时钟再读收益、150D人口/漏日/UNKNOWN不删、精确模型重算/错误身份/NaN、评价首个完整版本及截止限制、日历缺行/重复/非布尔、源码换行/实质变更、输出隔离及不可变冲突/复用。调用拦截证明不执行拟合/源finance/调度。合成完整成熟数据验证将来路径，只算合同测试，不算真实经济成果。

真实本轮只完成freeze、日历快照、inspect和未到揭盲的finalize，保存0评价文件读取证据；不虚构150个真实前向结果。R1数值/时钟，R2研究人口/独立性/统计解释，R3 scope/无副作用/全设计项自审后修订；lint/compile、直接测试、L0、F1和蓝图F2、CI覆盖和合入另列。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-591 | 精确模型/来源/源码冻结及仅汇总噪声的条件样本预算 |
| F-592 | 150预定D/真实交易时钟/唯一揭盲/有限截止/UNKNOWN不删 |
| F-593 | 原模型预测与paired_result复核、评价版本和来源绑定 |
| F-594 | 固定HAC统计/单主对照/敏感性及非自动生产结论 |
| F-595 | 文件型CLI不可变、收益读取隔离、无拟合/DB写/后台影响 |
| F-596 | 真实预检、成熟路径合同验证、多轮审核与全部交付分态 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-591 | scripts/advisory_lstm_prospective_confirmation.py | backend/tests/advisory_model_first/test_lstm_prospective_confirmation.py | VERIFIED_CONTRACT_AND_REAL_PREREGISTERED_PREFLIGHT | none |
| F-592 | scripts/advisory_lstm_prospective_confirmation.py | backend/tests/advisory_model_first/test_lstm_prospective_confirmation.py | VERIFIED_CONTRACT_AND_REAL_PREREGISTERED_PREFLIGHT | none |
| F-593 | scripts/advisory_lstm_prospective_confirmation.py | backend/tests/advisory_model_first/test_lstm_prospective_confirmation.py | VERIFIED_CONTRACT_AND_REAL_PREREGISTERED_PREFLIGHT | none |
| F-594 | scripts/advisory_lstm_prospective_confirmation.py | backend/tests/advisory_model_first/test_lstm_prospective_confirmation.py | VERIFIED_CONTRACT_AND_REAL_PREREGISTERED_PREFLIGHT | none |
| F-595 | scripts/advisory_lstm_prospective_confirmation.py | backend/tests/advisory_model_first/test_lstm_prospective_confirmation.py | VERIFIED_CONTRACT_AND_REAL_PREREGISTERED_PREFLIGHT | none |
| F-596 | 本文Verification Plan | artifact: X:/AIstock_temp/ADVISORY/lstm-confirmation-20261101/validation_receipt.json | VERIFIED_CONTRACT_AND_REAL_PREREGISTERED_PREFLIGHT | none |

## Risks / Production Gates / Rollback

150D可能不足检测较小增量或噪声上升；不能为显著性扩样。任一不可恢复漏日会导致全研究无法评价，原生推荐和P20观察继续正常运行，不能以此卡生产发布。研究工具无调度钩子，无需后端重启；production_ddl/dependency/client/activation均noop。暂停使用CLI即可回退，保留证据，无需删资产。工作树和分支保留；不推导新清理授权。
