# QE L2 原始登记修复与同 ID 接续

BUG-2077 / #6636。仅适用于原D1～D7研究的6ed6c1be、aeae55e7、d1ab9d9e。

前两条真实125日分钟回测已完成，登记漏存执行合同导致Archive拒绝；第三条仅created，容量事务拒绝后没有派发。不能重新跑已完成结果或把created送入FULL_TRAIN队列。

`scripts/qe_l2_registration_recovery.py`默认preview；`--mode dev`只在现有DEV事务插入精确源行、修正、幂等读回并全部回滚。没有生产apply命令，没有派发、服务控制或数据集写入。

## 修复合同

1. 限定原三ID及其task/status，固定原compose plan/preflight摘要；conf按原提交入口的文本读取规则转换LF，同时记录本地原始文件SHA。
2. 完成臂还需原封存回执、真实conf SHA与事后分钟guard通过证据。已存在的运行合同若冲突立即拒绝，不以新值覆盖。
3. 只补九个遗漏的分钟执行元数据键及审计字段，审计记录原参数摘要、实际conf/封存摘要。状态、时间、结果、因子、模型、数据和原运行包不变。
4. apply持有源行锁并校验完整源行摘要；任何并发变更拒绝。相同已应用计划幂等返回，未知ID、已尝试created、未释放reservation均拒绝。
5. 生产写入必须以同一DEV已验证计划、不可变源码版本和明确生产授权执行，再重新从正式Archive接口预览、归档和读回。不能强设eligible或修改既有结果。

## L2 接续顺序

修复两条完成源并核对Archive 2/2后，原F2工作树生成未派发四臂的新版本运行包：仅补登记和精确终端executor guard。四臂继续原ID、原预测、原数据身份、原125日窗口；旧stop/intent和旧compose保留，通过独立接续回执说明替代关系。

d1ab9d9e的数据库无attempt证据还需远端workspace not_found实时证据。入口只允许一次同ID认领，仍由现有容量事务保证排他；不允许重试已派发ID或进入通用训练队列。这个修复工具本身不派发，也不授权跳过该门禁。

## 验收边界

DEV通过不代表生产已应用；源修复不代表Archive已入仓；首seed有效不代表完整三seed经济验收。旧preset_A/HMM/默认策略不受影响，HMM和AF训练次数均0。

当前验证：11项定向测试通过；现有`aistock_dev`对上述三条真实源行完成修正、幂等及逐字段不变校验，全部回滚，生产写入0。DEV计划摘要`ad2e4e02ab9b05b6909ac6a6d242ee4e009c13acdba755f5a2508553528e8764`，回执位于`F:/Dev/AIstock/tmp/handoff/qe-mainline-continuation-20261023/l2-recovery-dev-v2/receipt.json`。l0及模块归属门禁通过；广泛QE回归交CI/Nightly。

DESIGN-COMPLIANCE-001：交付完整的限定登记修复路径，不将未执行的生产修复/四臂续接称为完成；冲突与缺失显式失败；策略、数据和结果语义不变；审批只沿用现有生产门禁，没有新增公式审批。
