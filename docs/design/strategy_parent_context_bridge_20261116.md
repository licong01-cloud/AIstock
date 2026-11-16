# 新研究多Alpha父包原执行上下文桥接（F1）

## 范围与目标

仅恢复新研究 OLD5 / UPDATED3 父包正式导出所需的原执行上下文；不修补或覆盖旧父包38项冻结身份。16份真实模型、原拟合处理器及源码已经完成CAS闭合。本次不训练、不重新生成完整预测、不重跑账户；DEV整包冷消费最多16模型×3日期=48次真实预测。

原已发布组合的 `backtest_config_json` 缺少扁平上下文字段，但选定发布attempt的 SHA 校验配置、数据绑定和构建规格均存在。通过版本化原上下文证据封装解析原 `n_drop=1`、`max_n_drop=2`、PIT pool pins、h20，以及精确执行类/模块/参数；执行标识只能映射到现有且未退役的正式定义，不能臆造旧alias、缺省填充或绕过退役。原冻结来源记录及其哈希不修改。

## Design Acceptance Index

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| PC-01 选定发布来源、原始字节及SHA | 待实现：版本上下文桥接 | 待验证 | PLANNED | 不使用未选定尝试 |
| PC-02 原字段提取与冲突失败关闭 | 待实现：promotion桥接接线 | 待验证 | PLANNED | 不混淆n_drop和max_n_drop |
| PC-03 原执行标识及退役检查 | 待实现：精确映射 | 待验证 | PLANNED | 不创造执行别名 |
| PC-04 DEV父包完整消费及不可变旧身份 | 待实现：DEV验证收据 | 待验证 | PLANNED | 生产写入激活未授权 |
