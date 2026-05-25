QE 回测实验必须区分回测与实盘：回测优先使用固定 PIT 股票池或用户指定股票池，不得默认使用最新实盘股票池。创建 QE 10 loop 这类任务时，先生成 loop 草稿、股票池/时间窗/因子来源确认点和 MCP preflight 计划，不得在确认前调用 materialize 或 run。
