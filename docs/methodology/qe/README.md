# QE 演进方法论目录

本目录是 AIstock 智能助手设计 QE 实验演进路线的**权威指导入口**。任何工具在设计 QE 实验前应先加载本目录。

## 文档清单（按阅读顺序）

1. **`QE_Evolution_Methodology_v1_20260529.md`** —— 方法论本体（先读这个）
   - 实证基础（seed/训练深度/IC-vs-收益的真实发现）
   - 三大第一性原则
   - 实验基因（搜索轴，泛化到任意模型）
   - 6 条演进路线（A 信号 / B 训练深度 / C seed集成 / D HMM减震 / E 模型多样性 / F 容量）
   - 因子组合筛选方法论（Q1，含量化机构实践/论文引用）
   - 模型演进方法论（Q2）
   - 每次实验的考核指标与数据分析方法论
   - 晋升漏斗与决策门
   - 给智能助手的执行契约

2. **`QE_Experiment_Template_Schema_v1_20260529.md`** —— 机器可读契约
   - 把"路线 + 动作轴"转换成 QE custom task 的 `loops` 配置
   - 字段契约、路线→loops 生成规则、自检清单

3. **`QE_DataWarehouse_Analytics_Design_v1_20260529.md`** —— 数仓分析层设计
   - 8 个分析视图（双轴榜/seed鲁棒性/因子稳定性/超参×seed/过拟合红旗/晋升候选等）
   - 已知数仓缺陷登记（model_trials 500 / score_total NULL / outbox 积压）
   - 实现：`backend/db/migrations/qe_archive_analytics_views_20260529.sql`（`production_ddl_pending`）

## 一句话工作流

> 选基线（查 `v_run_leaderboard`/`v_promotion_candidates`）→ 选路线（Part 3）→ 锁 1~2 条轴（Part 2）→ 按模板契约生成 loops（含 Route C 多 seed）→ 预登记考核（Part 6.7）→ `qe_custom_evo` 创建 `auto_start=false` → 跑后查视图出结论 → 晋升/复检/否决。

## 核心纪律

- 单次结果先标 `unverified`；经 seed 集成 + walk-forward 才 `verified`。
- 单轴爆表（尤其只有收益高）先标 `suspicious` 复检。
- 一次只动 1~2 条搜索轴，对照基线，归因清晰。

*活文档：每完成一轮里程碑实验（尤其 seed×训练深度解耦 `ad82`），回写方法论的"实证基础/现役锚点"两节。*
