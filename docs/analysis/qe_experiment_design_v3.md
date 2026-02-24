# QE实验设计（原组合配置）重构与升级详细设计方案 v3

## 1. 核心业务逻辑与架构定位

### 1.1 自动演进的触发与基座
- **演进基座机制**：QE 目前生成的因子组合与模型演进任务，**完全以 AIstock 作为调度大脑**，WSL 环境仅作为执行终端。
- **冷启动与独立任务**：演进不强制要求从已完成的实验开始。系统支持从前端零选因子和模型，组装成**独立实验（独立任务）**。独立实验完全复用自动演进的运行和监控体系，这确保了以后能够无缝支持“从独立任务开始自动演进”。
- **系统边界**：QE 的演进专门聚焦于**因子组合和模型选择**，不涉及底层因子和模型的具体代码开发，因此**绝对不修改任何 RDAgent 核心代码**。

### 1.2 核心改造目标
1. **页面重命名与 UI 重构**：“组合配置”全面更名为“QE实验设计”，整体色调、卡片阴影、背景、字体排版严格对齐现有的“自动演进”页面，依托前端现有的 Tailwind CSS 架构实现全局样式统一。
2. **双轨驱动入口**：支持“AI智能生成配置”与“人工分步流程式选择”双轨并行，互不冲突。
3. **组件高复用**：因子、模型、策略库的 UI 组件实施高度复用，转为“选择模式”（Selection Mode）。
4. **任务分流**：配置完成后，支持分流为**独立任务（单次回测）**与**QE演进任务（循环迭代）**，且两者均可在“自动演进”看板中统一监控。

---

## 2. 页面整体布局与 UI/UX 设计

### 2.1 顶部：AI 智能实验设计区
- **标题**：AI 智能实验设计 (AI Smart Experiment Design)
- **UI 元素**：
  - 一个开阔的文本输入域（Textarea），引导语：“请输入您的实验组合设计目标，例如：构建一个偏向于动量反转的日频量化组合，配合高频微观结构因子...”
  - **“智能生成配置”按钮**（带加载动效）。
- **交互逻辑**：点击生成后，调用后台 LLM API 解析意图。解析成功后，**自动锚定并高亮**下方人工配置区对应的因子、模型和策略复选框，并在各卡片中自动打勾，用户可继续在此基础上人工微调。

### 2.2 中部：全链路选择流程图（Stepper组件）
- **UI 元素**：横向步骤条（Step Navigation）。
- **节点**：`1. 因子选择` -> `2. 模型选择` -> `3. 策略选择` -> `4. 组合配置` -> `5. 生成执行 (WSL)` -> `6. 结果查看`
- **交互逻辑**：支持点击任意已激活节点进行无损跳转（不丢失已选数据）。

### 2.3 下部主体：人工配置分步卡片区（复用库组件）
- **通用设计规范**：
  - 采用 **Tailwind CSS**，提取统一的 Card 样式变量（如 `bg-card`, `border-border`, `shadow-sm`），确保与“自动演进”页面视觉完全一致。
  - 列表呈现形式必须为**卡片（Card）**，去除原有的“批量分析”、“分析”等直接操作按钮，统一替换为**复选框（Checkbox）/选择按钮**。
  - 默认分页：每页 20 条，支持自定义。

#### 步骤 1：因子选择 (Factor Selection)
- 传入 `mode="selection"` 复用现有的 QE 因子库列表，增加复选框进行多选。

#### 步骤 2：模型选择 (Model Selection)
- 传入 `mode="selection"` 复用现有的模型库显示模式，提供单选/多选按钮。

#### 步骤 3：策略选择 (Strategy Selection)
- 完全复用现在的策略库显示模式。
- **分组展示**：
  1. **日频策略**
  2. **日内高频策略**（单独列出）
- **高频策略限制**：虽然 RDAgent 和 Qlib 底层已支持日内高抛策略，但因回测数据文件尚未就绪，UI 层面开放此区域但**暂时将日内高频策略的选择框置灰（Disabled）**，不可选中。

#### 步骤 4：组合配置与 AI 评估 (Portfolio Assembly & Evaluation)
- **展示已选项**：清单式列出选中的因子、模型、策略。
- **“AI 评估组合”按钮**：针对生成的组合进行分析，调用 LLM 给出投资组合合理性分析报告。
- **任务分流选择**：提供 **“作为独立任务执行”** 或 **“启动 QE 自动演进”** 选项。

#### 步骤 5：任务下发设置 (Task Dispatching)

##### 分支 A：独立任务 (Independent Task)
- **操作**：单次生成 Qlib 配置并执行，通过 API 传输到 WSL 环境。此流程会参考现有独立任务逻辑，从本地文件系统提取因子/模型源代码。

##### 分支 B：QE 演进任务 (QE Evolution Task)
- **保留旧功能**：提供选项允许用户从已完成的现有实验开始延续演进。
- **参数设置**：若从零组装演进，需确认演进目标（AI填充或人工输入）及 Loop 数量。提交后交由 AIstock 调度。

---

## 3. 后端 API 与底层逻辑支持补齐

1. **源码提取与执行组装**：
   - 依据现有架构，独立任务已具备源代码提取能力（数据库及文件系统均有记录）。新功能将复用该机制，直接从文件系统中读取因子和模型的 Python 源代码，下发至 WSL 作为独立实验或演进 Task 0。
2. **AI 智能选择接口 (`POST /api/experiment/smart-select`)**：
   - 解析用户输入，匹配因子、模型 ID，返回给前端打勾。
3. **前端样式统一框架 (Tailwind)**：
   - 前端已全面使用 TailwindCSS，通过排查无需引入新的 CSS 框架。将通过复用 `@/components/ui/card` 和抽取共用的 `className` 变量实现风格强对齐。

---

## 4. 针对 `KeyError: 'total_factors'` 的 Bug 分析与修复方案

### 4.1 Bug 根因分析
在最近的 Agent 提示词管理架构重构中，数据库表 `qe_agent_prompts` 集中存储了所有的 LLM 提示词模板。
报错发生在 `portfolio_architect.py:1008`：
```python
user_prompt = prompt_data["user_prompt_template"].format(
    user_requirement=user_requirement,
    max_factors=max_factors,
    factor_summary=factor_summary,
    available_factors=available_factors,
    model_summary=model_summary
)
```
**原因**：数据库中配置的 `user_prompt_template` 包含了占位符 `{total_factors}`（以及可能还有其他变量），但 Python 代码在调用原生的 `.format()` 时，并未传入 `total_factors=xxx` 的命名参数。原生 `.format()` 是严格匹配的，一旦遇到未提供的占位符就会直接抛出 `KeyError`。

### 4.2 修复方案

#### A. 针对智能组合 (`portfolio_architect`) 的直接修复
修改 `PortfolioArchitect._generate_with_llm` 方法，在 `.format()` 参数中补齐所有缺失的变量：
```python
user_prompt = prompt_data["user_prompt_template"].format(
    user_requirement=user_requirement,
    max_factors=max_factors,
    total_factors=len(factor_metadata["factors"]), # 补齐此参数
    factor_summary=factor_summary,
    available_factors=available_factors,
    model_summary=model_summary
)
```

#### B. 针对所有 Agent 的架构级防御修复（推荐）
为了防止未来在后台（数据库）修改提示词模板时，因为增加了一个占位符导致整个系统后端崩溃，我们应该在所有 Agent 解析提示词时引入 **Safe Format（安全格式化）** 机制。

在后端的提示词管理基类中，或直接在 Agent 调用时使用自定义的字典扩展：
```python
class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}' # 若缺少参数，原样保留占位符而不报错

# 使用方法：
mapping = SafeDict(
    user_requirement=user_requirement,
    max_factors=max_factors,
    total_factors=len(factor_metadata["factors"]),
    factor_summary=factor_summary,
    available_factors=available_factors,
    model_summary=model_summary
)
user_prompt = prompt_data["user_prompt_template"].format_map(mapping)
```
**全局修复执行计划**：
1. 先在 `portfolio_architect.py` 中传入 `total_factors` 和相关的缺失变量，确保当下的智能组合功能立即恢复。
2. 封装一个通用的 `safe_format(template_str, **kwargs)` 函数。
3. 批量替换 `factor_analyst.py`, `model_analyst.py`, `qe_evolution_agents.py` 等文件中所有的 `.format(...)` 调用为 `safe_format(...)`，彻底根除后续因提示词占位符不匹配导致的 500 崩溃错误。
