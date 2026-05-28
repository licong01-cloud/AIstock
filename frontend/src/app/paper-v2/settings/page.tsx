"use client";

import Link from "next/link";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import SectionCard from "@/components/paper-v2/SectionCard";

export default function PaperV2SettingsPage() {
  return (
    <main>
      <NoticePanel title="模拟盘 v2 护栏" tone="info">
        本页说明当前启用的 v2 运行边界。这里不会暴露原始执行覆盖、日频兜底、QMT、Shadow 或实盘交易控制。
      </NoticePanel>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="支持流程" eyebrow="权威入口">
          <ol>
            <li>从 QE 生成或批准 StrategyPackage。</li>
            <li>资产合格策略包可直接进入选股中心和模拟盘 v2。</li>
            <li>执行单策略包或多策略包选股；只有单策略包选股可以创建模拟盘 v2 实例。</li>
            <li>创建模拟盘实例并冻结策略包、manifest hash、资金、数据源和执行策略快照。</li>
            <li>先执行就绪检查，再执行单日运行或历史回放。</li>
            <li>复核订单、成交、现金流水、持仓、快照、事件、错误和绩效。</li>
          </ol>
        </SectionCard>

        <SectionCard title="不支持边界" eyebrow="Fail-fast">
          <ul>
            <li>不提供日频模拟盘兜底。</li>
            <li>不提供模拟盘独有的原始分钟算法配置。</li>
            <li>在组合策略包或 SelectionBundle 合约出现前，不支持多策略包聚合选股直接创建执行模拟盘实例。</li>
            <li>V25_TWO_STAGE 可作为已验证分钟执行策略接入；本 UI 不提供 QMT、Shadow 或实盘交易控制。</li>
            <li>缺数据、运行时产物、pre_close、涨跌停、交易日历、停牌或分钟线时，禁止返回静默空成功。</li>
          </ul>
        </SectionCard>
      </div>

      <div className="pv2-grid pv2-grid-3">
        <SectionCard title="StrategyPackage API" eyebrow="后端">
          <p><code>/api/v1/strategy-packages</code></p>
          <p>策略包列表、指标、状态流转、模型状态、已验证执行策略和人工模型重训练任务。</p>
          <Link className="pv2-button" href="/paper-v2/packages">打开策略包</Link>
        </SectionCard>

        <SectionCard title="选股中心 API" eyebrow="后端">
          <p><code>/api/v1/selection-center</code></p>
          <p>可选策略包、单策略包选股、动态并集/交集/加权融合、剔除结果追踪和单策略包模拟盘创建。</p>
          <Link className="pv2-button" href="/paper-v2/selection">打开选股中心</Link>
        </SectionCard>

        <SectionCard title="模拟盘 v2 API" eyebrow="后端">
          <p><code>/api/v1/paper-v2</code></p>
          <p>模拟盘生命周期、就绪检查、单日运行、回放/重置、执行策略激活、账本产物、错误和绩效报告。</p>
          <Link className="pv2-button" href="/paper-v2/portfolios">打开模拟盘中心</Link>
        </SectionCard>
      </div>
    </main>
  );
}
