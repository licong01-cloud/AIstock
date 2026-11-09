# QE WSL 控制API与作业内存隔离

BUG-2208：原9000 API及其训练子进程共用`qe-execution.slice`的软限。
训练触发提前回收时控制API也被节流；原服务`OOMPolicy=stop`还有连带退出风险。

源码通过统一QE提交协调器对WSL新增作业包裹运行环境，远端CPU提交保持原样。
原科学命令逐字节保存并校验SHA，因子、模型、预测、窗口、策略和费用不变。
每项新作业进入用户systemd的独立scope，合计40GiB内存、8GiB交换；API在系统
`qe-control.slice`独立使用2GiB内存、1GiB交换。两组软限为infinity。
不是取消WSL全部内存保护，也不修改Docker、数据库、数据集或旧冻结资产。

## 内存准入与并行

保留既有GPU最多2、CPU最多4、纯回测最多4及跨来源reservation规则。
节点内存锁同时约束所有WSL来源；完整GPU拟合暂按24GiB峰值、CPU拟合10GiB、
纯回放6GiB预算。峰值是保守运行预算，不是已证明每个模型峰值；超出单作业硬限
会形成明确执行失败，不能重训冒充原资产。
40GiB预算下两个24GiB作业不能同时进入scope；允许有预算的混合作业。
`backtest_only`会加载模型、数据集并重新推理，不能据此认定为6GiB纯回放。
只有明确的`prediction_replay`或`--pred-backtest`消费采用6GiB预算；
其他模型推理按已知CPU类别或未知类别的24GiB保守预算准入。
同时扣除尚未实际分配的内存预留，并要求WSL至少保留4GiB可用余量，避免只看资源组限额。
等内存期间写`qe_wsl_workload_receipt.json`的WAITING_MEMORY状态，不标记节点offline。
进程启动时间和scope状态共同验证租约；包装进程退出但scope仍活跃时不释放预算。
已有launch intent禁止再次执行原命令。所有进程控制仍沿原提交身份核对，不新增竞争观察器。
进入scope使用原QE会话首进程exec；模型启动前必须核对PID、PGID、SID和启动时间均未改变，
从而保留现有精确进程身份及取消语义。终端结果沿用正式QE回执，下一项准入核验并释放已结束租约。

## 部署顺序

1. 当前实验继续；禁止迁移现有PID，迁移不能可靠转移既有页面计账。
2. 运行`python -m backend.services.quantevolver.qe_wsl_resource_isolation`只打印完整部署计划。
3. 待旧作业全部完成、入仓并核对无活动进程后，由用户停止WSL9000 API。
4. 核对WSL `/etc/systemd/system/rdagent-qe-api.service` SHA256。
   在WSL以root运行本脚本`--apply --expected-api-unit-sha256 <实际SHA>`。
   它要求API inactive/MainPID0，逐一检查冲突并只新增三项精确unit文件；不覆盖旧文件。
5. 用户执行系统及lc999用户systemd manager的daemon-reload并启动原9000 API。
   应核对MainPID的新cgroup为`/qe.slice/qe-control.slice/rdagent-qe-api.service`，
   用户`qe-training.slice`为40GiB/8GiB且MemoryHigh=infinity。
6. 再由用户重启本机8001加载新提交包装。源码合入、unit安装和实际加载分别报告。
7. 先用无训练的微型scope验证cgroup、限额、stdout/退出码及终止身份；再接续原任务。
   新作业必须生成原命令SHA、scope、内存等待及终端回执。9000健康200不能替代这些验收。
   `qe_wsl_workload_started.json`必须证明模型启动前的真实cgroup身份、内核单作业限额和父组限额。

安装脚本不执行daemon-reload、启动/停止/重启、删除、kill、Docker或数据库操作。
旧`qe-execution.slice`文件保留；新API不再属于它，旧实验身份不改。
当前进程尚未排空时只能报告源码/准备完成，不能宣称生产隔离已生效。
