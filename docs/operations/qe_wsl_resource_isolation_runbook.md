# QE WSL 控制API与作业内存隔离

BUG-2208：原9000 API及其训练子进程共用`qe-execution.slice`的软限。
训练触发提前回收时控制API也被节流；原服务`OOMPolicy=stop`还有连带退出风险。

源码通过统一QE提交协调器对WSL新增作业包裹运行环境，远端CPU提交保持原样。
原科学命令逐字节保存并校验SHA，因子、模型、预测、窗口、策略和费用不变。
每项新作业进入用户systemd的独立scope，v2合计56GiB内存、32GiB交换；API在系统
`qe-control.slice`及其中的9000服务各配置4 GiB内存上限和4 GiB交换上限，两层共享同一份额度。两组软限为infinity。
不是取消WSL全部内存保护，也不修改Docker、数据库、数据集或旧冻结资产。

## 内存准入与并行

保留既有GPU最多2、CPU最多4、纯回测最多4及跨来源reservation规则。
节点内存锁同时约束所有WSL来源；完整GPU拟合暂按32GiB、CPU拟合20GiB、
纯回放20GiB预算。它们是保守运行预算，不是已证明每个模型峰值；超出单作业硬限
会形成明确执行失败，不能重训冒充原资产。
BUG-2217修复原40GiB固定预留使GPU2变串行、回放4变2的问题。v2只将QE获准使用的
32GiB交换计入容量，不能把WSL全部256GiB交换视作可用物理内存。内存与交换硬限分别
由内核执行；每作业内存上限仍为32/20GiB，交换上限16GiB。准入同时约束GPU最多2、
全部scope最多4、总预留不超过56+32GiB。两个GPU或四个纯回放可在资源充足时准入，
不改变远端CPU4上限、模型亲和性或已有跨来源reservation。
`backtest_only`会加载模型、数据集并重新推理，不能据此认定为纯回放。
只有明确的`prediction_replay`或`--pred-backtest`消费采用20GiB预算；
其他模型推理按已知CPU类别或未知类别的32GiB保守预算准入。
旧6GiB纯回放预算已在完整565日账户中触发scope OOM，不得再次使用。
同时扣除活跃租约尚未实际分配的预留，只使用训练组剩余交换额度及全局剩余交换的较小值。
无论交换还有多少，WSL可用物理内存仍须至少4GiB；宿主至少具备62GiB内存及36GiB交换。
memory或I/O的full avg10分别达到10%/20%时暂缓新准入，不终止已运行作业。
缺失/非数值压力指标、未知租约或未批准profile均明确报错，不回落安全默认值。
等待写`qe_wsl_workload_receipt.json`的WAITING_MEMORY、具体reason与资源snapshot；
不标记节点offline，也不能把两个running回执当成两个模型已经训练。
进程启动时间和scope状态共同验证租约；包装进程退出但scope仍活跃时不释放预算。
已有launch intent禁止再次执行原命令。所有进程控制仍沿原提交身份核对，不新增竞争观察器。
进入scope使用原QE会话首进程exec；模型启动前必须核对PID、PGID、SID和启动时间均未改变，
从而保留现有精确进程身份及取消语义。终端结果沿用正式QE回执，下一项准入核验并释放已结束租约。

## 已完成API隔离的增量升级

1. 保持当前实验运行。新v2代码不会重写已有v1封套；等旧scope及等待中的guard全部结束，
   完成验收并核对无活动进程。不能改旧租约或终止旧训练来制造准入。
2. 读取`/etc/systemd/user/qe-training.slice`实际SHA256，在WSL以root运行：
   `python -m backend.services.quantevolver.qe_wsl_resource_isolation --upgrade-training-profile --apply --expected-training-unit-sha256 <SHA>`。
   脚本只接受准确旧版或新版训练unit，检查活动scope与尚未进入scope的guard，原子替换
   唯一训练unit并返回readback；不改9000 API/control unit，不启动或停止服务。
3. 操作者在lc999用户manager执行`systemctl --user daemon-reload`，再执行
   `systemctl --user set-property --runtime qe-training.slice MemoryHigh=infinity MemoryMax=56G MemorySwapMax=32G`。
   读取实际cgroup三项值，同时单独核对9000控制组及服务的生效限额。
4. 用户重启本机8001加载新封套生成代码；本次增量升级不需要重启WSL9000或WSL。
   在此之前不提交会被旧常驻代码包装的新实验。源码合入、unit文件、实时限额与API加载分开报告。
5. fresh-process Composer检查v2、原科学命令SHA及GPU2/回放4容量；沿下一批既有授权的
   实际任务验证scope重叠、峰值、交换I/O、压力和完成吞吐。没有两任务实证时只报准入测试通过，
   不宣称真实两并行已验证；不为性能证明重复训练、推理或回放。

该profile不修改`.wslconfig`、全局swap、swappiness、Docker或数据库容器。swap仅承接
冷页及有限峰值，持续换页导致吞吐下降时应排查数据加载/阶段峰值，不无限增加swap限额。

## 已隔离的9000在线升级

BUG-2271 的故障记录显示，控制组用满 2 GiB 内存及 1 GiB 交换后被内核 OOM 杀进程；独立训练仍产生了完整预测。控制组的新上限为 4 GiB 内存及 4 GiB 交换。

1. 同步源码后，提交方会在包装下一项 WSL 作业时从磁盘读取新版 guard。它识别完整的旧版 2/1 或新版 4/4 配置。核对已冻结且尚等候启动的旧版封套；如存在，先让它按原合同进入训练 scope。已在训练 scope 中的作业及远端回放可继续。
2. 在 WSL 以 root 用系统 Python 执行已审核源码 `qe_wsl_resource_isolation.py --upgrade-control-profile --apply`。它先核对两个既有 unit 的完整内容，再逐一原子更新，可在两次写入之间中断后重入。记录返回的文件 SHA。
3. 执行 `systemctl daemon-reload`。分别读取 `qe-control.slice` 与 `rdagent-qe-api.service` 的 ControlGroup，并在两者实际 cgroup 核对 `memory.high=max`、`memory.max=4294967296`、`memory.swap.max=4294967296`。若有其他 drop-in 覆盖，以实际读回值报告并查明来源。核对 API 原 MainPID、9000 健康响应、原任务状态及训练组 56/32。后续沿既有实验观察控制组的 `memory.events`、峰值和内存压力。

## 首次部署API隔离

1. 当前实验继续；禁止迁移现有PID，迁移不能可靠转移既有页面计账。
2. 运行`python -m backend.services.quantevolver.qe_wsl_resource_isolation`只打印完整部署计划。
3. 待旧作业全部完成、入仓并核对无活动进程后，由用户停止WSL9000 API。
4. 核对WSL `/etc/systemd/system/rdagent-qe-api.service` SHA256。
   在WSL以root运行本脚本`--apply --expected-api-unit-sha256 <实际SHA>`。
   它要求API inactive/MainPID0，逐一检查冲突并只新增三项精确unit文件；不覆盖旧文件。
5. 用户执行系统及lc999用户systemd manager的daemon-reload并启动原9000 API。
   应核对MainPID的新cgroup为`/qe.slice/qe-control.slice/rdagent-qe-api.service`，
   用户`qe-training.slice`为56GiB/32GiB且MemoryHigh=infinity。
6. 再由用户重启本机8001加载新提交包装。源码合入、unit安装和实际加载分别报告。
7. 先用无训练的微型scope验证cgroup、限额、stdout/退出码及终止身份；再接续原任务。
   新作业必须生成原命令SHA、scope、内存等待及终端回执。9000健康200不能替代这些验收。
   `qe_wsl_workload_started.json`必须证明模型启动前的真实cgroup身份、内核单作业限额和父组限额。

安装脚本不执行daemon-reload、启动/停止/重启、删除、kill、Docker或数据库操作。
旧`qe-execution.slice`文件保留；新API不再属于它，旧实验身份不改。
当前进程尚未排空时只能报告源码/准备完成，不能宣称生产隔离已生效。
