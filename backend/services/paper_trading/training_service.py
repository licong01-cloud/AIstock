"""模型重训练服务 — 配置生成 + WSL 执行 + SSE 日志流."""
from __future__ import annotations

import json
import logging
import os
import queue
import re
import subprocess
import threading
import uuid
from datetime import date, datetime
from typing import Any, Dict, Generator, List, Optional

import yaml

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.paper_trading.training")

_TRAINING_LOCK = threading.Lock()
_CURRENT_TRAINING: Optional[Dict[str, Any]] = None
_LOG_QUEUES: Dict[str, queue.Queue] = {}  # job_id -> Queue


class TrainingService:

    @staticmethod
    def start_training(job_params: Dict[str, Any]) -> str:
        """启动训练任务（同时仅允许一个）."""
        global _CURRENT_TRAINING

        if not _TRAINING_LOCK.acquire(blocking=False):
            raise RuntimeError(f"当前已有训练任务运行中: {_CURRENT_TRAINING['job_id'] if _CURRENT_TRAINING else 'unknown'}")

        job_id = str(uuid.uuid4())[:12]
        try:
            # 从源组合读取配置
            source_config = TrainingService._load_source_config(job_params)

            # 生成训练配置
            retrain_config = TrainingService.generate_retrain_config(
                source_config,
                job_params["train_start"],
                job_params["train_end"],
                job_params["valid_start"],
                job_params["valid_end"],
                n_epochs=job_params.get("n_epochs"),
                batch_size=job_params.get("batch_size"),
                lr=job_params.get("lr"),
                early_stop=job_params.get("early_stop"),
            )

            # 记录到 DB
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO paper_trading.training_jobs (
                            job_id, signal_source, signal_source_id, signal_loop_id,
                            train_start, train_end, valid_start, valid_end,
                            n_epochs, batch_size, lr, early_stop,
                            source_config_path, status, started_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'running',NOW())
                        """,
                        (
                            job_id,
                            job_params["signal_source"],
                            job_params["signal_source_id"],
                            job_params.get("signal_loop_id"),
                            job_params["train_start"],
                            job_params["train_end"],
                            job_params["valid_start"],
                            job_params["valid_end"],
                            job_params.get("n_epochs"),
                            job_params.get("batch_size"),
                            job_params.get("lr"),
                            job_params.get("early_stop"),
                            job_params.get("source_config_path"),
                        ),
                    )
                    conn.commit()

            # 创建日志队列
            log_q: queue.Queue = queue.Queue(maxsize=10000)
            _LOG_QUEUES[job_id] = log_q

            _CURRENT_TRAINING = {
                "job_id": job_id,
                "config": retrain_config,
                "params": job_params,
                "started_at": datetime.now().isoformat(),
            }

            # 启动后台线程
            t = threading.Thread(
                target=TrainingService._run_training,
                args=(job_id, retrain_config, job_params, log_q),
                daemon=True,
                name=f"training-{job_id}",
            )
            t.start()

            return job_id

        except Exception:
            _TRAINING_LOCK.release()
            raise

    @staticmethod
    def generate_retrain_config(
        source_config: str,
        train_start: str,
        train_end: str,
        valid_start: str,
        valid_end: str,
        n_epochs: Optional[int] = None,
        batch_size: Optional[int] = None,
        lr: Optional[float] = None,
        early_stop: Optional[int] = None,
    ) -> str:
        """从 SOTA 配置动态生成训练专用配置."""
        config = yaml.safe_load(source_config)

        # 更新数据范围
        if "data_handler_config" in config:
            dhc = config["data_handler_config"]
            dhc["start_time"] = train_start
            dhc["end_time"] = valid_end
            if "infer_processors" in dhc and dhc["infer_processors"]:
                proc = dhc["infer_processors"][0]
                if "kwargs" in proc:
                    proc["kwargs"]["fit_start_time"] = train_start
                    proc["kwargs"]["fit_end_time"] = train_end

        # 更新训练/验证段
        if "task" in config and "dataset" in config["task"]:
            config["task"]["dataset"]["kwargs"]["segments"] = {
                "train": [train_start, train_end],
                "valid": [valid_start, valid_end],
            }

        # 移除回测（只训练）
        config.pop("port_analysis_config", None)
        if "task" in config and "record" in config["task"]:
            config["task"]["record"] = [
                r for r in config["task"]["record"]
                if r.get("class") != "PortAnaRecord"
            ]

        # 覆盖高级参数
        if "task" in config and "model" in config["task"]:
            model_kwargs = config["task"]["model"].get("kwargs", {})
            if n_epochs is not None:
                model_kwargs["n_epochs"] = n_epochs
            if batch_size is not None:
                model_kwargs["batch_size"] = batch_size
            if lr is not None:
                model_kwargs["lr"] = lr
            if early_stop is not None:
                model_kwargs["early_stop"] = early_stop
            config["task"]["model"]["kwargs"] = model_kwargs

        return yaml.dump(config, default_flow_style=False)

    @staticmethod
    def get_training_status() -> Optional[Dict[str, Any]]:
        if _CURRENT_TRAINING is None:
            return None
        job_id = _CURRENT_TRAINING["job_id"]
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM paper_trading.training_jobs WHERE job_id = %s",
                    (job_id,),
                )
                cols = [d[0] for d in cur.description]
                row = cur.fetchone()
        if row:
            return dict(zip(cols, row))
        return _CURRENT_TRAINING

    @staticmethod
    def cancel_training(job_id: str) -> bool:
        global _CURRENT_TRAINING
        if _CURRENT_TRAINING and _CURRENT_TRAINING["job_id"] == job_id:
            # 标记取消
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE paper_trading.training_jobs SET status = 'cancelled', completed_at = NOW() WHERE job_id = %s",
                        (job_id,),
                    )
                    conn.commit()
            _CURRENT_TRAINING = None
            try:
                _TRAINING_LOCK.release()
            except RuntimeError:
                pass
            return True
        return False

    @staticmethod
    def get_training_history() -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM paper_trading.training_jobs ORDER BY created_at DESC LIMIT 50"
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]

    @staticmethod
    def stream_logs(job_id: str) -> Generator[str, None, None]:
        """SSE 日志流."""
        log_q = _LOG_QUEUES.get(job_id)
        if log_q is None:
            yield _sse_format("error", "训练任务不存在或已结束")
            return

        yield _sse_format("status", "connected")

        while True:
            try:
                msg = log_q.get(timeout=2.0)
                if msg == "__DONE__":
                    yield _sse_format("done", "训练完成")
                    break
                if msg == "__ERROR__":
                    yield _sse_format("error", "训练失败")
                    break
                yield _sse_format("log", msg)
            except queue.Empty:
                yield _sse_format("heartbeat", "")
                # 检查任务是否还在运行
                if _CURRENT_TRAINING is None or _CURRENT_TRAINING.get("job_id") != job_id:
                    yield _sse_format("done", "训练结束")
                    break

    # ── 内部方法 ──

    @staticmethod
    def _run_training(job_id: str, config_yaml: str, params: Dict, log_q: queue.Queue) -> None:
        """后台线程：执行 WSL 训练."""
        global _CURRENT_TRAINING
        try:
            # 写入临时配置文件
            workspace = params.get("workspace_path", "/tmp/paper_training")
            wsl_workspace = workspace.replace("F:", "/mnt/f").replace("\\", "/")

            # 通过 WSL 写配置
            config_path = f"{wsl_workspace}/retrain_config_{job_id}.yaml"
            mkdir_cmd = f'wsl bash -c "mkdir -p {wsl_workspace}"'
            subprocess.run(mkdir_cmd, shell=True, check=False, capture_output=True)

            write_cmd = f'wsl bash -c "cat > {config_path}"'
            subprocess.run(write_cmd, shell=True, input=config_yaml.encode(), check=False, capture_output=True)

            # 执行训练
            train_cmd = (
                f'wsl bash -c "'
                f'source ~/miniconda3/etc/profile.d/conda.sh && '
                f'conda activate rdagent-gpu && '
                f'cd {wsl_workspace} && '
                f'python -m qlib.workflow.cli {config_path} 2>&1'
                f'"'
            )

            log_q.put(f"[{datetime.now().strftime('%H:%M:%S')}] 开始训练 job_id={job_id}")
            log_q.put(f"[{datetime.now().strftime('%H:%M:%S')}] 配置文件: {config_path}")

            process = subprocess.Popen(
                train_cmd, shell=True,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1,
            )

            best_epoch = None
            best_valid_loss = None

            for line in iter(process.stdout.readline, ""):
                line = line.rstrip()
                if not line:
                    continue
                log_q.put(f"[{datetime.now().strftime('%H:%M:%S')}] {line}")

                # 解析训练指标
                # PyTorch: "Epoch5: train 0.987, valid 0.991"
                m = re.search(r"Epoch\s*(\d+).*?valid\s+([\d.]+)", line)
                if m:
                    epoch = int(m.group(1))
                    vloss = float(m.group(2))
                    if best_valid_loss is None or vloss < best_valid_loss:
                        best_epoch = epoch
                        best_valid_loss = vloss

                # LightGBM: "[100] train's l2: 0.9890 valid's l2: 0.9977"
                m2 = re.search(r"\[(\d+)\].*?valid's\s+\w+:\s+([\d.]+)", line)
                if m2:
                    epoch = int(m2.group(1))
                    vloss = float(m2.group(2))
                    if best_valid_loss is None or vloss < best_valid_loss:
                        best_epoch = epoch
                        best_valid_loss = vloss

            process.wait()
            exit_code = process.returncode

            if exit_code == 0:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE paper_trading.training_jobs
                            SET status = 'completed', completed_at = NOW(),
                                best_epoch = %s, best_valid_loss = %s,
                                workspace_path = %s
                            WHERE job_id = %s
                            """,
                            (best_epoch, best_valid_loss, workspace, job_id),
                        )
                        conn.commit()
                log_q.put(f"[{datetime.now().strftime('%H:%M:%S')}] 训练完成! best_epoch={best_epoch} best_valid_loss={best_valid_loss}")
                log_q.put("__DONE__")
            else:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE paper_trading.training_jobs SET status = 'failed', completed_at = NOW(), error_message = %s WHERE job_id = %s",
                            (f"exit_code={exit_code}", job_id),
                        )
                        conn.commit()
                log_q.put(f"[{datetime.now().strftime('%H:%M:%S')}] 训练失败 exit_code={exit_code}")
                log_q.put("__ERROR__")

        except Exception as e:
            logger.error("训练异常 job=%s: %s", job_id, e, exc_info=True)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE paper_trading.training_jobs SET status = 'failed', completed_at = NOW(), error_message = %s WHERE job_id = %s",
                        (str(e), job_id),
                    )
                    conn.commit()
            try:
                log_q.put(f"[ERROR] {e}")
                log_q.put("__ERROR__")
            except Exception:
                pass
        finally:
            _CURRENT_TRAINING = None
            try:
                _TRAINING_LOCK.release()
            except RuntimeError:
                pass
            # 延迟清理日志队列（给 SSE 消费端时间读取）
            def _cleanup():
                import time
                time.sleep(60)
                _LOG_QUEUES.pop(job_id, None)
            threading.Thread(target=_cleanup, daemon=True).start()

    @staticmethod
    def _load_source_config(params: Dict[str, Any]) -> str:
        """从源组合读取 conf_sota_factors_model.yaml."""
        config_path = params.get("source_config_path")
        if config_path and os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                return f.read()

        # 尝试从模型目录读取
        source = params.get("signal_source")
        source_id = params.get("signal_source_id")
        loop_id = params.get("signal_loop_id")

        if source == "rdagent_task" and source_id:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT workspace_path FROM aistock_model_catalog WHERE task_run_id = %s AND loop_id = %s LIMIT 1",
                        (source_id, loop_id),
                    )
                    row = cur.fetchone()
            if row and row[0]:
                ws_path = row[0]
                # 尝试多个配置文件位置
                for candidate in [
                    os.path.join(ws_path, "conf_sota_factors_model.yaml"),
                    os.path.join(ws_path, "conf.yaml"),
                ]:
                    if os.path.exists(candidate):
                        with open(candidate, "r", encoding="utf-8") as f:
                            return f.read()

        raise FileNotFoundError(f"无法找到源配置文件: source={source} id={source_id}")


def _sse_format(event: str, data: str) -> str:
    """格式化 SSE 消息."""
    data = (data or "").replace("\r", "")
    lines = data.split("\n")
    out = [f"event: {event}"]
    for ln in lines:
        out.append(f"data: {ln}")
    out.append("")
    return "\n".join(out) + "\n"
