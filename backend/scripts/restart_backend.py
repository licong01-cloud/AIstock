import os
import sys
import subprocess
import time

def start_backend():
    # 获取 backend 目录路径
    backend_dir = r"f:\Dev\AIstock\backend"
    
    # 构建命令，使用 python -m 启动以处理相对导入
    # 假设 main.py 在 backend 目录下，且 backend 是一个包
    cmd = [sys.executable, "-m", "main"]
    
    print(f"尝试启动后端服务: {' '.join(cmd)}")
    print(f"工作目录: {backend_dir}")
    
    # 设置环境变量，确保 python 能找到包
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.dirname(backend_dir)
    
    try:
        # 使用 Popen 启动后台进程
        process = subprocess.Popen(
            cmd,
            cwd=backend_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        
        # 等待几秒检查是否立即崩溃
        time.sleep(5)
        
        if process.poll() is not None:
            output = process.stdout.read()
            print(f"后端启动失败，输出信息:\n{output}")
            return False
        
        print("后端服务似乎已成功启动。")
        return True
        
    except Exception as e:
        print(f"启动过程中发生错误: {e}")
        return False

if __name__ == "__main__":
    start_backend()
