"""Kill the running backend and restart it using the project .env bind settings."""

from pathlib import Path
import os
import subprocess
import time

from dotenv import load_dotenv

backend_dir = Path(__file__).resolve().parents[1]
load_dotenv(backend_dir / ".env", override=True)

backend_host = os.environ["NEXT_APP_HOST"]
backend_port = os.environ["NEXT_APP_PORT"]

# Find the backend process by configured port.
result2 = subprocess.run(
    ['netstat', '-ano'],
    capture_output=True, text=True
)
for line in result2.stdout.strip().split('\n'):
    if f':{backend_port}' in line and 'LISTEN' in line:
        print(f"Port {backend_port} listener: {line.strip()}")
        pid = line.strip().split()[-1]
        print(f"PID: {pid}")
        # Kill it
        print(f"Killing PID {pid}...")
        subprocess.run(['taskkill', '/F', '/PID', pid], capture_output=True, text=True)
        time.sleep(2)
        break
else:
    print(f"No process listening on port {backend_port}")

# Restart using conda.
print("\nStarting backend with conda...")
cmd = (
    "conda activate AIstock && "
    f"uvicorn backend.main:app --host {backend_host} --port {backend_port}"
)
proc = subprocess.Popen(
    ['cmd', '/c', cmd],
    cwd=str(backend_dir),
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    encoding='utf-8',
    errors='replace',
)

# Wait a few seconds and check
time.sleep(8)
if proc.poll() is None:
    print(f"Backend started successfully, PID={proc.pid}")
else:
    print(f"Backend failed to start, return code={proc.returncode}")
    # Read output
    out = proc.stdout.read()
    print(out[:2000])
