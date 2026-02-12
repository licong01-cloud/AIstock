"""Kill the running backend and restart it using conda."""
import subprocess, time, sys, os

# Find the backend process by port 8001
result2 = subprocess.run(
    ['netstat', '-ano'],
    capture_output=True, text=True
)
for line in result2.stdout.strip().split('\n'):
    if ':8001' in line and 'LISTEN' in line:
        print(f"Port 8001 listener: {line.strip()}")
        pid = line.strip().split()[-1]
        print(f"PID: {pid}")
        # Kill it
        print(f"Killing PID {pid}...")
        subprocess.run(['taskkill', '/F', '/PID', pid], capture_output=True, text=True)
        time.sleep(2)
        break
else:
    print("No process listening on port 8001")

# Restart using conda
backend_dir = r"f:\Dev\AIstock"
print("\nStarting backend with conda...")
cmd = 'conda activate AIstock && uvicorn backend.main:app --host 127.0.0.1 --port 8001'
proc = subprocess.Popen(
    ['cmd', '/c', cmd],
    cwd=backend_dir,
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
