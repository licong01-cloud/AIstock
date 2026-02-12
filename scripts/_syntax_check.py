import py_compile, sys
try:
    py_compile.compile(r"f:\Dev\AIstock\backend\routers\rdagent_catalog_admin.py", doraise=True)
    print("Backend syntax OK")
except py_compile.PyCompileError as e:
    print(f"Backend syntax ERROR: {e}")
    sys.exit(1)
