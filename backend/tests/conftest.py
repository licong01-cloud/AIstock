"""
Root conftest.py for the backend test suite.

Adds the backend package root to sys.path so tests can import
backend modules without installing the package.
"""
import sys
from pathlib import Path

# backend/ directory
BACKEND_ROOT = Path(__file__).parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))
