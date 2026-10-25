"""Exact, consumer-session feature reuse; arbitrary code sees only legal input."""

from __future__ import annotations

import ast
from collections import OrderedDict
from collections.abc import Callable
from pathlib import Path
import hashlib
import inspect
import json
import platform
from importlib.metadata import PackageNotFoundError, version
from threading import RLock
from typing import Any

import numpy as np
import pandas as pd

from .inference_inputs import frame_identity


def _self_contained_factor(source: bytes, data_files: Any) -> bool:
    """Optional reuse only; unbound imports/IO/time/RNG run the original code."""
    tree = ast.parse(source)
    files = set(data_files) if isinstance(data_files, (tuple, list)) else set()
    allowed = {"numpy", "pandas", "math", "pathlib", "scipy", "__future__"}
    volatile = {"random", "seed", "RandomState", "default_rng", "rvs", "now", "today", "utcnow",
                "environ", "getenv", "cwd", "getcwd", "home", "resolve",
                "load", "loadtxt", "genfromtxt", "fromfile", "HDFStore"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import) and any(alias.name.split(".")[0] not in allowed
                                               or any(part in volatile for part in alias.name.split("."))
                                               for alias in node.names):
            return False
        if isinstance(node, ast.ImportFrom) and (node.level or (node.module or "").split(".")[0] not in allowed
                                                or any(part in volatile for part in (node.module or "").split("."))
                                                or any(alias.name == "*" or alias.name in volatile for alias in node.names)):
            return False
        if isinstance(node, ast.Name) and node.id in {"__file__", "__package__", "__path__", "__loader__", "__spec__", "__builtins__"}:
            return False
        if isinstance(node, ast.Attribute) and (node.attr in volatile or node.attr.startswith("__")):
            return False
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {
            "open", "eval", "exec", "getattr", "setattr", "globals", "locals", "__import__"}:
            return False
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr.startswith("read"):
            if node.func.attr not in {"read_hdf", "read_parquet"} or not node.args:
                return False
            path = node.args[0]
            if not isinstance(path, ast.Constant) or not isinstance(path.value, str):
                return False
            if path.value not in files and not any(path.value == "./" + name for name in files):
                return False
    return True


class InferenceFeatureSession:
    def __init__(self, *, max_bytes: int = 512 * 1024**2, max_entries: int = 64) -> None:
        if max_bytes < 0 or max_entries < 1:
            raise ValueError("invalid feature retention budget")
        self.max_bytes, self.max_entries = max_bytes, max_entries
        self._entries: OrderedDict[str, tuple[pd.DataFrame, int]] = OrderedDict()
        self._lock = RLock()
        self.retained_bytes = self.hits = self.computations = 0
        self.source_scope: tuple[str, str, str] | None = None
        try:
            scipy_version = version("scipy")
        except PackageNotFoundError:
            scipy_version = None
        self.environment = json.dumps({"python": platform.python_version(), "pandas": pd.__version__,
                                       "numpy": np.__version__, "scipy": scipy_version, "code": {
                                           name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                                           for name in ("inference_features.py", "alpha158_batch.py", "inference_inputs.py")}}, sort_keys=True)

    def evaluate(self, *, identity: str, calculate: Callable[[], pd.DataFrame]) -> pd.DataFrame:
        with self._lock:
            key = hashlib.sha256((self.environment + json.dumps(self.source_scope) + identity).encode()).hexdigest()
            if key in self._entries:
                self._entries.move_to_end(key)
                self.hits += 1
                return self._entries[key][0].copy(deep=True)
            result = calculate()
            self.computations += 1
            if not isinstance(result, pd.DataFrame):
                raise TypeError("factor output must be a DataFrame")
            size = int(result.memory_usage(index=True, deep=True).sum())
            if size <= self.max_bytes:
                while self._entries and (self.retained_bytes + size > self.max_bytes
                                         or len(self._entries) >= self.max_entries):
                    _, (_, removed) = self._entries.popitem(last=False)
                    self.retained_bytes -= removed
                self._entries[key] = (result.copy(deep=True), size)
                self.retained_bytes += size
            return result

    def bind_source(self, scope: tuple[str, str, str] | None) -> None:
        """A revised source/consumer/run is not an old cache provenance hit."""
        with self._lock:
            if scope != self.source_scope:
                self._entries.clear()
                self.retained_bytes = 0
                self.source_scope = scope

    def dynamic_identity(self, *, module: Any, function: Callable, input_identity: str) -> str | None:
        """Reuse only an existing generated, explicit source-backed wrapper.

        Unknown/custom wrappers execute normally. Their eligibility does not
        depend on cache support. Full input identity includes origin/population.
        """
        files = getattr(module, "_FACTOR_FILES", None)
        if not isinstance(files, dict):
            return None
        try:
            tree = ast.parse(inspect.getsource(function))
            definition = tree.body[0]
            statements = definition.body
            if len(statements) != 1 or not isinstance(statements[0], ast.Return):
                return None
            call = statements[0].value
            if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name) or call.func.id != "_run_factor":
                return None
            if call.keywords or len(call.args) != 1 or not isinstance(call.args[0], ast.Constant):
                return None
            source = Path(files[call.args[0].value])
            source_bytes = source.read_bytes()
            if not _self_contained_factor(source_bytes, getattr(module, "_DATA_FILES", ())):
                return None
            helpers = "\n".join(inspect.getsource(getattr(module, name))
                                for name in ("_run_factor", "_ensure_h5_aliases", "_link_data_file"))
            constants = {name: value for name, value in vars(module).items()
                         if name.startswith("_") and name.isupper() and name != "_FACTOR_FILES"}
            # Alias/config constants affect what a generated wrapper executes.
            # Non-JSON/custom state simply doesn't support this optional reuse.
            parameters = json.dumps(constants, sort_keys=True, allow_nan=False).encode()
            return hashlib.sha256(source_bytes + helpers.encode() + parameters).hexdigest() + ":" + str(call.args[0].value) + ":" + input_identity
        except (OSError, TypeError, ValueError, KeyError, IndentationError, SyntaxError, AttributeError):
            return None

    def alpha_identity(self, history: pd.DataFrame, day: pd.Timestamp, columns: list[str]) -> str:
        prefix = history.loc[history.index.get_level_values("datetime") <= day]
        # Known formulas need at most 60 observations plus a predecessor. Long
        # missing runs have unbounded pct_change fill state: use full prefix in
        # that case, rather than pretending a finite tail is equivalent.
        tail = prefix.groupby(level="instrument", group_keys=False).tail(61)
        basis = prefix if tail[["close", "volume" if "volume" in tail else "vol"]].isna().any().any() else tail
        # A few original formulas have a positional unique-member convention.
        # Even identical finite tails may come from different first-observed
        # orders after a suspension or moving warm-up origin. Preserve that
        # native ordering too; don't reuse a differently attributed result.
        population_order = prefix.index.get_level_values("instrument").unique().tolist()
        return "alpha:" + str(day) + ":" + json.dumps([columns, population_order]) + ":" + frame_identity(basis)

    def close(self) -> None:
        with self._lock:
            self._entries.clear()
            self.retained_bytes = 0
