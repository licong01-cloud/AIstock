"""Bounded static source/column projection; factor code is never executed."""
import ast
from collections import defaultdict
import inspect


class DependencyUnknown(ValueError):
    pass


FILES = {"daily_pv.h5": "kline_daily_raw", "daily_basic.h5": "daily_basic",
         "bak_basic.h5": "bak_basic", "moneyflow.h5": "moneyflow_ts",
         "cyq_perf.h5": "cyq_perf", "margin_detail.h5": "margin_detail",
         "sector_data.h5": "sector_data", "static_factors.parquet": None}


def literal(node, values):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return values.get(node.id)
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        items = [literal(n, values) for n in node.elts]
        return items if all(x is not None for x in items) else None
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = literal(node.left, values), literal(node.right, values)
        if isinstance(left, str) and isinstance(right, str):
            return left + right
    if isinstance(node, ast.JoinedStr):
        parts = [literal(n.value, values) if isinstance(n, ast.FormattedValue) else literal(n, values) for n in node.values]
        return "".join(map(str, parts)) if all(x is not None for x in parts) else None
    return None


def _output_fields(function):
    """Read field names from the shared producer, including its finite loops."""
    tree = ast.parse(inspect.getsource(function))
    result = set()

    def walk(node, values):
        if isinstance(node, ast.For) and isinstance(node.target, ast.Name):
            items = literal(node.iter, values)
            if isinstance(items, list) and len(items) <= 100:
                for item in items:
                    for child in node.body:
                        walk(child, {**values, node.target.id: item})
                return
        if isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Store) and isinstance(node.value, ast.Name):
            value = literal(node.slice, values)
            if isinstance(value, str) and node.value.id in {"out", "df_price_momentum"}:
                result.add(value)
        for child in ast.iter_child_nodes(node):
            walk(child, values)
    walk(tree, {})
    return result


def field_sources():
    from backend.data_service import qe_data_service as data
    from backend.data_service.moneyflow_contract import MONEYFLOW_FIELD_MAP, derive_moneyflow_factors
    result = {f: frozenset({"kline_daily_raw"}) for f in ("open", "high", "low", "close", "volume", "amount", "vwap")}
    result["factor"] = frozenset({"adj_factor"})
    for dataset, fields in (("daily_basic", data.DAILY_BASIC_FIELD_MAP.values()),
            ("moneyflow_ts", MONEYFLOW_FIELD_MAP.values()), ("bak_basic", data.BAK_BASIC_FIELD_MAP.values()),
            ("cyq_perf", data.CYQ_PERF_FIELD_MAP.values()), ("margin_detail", data.MARGIN_DETAIL_FIELD_MAP.values()),
            ("sector_data", data.SECTOR_DATA_COLUMNS)):
        result.update({f: frozenset({dataset}) for f in fields})
    # Formulas remain in their existing producers. Ratios may also read price/
    # amount; this conservative dependency never changes their units or values.
    result.update({f: frozenset({"moneyflow_ts", "kline_daily_raw"}) for f in _output_fields(derive_moneyflow_factors)})
    result.update({f: frozenset({"daily_basic"}) for f in _output_fields(data.compute_daily_basic_precomputed_factors)})
    result.update({f: frozenset({"kline_daily_raw"}) for f in _output_fields(data.build_static_factors)})
    return result


def factor_fields(source, mapping):
    if len(source.encode("utf-8")) > 512 * 1024:
        raise DependencyUnknown("factor definition exceeds static analysis budget")
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise DependencyUnknown("factor definition is not valid Python syntax") from exc
    nodes = list(ast.walk(tree))
    if len(nodes) > 30000:
        raise DependencyUnknown("factor syntax exceeds static analysis budget")
    values, frames, found, derived = {}, set(), set(), defaultdict(dict)
    ambiguous = set()
    assignments = sorted((n for n in nodes if isinstance(n, ast.Assign)), key=lambda n: n.lineno)
    for node in nodes:
        if isinstance(node, ast.Call):
            name = node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id if isinstance(node.func, ast.Name) else ""
            if (name.startswith("read_") and name not in {"read_hdf", "read_parquet"}) or name in {"open", "load", "loads"}:
                raise DependencyUnknown(f"unsupported source reader at line {node.lineno}")
    for n in assignments:
        value = literal(n.value, values)
        for target in n.targets:
            if isinstance(target, ast.Name):
                if value is None or (target.id in values and values[target.id] != value):
                    ambiguous.add(target.id)
                    values.pop(target.id, None)
                elif target.id not in ambiguous:
                    values[target.id] = value
    # Conventional function input frames remain supported; file-backed frames
    # are discovered by their actual reader assignment, not a variable whitelist.
    for n in nodes:
        if isinstance(n, ast.FunctionDef):
            frames.update(a.arg for a in n.args.args if a.arg in {"data", "df", "df_factor", "df_pv"})
    for n in assignments:
        readers = [c for c in ast.walk(n.value) if isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute)
                   and c.func.attr in {"read_hdf", "read_parquet", "read_csv", "read_pickle"}]
        for call in readers:
            path = call.args[0] if call.args else None
            filename = literal(path, values)
            if isinstance(path, ast.BinOp) and isinstance(path.op, ast.Div):
                filename = literal(path.right, values)
            if filename not in FILES:
                raise DependencyUnknown(f"unresolved factor input file at line {call.lineno}")
            for target in n.targets:
                if isinstance(target, ast.Name):
                    frames.add(target.id)
            for kw in call.keywords:
                if kw.arg == "columns":
                    columns = literal(kw.value, values)
                    if not isinstance(columns, list) or not all(isinstance(x, str) for x in columns):
                        raise DependencyUnknown(f"dynamic input columns at line {call.lineno}")
                    for column in columns:
                        if column not in mapping:
                            raise DependencyUnknown(f"unknown source field {column} at line {call.lineno}")
                        found.add(column)
    # Propagate copies/joins/renames, not a selected column's later time/index
    # operations. A bounded fixed point also covers aliases declared in helpers.
    functions = {n.name: n for n in nodes if isinstance(n, ast.FunctionDef)}
    for _ in range(min(len(assignments)+1, 100)):
        before = len(frames)
        for n in assignments:
            base = n.value
            while isinstance(base, ast.Call) and isinstance(base.func, ast.Attribute):
                base = base.func.value
            if isinstance(base, ast.Name) and base.id in frames:
                frames.update(t.id for t in n.targets if isinstance(t, ast.Name))
            call = n.value
            if isinstance(call, ast.Call) and any(isinstance(arg, ast.Name) and arg.id in frames for arg in ast.walk(call)):
                if ((isinstance(call.func, ast.Attribute) and call.func.attr in {"concat", "merge"}
                     and isinstance(call.func.value, ast.Name) and call.func.value.id in {"pd", "pandas"})
                        or isinstance(call.func, ast.Name) and call.func.id in functions):
                    frames.update(t.id for t in n.targets if isinstance(t, ast.Name))
        for call in (n for n in nodes if isinstance(n, ast.Call)):
            if isinstance(call.func, ast.Name) and call.func.id in functions:
                function = functions[call.func.id]
                for arg, parameter in zip(call.args, function.args.args):
                    if isinstance(arg, ast.Name) and arg.id in frames:
                        frames.add(parameter.arg)
            base = call.func.value if isinstance(call.func, ast.Attribute) else None
            while isinstance(base, ast.Call) and isinstance(base.func, ast.Attribute):
                base = base.func.value
            if isinstance(base, ast.Name) and base.id in frames:
                for arg in call.args:
                    if isinstance(arg, ast.Lambda):
                        frames.update(a.arg for a in arg.args.args)
        if len(frames) == before:
            break
    for n in nodes:
        if (isinstance(n, ast.Subscript) and isinstance(n.ctx, ast.Store)
                and isinstance(n.value, ast.Name) and n.value.id in frames):
            column = literal(n.slice, values)
            if not isinstance(column, str):
                raise DependencyUnknown(f"dynamic derived field at line {n.lineno}")
            derived[n.value.id][column] = min(n.lineno, derived[n.value.id].get(column, n.lineno))

    def field(name, frame, line):
        name = name.lstrip("$")
        if name in derived[frame] and derived[frame][name] < line:
            return
        if name not in mapping:
            raise DependencyUnknown(f"unknown source field {name} at line {line}")
        found.add(name)

    for n in nodes:
        if (isinstance(n, ast.Subscript) and isinstance(n.value, ast.Attribute) and n.value.attr == "loc"
                and isinstance(n.value.value, ast.Name) and n.value.value.id in frames):
            if not isinstance(n.slice, ast.Tuple) or len(n.slice.elts) != 2:
                raise DependencyUnknown(f"dynamic source indexer at line {n.lineno}")
            columns = literal(n.slice.elts[1], values)
            columns = [columns] if isinstance(columns, str) else columns
            if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
                raise DependencyUnknown(f"dynamic source indexer at line {n.lineno}")
            for column in columns:
                field(column, n.value.value.id, n.lineno)
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in {"getattr", "eval", "exec"}
                and any(isinstance(v, ast.Name) and v.id in frames for v in ast.walk(n))):
            raise DependencyUnknown(f"reflected source access at line {n.lineno}")
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id not in functions
                and any(isinstance(arg, ast.Name) and arg.id in frames for arg in n.args)):
            raise DependencyUnknown(f"unresolved function consumes source frame at line {n.lineno}")
        if (isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id in frames
                and n.attr in {"iloc", "values", "to_numpy"}):
            raise DependencyUnknown(f"unresolved whole-frame or indexer access at line {n.lineno}")
        if isinstance(n, ast.Subscript) and isinstance(n.ctx, ast.Load) and isinstance(n.value, ast.Name) and n.value.id in frames:
            columns = literal(n.slice, values)
            if isinstance(columns, str):
                field(columns, n.value.id, n.lineno)
            elif isinstance(columns, list) and all(isinstance(c, str) for c in columns):
                for column in columns:
                    field(column, n.value.id, n.lineno)
            elif (isinstance(n.slice, ast.UnaryOp) and isinstance(n.slice.op, ast.Invert)
                  and isinstance(n.slice.operand, ast.Call) and isinstance(n.slice.operand.func, ast.Attribute)
                  and n.slice.operand.func.attr == "duplicated"
                  and isinstance(n.slice.operand.func.value, ast.Attribute) and n.slice.operand.func.value.attr == "index"):
                pass  # row mask; all column reads inside it are inspected separately
            else:
                raise DependencyUnknown(f"dynamic field selection at line {n.lineno}")
        if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id in frames and n.attr in mapping:
            field(n.attr, n.value.id, n.lineno)
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name)
                and n.func.value.id in frames and n.func.attr in {"get", "filter", "__getitem__"}):
            columns = literal(n.args[0], values) if n.args else None
            if not isinstance(columns, str):
                raise DependencyUnknown(f"dynamic field method at line {n.lineno}")
            field(columns, n.func.value.id, n.lineno)
    if not found:
        raise DependencyUnknown("factor has no deterministically resolved source fields")
    return found


def factor_window(source):
    """Propagate finite rolling/lag requirements through assignments and lambdas."""
    tree = ast.parse(source)
    values, depths = {}, {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.args.defaults:
            for arg, default in zip(node.args.args[-len(node.args.defaults):], node.args.defaults, strict=True):
                value = literal(default, values)
                if type(value) is int:
                    values[arg.arg] = value
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    value = literal(node.value, values)
                    if type(value) is int:
                        values[target.id] = value
    def depth(node, env):
        if isinstance(node, ast.Name):
            return env.get(node.id, 1)
        if isinstance(node, ast.Subscript):
            return env.get(ast.unparse(node), depth(node.value, env))
        base = depth(node.func.value, env) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) else 1
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in {"rolling", "shift", "pct_change", "diff"}:
            key = "window" if node.func.attr == "rolling" else "periods"
            arg = next((k.value for k in node.keywords if k.arg == key), node.args[0] if node.args else ast.Constant(1))
            value = literal(arg, values)
            if type(value) is not int or value < 0 or value > 10000:
                raise DependencyUnknown(f"dynamic or future factor window at line {node.lineno}")
            return base + max(0, value - (node.func.attr == "rolling"))
        if isinstance(node, ast.Call):
            children = []
            for arg in [*node.args, *(k.value for k in node.keywords)]:
                if isinstance(arg, ast.Lambda):
                    children.append(depth(arg.body, {**env, **{a.arg: base for a in arg.args.args}}))
                else:
                    children.append(depth(arg, env))
            return max([base, *children])
        return max([1, *(depth(c, env) for c in ast.iter_child_nodes(node))])
    windows = [1]
    for node in sorted((n for n in ast.walk(tree) if isinstance(n, (ast.Assign, ast.Return))), key=lambda n: n.lineno):
        size = depth(node.value, depths)
        windows.append(size)
        if isinstance(node, ast.Assign):
            for target in node.targets:
                depths[ast.unparse(target)] = size
    if max(windows) > 10000:
        raise DependencyUnknown("factor window exceeds bounded static contract")
    return max(windows)
