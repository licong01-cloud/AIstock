"""Inspect pickle metadata as inert syntax. Never import or invoke its globals."""
from dataclasses import dataclass, field
import pickletools


@dataclass(eq=False)
class Symbol:
    kind: str
    args: object = None
    state: object = None
    items: list = field(default_factory=list)


def dataset_sequence_length(raw: bytes) -> int:
    """Read ONLY a real DatasetH/TSDatasetH root's scalar state, not nested keys.

    REDUCE/NEWOBJ/BUILD construct inert symbols; no unpickler, imports, callbacks,
    numpy constructors, tensor loading, or __setstate__ are used. Unknown opcodes
    and ambiguous dictionary writes fail closed rather than guessing a length.
    """
    if not raw or len(raw) > 16 * 1024**2:
        raise ValueError("frozen dataset metadata exceeds its size budget")
    stack, memo, marker = [], {}, object()

    def marked():
        i = len(stack) - 1
        while i >= 0 and stack[i] is not marker:
            i -= 1
        if i < 0:
            raise ValueError("invalid pickle mark")
        result = stack[i+1:]
        del stack[i:]
        return result

    def set_items(target, items):
        if len(items) % 2 or not isinstance(target, dict):
            raise ValueError("invalid metadata dictionary")
        for key, value in zip(items[::2], items[1::2], strict=True):
            if key in target:
                raise ValueError("duplicate metadata dictionary key")
            target[key] = value

    try:
        for count, (op, arg, position) in enumerate(pickletools.genops(raw)):
            if count > 300000 or len(stack) > 100000 or len(memo) > 100000:
                raise ValueError("frozen metadata opcode budget exceeded")
            name = op.name
            if name in {"PROTO", "FRAME"}:
                continue
            if name == "MARK":
                stack.append(marker)
            elif name in {"BINUNICODE", "SHORT_BINUNICODE", "BINUNICODE8", "UNICODE", "STRING",
                          "BINSTRING", "SHORT_BINSTRING", "BININT", "BININT1", "BININT2", "INT",
                          "LONG", "LONG1", "LONG4", "BINFLOAT", "FLOAT"}:
                stack.append(arg)
            elif name in {"BINBYTES", "SHORT_BINBYTES", "BINBYTES8", "BYTEARRAY8"}:
                stack.append(Symbol("bytes"))
            elif name in {"NONE", "NEWTRUE", "NEWFALSE"}:
                stack.append({"NONE": None, "NEWTRUE": True, "NEWFALSE": False}[name])
            elif name in {"EMPTY_TUPLE", "EMPTY_DICT", "EMPTY_LIST", "EMPTY_SET"}:
                stack.append({"EMPTY_TUPLE": tuple, "EMPTY_DICT": dict, "EMPTY_LIST": list,
                              "EMPTY_SET": set}[name]())
            elif name in {"MEMOIZE", "BINPUT", "LONG_BINPUT", "PUT"}:
                key = len(memo) if name == "MEMOIZE" else int(arg)
                if key in memo:
                    raise ValueError("duplicate pickle memo slot")
                memo[key] = stack[-1]
            elif name in {"BINGET", "LONG_BINGET", "GET"}:
                stack.append(memo[int(arg)])
            elif name == "STACK_GLOBAL":
                module, attribute = stack[-2:]
                del stack[-2:]
                if not isinstance(module, str) or not isinstance(attribute, str):
                    raise ValueError("invalid global symbol")
                stack.append(Symbol("global", (module, attribute)))
            elif name == "GLOBAL":
                stack.append(Symbol("global", tuple(arg.split(" "))))
            elif name in {"TUPLE", "LIST", "DICT", "FROZENSET"}:
                values = marked()
                if name == "DICT":
                    value = {}
                    set_items(value, values)
                else:
                    value = {"TUPLE": tuple, "LIST": list, "FROZENSET": frozenset}[name](values)
                stack.append(value)
            elif name in {"TUPLE1", "TUPLE2", "TUPLE3"}:
                n = int(name[-1])
                values = tuple(stack[-n:])
                del stack[-n:]
                stack.append(values)
            elif name in {"REDUCE", "NEWOBJ"}:
                constructor, args = stack[-2:]
                del stack[-2:]
                stack.append(Symbol(name, (constructor, args)))
            elif name == "NEWOBJ_EX":
                constructor, args, kwargs = stack[-3:]
                del stack[-3:]
                stack.append(Symbol(name, (constructor, args, kwargs)))
            elif name == "BUILD":
                state = stack.pop()
                if not isinstance(stack[-1], Symbol) or stack[-1].state is not None:
                    raise ValueError("invalid or repeated object state")
                stack[-1].state = state
            elif name == "SETITEM":
                key, value = stack[-2:]
                del stack[-2:]
                set_items(stack[-1], [key, value])
            elif name == "SETITEMS":
                values = marked()
                set_items(stack[-1], values)
            elif name in {"APPEND", "APPENDS", "ADDITEMS"}:
                values = [stack.pop()] if name == "APPEND" else marked()
                target = stack[-1]
                if isinstance(target, list):
                    target.extend(values)
                elif isinstance(target, set):
                    target.update(values)
                elif isinstance(target, Symbol):
                    target.items.extend(values)
                else:
                    raise ValueError("invalid metadata collection")
            elif name == "STOP":
                if position != len(raw)-1 or len(stack) != 1:
                    raise ValueError("trailing or ambiguous pickle root")
                break
            else:
                raise ValueError("unsupported frozen metadata opcode: " + name)
        else:
            raise ValueError("frozen metadata has no STOP")
        root = stack[0]
        if not isinstance(root, Symbol) or root.kind != "NEWOBJ" or not isinstance(root.state, dict):
            raise ValueError("frozen dataset root is not a declared dataset")
        constructor, args = root.args
        if (not isinstance(constructor, Symbol) or constructor.kind != "global" or args != ()
                or constructor.args not in {("qlib.data.dataset", "DatasetH"), ("qlib.data.dataset", "TSDatasetH")}):
            raise ValueError("unsupported frozen dataset class")
        if constructor.args[1] == "DatasetH":
            length = root.state.get("step_len", 1)
        else:
            length = root.state.get("step_len")
        if type(length) is not int or not 1 <= length <= 10000:
            raise ValueError("frozen sequence window is not a positive bounded integer")
        return length
    except (KeyError, IndexError, TypeError, OverflowError) as exc:
        raise ValueError("malformed frozen dataset metadata") from exc
