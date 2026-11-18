"""Capture revision references and actual inputs in the SAME short snapshot."""
from contextlib import contextmanager
from datetime import date
import json

from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import capture_read_snapshot
from .contracts import digest
from .repository import schema_installed
from backend.services.trading_core.errors import DataUnavailableError


class InputReadinessError(DataUnavailableError):
    error_code = "ADVISORY_INPUT_NOT_READY"


def ready_binding(readiness):
    """Bind resolved content once; producer quality is diagnostic, not a token."""
    required = ("binding_id", "manifest_sha256", "data_date", "dependencies", "partition_refs", "global_pit")
    if (not isinstance(readiness, dict) or any(not readiness.get(key) for key in required)
            or not readiness["dependencies"].get("datasets")):
        raise InputReadinessError("策略包、输入窗口或 PIT 身份无法绑定")
    body = {key: readiness[key] for key in required}
    if "input_scope" in readiness:
        body["input_scope"] = readiness["input_scope"]
    body = json.loads(json.dumps(body, default=str))
    body["partition_refs"] = [{**{key: ref.get(key) for key in ("dataset", "fact_date", "data_revision")},
        **({"consumer_projection": {key: ref["consumer_projection"][key]
            for key in ("policy", "scope_sha256", "missing_as_nan") if key in ref["consumer_projection"]}}
           if ref.get("consumer_projection") is not None else {})} for ref in body["partition_refs"]]
    body["input_binding_sha256"] = digest({k:v for k,v in body.items() if k != "input_binding_sha256"})
    return {**body, "binding_sha256": digest(body)}


def check_binding(binding):
    if (not isinstance(binding, dict) or not binding.get("binding_sha256")
            or digest({k: v for k, v in binding.items() if k != "binding_sha256"}) != binding["binding_sha256"]):
        raise InputReadinessError("冻结输入绑定身份不符")


def verify_partition_refs(expected, actual, *, input_scope=None):
    """Check frozen content identity only, never producer acceptance metadata.

    Missing historical ledger rows remain untracked, not a fabricated VALID.
    A known newer content revision must not silently replace a frozen input.
    """
    rows = {(r["dataset"], str(r.get("trade_date", r.get("fact_date")))): r for r in (actual or [])}
    if len(rows) != len(actual or []):
        raise InputReadinessError("输入分区证明重复")
    for ref in expected:
        row = rows.get((ref["dataset"], ref["fact_date"]))
        revision = row.get("data_revision") if row else None
        expected_revision = ref.get("data_revision")
        if revision is not None and revision != expected_revision and not (expected_revision is None and revision == 0):
            raise InputReadinessError("冻结输入内容已修订", context={"dataset": ref["dataset"], "fact_date": ref["fact_date"]})


def verify_capture(binding, capture):
    check_binding(binding)
    dates = [r["fact_date"] for r in binding["partition_refs"]]
    if (not isinstance(capture, dict)
            or capture.get("start") != min(dates) or capture.get("end") != binding["data_date"]
            or capture.get("snapshot_kind") != "REPEATABLE_READ_READ_ONLY"
            or capture.get("external_fallback_allowed") is not False):
        raise InputReadinessError("实际读取窗口或数据库快照与就绪绑定不符")
    verify_partition_refs(binding["partition_refs"], capture.get("partition_refs"), input_scope=binding.get("input_scope"))
    if binding.get("input_scope") is not None and (
            capture.get("universe_sha256") != binding["input_scope"].get("universe_sha256")):
        raise InputReadinessError("实际读取股票范围与荐股 PIT 输入范围不符")
    actual_pit = capture.get("pit_identity") or {}
    if any(actual_pit.get(k) != v for k, v in binding["global_pit"].items() if k != "reproduction_mode"):
        raise InputReadinessError("实际输入 PIT 身份与就绪绑定不符")


def verify_current(conn, bindings, *, lock=False):
    """Recheck on caller's short transaction, never during model computation."""
    from dataclasses import asdict
    from backend.services.canonical_equity_pit import CanonicalPitAuthorityResolver, CanonicalPitContractError, CANONICAL_PIT_AUTHORITY_ID
    if lock:
        with conn.cursor() as cur:
            cur.execute("""SELECT p.authority_id FROM market.stock_universe_pit_authority_pointer p
                JOIN market.stock_universe_pit_authority_versions v ON v.authority_id=p.authority_id
                  AND v.rule_version=p.current_rule_version AND v.rolling_key=p.current_rolling_key
                JOIN market.stock_universe_pit_state s ON s.universe_key=p.current_rolling_key
                WHERE p.authority_id=%s FOR SHARE OF p,v,s""", (CANONICAL_PIT_AUTHORITY_ID,))
            if cur.fetchone() is None:
                raise InputReadinessError("发布时 PIT authority 不可用")
    @contextmanager
    def borrowed():
        yield conn
    try:
        pit = json.loads(json.dumps(asdict(CanonicalPitAuthorityResolver(borrowed).resolve_live_binding()), default=str))
    except CanonicalPitContractError as exc:
        raise InputReadinessError("发布时 PIT authority 已不可用") from exc
    for package_id, binding in bindings.items():
        check_binding(binding)
        if package_id != binding["binding_id"] or pit != binding["global_pit"]:
            raise InputReadinessError("策略包或 PIT 绑定已改变")
        with conn.cursor() as cur:
            cur.execute("SELECT manifest_sha256,package_status FROM strategy_pkg.package WHERE package_id=%s" +
                        (" FOR SHARE" if lock else ""), (package_id,))
            row = cur.fetchone()
        if not row or row[0] != binding["manifest_sha256"] or row[1] in {"DRAFT", "RETIRED"}:
            raise InputReadinessError("策略包冻结版本或准入状态已改变")
        refs = binding["partition_refs"]
        actual = partition_pins(conn, date.fromisoformat(min(r["fact_date"] for r in refs)),
            date.fromisoformat(binding["data_date"]), list(binding["dependencies"]["datasets"]), lock=lock)
        scope = binding.get("input_scope")
        verify_partition_refs(refs, actual, input_scope=scope)
        if scope is not None:
            from .advisory_scope import resolve_scope
            current_scope, _members = resolve_scope(conn, pit, date.fromisoformat(binding["data_date"]))
            if current_scope != scope:
                raise InputReadinessError("荐股 PIT 输入范围已改变")


def partition_pins(conn, start: date, end: date, datasets=None, *, lock=False):
    if not schema_installed(conn):
        return None  # legacy input remains explicitly not ledger-certified
    datasets = list(datasets) if datasets is not None else None
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""SELECT dataset,trade_date,data_revision,validated_revision,
            validation_status,validation_id,scope_version,rule_version,
            jsonb_build_object('validation',metadata->'validation') AS metadata
            FROM market.dataset_date_refresh_audit WHERE data_source='canonical_db'
              AND trade_date BETWEEN %s AND %s
              AND (%s::text[] IS NULL OR dataset=ANY(%s::text[]))
            ORDER BY dataset,trade_date""" + (" FOR SHARE" if lock else ""), (start, end, datasets, datasets))
        return [{**dict(row), "trade_date": row["trade_date"].isoformat()} for row in cur.fetchall()]


@contextmanager
def capture_inputs(start: date, end: date, *, datasets=None):
    """Callers consume/copy frames inside, then compute only AFTER this closes."""
    if start > end:
        raise ValueError("invalid source capture interval")
    with capture_read_snapshot() as conn:
        pins = partition_pins(conn, start, end, datasets)
        # A token is not certification: unknown/missing validations stay unknown.
        receipt = {"schema_version": "aistock_input_capture_v1", "start": str(start), "end": str(end),
                   "partition_refs": pins, "ledger_available": pins is not None,
                   "snapshot_kind": "REPEATABLE_READ_READ_ONLY", "model_compute_in_transaction": False}
        receipt["input_revision_sha256"] = digest(receipt)
        yield receipt
