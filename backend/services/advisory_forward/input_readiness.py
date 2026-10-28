"""Daily ledger consumption for new forward publications; never starts collection."""
from backend.services.data_integrity.inputs import InputReadinessError, ready_binding, verify_current
from backend.services.data_integrity.repository import IntegritySchemaUnavailable
from backend.services.data_integrity.service import DailyIntegrityService


class AdvisoryInputReadiness:
    def __init__(self, service=None):
        self.service = service or DailyIntegrityService()

    def prepare(self, package_ids, decision_date):
        if not package_ids or len(set(package_ids)) != len(package_ids):
            raise ValueError("forward package identities must be nonempty and unique")
        try:
            return {p: ready_binding(self.service.readiness(data_date=decision_date.isoformat(),
                consumer="ADVISORY", binding_id=p)) for p in package_ids}
        except IntegritySchemaUnavailable as exc:
            raise InputReadinessError("每日输入台账尚未可用") from exc

    def recheck(self, bindings):
        with self.service.repository.transaction(readonly=True) as conn:
            verify_current(conn, bindings)


def summaries(bindings):
    return {p: {"binding_sha256": b["binding_sha256"], "input_binding_sha256": b["input_binding_sha256"],
        "manifest_sha256": b["manifest_sha256"], "data_date": b["data_date"],
        "window_trading_days": b["dependencies"]["window_trading_days"],
        "partition_count": len(b["partition_refs"]), "historical_first_availability_claimed": False}
        for p, b in bindings.items()}
