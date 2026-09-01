from argparse import Namespace
from contextlib import nullcontext
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import prepare_localsim_successor_cutover as subject


def _args(**overrides: object) -> Namespace:
    values = {
        "mode": "inventory",
        "target": "dev",
        "retained_account_id": ["paper_keep_1"],
        "authority_trade_date": date(2026, 8, 31),
        "env_file": Path(".env"),
        "expected_source_commit": None,
        "expected_database_host": None,
        "expected_database_port": None,
        "expected_database_name": None,
        "authorization": None,
        "confirm_production": False,
        "created_by": "test",
        "expected_partial_account_id": None,
        "expected_partial_account_hash": None,
        "expected_partial_lineage_id": None,
        "expected_partial_lineage_hash": None,
        "expected_partial_economic_facts_sha256": None,
        "expected_partial_created_by": None,
        "receipt": None,
    }
    values.update(overrides)
    return Namespace(**values)


def test_inventory_is_default_and_apply_requires_all_explicit_guards() -> None:
    parsed = subject.parser().parse_args(
        [
            "--target",
            "dev",
            "--retained-account-id",
            "paper_keep_1",
            "--authority-trade-date",
            "2026-08-31",
        ]
    )
    assert parsed.mode == "inventory"
    assert parsed.authority_trade_date == date(2026, 8, 31)
    with pytest.raises(subject.CutoverPreparationError, match="exact lineage authorization"):
        subject._authorization(
            _args(mode="apply", expected_database_name="aistock_dev"),
            ("paper_keep_1",),
        )
    with pytest.raises(subject.CutoverPreparationError, match="confirm-production"):
        subject._authorization(
            _args(
                mode="apply",
                target="production",
                expected_database_name="aistock",
                authorization=("AUTHORIZE_LOCALSIM_LINEAGE_APPLY:production:aistock:2026-08-31:paper_keep_1"),
            ),
            ("paper_keep_1",),
        )
    with pytest.raises(subject.CutoverPreparationError, match="exact lineage authorization"):
        subject._authorization(
            _args(
                mode="apply",
                target="production",
                expected_database_name="aistock",
                authorization="AUTHORIZE_LOCALSIM_LINEAGE_APPLY:production:aistock:paper_keep_1",
                confirm_production=True,
            ),
            ("paper_keep_1",),
        )


def test_preflight_requires_exact_database_identity_before_schema_read() -> None:
    class NoCursorConnection:
        def cursor(self, **kwargs: object) -> object:
            del kwargs
            raise AssertionError("schema reads must not start after identity mismatch")

    with pytest.raises(subject.CutoverPreparationError, match="database identity mismatch"):
        subject._database_preflight(
            NoCursorConnection(),
            _args(
                mode="preflight",
                expected_database_host="expected-host",
                expected_database_port=5432,
                expected_database_name="aistock_dev",
            ),
            {"host": "other-host", "port": 5432, "dbname": "aistock_dev"},
        )


def test_partial_repair_requires_every_exact_identity_and_separate_authorization() -> None:
    with pytest.raises(subject.CutoverPreparationError, match="every exact partial identity"):
        subject._partial_repair_authorization(
            _args(mode="repair-partial", expected_database_name="aistock"),
            ("paper_keep_1",),
        )

    exact = _args(
        mode="repair-partial",
        target="production",
        expected_database_name="aistock",
        authorization=(
            "AUTHORIZE_LOCALSIM_LINEAGE_REPAIR:production:aistock:2026-08-31:paper_keep_1:lineage_keep:account_keep"
        ),
        confirm_production=True,
        expected_partial_account_id="account_keep",
        expected_partial_account_hash="a" * 64,
        expected_partial_lineage_id="lineage_keep",
        expected_partial_lineage_hash="b" * 64,
        expected_partial_economic_facts_sha256="c" * 64,
        expected_partial_created_by="cutover_owner",
    )
    subject._partial_repair_authorization(exact, ("paper_keep_1",))


def test_execute_inventory_never_constructs_mutation_service(monkeypatch: pytest.MonkeyPatch) -> None:
    candidate = SimpleNamespace(
        legacy_account_id="paper_keep_1",
        model_dump=lambda **kwargs: {"legacy_account_id": "paper_keep_1", "retained_by_user": True},
    )
    monkeypatch.setattr(subject, "_settings", lambda target, env_file: {"target": target, "env": env_file})
    monkeypatch.setattr(subject, "_source_commit", lambda expected, required: "a" * 40)
    inventory_calls: list[tuple[tuple[str, ...], date]] = []

    def inventory(
        settings: dict[str, object],
        account_ids: tuple[str, ...],
        *,
        authority_trade_date: date,
    ) -> tuple[SimpleNamespace, ...]:
        del settings
        inventory_calls.append((account_ids, authority_trade_date))
        return (candidate,)

    monkeypatch.setattr(subject, "_inventory", inventory)

    class ForbiddenRepository:
        def __init__(self, *args: object, **kwargs: object) -> None:
            raise AssertionError("inventory must not construct a writer repository")

    monkeypatch.setattr(subject, "LocalSimSuccessorRepository", ForbiddenRepository)
    receipt = subject.execute(_args())
    assert receipt["mode"] == "inventory"
    assert receipt["retained_account_ids"] == ["paper_keep_1"]
    assert receipt["authority_trade_date"] == "2026-08-31"
    assert inventory_calls == [(("paper_keep_1",), date(2026, 8, 31))]
    assert receipt["applied"] == []
    assert receipt["readback"] == []


def test_readback_rejects_exact_binding_authority_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    candidate = SimpleNamespace(
        legacy_account_id="paper_keep_1",
        release_id="release_current",
        binding_id="binding_current",
        ledger_scope_id="paper_keep_1",
        economic_facts_sha256="a" * 64,
        package_id="pkg_keep",
        manifest_sha256="b" * 64,
        model_dump=lambda **kwargs: {"legacy_account_id": "paper_keep_1"},
    )
    lineage = SimpleNamespace(
        account_id="account_keep",
        release_id="release_current",
        binding_id="binding_stale",
        ledger_scope_id="paper_keep_1",
        economic_facts_sha256="a" * 64,
    )
    monkeypatch.setattr(
        subject,
        "_settings",
        lambda target, env_file: {"host": "db", "port": 5432, "dbname": "aistock", "target": target},
    )
    monkeypatch.setattr(subject, "_source_commit", lambda expected, required: "c" * 40)
    monkeypatch.setattr(
        subject,
        "_inventory",
        lambda settings, account_ids, *, authority_trade_date: (candidate,),
    )
    monkeypatch.setattr(subject, "_connection", lambda settings: nullcontext(object()))
    monkeypatch.setattr(subject, "_database_preflight", lambda conn, args, settings: {"ok": True})
    monkeypatch.setattr(
        subject,
        "LocalSimSuccessorRepository",
        lambda **kwargs: SimpleNamespace(get_lineage_by_legacy_account=lambda account_id: lineage),
    )

    with pytest.raises(subject.CutoverPreparationError, match="authority/economic hash drifted"):
        subject.execute(
            _args(
                mode="readback",
                target="production",
                expected_source_commit="c" * 40,
                expected_database_host="db",
                expected_database_port=5432,
                expected_database_name="aistock",
            )
        )


def test_apply_compensates_exact_bundle_when_independent_readback_drifts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def candidate(economic_hash: str) -> SimpleNamespace:
        payload = {
            "legacy_account_id": "paper_keep_1",
            "release_id": "release_current",
            "binding_id": "binding_current",
            "ledger_scope_id": "paper_keep_1",
            "economic_facts_sha256": economic_hash,
            "package_id": "pkg_keep",
            "manifest_sha256": "b" * 64,
        }
        return SimpleNamespace(**payload, model_dump=lambda **kwargs: dict(payload))

    stable = candidate("a" * 64)
    drifted = candidate("d" * 64)
    inventory_values = iter(((stable,), (stable,), (drifted,)))
    monkeypatch.setattr(
        subject,
        "_settings",
        lambda target, env_file: {"host": "db", "port": 5432, "dbname": "aistock"},
    )
    monkeypatch.setattr(subject, "_source_commit", lambda expected, required: "c" * 40)
    monkeypatch.setattr(
        subject,
        "_inventory",
        lambda settings, account_ids, *, authority_trade_date: next(inventory_values),
    )
    monkeypatch.setattr(subject, "_connection", lambda settings: nullcontext(object()))
    monkeypatch.setattr(subject, "_database_preflight", lambda conn, args, settings: {"ok": True})
    monkeypatch.setattr(subject, "_authorization", lambda args, account_ids: None)

    account = SimpleNamespace(account_id="account_keep", account_hash="e" * 64)
    lineage = SimpleNamespace(
        lineage_id="lineage_keep",
        lineage_hash="f" * 64,
        account_id="account_keep",
        release_id="release_current",
        binding_id="binding_current",
        ledger_scope_id="paper_keep_1",
        economic_facts_sha256="a" * 64,
    )

    class Repository:
        def __init__(self) -> None:
            self.persisted = False
            self.repairs: list[dict[str, object]] = []

        def get_lineage_by_legacy_account(self, account_id: str) -> object | None:
            del account_id
            return lineage if self.persisted else None

        def delete_prepared_lineage_bundle(self, **kwargs: object) -> None:
            self.repairs.append(kwargs)
            self.persisted = False

        def get_account(self, account_id: str) -> object:
            raise subject.DataUnavailableError("missing", context={"account_id": account_id})

    repository = Repository()
    monkeypatch.setattr(subject, "LocalSimSuccessorRepository", lambda **kwargs: repository)

    class Control:
        def __init__(self, *, repository: Repository) -> None:
            self.repository = repository

        def prepare_legacy_lineage(self, candidate: object, *, created_by: str) -> tuple[object, object]:
            del candidate, created_by
            self.repository.persisted = True
            return account, lineage

    monkeypatch.setattr(subject, "LocalSimControlPlaneService", Control)

    with pytest.raises(subject.CutoverPreparationError, match="authority/economic hash drifted"):
        subject.execute(
            _args(
                mode="apply",
                target="production",
                expected_source_commit="c" * 40,
                expected_database_host="db",
                expected_database_port=5432,
                expected_database_name="aistock",
            )
        )

    assert repository.persisted is False
    assert repository.repairs == [
        {
            "legacy_account_id": "paper_keep_1",
            "expected_lineage_id": "lineage_keep",
            "expected_lineage_hash": "f" * 64,
            "expected_account_id": "account_keep",
            "expected_account_hash": "e" * 64,
            "expected_economic_facts_sha256": "a" * 64,
            "expected_created_by": "test",
        }
    ]


def test_apply_rejects_inventory_drift_before_constructing_control_plane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = SimpleNamespace(model_dump=lambda **kwargs: {"economic_facts_sha256": "a" * 64})
    second = SimpleNamespace(model_dump=lambda **kwargs: {"economic_facts_sha256": "b" * 64})
    inventory_values = iter(((first,), (second,)))
    monkeypatch.setattr(subject, "_settings", lambda target, env_file: {"host": "db"})
    monkeypatch.setattr(subject, "_source_commit", lambda expected, required: "c" * 40)
    monkeypatch.setattr(
        subject,
        "_inventory",
        lambda settings, account_ids, *, authority_trade_date: next(inventory_values),
    )
    monkeypatch.setattr(subject, "_connection", lambda settings: nullcontext(object()))
    monkeypatch.setattr(subject, "_database_preflight", lambda conn, args, settings: {"ok": True})
    monkeypatch.setattr(subject, "_authorization", lambda args, account_ids: None)
    monkeypatch.setattr(subject, "LocalSimSuccessorRepository", lambda **kwargs: SimpleNamespace())

    class ForbiddenControl:
        def __init__(self, *args: object, **kwargs: object) -> None:
            raise AssertionError("drifted inventory must fail before constructing the writer")

    monkeypatch.setattr(subject, "LocalSimControlPlaneService", ForbiddenControl)
    with pytest.raises(subject.CutoverPreparationError, match="drifted before DML"):
        subject.execute(
            _args(
                mode="apply",
                target="production",
                expected_source_commit="c" * 40,
                expected_database_host="db",
                expected_database_port=5432,
                expected_database_name="aistock",
            )
        )


def test_partial_repair_executes_exact_delete_and_independent_absence_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = SimpleNamespace(model_dump=lambda **kwargs: {"legacy_account_id": "paper_keep_1"})
    monkeypatch.setattr(subject, "_settings", lambda target, env_file: {"host": "db"})
    monkeypatch.setattr(subject, "_source_commit", lambda expected, required: "c" * 40)
    monkeypatch.setattr(
        subject,
        "_inventory",
        lambda settings, account_ids, *, authority_trade_date: (candidate,),
    )
    monkeypatch.setattr(subject, "_connection", lambda settings: nullcontext(object()))
    monkeypatch.setattr(subject, "_database_preflight", lambda conn, args, settings: {"ok": True})
    monkeypatch.setattr(subject, "_partial_repair_authorization", lambda args, account_ids: None)

    class Repository:
        def __init__(self) -> None:
            self.delete_kwargs: dict[str, object] | None = None

        def delete_prepared_lineage_bundle(self, **kwargs: object) -> None:
            self.delete_kwargs = kwargs

        def get_lineage_by_legacy_account(self, account_id: str) -> None:
            del account_id
            return None

        def get_account(self, account_id: str) -> object:
            raise subject.DataUnavailableError("missing", context={"account_id": account_id})

    repository = Repository()
    monkeypatch.setattr(subject, "LocalSimSuccessorRepository", lambda **kwargs: repository)
    receipt = subject.execute(
        _args(
            mode="repair-partial",
            target="production",
            expected_source_commit="c" * 40,
            expected_database_host="db",
            expected_database_port=5432,
            expected_database_name="aistock",
            expected_partial_account_id="account_keep",
            expected_partial_account_hash="a" * 64,
            expected_partial_lineage_id="lineage_keep",
            expected_partial_lineage_hash="b" * 64,
            expected_partial_economic_facts_sha256="c" * 64,
            expected_partial_created_by="cutover_owner",
        )
    )

    assert receipt["repaired"] == [
        {
            "legacy_account_id": "paper_keep_1",
            "account_id": "account_keep",
            "lineage_id": "lineage_keep",
        }
    ]
    assert repository.delete_kwargs is not None
