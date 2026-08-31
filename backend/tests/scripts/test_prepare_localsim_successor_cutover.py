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
                authorization=(
                    "AUTHORIZE_LOCALSIM_LINEAGE_APPLY:production:aistock:2026-08-31:paper_keep_1"
                ),
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
    assert inventory_calls == [(('paper_keep_1',), date(2026, 8, 31))]
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
