from __future__ import annotations

import copy

import pytest

from backend.services.hmm_risk import formal_state_authority as subject
from backend.services.hmm_risk.contracts import StateModelSetError, canonical_sha256


@pytest.fixture
def sources():
    identities, catalog, members = {}, [], []
    for i in range(31):
        code, name, index = f"{(i + 10) * 10000:06}", f"parent-{i}", f"801{i:03}.SI"
        identities[code] = {"l1_code": code, "l1_name": name, "l2_code": code, "l2_name": "nan"}
        catalog.append(dict(level="L1", industry_code=code, index_code=index, industry_name=name, parent_code="0"))
    for i in range(134):
        parent = f"{(i % 31 + 10) * 10000:06}"
        code, name, index = f"{int(parent) + (i // 31 + 1) * 100:06}", f"child-{i}", f"801{i + 100:03}.SI"
        identities[code] = {"l1_code": parent, "l1_name": f"parent-{i % 31}", "l2_code": code, "l2_name": name}
        catalog.append(dict(level="L2", industry_code=code, index_code=index, industry_name=name, parent_code=parent))
        if i < 131:
            members.append({"l1_code": f"801{i % 31:03}.SI", "l2_code": index})
    taxonomy = {
        "schema_version": "sw2021_taxonomy_catalog_v1",
        "contract_id": "sw2021_classification_catalog_v1",
        "version": "SW2021",
        "identities": identities,
    }
    read = {
        "catalog": catalog,
        "members": members,
        "query_sha256": canonical_sha256([subject.CATALOG_QUERY, subject.MEMBER_QUERY]),
        "transaction_read_only": True,
        "database_written": False,
    }
    return taxonomy, read


def build(sources):
    taxonomy, read = sources
    return subject.build_authority(
        taxonomy=taxonomy,
        database_read=read,
        artifact_root="X:/full-v3",
        identity={"bundle_hash": "a" * 64},
        taxonomy_file_sha256="b" * 64,
    )


def test_official_parent_join_and_131_member_catalog_without_renumbering(sources):
    before = copy.deepcopy(sources)
    authority = build(sources)
    assert sources == before
    assert len(authority["l1_projection"]["rows"]) == 31
    assert len(authority["l2_projection"]["rows"]) == 131
    row = next(x for x in authority["l2_projection"]["rows"] if x["canonical_l2_code"] == "801130.SI")
    assert row["taxonomy_l2_code"] == "400100"
    assert row["canonical_l1_code"] == "801030.SI"
    assert authority["research_basis"]["historical_non_as_known_taxonomy"] is True
    assert build(sources) == authority


@pytest.mark.parametrize("fault", ["duplicate", "parent", "name", "members", "unknown_taxonomy", "writable"])
def test_catalog_drift_fails_closed(sources, fault):
    taxonomy, read = sources
    if fault == "duplicate":
        read["catalog"][0] = copy.deepcopy(read["catalog"][1])
    elif fault == "parent":
        read["catalog"][31]["parent_code"] = "999999"
    elif fault == "name":
        read["catalog"][31]["industry_name"] = "not-authority-name"
    elif fault == "members":
        read["members"].pop()
    elif fault == "unknown_taxonomy":
        taxonomy["identities"].pop(read["catalog"][31]["industry_code"])
    else:
        read["transaction_read_only"] = False
    with pytest.raises((ValueError, StateModelSetError)):
        build(sources)


@pytest.mark.parametrize("readonly", ["on", "off"])
def test_database_reader_is_readonly_and_always_rolls_back(readonly):
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql):
            self.sql = sql
            connection.statements.append(sql)

        def fetchone(self):
            return (readonly,)

        def fetchall(self):
            return []

    class Connection:
        statements = []
        rolled_back = False

        def set_session(self, **kwargs):
            self.session = kwargs

        def cursor(self):
            return Cursor()

        def rollback(self):
            self.rolled_back = True

    connection = Connection()
    if readonly == "on":
        result = subject.read_catalog(connection)
        assert result["database_written"] is False
        assert connection.statements[-2:] == [subject.CATALOG_QUERY, subject.MEMBER_QUERY]
    else:
        with pytest.raises(ValueError, match="not read-only"):
            subject.read_catalog(connection)
        assert len(connection.statements) == 2
    assert connection.session == {"isolation_level": "REPEATABLE READ", "readonly": True, "autocommit": False}
    assert connection.rolled_back
