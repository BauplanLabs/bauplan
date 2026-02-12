"""Tests for query_to_generator and PyPaginator functionality."""

import pytest
import bauplan


@pytest.fixture
def client():
    return bauplan.Client()


def test_query_to_generator(client):
    """Verify row contents match expected schema."""
    gen = client.query_to_generator(
        "SELECT PassengerId, Name, Age FROM bauplan.titanic ORDER BY PassengerId"
    )
    rows = list(gen)

    assert rows[0]["PassengerId"] == 1
    assert rows[0]["Name"]
    assert rows[0]["Age"] > 0

    assert rows[-1]["PassengerId"] == len(rows)


def test_get_tags_pagination(client):
    tags = client.get_tags()

    assert hasattr(tags, "__iter__")
    assert hasattr(tags, "__next__")

    # Exhaust iterator (may be empty)
    all_tags = list(tags)
    for t in all_tags:
        assert hasattr(t, "name")
        assert hasattr(t, "hash")


def test_get_namespaces_pagination(client):
    namespaces = client.get_namespaces(ref="main")

    assert hasattr(namespaces, "__iter__")
    assert hasattr(namespaces, "__next__")

    all_ns = list(namespaces)
    assert len(all_ns) > 0

    for ns in all_ns:
        assert hasattr(ns, "name")


def test_get_tables_pagination(client):
    tables = client.get_tables(ref="main", filter_by_namespace="bauplan")

    assert hasattr(tables, "__iter__")
    assert hasattr(tables, "__next__")

    all_tables = list(tables)
    assert len(all_tables) > 0

    # titanic should be there
    table_names = [t.name for t in all_tables]
    assert "titanic" in table_names


def test_get_tables_with_limit(client):
    all_tables = list(client.get_tables(ref="main", filter_by_namespace="bauplan"))

    if len(all_tables) < 2:
        pytest.skip("Need at least 2 tables to test limit")

    limited = list(client.get_tables(ref="main", filter_by_namespace="bauplan", limit=1))
    assert len(limited) == 1


def test_get_jobs_pagination(client):
    jobs = client.get_jobs()

    assert hasattr(jobs, "__iter__")
    assert hasattr(jobs, "__next__")

    all_jobs = list(jobs)
    assert len(all_jobs) > 0

    for job in all_jobs:
        assert hasattr(job, "id")
        assert hasattr(job, "status")
        assert hasattr(job, "kind")
        assert hasattr(job, "status_type")
        assert hasattr(job, "kind_type")


def test_get_jobs_filter_by_kind_lowercase(client):
    jobs = list(client.get_jobs(filter_by_kinds="query", limit=5))
    for job in jobs:
        assert job.kind_type == bauplan.JobKind.QUERY


def test_get_jobs_filter_by_status_lowercase(client):
    jobs = list(client.get_jobs(filter_by_statuses="complete", limit=5))
    for job in jobs:
        assert job.status_type == bauplan.JobState.COMPLETE
