"""Tests for query operations."""

import pytest
import bauplan


@pytest.fixture
def client():
    return bauplan.Client()


def test_query_returns_arrow_table(client):
    result = client.query(
        "SELECT PassengerId, Name FROM bauplan.titanic LIMIT 5",
    )

    assert result.num_rows == 5
    assert "PassengerId" in result.column_names
    assert "Name" in result.column_names


def test_query_with_ref(client):
    result = client.query(
        "SELECT count(*) AS cnt FROM bauplan.titanic",
        ref="main",
    )

    assert result.num_rows == 1
    assert result.column("cnt")[0].as_py() > 0


def test_query_with_max_rows(client):
    result = client.query(
        "SELECT * FROM bauplan.titanic",
        max_rows=3,
    )

    assert result.num_rows == 3


def test_scan_returns_arrow_table(client):
    result = client.scan(
        table="titanic",
        namespace="bauplan",
        ref="main",
        columns=["PassengerId", "Name"],
        filters="Survived = 1",
        limit=5,
    )

    assert result.num_rows == 5
    assert "PassengerId" in result.column_names
    assert "Name" in result.column_names
