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


def test_query_taxi_fhvhv(client):
    result = client.query(
        query=(
            "SELECT trip_time, trip_miles FROM taxi_fhvhv"
            " WHERE pickup_datetime >= '2023-01-01T00:00:00-05:00'"
            "   AND pickup_datetime < '2023-01-02T00:00:00-05:00'"
        ),
        ref="main",
        cache="off",
    )

    assert len(result) == 448004


def test_parallel_query_correctness(client):
    """Verify row numbering is sequential across parallel query endpoints."""
    result = client.query(
        query=(
            "SELECT tips, row_number() OVER () AS row_number"
            " FROM taxi_fhvhv"
            " WHERE pickup_datetime >= '2023-01-01T10:00:01-05:00'"
            "   AND pickup_datetime < '2023-01-01T11:00:00-05:00'"
            " ORDER BY row_number, tips"
        ),
        ref="main",
        cache="off",
        args={
            "num_endpoints": "10",
            "flight-python": "on",
            "flight_batch_size": "50",
            "query_concurrency": "10",
        },
    )

    assert result.num_rows == 25161

    column = result["row_number"].to_pylist()
    assert column == list(range(1, 25162))


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


def test_scan_empty_columns(client):
    with pytest.raises(ValueError) as exc_info:
       client.scan(
            table="titanic",
            namespace="bauplan",
            ref="main",
            columns=[],
            filters="Survived = 1",
            limit=5,
        )

    assert "Empty column list" in str(exc_info.value)


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


INVALID_FILTERS = [
    "",
    " ",
    ";",
    ";;;",
    "DROP TABLE titanic",
    "1; DROP TABLE titanic",
    "SELECT * FROM titanic",
    "Survived = 1 UNION SELECT * FROM titanic",
    "(((((",
    "))))",
    "Survived =",
    "= 1",
    "Survived = 1 AND",
    "Survived = 1 OR",
    "Survived BETWEEN",
    "Survived IN",
    "Survived LIKE",
    "NOT",
    "1 = 1; SELECT 1",
    "\x00",
    "Survived = '",
    "Survived = \"",
]


@pytest.mark.parametrize("bad_filter", INVALID_FILTERS)
def test_scan_invalid_filter(client, bad_filter):
    with pytest.raises(ValueError):
        client.scan(
            table="titanic",
            namespace="bauplan",
            ref="main",
            columns=["PassengerId"],
            filters=bad_filter,
            limit=1,
        )
