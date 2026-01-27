"""Tests for query_to_generator functionality."""

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
