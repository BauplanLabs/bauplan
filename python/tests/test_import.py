"""Tests for table create and import operations."""

import time
import uuid

import pytest
import bauplan

SEARCH_URI = "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*"


@pytest.fixture
def client():
    return bauplan.Client()


@pytest.fixture
def temp_branch(client):
    name = f"bauplan-e2e-check.pysdk_import_{uuid.uuid4().hex[:8]}"
    client.create_branch(name, from_ref="main")
    yield name
    client.delete_branch(name, if_exists=True)


def test_create_table_and_import(client, temp_branch):
    table = client.create_table(
        table="my_import_table",
        search_uri=SEARCH_URI,
        branch=temp_branch,
    )

    assert table.name == "my_import_table"

    import_state = client.import_data(
        table="my_import_table",
        search_uri=SEARCH_URI,
        branch=temp_branch,
    )

    assert import_state.job_status == "SUCCESS", import_state.error
    assert import_state.error is None

    result = client.query(
        query='SELECT COUNT(*) FROM "my_import_table"',
        ref=temp_branch,
        cache="off",
    )

    assert result.num_rows > 0


def test_plan_and_apply(client, temp_branch):
    plan_state = client.plan_table_creation(
        table="my_plan_table",
        search_uri=SEARCH_URI,
        branch=temp_branch,
    )

    assert plan_state.job_status == "SUCCESS", plan_state.error
    assert plan_state.plan is not None
    assert plan_state.can_auto_apply is True

    apply_state = client.apply_table_creation_plan(plan=plan_state)

    assert apply_state.job_status == "SUCCESS", apply_state.error

    import_state = client.import_data(
        table="my_plan_table",
        search_uri=SEARCH_URI,
        branch=temp_branch,
    )

    assert import_state.job_status == "SUCCESS", import_state.error

    result = client.query(
        query='SELECT COUNT(*) FROM "my_plan_table"',
        ref=temp_branch,
        cache="off",
    )

    assert result.num_rows > 0
