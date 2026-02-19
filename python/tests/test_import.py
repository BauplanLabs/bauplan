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
def username(client):
    return client.info().user.username


@pytest.fixture
def temp_branch(client, username):
    name = f"{username}.pysdk_import_{uuid.uuid4().hex[:8]}"
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


def test_detached_import(client, temp_branch):
    client.create_table(
        table="my_detached_table",
        search_uri=SEARCH_URI,
        branch=temp_branch,
    )

    state = client.import_data(
        table="my_detached_table",
        search_uri=SEARCH_URI,
        branch=temp_branch,
        detach=True,
    )

    assert state.job_id is not None
    assert state.job_status is None

    for _ in range(120):
        job = client.get_job(state.job_id)
        if job.status not in (bauplan.JobState.RUNNING, bauplan.JobState.NOT_STARTED):
            break
        time.sleep(1)

    assert job.status == bauplan.JobState.COMPLETE


def test_create_external_table_from_parquet(client, temp_branch):
    search_patterns = ["s3://bauplan-openlake-db87a23/stage/taxi_fhvhv/*2023*"]

    state = client.create_external_table_from_parquet(
        table="ext_parquet_table",
        search_patterns=search_patterns,
        branch=temp_branch,
        overwrite=True,
    )

    assert state.error is None

    result = client.query(
        query="SELECT COUNT(*) AS row_count FROM ext_parquet_table",
        ref=temp_branch,
    )

    assert result["row_count"][0].as_py() == 134344870

    # Importing into a read-only external table should fail.
    import_state = client.import_data(
        table="ext_parquet_table",
        search_uri=search_patterns[0],
        branch=temp_branch,
    )

    assert "Cannot import files to read-only table" in import_state.error


def test_create_external_table_from_metadata(client, temp_branch):
    metadata_uri = (
        "s3://bauplan-openlake-db87a23/iceberg/tpch_1/"
        "customer_e53c682c-36c4-4e3d-9ded-1214d0ee157f/"
        "metadata/00000-b6f502e1-5140-499e-bf83-22f943067e36.metadata.json"
    )

    client.create_external_table_from_metadata(
        table="ext_metadata_table",
        metadata_json_uri=metadata_uri,
        namespace="bauplan",
        branch=temp_branch,
        overwrite=True,
    )

    result = client.query(
        query="SELECT * FROM ext_metadata_table LIMIT 10",
        ref=temp_branch,
    )

    assert result.num_rows == 10

    # Creating the same table without overwrite should raise.
    with pytest.raises(Exception):
        client.create_external_table_from_metadata(
            table="ext_metadata_table",
            metadata_json_uri=metadata_uri,
            namespace="bauplan",
            branch=temp_branch,
            overwrite=False,
        )


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
