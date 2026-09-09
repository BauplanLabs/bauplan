"""Tests for run operations."""

import time
from typing import Any

import pytest
import bauplan


@pytest.fixture
def client() -> bauplan.Client:
    return bauplan.Client()


def test_dry_run(client: bauplan.Client):
    state = client.run(
        project_dir="tests/fixtures/simple_taxi_dag",
        dry_run=True,
        cache=False,
    )

    assert state.job_id is not None
    assert state.job_status == "SUCCESS"
    assert state.error is None
    assert state.ctx is not None
    assert state.ctx.dry_run is True
    assert state.ctx.cache is False
    assert state.ctx.transaction is True
    assert state.ctx.strict is True


@pytest.mark.parametrize(
    "strict,expected_status", [(True, "FAILED"), (False, "SUCCESS")]
)
def test_strict_expectation(
    client: bauplan.Client, strict: bool, expected_status: str
) -> None:
    """Check that strict mode controls whether a failing expectation fails the run."""
    state = client.run(
        project_dir="tests/fixtures/failing_expectation",
        dry_run=True,
        cache=False,
        strict=strict,
    )

    assert state.ctx.strict is strict
    assert state.job_status == expected_status


@pytest.mark.parametrize("option", ["strict", "cache", "transaction"])
@pytest.mark.parametrize("value", [None, "on", "off"])
def test_run_rejects_non_boolean(
    client: bauplan.Client, option: str, value: Any
) -> None:
    """Reject legacy strings and None before submitting a run."""
    with pytest.raises(TypeError):
        client.run(project_dir="tests/fixtures/failing_expectation", **{option: value})


@pytest.mark.parametrize("cache,transaction", [(True, False), (False, True)])
def test_run_modes(client: bauplan.Client, cache: bool, transaction: bool) -> None:
    """Check that cache and transaction modes are independent boolean settings."""
    state = client.run(
        project_dir="tests/fixtures/simple_taxi_dag",
        dry_run=True,
        cache=cache,
        transaction=transaction,
    )

    assert state.job_status == "SUCCESS"
    assert state.ctx.cache is cache
    assert state.ctx.transaction is transaction


def test_dry_run_duration(client: bauplan.Client):
    state = client.run(
        project_dir="tests/fixtures/simple_taxi_dag",
        dry_run=True,
    )

    assert state.ended_at_ns is not None
    assert state.duration is not None
    assert state.duration > 0
    assert state.duration_ns is not None
    assert state.duration_ns > 0
    assert state.ctx.cache is True
    assert state.ctx.transaction is True


def test_dry_run_tasks(client: bauplan.Client):
    state = client.run(
        project_dir="tests/fixtures/simple_taxi_dag",
        dry_run=True,
        cache=False,
    )

    assert len(state.tasks_started) > 0
    assert len(state.tasks_stopped) > 0


def test_detach(client: bauplan.Client):
    state = client.run(
        project_dir="tests/fixtures/simple_taxi_dag",
        dry_run=True,
        cache=False,
        detach=True,
    )

    assert state.job_id is not None
    assert state.job_status is None
    assert state.ended_at_ns is None
    assert state.ctx.detach is True

    # Poll until the detached job finishes.
    for _ in range(120):
        job = client.get_job(state.job_id)
        if job.status not in (bauplan.JobState.RUNNING, bauplan.JobState.NOT_STARTED):
            break
        time.sleep(1)

    assert job.status == bauplan.JobState.COMPLETE


def test_cancel_job(client: bauplan.Client):
    state = client.run(
        project_dir="tests/fixtures/long_running_dag",
        cache=False,
        detach=True,
        dry_run=True,
    )

    assert state.job_id is not None
    time.sleep(3)
    client.cancel_job(state.job_id)

    # Poll until the job finishes.
    for _ in range(120):
        job = client.get_job(state.job_id)
        if job.status not in (bauplan.JobState.RUNNING, bauplan.JobState.NOT_STARTED):
            break
        time.sleep(1)

    assert job.status == bauplan.JobState.ABORT


def test_job_context_snapshot(client: bauplan.Client):
    # TODO: For some reason, this is timing out ocassionally in automated tests.
    client = bauplan.Client(client_timeout=60)
    state = client.run(
        project_dir="tests/fixtures/simple_taxi_dag",
        dry_run=True,
        cache=False,
    )

    assert state.job_id is not None
    ctx = client.get_job_context(state.job_id, include_snapshot=True)

    assert ctx.id == state.job_id
    assert len(ctx.snapshot_dict) > 0

    assert "models.py" in ctx.snapshot_dict
    assert "bauplan_project.yml" in ctx.snapshot_dict
    assert "query_model.sql" in ctx.snapshot_dict

    assert "normalize_data" in ctx.snapshot_dict["models.py"]
    assert "taxi_fhvhv" in ctx.snapshot_dict["query_model.sql"]
