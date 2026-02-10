"""Tests for run operations."""

import time

import pytest
import bauplan


@pytest.fixture
def client():
    return bauplan.Client()


def test_dry_run(client):
    state = client.run(
        project_dir="tests/fixtures/simple_taxi_dag",
        dry_run=True,
        cache="off",
    )

    assert state.job_id is not None
    assert state.job_status == "SUCCESS"
    assert state.error is None
    assert state.ctx is not None
    assert state.ctx.dry_run is True
    assert state.ctx.cache == "off"


def test_dry_run_duration(client):
    state = client.run(
        project_dir="tests/fixtures/simple_taxi_dag",
        dry_run=True,
        cache="off",
    )

    assert state.ended_at_ns is not None
    assert state.duration is not None
    assert state.duration > 0
    assert state.duration_ns is not None
    assert state.duration_ns > 0


def test_dry_run_tasks(client):
    state = client.run(
        project_dir="tests/fixtures/simple_taxi_dag",
        dry_run=True,
        cache="off",
    )

    assert len(state.tasks_started) > 0
    assert len(state.tasks_stopped) > 0


def test_detach(client):
    state = client.run(
        project_dir="tests/fixtures/simple_taxi_dag",
        dry_run=True,
        cache="off",
        detach=True,
    )

    assert state.job_id is not None
    assert state.job_status is None
    assert state.ended_at_ns is None
    assert state.ctx.detach is True

    # Poll until the detached job finishes.
    for _ in range(60):
        job = client.get_job(state.job_id)
        if job.status_type != bauplan.JobState.RUNNING:
            break
        time.sleep(1)

    assert job.status_type == bauplan.JobState.COMPLETE


def test_job_context_snapshot(client):
    state = client.run(
        project_dir="tests/fixtures/simple_taxi_dag",
        dry_run=True,
        cache="off",
    )

    ctx = client.get_job_context(state.job_id, include_snapshot=True)

    assert ctx.id == state.job_id
    assert len(ctx.snapshot_dict) > 0

    assert "models.py" in ctx.snapshot_dict
    assert "bauplan_project.yml" in ctx.snapshot_dict
    assert "query_model.sql" in ctx.snapshot_dict

    assert "normalize_data" in ctx.snapshot_dict["models.py"]
    assert "taxi_fhvhv" in ctx.snapshot_dict["query_model.sql"]
