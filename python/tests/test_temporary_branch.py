"""Tests for Branch context manager support (create_branch)."""

import uuid

import pytest
import bauplan
from bauplan import exceptions
from bauplan.schema import Branch


@pytest.fixture
def client() -> bauplan.Client:
    return bauplan.Client()


@pytest.fixture
def username(client: bauplan.Client):
    user = client.info().user
    assert user is not None
    return user.username


def random_branch_name(username: str) -> str:
    return f"{username}.tmp_{uuid.uuid4().hex[:8]}"


class TestBranchContextManager:
    def test_create_delete(self, client: bauplan.Client, username: str):
        name = random_branch_name(username)

        with client.create_branch(name, from_ref="main") as b:
            assert isinstance(b, Branch)
            assert b.name == name
            found = client.get_branch(name)
            assert found.name == name

        # After exiting, the branch should be gone.
        with pytest.raises(exceptions.BranchNotFoundError):
            client.get_branch(name)

    def test_merge(self, client: bauplan.Client, username: str):
        src = random_branch_name(username)
        dest = random_branch_name(username)

        # Create a destination branch to merge into.
        client.create_branch(dest, from_ref="main")
        try:
            with client.create_branch(src, from_ref="main") as b:
                client.merge_branch(source_ref=b, into_branch=dest)

            # Source branch should be deleted by the context manager.
            with pytest.raises(exceptions.BranchNotFoundError):
                client.get_branch(src)
        finally:
            client.delete_branch(dest, if_exists=True)

    def test_delete_on_exception(self, client: bauplan.Client, username: str):
        name = random_branch_name(username)

        with pytest.raises(RuntimeError, match="boom"):
            with client.create_branch(name, from_ref="main") as b:
                assert b.name == name
                raise RuntimeError("boom")

        with pytest.raises(exceptions.BranchNotFoundError):
            client.get_branch(name)
