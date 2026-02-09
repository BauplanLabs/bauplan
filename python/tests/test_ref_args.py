"""Tests for ref argument handling (BranchArg vs RefArg semantics).

BranchArg extracts just the name from a Branch object.
RefArg extracts name@hash from a Branch/Tag/Ref object.

These tests verify that passing an object produces identical results to passing
the equivalent string.
"""

import uuid
import pytest
import bauplan


@pytest.fixture
def client():
    return bauplan.Client()


@pytest.fixture
def username(client):
    return client.info().user.username


@pytest.fixture
def temp_branch(client, username):
    """Create a temporary branch for testing, cleaned up after the test."""
    branch_name = f"{username}.test_{uuid.uuid4().hex[:8]}"
    branch = client.create_branch(branch=branch_name, from_ref="main")
    yield branch
    client.delete_branch(branch=branch_name, if_exists=True)


class TestBranchArg:
    def test_get_branch(self, client):
        """Branch object and string name should produce identical results."""
        branch = client.get_branch("main")

        result_str = client.get_branch("main")
        result_obj = client.get_branch(branch)

        assert result_str.name == result_obj.name
        assert result_str.hash == result_obj.hash

    def test_has_branch(self, client):
        branch = client.get_branch("main")

        result_str = client.has_branch("main")
        result_obj = client.has_branch(branch)

        assert result_str is True
        assert result_obj is True

    def test_delete_branch(self, client, username):
        name = f"{username}.test_{uuid.uuid4().hex[:8]}"
        branch = client.create_branch(branch=name, from_ref="main")
        client.delete_branch(branch)

        assert client.has_branch(name) is False


class TestRefArg:
    def test_create_branch_ref(self, client, username, temp_branch):
        name = f"{username}.test_{uuid.uuid4().hex[:8]}"

        try:
            client.create_branch(
                branch=name,
                from_ref=temp_branch,
            )
        finally:
            client.delete_branch(name, if_exists=True)

    def test_get_table_ref(self, client):
        """Branch object as ref should equal explicit name@hash string."""
        branch = client.get_branch("main")

        table_str = client.get_table(
            table="titanic",
            ref=f"{branch.name}@{branch.hash}",
            namespace="bauplan",
        )

        table_obj = client.get_table(
            table="titanic",
            ref=branch,
            namespace="bauplan",
        )

        assert table_str.name == table_obj.name
        assert table_str.namespace == table_obj.namespace

    def test_get_tables_ref_object_equals_string(self, client):
        """Branch object as ref should equal explicit name@hash string."""
        branch = client.get_branch("main")

        tables_str = list(client.get_tables(
            ref=f"{branch.name}@{branch.hash}",
            filter_by_namespace="bauplan",
            limit=5,
        ))

        tables_obj = list(client.get_tables(
            ref=branch,
            filter_by_namespace="bauplan",
            limit=5,
        ))

        assert len(tables_str) == len(tables_obj)
        assert [t.name for t in tables_str] == [t.name for t in tables_obj]


class TestRefTypes:
    """Tests for ref type properties."""

    def test_branch_type(self, client):
        branch = client.get_branch("main")
        assert branch.type == bauplan.RefType.BRANCH

    def test_branch_str_includes_hash(self, client):
        branch = client.get_branch("main")
        s = str(branch)
        assert s == f"{branch.name}@{branch.hash}"

    def test_tag_type(self, client):
        tags = list(client.get_tags(limit=1))
        if not tags:
            pytest.skip("No tags available")
        assert tags[0].type == bauplan.RefType.TAG

    def test_tag_str_includes_hash(self, client):
        tags = list(client.get_tags(limit=1))
        if not tags:
            pytest.skip("No tags available")
        tag = tags[0]
        assert str(tag) == f"{tag.name}@{tag.hash}"
