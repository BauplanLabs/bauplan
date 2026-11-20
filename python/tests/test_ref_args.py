"""Tests for the ref objects."""

import bauplan
import pytest


@pytest.fixture
def client() -> bauplan.Client:
    return bauplan.Client()


class TestRefTypes:
    """Tests for ref type properties."""

    def test_branch_type(self, client: bauplan.Client):
        branch = client.get_branch("main")
        assert branch.type == bauplan.RefType.BRANCH

    def test_branch_str_includes_hash(self, client: bauplan.Client):
        branch = client.get_branch("main")
        s = str(branch)
        assert s == f"{branch.name}@{branch.hash}"

    def test_tag_type(self, client: bauplan.Client):
        tags = list(client.get_tags(limit=1))
        if not tags:
            pytest.skip("No tags available")
        assert tags[0].type == bauplan.RefType.TAG

    def test_tag_str_includes_hash(self, client: bauplan.Client):
        tags = list(client.get_tags(limit=1))
        if not tags:
            pytest.skip("No tags available")
        tag = tags[0]
        assert str(tag) == f"{tag.name}@{tag.hash}"
