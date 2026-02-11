"""Tests for exception types, hierarchy, and typed context fields."""

import uuid
import pytest
import bauplan
from bauplan import exceptions


@pytest.fixture
def client():
    return bauplan.Client()


@pytest.fixture
def username(client):
    return client.info().user.username


@pytest.fixture
def temp_branch(client, username):
    branch_name = f"{username}.test_{uuid.uuid4().hex[:8]}"
    client.create_branch(branch=branch_name, from_ref="main")
    yield branch_name
    client.delete_branch(branch_name, if_exists=True)


class TestExceptionHierarchy:
    def test_table_not_found_is_resource_not_found(self):
        assert issubclass(exceptions.TableNotFoundError, exceptions.ResourceNotFoundError)

    def test_table_not_found_is_not_found(self):
        assert issubclass(exceptions.TableNotFoundError, exceptions.NotFoundError)

    def test_table_not_found_is_http_error(self):
        assert issubclass(exceptions.TableNotFoundError, exceptions.BauplanHTTPError)

    def test_http_error_is_bauplan_error(self):
        assert issubclass(exceptions.BauplanHTTPError, exceptions.BauplanError)

    def test_namespace_not_found_is_resource_not_found(self):
        assert issubclass(exceptions.NamespaceNotFoundError, exceptions.ResourceNotFoundError)

    def test_branch_exists_is_update_conflict(self):
        assert issubclass(exceptions.BranchExistsError, exceptions.UpdateConflictError)

    def test_branch_exists_is_conflict(self):
        assert issubclass(exceptions.BranchExistsError, exceptions.ConflictError)

    def test_tag_exists_is_update_conflict(self):
        assert issubclass(exceptions.TagExistsError, exceptions.UpdateConflictError)

    def test_namespace_exists_is_update_conflict(self):
        assert issubclass(exceptions.NamespaceExistsError, exceptions.UpdateConflictError)

    def test_plan_status_error_is_plan_error(self):
        assert issubclass(exceptions.TableCreatePlanStatusError, exceptions.TableCreatePlanError)

    def test_plan_error_is_bauplan_error(self):
        assert issubclass(exceptions.TableCreatePlanError, exceptions.BauplanError)

    def test_plan_apply_status_error_is_bauplan_error(self):
        assert issubclass(exceptions.TableCreatePlanApplyStatusError, exceptions.BauplanError)


class TestTableNotFoundContext:
    def test_get_nonexistent_table(self, client):
        with pytest.raises(exceptions.TableNotFoundError) as exc_info:
            client.get_table("nonexistent_table_xyz", "main")

        e = exc_info.value
        assert e.kind is not None
        assert "nonexistent_table_xyz" in e.kind.table_name
        assert e.kind.catalog_ref is not None
        assert e.kind.catalog_ref.type == bauplan.RefType.BRANCH

    def test_delete_nonexistent_table_raises(self, client, temp_branch):
        with pytest.raises(exceptions.TableNotFoundError) as exc_info:
            client.delete_table("nonexistent_table_xyz", temp_branch)

        e = exc_info.value
        assert e.kind is not None
        assert "nonexistent_table_xyz" in e.kind.table_name

    def test_delete_table_if_exists(self, client, temp_branch):
        ref = client.delete_table(
            "nonexistent_table_xyz", temp_branch, if_exists=True
        )
        assert ref.type == bauplan.RefType.BRANCH

    def test_has_table_false(self, client):
        assert client.has_table("nonexistent_table_xyz", "main") is False


class TestBranchExistsContext:
    def test_create_duplicate_branch(self, client, temp_branch):
        with pytest.raises(exceptions.BranchExistsError) as exc_info:
            client.create_branch(branch=temp_branch, from_ref="main")

        e = exc_info.value
        assert e.kind is not None
        assert e.kind.catalog_ref is not None
        assert e.kind.catalog_ref.type == bauplan.RefType.BRANCH

    def test_create_branch_if_not_exists(self, client, temp_branch):
        branch = client.create_branch(
            branch=temp_branch, from_ref="main", if_not_exists=True
        )
        assert branch.name == temp_branch
        assert branch.type == bauplan.RefType.BRANCH


class TestNamespaceNotFoundContext:
    def test_get_table_bad_namespace(self, client):
        with pytest.raises(exceptions.NamespaceNotFoundError) as exc_info:
            client.get_table(
                "titanic", "main", namespace="nonexistent_ns_xyz"
            )

        e = exc_info.value
        assert e.kind is not None
        assert e.kind.namespace_name == "nonexistent_ns_xyz"
        assert e.kind.catalog_ref is not None


class TestHttpErrorProperties:
    def test_table_not_found_has_code(self, client):
        with pytest.raises(exceptions.TableNotFoundError) as exc_info:
            client.get_table("nonexistent_table_xyz", "main")

        e = exc_info.value
        assert e.code == 404
        assert e.type == "TABLE_NOT_FOUND"
        assert "nonexistent_table_xyz" in e.message
