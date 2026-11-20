"""Tests for exception types, hierarchy, and typed context fields."""

import uuid

import bauplan
import pytest
from bauplan import exceptions


@pytest.fixture
def client() -> bauplan.Client:
    return bauplan.Client()


@pytest.fixture
def username(client: bauplan.Client):
    return client.info().user.username


@pytest.fixture
def temp_branch(client: bauplan.Client, username: str):
    branch_name = f"{username}.test_{uuid.uuid4().hex[:8]}"
    client.create_branch(branch=branch_name, from_ref="main")
    yield branch_name
    client.delete_branch(branch_name, if_exists=True)


class TestExceptionHierarchy:
    def test_table_not_found_is_resource_not_found(self):
        assert issubclass(
            exceptions.TableNotFoundError, exceptions.ResourceNotFoundError
        )

    def test_table_not_found_is_not_found(self):
        assert issubclass(exceptions.TableNotFoundError, exceptions.NotFoundError)

    def test_table_not_found_is_http_error(self):
        assert issubclass(exceptions.TableNotFoundError, exceptions.BauplanHTTPError)

    def test_unauthorized_is_http_error(self):
        assert issubclass(exceptions.UnauthorizedError, exceptions.BauplanHTTPError)

    def test_http_error_is_bauplan_error(self):
        assert issubclass(exceptions.BauplanHTTPError, exceptions.BauplanError)

    def test_namespace_not_found_is_resource_not_found(self):
        assert issubclass(
            exceptions.NamespaceNotFoundError, exceptions.ResourceNotFoundError
        )

    def test_branch_exists_is_update_conflict(self):
        assert issubclass(exceptions.BranchExistsError, exceptions.UpdateConflictError)

    def test_branch_exists_is_conflict(self):
        assert issubclass(exceptions.BranchExistsError, exceptions.ConflictError)

    def test_tag_exists_is_update_conflict(self):
        assert issubclass(exceptions.TagExistsError, exceptions.UpdateConflictError)

    def test_namespace_exists_is_update_conflict(self):
        assert issubclass(
            exceptions.NamespaceExistsError, exceptions.UpdateConflictError
        )

    def test_table_exists_is_update_conflict(self):
        assert issubclass(exceptions.TableExistsError, exceptions.UpdateConflictError)

    def test_plan_status_error_is_plan_error(self):
        assert issubclass(
            exceptions.TableCreatePlanStatusError, exceptions.TableCreatePlanError
        )

    def test_plan_error_is_bauplan_error(self):
        assert issubclass(exceptions.TableCreatePlanError, exceptions.BauplanError)

    def test_plan_apply_status_error_is_bauplan_error(self):
        assert issubclass(
            exceptions.TableCreatePlanApplyStatusError, exceptions.BauplanError
        )


class TestTableNotFound:
    def test_delete_nonexistent_table_raises(
        self, client: bauplan.Client, temp_branch: str
    ):
        with pytest.raises(exceptions.TableNotFoundError) as exc_info:
            client.delete_table("bauplan.nonexistent_table_xyz", temp_branch)

        e = exc_info.value
        assert e.type == "TABLE_NOT_FOUND"
        assert "nonexistent_table_xyz" in e.message

    def test_delete_table_if_exists(self, client: bauplan.Client, temp_branch: str):
        commit = client.delete_table(
            "bauplan.nonexistent_table_xyz", temp_branch, if_exists=True
        )
        assert commit is None

    def test_has_table_false(self, client: bauplan.Client):
        assert client.has_table("bauplan.nonexistent_table_xyz", "main") is False


class TestBranchNameConflict:
    def test_create_duplicate_branch(self, client: bauplan.Client, temp_branch: str):
        with pytest.raises(exceptions.BranchNameConflictError) as exc_info:
            client.create_branch(branch=temp_branch, from_ref="main")

        e = exc_info.value
        assert e.code == 409
        assert e.type == "BRANCH_NAME_CONFLICT"
        assert temp_branch in e.message

    def test_create_branch_if_not_exists(
        self, client: bauplan.Client, temp_branch: str
    ):
        branch = client.create_branch(
            branch=temp_branch, from_ref="main", if_not_exists=True
        )
        assert branch is None


class TestNamespaceNotFound:
    def test_delete_nonexistent_namespace(
        self, client: bauplan.Client, temp_branch: str
    ):
        with pytest.raises(exceptions.NamespaceNotFoundError) as exc_info:
            client.delete_namespace("nonexistent_ns_xyz", temp_branch)

        e = exc_info.value
        assert e.type == "NAMESPACE_NOT_FOUND"
        assert "nonexistent_ns_xyz" in e.message


class TestExceptionInstantiation:
    def test_bauplan_error_instantiation(self):
        e = exceptions.BauplanError("test error")
        assert isinstance(e, exceptions.BauplanError)

    def test_query_error_instantiation(self):
        e = exceptions.BauplanQueryError("test error")
        assert isinstance(e, exceptions.BauplanQueryError)
        assert isinstance(e, exceptions.BauplanJobError)
        assert isinstance(e, exceptions.BauplanError)

    def test_job_error_instantiation(self):
        e = exceptions.BauplanJobError("test error")
        assert isinstance(e, exceptions.BauplanJobError)
        assert isinstance(e, exceptions.BauplanError)


class TestUnauthorized:
    def test_invalid_api_key(self):
        client = bauplan.Client(api_key="invalid")
        with pytest.raises(exceptions.UnauthorizedError) as exc_info:
            client.get_branch("main")

        e = exc_info.value
        assert e.code == 401
        assert e.type == "UNAUTHORIZED"


class TestHttpErrorProperties:
    def test_table_not_found_has_code(self, client: bauplan.Client):
        with pytest.raises(exceptions.TableNotFoundError) as exc_info:
            client.get_table("bauplan.nonexistent_table_xyz", "main")

        e = exc_info.value
        assert e.code == 404
        assert e.type == "TABLE_NOT_FOUND"
        assert "nonexistent_table_xyz" in e.message
