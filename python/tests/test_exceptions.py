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
    def test_table_not_found_is_api_error(self):
        assert exceptions.TableNotFoundError.__bases__ == (exceptions.BauplanAPIError,)

    def test_unauthorized_is_api_error(self):
        assert exceptions.UnauthorizedError.__bases__ == (exceptions.BauplanAPIError,)

    def test_api_error_is_status_error(self):
        assert exceptions.BauplanAPIError.__bases__ == (exceptions.BauplanStatusError,)

    def test_status_error_is_bauplan_error(self):
        assert exceptions.BauplanStatusError.__bases__ == (exceptions.BauplanError,)

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
        assert e.status == 409
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

    def test_api_error_instantiation(self):
        e = exceptions.TableNotFoundError(
            404, "TABLE_NOT_FOUND", "Table 'x' not found.", {"table_name": "x"}
        )
        assert isinstance(e, exceptions.BauplanAPIError)
        assert str(e) == "Table 'x' not found."
        assert e.status == 404
        assert e.type == "TABLE_NOT_FOUND"
        assert e.table_name == "x"

    def test_status_error_instantiation(self):
        e = exceptions.BauplanStatusError(502, "Bad gateway.")
        assert isinstance(e, exceptions.BauplanError)
        assert str(e) == "Bad gateway."
        assert e.status == 502

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
        assert e.status == 401
        assert e.type == "UNAUTHORIZED"


class TestApiErrorProperties:
    def test_table_not_found_has_fields(self, client: bauplan.Client):
        with pytest.raises(exceptions.TableNotFoundError) as exc_info:
            client.get_table("bauplan.nonexistent_table_xyz", "main")

        e = exc_info.value
        assert e.type == "TABLE_NOT_FOUND"
        assert "nonexistent_table_xyz" in e.message
        assert "nonexistent_table_xyz" in e.table_name
