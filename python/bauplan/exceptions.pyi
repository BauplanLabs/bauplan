from bauplan.schema import Ref
from bauplan.state import TableCreatePlanApplyState, TableCreatePlanState
from typing import Final, final

class ApiErrorKind:
    """
    A typed API error kind, deserialized from the `type` and `context` fields
    of an error response.
    """
    @final
    class BadRequest(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.BadRequest: ...

    @final
    class BranchExists(ApiErrorKind):
        __match_args__: Final = ("branch_name", "catalog_ref")
        def __new__(
            cls, /, branch_name: str, catalog_ref: Ref
        ) -> ApiErrorKind.BranchExists: ...
        @property
        def branch_name(self, /) -> str: ...
        @property
        def catalog_ref(self, /) -> Ref: ...

    @final
    class BranchHeadChanged(ApiErrorKind):
        __match_args__: Final = ("input_ref", "head_ref")
        def __new__(
            cls, /, input_ref: Ref, head_ref: Ref
        ) -> ApiErrorKind.BranchHeadChanged: ...
        @property
        def head_ref(self, /) -> Ref: ...
        @property
        def input_ref(self, /) -> Ref: ...

    @final
    class BranchNotFound(ApiErrorKind):
        __match_args__: Final = ("branch_name",)
        def __new__(cls, /, branch_name: str) -> ApiErrorKind.BranchNotFound: ...
        @property
        def branch_name(self, /) -> str: ...

    @final
    class CreateBranchForbidden(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.CreateBranchForbidden: ...

    @final
    class CreateNamespaceForbidden(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.CreateNamespaceForbidden: ...

    @final
    class CreateTagForbidden(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.CreateTagForbidden: ...

    @final
    class DeleteBranchForbidden(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.DeleteBranchForbidden: ...

    @final
    class DeleteNamespaceForbidden(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.DeleteNamespaceForbidden: ...

    @final
    class DeleteTableForbidden(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.DeleteTableForbidden: ...

    @final
    class DeleteTagForbidden(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.DeleteTagForbidden: ...

    @final
    class Forbidden(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.Forbidden: ...

    @final
    class InvalidRef(ApiErrorKind):
        __match_args__: Final = ("input_ref",)
        def __new__(cls, /, input_ref: str) -> ApiErrorKind.InvalidRef: ...
        @property
        def input_ref(self, /) -> str: ...

    @final
    class MergeConflict(ApiErrorKind):
        __match_args__: Final = ("source_ref", "destination_ref")
        def __new__(
            cls, /, source_ref: Ref, destination_ref: Ref
        ) -> ApiErrorKind.MergeConflict: ...
        @property
        def destination_ref(self, /) -> Ref: ...
        @property
        def source_ref(self, /) -> Ref: ...

    @final
    class MergeForbidden(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.MergeForbidden: ...

    @final
    class NamespaceExists(ApiErrorKind):
        __match_args__: Final = ("namespace_name", "catalog_ref")
        def __new__(
            cls, /, namespace_name: str, catalog_ref: Ref
        ) -> ApiErrorKind.NamespaceExists: ...
        @property
        def catalog_ref(self, /) -> Ref: ...
        @property
        def namespace_name(self, /) -> str: ...

    @final
    class NamespaceIsNotEmpty(ApiErrorKind):
        __match_args__: Final = ("namespace_name", "branch_name")
        def __new__(
            cls, /, namespace_name: str, branch_name: str
        ) -> ApiErrorKind.NamespaceIsNotEmpty: ...
        @property
        def branch_name(self, /) -> str: ...
        @property
        def namespace_name(self, /) -> str: ...

    @final
    class NamespaceNotFound(ApiErrorKind):
        __match_args__: Final = ("namespace_name", "input_ref", "catalog_ref")
        def __new__(
            cls, /, namespace_name: str, input_ref: str, catalog_ref: Ref
        ) -> ApiErrorKind.NamespaceNotFound: ...
        @property
        def catalog_ref(self, /) -> Ref: ...
        @property
        def input_ref(self, /) -> str: ...
        @property
        def namespace_name(self, /) -> str: ...

    @final
    class NamespaceUnresolved(ApiErrorKind):
        __match_args__: Final = ("table_name", "namespace_name")
        def __new__(
            cls, /, table_name: str, namespace_name: str
        ) -> ApiErrorKind.NamespaceUnresolved: ...
        @property
        def namespace_name(self, /) -> str: ...
        @property
        def table_name(self, /) -> str: ...

    @final
    class NotABranchRef(ApiErrorKind):
        __match_args__: Final = ("input_ref",)
        def __new__(cls, /, input_ref: str) -> ApiErrorKind.NotABranchRef: ...
        @property
        def input_ref(self, /) -> str: ...

    @final
    class NotATagRef(ApiErrorKind):
        __match_args__: Final = ("input_ref",)
        def __new__(cls, /, input_ref: str) -> ApiErrorKind.NotATagRef: ...
        @property
        def input_ref(self, /) -> str: ...

    @final
    class NotAWriteBranchRef(ApiErrorKind):
        __match_args__: Final = ("input_ref",)
        def __new__(cls, /, input_ref: str) -> ApiErrorKind.NotAWriteBranchRef: ...
        @property
        def input_ref(self, /) -> str: ...

    @final
    class RefNotFound(ApiErrorKind):
        __match_args__: Final = ("input_ref",)
        def __new__(cls, /, input_ref: str) -> ApiErrorKind.RefNotFound: ...
        @property
        def input_ref(self, /) -> str: ...

    @final
    class RenameBranchForbidden(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.RenameBranchForbidden: ...

    @final
    class RenameTagForbidden(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.RenameTagForbidden: ...

    @final
    class RevertDestinationTableExists(ApiErrorKind):
        __match_args__: Final = ("source_table_name", "destination_table_name")
        def __new__(
            cls, /, source_table_name: str, destination_table_name: str
        ) -> ApiErrorKind.RevertDestinationTableExists: ...
        @property
        def destination_table_name(self, /) -> str: ...
        @property
        def source_table_name(self, /) -> str: ...

    @final
    class RevertIdenticalTable(ApiErrorKind):
        __match_args__: Final = ("source_table_name", "destination_table_name")
        def __new__(
            cls, /, source_table_name: str, destination_table_name: str
        ) -> ApiErrorKind.RevertIdenticalTable: ...
        @property
        def destination_table_name(self, /) -> str: ...
        @property
        def source_table_name(self, /) -> str: ...

    @final
    class RevertTableForbidden(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.RevertTableForbidden: ...

    @final
    class SameRef(ApiErrorKind):
        __match_args__: Final = ("input_ref", "catalog_ref")
        def __new__(
            cls, /, input_ref: Ref, catalog_ref: Ref
        ) -> ApiErrorKind.SameRef: ...
        @property
        def catalog_ref(self, /) -> Ref: ...
        @property
        def input_ref(self, /) -> Ref: ...

    @final
    class TableNotFound(ApiErrorKind):
        __match_args__: Final = ("table_name", "input_ref", "catalog_ref")
        def __new__(
            cls, /, table_name: str, input_ref: str, catalog_ref: Ref
        ) -> ApiErrorKind.TableNotFound: ...
        @property
        def catalog_ref(self, /) -> Ref: ...
        @property
        def input_ref(self, /) -> str: ...
        @property
        def table_name(self, /) -> str: ...

    @final
    class TableExists(ApiErrorKind):
        __match_args__: Final = ("table_name", "catalog_ref")
        def __new__(
            cls, /, table_name: str, catalog_ref: Ref
        ) -> ApiErrorKind.TableExists: ...
        @property
        def catalog_ref(self, /) -> Ref: ...
        @property
        def table_name(self, /) -> str: ...

    @final
    class TagExists(ApiErrorKind):
        __match_args__: Final = ("tag_name", "catalog_ref")
        def __new__(
            cls, /, tag_name: str, catalog_ref: Ref
        ) -> ApiErrorKind.TagExists: ...
        @property
        def catalog_ref(self, /) -> Ref: ...
        @property
        def tag_name(self, /) -> str: ...

    @final
    class TagNotFound(ApiErrorKind):
        __match_args__: Final = ("tag_name",)
        def __new__(cls, /, tag_name: str) -> ApiErrorKind.TagNotFound: ...
        @property
        def tag_name(self, /) -> str: ...

    @final
    class Unauthorized(ApiErrorKind):
        __match_args__: Final = ()
        def __new__(cls, /) -> ApiErrorKind.Unauthorized: ...

class BauplanError(Exception):
    """
    Base class for all bauplan SDK exceptions.
    """
    def __new__(cls, /, *_args) -> BauplanError: ...

class BauplanHTTPError(BauplanError):
    """
    An HTTP error from the API.
    """
    def __new__(
        cls, /, code: int, type: str, message: str, kind: ApiErrorKind | None = None
    ) -> BauplanHTTPError: ...
    @property
    def code(self, /) -> int: ...
    @property
    def kind(self, /) -> ApiErrorKind | None: ...
    @property
    def message(self, /) -> str: ...
    @property
    def type(self, /) -> str: ...

@final
class TableCreatePlanApplyStatusError(BauplanError):
    """
    Raised when a table-create plan apply job finishes in a non-success state.
    """
    def __new__(
        cls, /, message: str, state: TableCreatePlanApplyState
    ) -> TableCreatePlanApplyStatusError: ...
    @property
    def message(self, /) -> str: ...
    @property
    def state(self, /) -> TableCreatePlanApplyState: ...

class TableCreatePlanError(BauplanError):
    """
    Base class for errors raised during a table-create plan workflow.
    """
    def __new__(cls, /) -> TableCreatePlanError: ...

@final
class TableCreatePlanStatusError(TableCreatePlanError):
    """
    Raised when a table-create plan job finishes in a non-success state.
    """
    def __new__(
        cls, /, message: str, state: TableCreatePlanState
    ) -> TableCreatePlanStatusError: ...
    @property
    def message(self, /) -> str: ...
    @property
    def state(self, /) -> TableCreatePlanState: ...

# 400 Bad Request
class BadRequestError(BauplanHTTPError):
    """Raised on an HTTP 400 response from the API."""

class InvalidDataError(BadRequestError):
    """Raised on an HTTP 400 response from the API."""

class InvalidRefError(BadRequestError):
    """Raised when the provided string is not a valid `bauplan.schema.Ref`."""

class NotABranchRefError(InvalidRefError):
    """Raised when the provided `bauplan.schema.Ref` is not of type `bauplan.schema.Branch` but a `bauplan.schema.Branch` was required."""

class NotATagRefError(InvalidRefError):
    """Raised when the provided `bauplan.schema.Ref` is not of type `bauplan.schema.Tag` but a `bauplan.schema.Tag` was required."""

class NotAWriteBranchRefError(NotABranchRefError):
    """Raised when the provided `bauplan.schema.Ref` is a `bauplan.schema.Branch` but not writable."""

class SameRefError(InvalidRefError):
    """Raised when the source and destination `bauplan.schema.Ref` values of an operation are the same."""

# 401 Unauthorized
class UnauthorizedError(BauplanHTTPError):
    """Raised on an HTTP 401 response: missing or invalid credentials."""

# 403 Forbidden
class ForbiddenError(BauplanHTTPError):
    """Raised on an HTTP 403 response: the caller is not permitted to perform the action."""

class CreateBranchForbiddenError(ForbiddenError):
    """Raised when the caller is not permitted to create a `bauplan.schema.Branch`."""

class CreateNamespaceForbiddenError(ForbiddenError):
    """Raised when the caller is not permitted to create a `bauplan.schema.Namespace`."""

class CreateTagForbiddenError(ForbiddenError):
    """Raised when the caller is not permitted to create a `bauplan.schema.Tag`."""

class DeleteBranchForbiddenError(ForbiddenError):
    """Raised when the caller is not permitted to delete a `bauplan.schema.Branch`."""

class DeleteNamespaceForbiddenError(ForbiddenError):
    """Raised when the caller is not permitted to delete a `bauplan.schema.Namespace`."""

class DeleteTableForbiddenError(ForbiddenError):
    """Raised when the caller is not permitted to delete tables."""

class DeleteTagForbiddenError(ForbiddenError):
    """Raised when the caller is not permitted to delete a `bauplan.schema.Tag`."""

class MergeForbiddenError(ForbiddenError):
    """Raised when the caller is not permitted to merge a `bauplan.schema.Branch`."""

class RenameBranchForbiddenError(ForbiddenError):
    """Raised when the caller is not permitted to rename a `bauplan.schema.Branch`."""

class RenameTagForbiddenError(ForbiddenError):
    """Raised when the caller is not permitted to rename a `bauplan.schema.Tag`."""

class RevertTableForbiddenError(ForbiddenError):
    """Raised when the caller is not permitted to revert tables."""

# 404 Not Found
class NotFoundError(BauplanHTTPError):
    """Raised on an HTTP 404 response from the API."""

class ResourceNotFoundError(NotFoundError):
    """Raised when a requested catalog resource does not exist."""

class TableNotFoundError(ResourceNotFoundError):
    """Raised when the referenced table does not exist on the given `bauplan.schema.Ref`."""

class NamespaceNotFoundError(ResourceNotFoundError):
    """Raised when the referenced `bauplan.schema.Namespace` does not exist on the given `bauplan.schema.Ref`."""

class BranchNotFoundError(ResourceNotFoundError):
    """Raised when the referenced `bauplan.schema.Branch` does not exist."""

class RefNotFoundError(ResourceNotFoundError):
    """Raised when the referenced `bauplan.schema.Ref` does not exist."""

class TagNotFoundError(ResourceNotFoundError):
    """Raised when the referenced `bauplan.schema.Tag` does not exist."""

class ApiMethodError(ResourceNotFoundError):
    """Raised on an HTTP 404 response from the API."""

# 405 Method Not Allowed
class MethodNotAllowedError(BauplanHTTPError):
    """Raised on an HTTP 405 response from the API."""

class ApiRouteError(MethodNotAllowedError):
    """Raised on an HTTP 405 response from the API."""

# 409 Conflict
class ConflictError(BauplanHTTPError):
    """Raised on an HTTP 409 response from the API."""

class UpdateConflictError(ConflictError):
    """Raised when an update conflicts with the current catalog state."""

class BranchExistsError(UpdateConflictError):
    """Raised when creating a `bauplan.schema.Branch` that already exists."""

class TableExistsError(UpdateConflictError):
    """Raised when creating a table that already exists on the target `bauplan.schema.Ref`."""

class TagExistsError(UpdateConflictError):
    """Raised when creating a `bauplan.schema.Tag` that already exists."""

class NamespaceExistsError(UpdateConflictError):
    """Raised when creating a `bauplan.schema.Namespace` that already exists on the target `bauplan.schema.Ref`."""

class NamespaceUnresolvedError(ConflictError):
    """Raised when a `bauplan.schema.Namespace` reference cannot be resolved."""

class BranchHeadChangedError(UpdateConflictError):
    """Raised when the `bauplan.schema.Branch` head hash has changed since it was last read."""

class MergeConflictError(UpdateConflictError):
    """Raised when a merge cannot be completed due to conflicting changes."""

class NamespaceIsNotEmptyError(UpdateConflictError):
    """Raised when attempting to delete a `bauplan.schema.Namespace` that still contains tables."""

class RevertDestinationTableExistsError(UpdateConflictError):
    """Raised when the destination of a revert operation already exists."""

class RevertIdenticalTableError(UpdateConflictError):
    """Raised when the source and destination of a revert are the same table."""

# 429 Too Many Requests
class TooManyRequestsError(BauplanHTTPError):
    """Raised on an HTTP 429 response from the API."""

# 5xx Server Errors
class InternalError(BauplanHTTPError):
    """Raised on an HTTP 500 response from the API."""

class BadGatewayError(BauplanHTTPError):
    """Raised on an HTTP 502 response from the API."""

class ServiceUnavailableError(BauplanHTTPError):
    """Raised on an HTTP 503 response from the API."""

class GatewayTimeoutError(BauplanHTTPError):
    """Raised on an HTTP 504 response from the API."""

# Non-HTTP errors
class BauplanJobError(BauplanError):
    """Base class for errors raised by bauplan job execution."""

class BauplanQueryError(BauplanJobError):
    """Raised when a query job fails."""

class NoResultsFoundError(BauplanError):
    """Raised when a query returns no results."""

class InvalidPlanError(BauplanError):
    """Raised when a pipeline or table-create plan is invalid."""
