from typing import Any, Self, final

from typing_extensions import disjoint_base

from bauplan.schema import Entity
from bauplan.state import TableCreatePlanApplyState, TableCreatePlanState

@disjoint_base
class BauplanError(Exception):
    """
    Base class for all bauplan SDK exceptions.
    """
    def __new__(cls, /, *_args) -> Self: ...

@disjoint_base
class BauplanStatusError(BauplanError):
    """
    An HTTP error, with the status of the response.

    The errors with a type we know are subclasses of `BauplanAPIError`. We raise
    this class for the others, like a 502 from a proxy or an error added after
    this release.
    """
    def __new__(cls, /, status: int, message: str) -> Self: ...
    @property
    def message(self, /) -> str: ...
    @property
    def status(self, /) -> int: ...

@disjoint_base
class BauplanAPIError(BauplanStatusError):
    """
    An error from the API, with a type we know.

    Each type of error has its own subclass. The fields of the error are in
    `details`, and each one is also an attribute.
    """
    def __new__(
        cls,
        /,
        status: int,
        type: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> Self: ...
    @property
    def details(self, /) -> dict[str, Any]: ...
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
    def __new__(cls, /) -> Self: ...

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

# API errors
class BadRequestError(BauplanAPIError):
    """Raised when the request is not valid."""

class BranchHeadChangedError(BauplanAPIError):
    """Raised when the `bauplan.schema.Branch` head hash has changed since it was last read."""
    @property
    def branch_name(self, /) -> str: ...
    @property
    def commit_hash(self, /) -> str: ...

class BranchNameConflictError(BauplanAPIError):
    """Raised when a `bauplan.schema.Branch` with the name already exists."""
    @property
    def name(self, /) -> str: ...

class BranchNotFoundError(BauplanAPIError):
    """Raised when the referenced `bauplan.schema.Branch` does not exist."""
    @property
    def branch_name(self, /) -> str: ...

class CatalogNotFoundError(BauplanAPIError):
    """Raised when the catalog does not exist."""
    @property
    def catalog_name(self, /) -> str: ...

class ForbiddenError(BauplanAPIError):
    """Raised when the caller is not permitted to perform the action."""

class FromRefNotFoundError(BauplanAPIError):
    """Raised when the ref to start from does not exist."""
    @property
    def from_ref(self, /) -> str: ...

class FromTableNotFoundError(BauplanAPIError):
    """Raised when the table to start from does not exist at its ref."""
    @property
    def from_ref(self, /) -> str: ...
    @property
    def from_table(self, /) -> str: ...

class InternalError(BauplanAPIError):
    """Raised when the API fails on its side."""

class MainBranchError(BauplanAPIError):
    """Raised when the operation is not allowed on the main branch."""

class MergeConflictError(BauplanAPIError):
    """Raised when a merge cannot be completed due to conflicting changes."""
    @property
    def commit_hash(self, /) -> str: ...
    @property
    def conflicts(self, /) -> list[Entity]: ...

class NameConflictError(BauplanAPIError):
    """Raised when a namespace or a table with the name already exists."""
    @property
    def name(self, /) -> str: ...

class NamespaceNotEmptyError(BauplanAPIError):
    """Raised when attempting to delete a namespace that still contains tables."""
    @property
    def namespace_name(self, /) -> str: ...

class NamespaceNotFoundError(BauplanAPIError):
    """Raised when the referenced namespace does not exist at the given ref."""
    @property
    def namespace_name(self, /) -> str: ...

class NotABranchError(BauplanAPIError):
    """Raised when the provided ref is not a branch."""
    @property
    def ref_name(self, /) -> str: ...

class RefNotFoundError(BauplanAPIError):
    """Raised when the referenced ref does not exist."""
    @property
    def ref_name(self, /) -> str: ...

class ServiceUnavailableError(BauplanAPIError):
    """Raised when the API is temporarily unavailable."""

class TableNotFoundError(BauplanAPIError):
    """Raised when the referenced table does not exist at the given ref."""
    @property
    def table_name(self, /) -> str: ...

class TagChangedError(BauplanAPIError):
    """Raised when a `bauplan.schema.Tag` changed during the request."""
    @property
    def tag_name(self, /) -> str: ...

class TagNameConflictError(BauplanAPIError):
    """Raised when a `bauplan.schema.Tag` with the name already exists."""
    @property
    def name(self, /) -> str: ...

class TagNotFoundError(BauplanAPIError):
    """Raised when the referenced `bauplan.schema.Tag` does not exist."""
    @property
    def tag_name(self, /) -> str: ...

class UnauthorizedError(BauplanAPIError):
    """Raised when the credentials are missing or not valid."""

class UnsupportedTableKindError(BauplanAPIError):
    """Raised when the kind of the table does not support the operation."""
    @property
    def from_table(self, /) -> str: ...

# Non-HTTP errors
class BauplanJobError(BauplanError):
    """Base class for errors raised by bauplan job execution."""

class BauplanQueryError(BauplanJobError):
    """Raised when a query job fails."""
