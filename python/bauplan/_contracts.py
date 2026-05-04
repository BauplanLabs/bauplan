"""
Module containing support classes for type contracts.

This includes a "registry" metaclass, `RegistryMeta`, that enables type-based syntax and
value-based syntax for a registry. For example, the Artifact registry supports type-based syntax
such as `Artifact[<schema_type>]` to represent the schema of a table that has been materialized in
the artifact store. In contrast, Parameter registry supports value-based syntax such as
`Parameter.param_name` to represent the value of the bauplan project parameter "param_name".
"""

import functools
from typing import Any, Callable, LiteralString, Optional, TypeVar, Generic

from bauplan._internal import __bpln_feature_typecontracts__


if __bpln_feature_typecontracts__:

    # Experimental: "registry" structures for Generic types

    EntryNameType = TypeVar(name='EntryNameType', bound=LiteralString)
    class GenericTypeEntry(Generic[EntryNameType]):
        """
        A base generic type for entries in a Type Registry. This supports syntax like
        `Registry['entry_name']` to specify that an object's type corresponds to the entry,
        'entry_name', in the registry 'Registry'.
        """

        ...

    class SchemaEntry(GenericTypeEntry[EntryNameType]):
        """A type for schemas stored in a Type Registry."""

        ...


    class TypeRegistryMeta(type):
        """
        A metaclass for a registry to support subscript syntax like `Registry['entry']` for use in type
        annotations.
        """

        def __init__(cls, name, bases, namespace):
            super().__init__(name, bases, namespace)

            cls._type_entries: dict[str, Any] = {}

        def __getitem__(cls, schema: type | str) -> SchemaEntry:
            schema_typename = ''
            if isinstance(schema, str):
                schema_typename = schema
            elif isinstance(schema, type):
                schema_typename = str(schema)

            if not schema_typename:
                raise TypeError(f'`{cls.__name__}` annotation must be a schema type or name')

            return cls._type_entries.get(schema_typename)

        def register(cls, name: str):
            cls._type_entries[name] = f'SchemaEntry["{name}"]'


    class ValRegistryMeta(type):
        """
        A metaclass for a registry to support subscript syntax like `Registry['entry']` and
        attribute syntax like `Registry.entry`.
        """

        def __init__(cls, name, bases, namespace):
            super().__init__(name, bases, namespace)

            cls._val_entries: dict[str, Any] = {}

        def __getattr__(cls, name: str, /) -> Any:
            """
            Accessor for a Registry that returns the registered value (defaults to `None`).
            """

            return cls._val_entries.get(name)

        def register(cls, name: str, entry_val: Optional[Any] = None):
            cls._val_entries[name] = entry_val


    # Experimental: return types for model tasks
    class Artifact(metaclass=TypeRegistryMeta):
        """
        A registry for artifact schemas. Supports subscript syntax `Artifact['name']`
        to reference a schema entry by name.
        """

        ...

    class Catalog(metaclass=TypeRegistryMeta):
        """
        A registry for catalog schemas. Supports subscript syntax `Catalog['name']`
        to reference a schema entry by name.
        """

        ...


    # Experimental: base types for defining table schemas
    class TableSchemaMeta(type):

        def __new__(metacls, name: str, bases: tuple[type, ...], namespace: dict[str, Any]):
            new_schema_cls = super().__new__(metacls, name, bases, namespace)

            return new_schema_cls


        def __init__(self, name: str, bases: tuple[type, ...], namespace: dict[str, Any]):
            super().__init__(name, bases, namespace)

    class TableSchema(metaclass=TableSchemaMeta):

        def __init__(self, **kwargs):
            super().__init__(**kwargs)


    # Experimental: proxy types for model tasks
    class ModelTask:
        """
        A proxy object representing a model task in a DAG.

        Wraps a decorated model function so that it can be called directly
        while carrying extra metadata (task name, result schema, etc.).
        Inspection of the instance (signature, docstring, name) delegates
        to the wrapped function via ``functools.update_wrapper``.
        """

        def __init__(
            self,
            task_name: str,
            task_fn: Callable,
            result_schema: Optional[str] = None,
        ):
            self._bpln_task_name = task_name
            self._bpln_task_fn = task_fn
            self._bpln_task_api: str = "pyarrow"
            self._bpln_result_schema = result_schema
            functools.update_wrapper(self, task_fn)

        def __call__(self, *args: Any, **kwargs: Any) -> Any:
            return self._bpln_task_fn(*args, **kwargs)


else:
    import warnings
    warnings.warn(
        'bauplan._contracts: type contract feature '
        'is not enabled, classes are unavailable',
        stacklevel=2,
    )
