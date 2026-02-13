# Stub Generation

This directory contains tooling for generating Python type stubs (`.pyi` files)
for the bauplan SDK.

## Background

The bauplan Python SDK is implemented as a Rust extension module using PyO3. The
`pyo3-introspection` crate can extract type information from the compiled
module, but it has limitations:

- Many types come out as `typing.Any` when PyO3 can't infer them
- Complex types like `datetime.datetime`, `list[T]`, `dict[K, V]` often degrade
- Enum variants and their associated data aren't fully captured
- Docstrings from Rust don't always transfer

## Workflow

The stubs live in `python/bauplan/_internal/` and are committed to the repo.

### Generating stubs

Build the extension and run the generator:

    cargo build --release --features python
    cargo run -p gen-stubs

This outputs the auto-generated stubs to stdout, with `# filename`
headers separating each file. The module has submodules (`schema`, `state`,
`exceptions`), so the output includes stubs for each.

### Refining types

Replace `typing.Any` with correct types by examining:

- The Rust source in `src/python/*.rs` (PyO3 method signatures)
- The API types in `src/api/*.rs` (underlying data structures)
- The original Python SDK in `~/dev/bpln/all-events/cli/bauplan/`

### Validating stubs

After editing stubs, run the type checker to catch errors:

    uv run ty check

### Preserving refinements

When regenerating after Rust changes, merge new methods/classes while keeping
hand-refined type annotations. The stdout output makes it easy to diff against
the existing stubs and selectively apply changes.

## Type Mapping Reference

Common patterns from Rust to Python stubs:

| Rust Type | Python Stub Type |
|-----------|------------------|
| `Option<T>` | `T \| None` |
| `Vec<T>` | `list[T]` |
| `HashMap<K, V>` | `dict[K, V]` |
| `String` / `&str` | `str` |
| `i32`, `i64`, `u32`, `u64` | `int` |
| `f32`, `f64` | `float` |
| `bool` | `bool` |
| `Uuid` | `str` (displayed as UUID string) |
| `chrono::DateTime<Utc>` | `datetime.datetime` |
| `PyResult<T>` | `T` (errors become exceptions) |
| Iterator types | `typing.Iterator[T]` |
| `Py<PyAny>` returning pyarrow | `pyarrow.Table` |

## Key Files

Rust sources to consult:

- `src/python.rs` - Module structure and re-exports
- `src/python/schema.rs` - Schema submodule (refs, catalog, job types)
- `src/python/state.rs` - State submodule (run/import/plan state types)
- `src/python/client.rs` - Client class definition
- `src/python/query.rs` - Query methods
- `src/python/run.rs` - Run/rerun methods and state types
- `src/python/table.rs` - Table operations
- `src/python/refs.rs` - Ref, Branch, Tag types
- `src/python/job.rs` - Job types
- `src/api/*.rs` - Underlying API types

Original SDK (for interface compatibility):

- `~/dev/bpln/all-events/cli/bauplan/_client.py`
- `~/dev/bpln/all-events/cli/bauplan/state.py`
- `~/dev/bpln/all-events/cli/bauplan/schema.py`
- `~/dev/bpln/all-events/cli/bauplan/__init__.py`

## Common Fixes

### Fix type references

The auto-generated stubs use fully-qualified names like `bauplan.X` or
`bauplan.schema.X`. These need to be fixed per file:

- In `schema.pyi` and `state.pyi`: strip the `bauplan.` prefix so
  `bauplan.Actor` becomes `Actor` (the type is local to the submodule).
- In `exceptions.pyi`: strip `bauplan.exceptions.` for self-references (e.g.
  `bauplan.exceptions.ApiErrorKind` -> `ApiErrorKind`). Cross-references to
  other submodules like `bauplan.TableCreatePlanApplyState` need an import
  from the sibling submodule (e.g.
  `from bauplan._internal.state import TableCreatePlanApplyState`).
- In `__init__.pyi`: types that live in submodules (like `Branch`, `Table`)
  appear as bare names without imports. Add imports from
  `bauplan._internal.schema` / `bauplan._internal.state` as needed.

### Properties that typically need fixing from `typing.Any`

```python
# Dates - look for chrono types in Rust
authored_date -> datetime.datetime | None
created_at -> datetime.datetime | None
finished_at -> datetime.datetime | None

# Lists of known types
authors -> list[Actor]
fields -> list[TableField]
runners -> list[RunnerNodeInfo]

# Nested objects
organization -> OrganizationInfo | None
user -> UserInfo | None
committer -> Actor

# String-like identifiers that are UUIDs or hashes
id -> str
hash -> str
catalog_ref -> Ref

# Dicts with known structure
properties -> dict[str, str]
```

## Stub-Only Types

Some types exist only in Python (not implemented in Rust). These belong in pure
Python modules under `python/bauplan/`, not in `_internal`. Examples:

- `bauplan.Model` (decorator infrastructure)
- `bauplan.Parameter`

The `schema`, `state`, and `exceptions` submodules are implemented in Rust
as `_internal` submodules. They are imported directly as submodule attributes
in `__init__.py` (no wrapper `.py` files needed).
