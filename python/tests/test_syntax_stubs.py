"""Tests for the pysdk stubs as a syntax."""

import importlib.util
from pathlib import Path

import pytest

from bauplan import TableSchema

fixture_rootdir = Path(__file__).parents[2] / "tests" / "fixtures"
syntax_fixtures = ["success-syntax-smoke"]


@pytest.mark.parametrize("fixture_name", syntax_fixtures, ids=syntax_fixtures)
def test_type_contract_syntax(fixture_name: str):
    """
    Test of type contract syntax.

    Reads the source of a models.py file that tries to maximize coverage of sdk stub
    definitions so as to evaluate the syntax for syntactical errors.
    """

    fixture_dirpath = fixture_rootdir / fixture_name
    module_path = fixture_dirpath / "models.py"

    spec = importlib.util.spec_from_file_location(fixture_name, module_path)
    assert spec is not None, f"Failed to create module spec from {module_path}"
    assert spec.loader is not None, f"Failed to define spec loader from {module_path}"

    module = importlib.util.module_from_spec(spec)
    assert module is not None, f"Failed to create module from spec for {module_path}"

    spec.loader.exec_module(module)

    # Verify schema classes are subclasses of TableSchema
    BareTypesSchema = getattr(module, "BareTypesSchema", None)
    assert BareTypesSchema is not None, "BareTypesSchema not defined"
    assert issubclass(BareTypesSchema, TableSchema), (
        "BareTypesSchema is not a TableSchema"
    )

    AnnotatedTypesSchema = getattr(module, "AnnotatedTypesSchema", None)
    assert AnnotatedTypesSchema is not None, "AnnotatedTypesSchema not defined"
    assert issubclass(AnnotatedTypesSchema, TableSchema), (
        "AnnotatedTypesSchema is not a TableSchema"
    )

    # Verify decorated model functions are callable
    model_names = [
        "model_all_options",
        "model_minimal",
        "model_append",
        "model_overwrite_partitions",
        "model_no_decorator_args",
    ]

    for model_name in model_names:
        model_fn = getattr(module, model_name, None)
        assert model_fn is not None, f"{model_name} not defined"
        assert callable(model_fn), f"{model_name} is not callable"
