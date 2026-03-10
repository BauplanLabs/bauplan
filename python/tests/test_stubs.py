import importlib

import griffe
import pytest


def load_module(module: str) -> griffe.Module:
    obj = griffe.load(module, search_paths=["python"], resolve_aliases=True)
    assert isinstance(obj, griffe.Module)
    return obj


class TestSubmoduleImports:
    @pytest.mark.parametrize(
        "mod,attr",
        [
            ("bauplan.schema", "Ref"),
            ("bauplan.exceptions", "BauplanError"),
            ("bauplan.state", "RunState"),
        ],
    )
    def test_import(self, mod: str, attr: str):
        assert hasattr(importlib.import_module(mod), attr)


class TestInheritance:
    @pytest.mark.parametrize(
        "mod,cls,base",
        [
            (
                "bauplan.exceptions",
                "BauplanHTTPError",
                "bauplan.exceptions.BauplanError",
            ),
            (
                "bauplan.exceptions",
                "ForbiddenError",
                "bauplan.exceptions.BauplanHTTPError",
            ),
            ("bauplan.schema", "Branch", "bauplan.schema.Ref"),
        ],
    )
    def test_base_class(self, mod: str, cls: str, base: str):
        obj = load_module(mod)
        bases = [
            b.canonical_path for b in obj.classes[cls].bases if not isinstance(b, str)
        ]
        assert bases == [base]


class TestCanonicalPaths:
    """Types should resolve to their public module, not bauplan._internal."""

    @pytest.mark.parametrize(
        "mod,cls",
        [
            ("bauplan", "Client"),
            ("bauplan.schema", "Ref"),
            ("bauplan.state", "RunState"),
        ],
    )
    def test_canonical_path(self, mod: str, cls: str):
        obj = load_module(mod)
        assert obj.classes[cls].canonical_path == f"{mod}.{cls}"


class TestRuntimeModule:
    """__module__ on live types should reflect the public path."""

    @pytest.mark.parametrize(
        "mod,cls",
        [
            ("bauplan", "Client"),
            ("bauplan.schema", "Ref"),
            ("bauplan.state", "RunState"),
        ],
    )
    def test_module(self, mod: str, cls: str):
        assert getattr(importlib.import_module(mod), cls).__module__ == mod
