import griffe

def canonical_bases(module: str, cls: str) -> list[str]:
    obj = griffe.load(
        module,
        search_paths="python",
        resolve_aliases=True,
    )
    return [
        b.canonical_path for b in obj.classes[cls].bases
        if not isinstance(b, str)
    ]


class TestSubmoduleImports:
    def test_import_schema(self):
        import bauplan.schema

        assert hasattr(bauplan.schema, "Ref")

    def test_import_exceptions(self):
        import bauplan.exceptions

        assert hasattr(bauplan.exceptions, "BauplanError")

    def test_import_state(self):
        import bauplan.state

        assert hasattr(bauplan.state, "RunState")


class TestGriffeBases:
    def test_exception_bases(self):
        assert canonical_bases("bauplan.exceptions", "BauplanHTTPError") == [
            "bauplan.exceptions.BauplanError",
        ]

    def test_exception_hierarchy(self):
        assert canonical_bases("bauplan.exceptions", "ForbiddenError") == [
            "bauplan.exceptions.BauplanHTTPError",
        ]

    def test_schema_bases(self):
        assert canonical_bases("bauplan.schema", "Branch") == [
            "bauplan.schema.Ref",
        ]


class TestRuntimeModule:
    def test_schema_types(self):
        from bauplan.schema import Ref, Branch, Job, JobKind

        assert Ref.__module__ == "bauplan.schema"
        assert Branch.__module__ == "bauplan.schema"
        assert Job.__module__ == "bauplan.schema"
        assert JobKind.__module__ == "bauplan.schema"

    def test_state_types(self):
        from bauplan.state import RunState, TableCreatePlanState

        assert RunState.__module__ == "bauplan.state"
        assert TableCreatePlanState.__module__ == "bauplan.state"
