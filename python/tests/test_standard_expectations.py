"""
Tests for `bauplan.standard_expectations`.

`_calculate_string_concatenation` hands `pc.binary_join_element_wise` a separator
built as an explicitly typed array sized to the columns being joined. The explicit
`type=pa.string()` is what the empty case guards: `[separator] * 0` is `[]`, and an
untyped `pa.array([])` is null-typed, leaving the kernel with no overload matching
`(string, string, null)`.

The expectations themselves are thin wrappers over pyarrow compute kernels, so the
cases below pin down what those kernels do at the edges: nulls, empty columns, and
columns whose aggregate is itself null.
"""

from typing import Callable

import pyarrow as pa
import pytest

from bauplan.standard_expectations import (
    _calculate_string_concatenation,
    expect_column_accepted_values,
    expect_column_all_null,
    expect_column_all_unique,
    expect_column_equal_concatenation,
    expect_column_mean_greater_or_equal_than,
    expect_column_mean_greater_than,
    expect_column_mean_smaller_or_equal_than,
    expect_column_mean_smaller_than,
    expect_column_no_nulls,
    expect_column_not_unique,
    expect_column_some_null,
)

# type alias for `expect_column_mean_*` family of expectations
MeanExpectation = Callable[[pa.Table, str, float], bool]


@pytest.mark.parametrize(
    "a, b, datatype, separator, expected_result",
    [
        pytest.param(
            ["v1", "v2"], ["A", "B"], pa.string(), "", ["v1A", "v2B"], id="basic"
        ),
        pytest.param(
            ["v1", "v2"], ["A", "B"], pa.string(), "-", ["v1-A", "v2-B"], id="separator"
        ),
        pytest.param(
            ["v1", None], ["A", "B"], pa.string(), "-", ["v1-A", None], id="some-nulls"
        ),
        pytest.param(
            [None, "v2"], ["A", None], pa.string(), "-", [None, None], id="null-results"
        ),
        pytest.param(
            [None, "v2"], [None, None], pa.string(), "-", [None, None], id="all-nulls"
        ),
        # Non-string columns are cast on the way in.
        pytest.param(
            [2022, 2023],
            [100, 400],
            pa.int64(),
            "-",
            ["2022-100", "2023-400"],
            id="cast",
        ),
        pytest.param([], [], pa.string(), "-", [], id="empty"),
    ],
)
def test_concatenation(
    a: list,
    b: list,
    datatype: pa.DataType,
    separator: str,
    expected_result: list,
):
    table = pa.table({"a": pa.array(a, datatype), "b": pa.array(b, datatype)})
    result = _calculate_string_concatenation(table, ["a", "b"], separator)

    assert result.type == pa.string()
    assert result.to_pylist() == expected_result


def test_concatenation_no_columns():
    """An empty column list is rejected: there is nothing to size the separator to."""
    table = pa.table({"col": [1, 2, 3, 4]})

    with pytest.raises(ValueError, match="at least 1 column is required"):
        _calculate_string_concatenation(table, [], "-")


@pytest.mark.parametrize(
    "target, a, b, separator, expected_result",
    [
        pytest.param(["v1A", "v2B"], ["v1", "v2"], ["A", "B"], "", True, id="basic"),
        pytest.param(
            ["v1-A", "v2-B"], ["v1", "v2"], ["A", "B"], "-", True, id="separator"
        ),
        pytest.param(
            ["v1A", "nope"], ["v1", "v2"], ["A", "B"], "", False, id="differs"
        ),
        # A null on either side compares null, and `pc.all` skips nulls, so the one
        # matching row carries the result.
        pytest.param(
            ["v1-A", None], ["v1", None], ["A", "B"], "-", True, id="null-row-skipped"
        ),
        # With every row null there is nothing left to reduce, so the expectation
        # returns None rather than a bool. An empty table reduces the same way.
        pytest.param(
            [None, None], [None, None], ["A", "B"], "-", None, id="all-rows-null"
        ),
        pytest.param([], [], [], "-", None, id="empty"),
    ],
)
def test_expect_column_equal_concatenation(
    target: list,
    a: list,
    b: list,
    separator: str,
    expected_result: bool | None,
):
    table = pa.table(
        {
            "target": pa.array(target, pa.string()),
            "a": pa.array(a, pa.string()),
            "b": pa.array(b, pa.string()),
        }
    )

    assert (
        expect_column_equal_concatenation(table, "target", ["a", "b"], separator)
        is expected_result
    )


@pytest.mark.parametrize(
    "col_vals", [[1, 2, 3], [1, None, 3]], ids=["no-nulls", "with-nulls"]
)
@pytest.mark.parametrize(
    "fn_expect, threshold_val, expected_result",
    [
        pytest.param(expect_column_mean_greater_than, 1.9, True, id="greater-than"),
        pytest.param(
            expect_column_mean_greater_than, 2.0, False, id="greater-than-equal"
        ),
        pytest.param(
            expect_column_mean_greater_or_equal_than, 2.0, True, id="greater-or-equal"
        ),
        pytest.param(
            expect_column_mean_greater_or_equal_than,
            2.1,
            False,
            id="greater-or-equal-below",
        ),
        pytest.param(expect_column_mean_smaller_than, 2.1, True, id="smaller-than"),
        pytest.param(
            expect_column_mean_smaller_than, 2.0, False, id="smaller-than-equal"
        ),
        pytest.param(
            expect_column_mean_smaller_or_equal_than, 2.0, True, id="smaller-or-equal"
        ),
        pytest.param(
            expect_column_mean_smaller_or_equal_than,
            1.9,
            False,
            id="smaller-or-equal-above",
        ),
    ],
)
def test_expect_column_mean(
    col_vals: list,
    fn_expect: MeanExpectation,
    threshold_val: float,
    expected_result: bool,
):
    """
    Both columns average 2.0 — nulls are excluded rather than counted as zero — so
    every comparison turns on the boundary at 2.0 either way.
    """
    table = pa.table({"col": pa.array(col_vals, pa.int64())})

    assert fn_expect(table, "col", threshold_val) is expected_result


@pytest.mark.parametrize("col_vals", [[None, None], []], ids=["all-null", "empty"])
@pytest.mark.parametrize(
    "fn_expect",
    [
        expect_column_mean_greater_than,
        expect_column_mean_greater_or_equal_than,
        expect_column_mean_smaller_than,
        expect_column_mean_smaller_or_equal_than,
    ],
    ids=["greater-than", "greater-or-equal", "smaller-than", "smaller-or-equal"],
)
def test_expect_column_mean_undefined(fn_expect: MeanExpectation, col_vals: list):
    """A column with no non-null values has a null mean, which compares as a TypeError."""
    table = pa.table({"col": pa.array(col_vals, pa.int64())})

    with pytest.raises(TypeError):
        fn_expect(table, "col", 1.0)


@pytest.mark.parametrize(
    "col_vals, expected_some_null, expected_no_nulls, expected_all_null",
    [
        pytest.param([1, 2, 3], False, True, False, id="none"),
        pytest.param([1, None, 3], True, False, False, id="some"),
        pytest.param([None, None], True, False, True, id="all"),
        # An empty column is vacuously both null-free and all-null.
        pytest.param([], False, True, True, id="empty"),
    ],
)
def test_expect_column_nulls(
    col_vals: list,
    expected_some_null: bool,
    expected_no_nulls: bool,
    expected_all_null: bool,
):
    table = pa.table({"col": pa.array(col_vals, pa.int64())})

    assert expect_column_some_null(table, "col") is expected_some_null
    assert expect_column_no_nulls(table, "col") is expected_no_nulls
    assert expect_column_all_null(table, "col") is expected_all_null


@pytest.mark.parametrize(
    "col_vals, expected_all_unique",
    [
        pytest.param([1, 2, 3], True, id="distinct"),
        pytest.param([1, 2, 2], False, id="repeat"),
        pytest.param([], True, id="empty"),
        # `pc.unique` keeps null as one distinct value, so a repeated null is a
        # duplicate like any other.
        pytest.param([1, None], True, id="single-null"),
        pytest.param([1, None, None], False, id="repeated-null"),
    ],
)
def test_expect_column_uniqueness(col_vals: list, expected_all_unique: bool):
    """The two expectations are exact complements of one another."""
    table = pa.table({"col": pa.array(col_vals, pa.int64())})

    assert expect_column_all_unique(table, "col") is expected_all_unique
    assert expect_column_not_unique(table, "col") is not expected_all_unique


@pytest.mark.parametrize(
    "col_vals, accepted, expected_result",
    [
        pytest.param(["a", "b"], ["a", "b", "c"], True, id="accepted"),
        pytest.param(["a", "z"], ["a", "b", "c"], False, id="rejected"),
        pytest.param([], ["a"], None, id="empty"),
        # `pc.is_in` reports a null value as not found rather than as null, so a
        # null fails the domain check instead of being skipped.
        pytest.param([None, "a"], ["a"], False, id="null-value"),
    ],
)
def test_expect_column_accepted_values(
    col_vals: list,
    accepted: list,
    expected_result: bool | None,
):
    table = pa.table({"col": pa.array(col_vals, pa.string())})

    assert expect_column_accepted_values(table, "col", accepted) is expected_result


def test_expect_column_accepted_values_empty_domain():
    """
    An empty accepted list builds a null-typed value set, which only matches a
    null-typed column: against any other column type the kernel raises.
    """
    table = pa.table({"col": pa.array(["a"], pa.string())})

    with pytest.raises(pa.lib.ArrowTypeError, match="doesn't match type of values set"):
        expect_column_accepted_values(table, "col", [])
