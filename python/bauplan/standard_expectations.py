"""
This module contains standard expectations that can be used to test data artifact in a
Bauplan pipeline. Using these expectations instead of hand-made ones will make your
pipeline easier to maintain, and significantly faster and more memory-efficient.

Each function returns a boolean, so that the wrapping function can assert or
print out messages in case of failure.
"""

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc


def _calculate_string_concatenation(table: pa.Table, columns: list, separator: str = '') -> Any:
    """
    Given a pyarrow table and a list of column names, concatenate the columns into a new column.
    The caller of the function can then used the column to compare it with an existing column or add it.

    The function does attempt type conversion to string if a column is not of type pa.string().

    """
    fields = []
    for column in columns:
        fields.append(
            pc.cast(table[column], pa.string()) if table[column].type != pa.string() else table[column]
        )
    # last item needs to be the separator
    fields.append(separator)

    return pc.binary_join_element_wise(*fields)


def _calculate_column_mean(table: pa.Table, column_name: str) -> float:
    """
    Use built-in pyarrow compute functions to calculate the mean of a column.
    """
    return pc.mean(table[column_name]).as_py()


def expect_column_equal_concatenation(
    table: pa.Table,
    target_column: str,
    columns: list,
    separator: str = '',
) -> bool:
    """
    Expect the target column to be equal to the concatenation of the columns in the list.

    If the columns are not of type pa.string(), the function will attempt to convert them to string.
    If a custom separator is needed (default: the empty string), it can be passed as an argument.

    Parameters:
        table: the pyarrow table to test.
        target_column: the column to compare with the concatenation of the columns.
        columns: the list of columns to concatenate.
        separator: the separator to use when concatenating the columns.

    Returns:
        a boolean.

    """
    # produce a new column that is the concatenation of the columns in the list
    # and compare the new column with the target column
    return pc.all(
        pc.equal(
            _calculate_string_concatenation(table, columns, separator),
            table[target_column],
        )
    ).as_py()


def expect_column_mean_greater_than(table: pa.Table, column_name: str, value: float) -> bool:
    """
    Expect the mean of a column to be greater than the supplied value.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to calculate the mean of.
        value: the value to compare the mean with.

    Returns:
        a boolean.

    """
    mean_ = _calculate_column_mean(table, column_name)
    return mean_ > value


def expect_column_mean_greater_or_equal_than(table: pa.Table, column_name: str, value: float) -> bool:
    """
    Expect the mean of a column to be equal or greater than the supplied value.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to calculate the mean of.
        value: the value to compare the mean with.

    Returns:
        a boolean.

    """
    mean_ = _calculate_column_mean(table, column_name)
    return mean_ >= value


def expect_column_mean_smaller_than(table: pa.Table, column_name: str, value: float) -> bool:
    """
    Expect the mean of a column to be smaller than the supplied value.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to calculate the mean of.
        value: the value to compare the mean with.

    Returns:
        a boolean.

    """
    mean_ = _calculate_column_mean(table, column_name)
    return mean_ < value


def expect_column_mean_smaller_or_equal_than(table: pa.Table, column_name: str, value: float) -> bool:
    """
    Expect the mean of a column to be equal or smaller than the supplied value.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to calculate the mean of.
        value: the value to compare the mean with.

    Returns:
        a boolean.

    """
    mean_ = _calculate_column_mean(table, column_name)
    return mean_ <= value


def _column_nulls(table: pa.Table, column_name: str) -> int:
    return table[column_name].null_count


def expect_column_some_null(table: pa.Table, column_name: str) -> bool:
    """
    Expect the column to have at least one null.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to test.

    Returns:
        a boolean.

    """
    return _column_nulls(table, column_name) > 0


def expect_column_no_nulls(table: pa.Table, column_name: str) -> bool:
    """
    Expect the column to have no null values.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to test.

    Returns:
        a boolean.

    """
    return _column_nulls(table, column_name) == 0


def expect_column_all_null(table: pa.Table, column_name: str) -> bool:
    """
    Expect the column to have all null values.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to test.

    Returns:
        a boolean.

    """
    return _column_nulls(table, column_name) == table[column_name].length()


def _column_unique(table: pa.Table, column_name: str) -> int:
    return len(pc.unique(table[column_name]))


def expect_column_all_unique(table: pa.Table, column_name: str) -> bool:
    """
    Expect the column to have all unique values (i.e. no duplicates).

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to test.

    Returns:
        a boolean.

    """
    return _column_unique(table, column_name) == len(table[column_name])


def expect_column_not_unique(table: pa.Table, column_name: str) -> bool:
    """
    Expect the column to have at least one duplicate value.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to test.

    Returns:
        a boolean.

    """
    return _column_unique(table, column_name) < len(table[column_name])


def _column_accepted_values(table: pa.Table, column_name: str, accepted_values: list) -> Any:
    return pc.all(pc.is_in(table[column_name], pa.array(accepted_values))).as_py()


def expect_column_accepted_values(table: pa.Table, column_name: str, accepted_values: list) -> bool:
    """
    Expect all values in the column to come from the list of accepted values.

    Parameters:
        table: the pyarrow table to test.
        column_name: the column to test.
        accepted_values: the list of accepted values.

    Returns:
        a boolean.

    """

    return _column_accepted_values(table, column_name, accepted_values)
